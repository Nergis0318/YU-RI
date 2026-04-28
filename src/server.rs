use crate::cache::{CacheStoreOptions, DiskCache};
use crate::config::Config;
use crate::http_cache::derive_ttl;
use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::{io::SeekFrom, path::PathBuf, sync::Arc};
use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::{debug, info, warn};

type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;
type SharedState = Arc<(Config, DiskCache, HttpClient, Arc<CacheStats>)>;

const USER_AGENT_STR: &str = concat!(
    "YU-RI/",
    env!("CARGO_PKG_VERSION"),
    " (https://github.com/DevNergis/YU-RI)"
);

#[derive(Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub stale_hits: AtomicU64,
    pub misses: AtomicU64,
    pub errors: AtomicU64,
    pub not_modified: AtomicU64,
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
    http_body_util::Empty::new()
        .map_err(|never| match never {})
        .boxed()
}

fn stream_file(path: PathBuf, start: u64, len: u64) -> BoxBody<Bytes, hyper::Error> {
    let (tx, body_stream) =
        tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, hyper::Error>>(32);

    tokio::spawn(async move {
        let result = async {
            let mut file = tfs::File::open(&path).await?;
            file.seek(SeekFrom::Start(start)).await?;

            let mut remaining = len;
            let mut buf = vec![0_u8; 64 * 1024];
            while remaining > 0 {
                let read_len = remaining.min(buf.len() as u64) as usize;
                let n = file.read(&mut buf[..read_len]).await?;
                if n == 0 {
                    break;
                }
                remaining -= n as u64;
                if tx
                    .send(Ok(hyper::body::Frame::data(Bytes::copy_from_slice(
                        &buf[..n],
                    ))))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok::<(), std::io::Error>(())
        }
        .await;

        if let Err(e) = result {
            warn!(error=?e, path=?path, "cached file stream failed");
        }
    });

    http_body_util::StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(body_stream))
        .boxed()
}

fn max_cacheable_body_bytes(config: &Config) -> usize {
    config
        .max_body_bytes
        .unwrap_or(config.max_cache_size_bytes)
        .min(usize::MAX as u64) as usize
}

fn add_cache_headers(headers: &mut http::HeaderMap, cache_status: &str, created_at: u64) {
    headers.insert(
        "X-YU-RI-Cache",
        header::HeaderValue::from_static(if cache_status == "HIT" {
            "HIT"
        } else if cache_status == "STALE" {
            "STALE"
        } else if cache_status == "MISS" {
            "MISS"
        } else {
            "ERROR"
        }),
    );
    if let Ok(hv) = header::HeaderValue::from_str(&created_at.to_string()) {
        headers.insert("X-YU-RI-Time", hv);
    }
}

pub async fn run(config: Config) -> Result<()> {
    let cache = DiskCache::new(
        &config.cache_dir,
        config.max_cache_size_bytes,
        config.default_ttl,
        config.eviction_policy,
    )
    .await?;

    let builder = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2();

    let https = builder.build();
    let client: HttpClient = Client::builder(TokioExecutor::new()).build(https);

    let stats = Arc::new(CacheStats::default());
    let shared: SharedState = Arc::new((config, cache, client, stats));

    // Cron Cache Clear Scheduler (Optional)
    if let Some(cron_expr) = shared.0.cache_clear_cron.clone() {
        let scheduler = tokio_cron_scheduler::JobScheduler::new().await?;
        let shared_clone = shared.clone();
        let job = tokio_cron_scheduler::Job::new_async(cron_expr.as_str(), move |_uuid, _l| {
            let shared_inner = shared_clone.clone();
            Box::pin(async move {
                tracing::info!("Running scheduled cache clear");
                if let Err(e) = shared_inner.1.clear_all().await {
                    tracing::warn!(error=?e, "cache clear failed");
                }
            })
        })?;
        scheduler.add(job).await?;
        scheduler.start().await?;
        tracing::info!(cron=%cron_expr, "Cache clear cron enabled");
    }

    // Interval Cache Clear (Optional)
    if let Some(interval) = shared.0.cache_clear_interval {
        let shared_clone = shared.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // initial
            loop {
                ticker.tick().await;
                tracing::info!(?interval, "Interval cache clear running");
                if let Err(e) = shared_clone.1.clear_all().await {
                    tracing::warn!(error=?e, "interval cache clear failed");
                }
            }
        });
        tracing::info!(every_secs=%interval.as_secs(), "Cache clear interval enabled");
    }

    let addr = shared.0.listen_addr.clone();
    let listener = TcpListener::bind(&addr).await?;
    info!(?addr, "Listening");

    let mut shutdown_signal = std::pin::pin!(shutdown_signal());

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, remote_addr) = match res {
                    Ok(conn) => conn,
                    Err(e) => {
                        warn!("accept error: {}", e);
                        continue;
                    }
                };
                debug!("Accepted connection from {}", remote_addr);

                let io = TokioIo::new(stream);
                let shared = shared.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let shared = shared.clone();
                        async move { handle(req, shared).await }
                    });

                    if let Err(err) = Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        debug!("Error serving connection: {:?}", err);
                    }
                });
            }
            _ = &mut shutdown_signal => {
                info!("Graceful shutdown signal received, stopping listener...");
                break;
            }
        }
    }

    info!("Server shutdown complete");
    Ok(())
}

/// Graceful shutdown signal handler (SIGTERM, SIGINT)
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn handle(
    req: Request<Incoming>,
    shared: SharedState,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    let start_time = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let (config, cache, client, stats) = (&shared.0, &shared.1, &shared.2, &shared.3);

    // Health check
    if path == "/_health" || path == "/_health/" {
        debug!(path = %path, "Health check request");
        return Ok(simple(StatusCode::OK, "OK"));
    }

    // Ready check
    if path == "/_ready" || path == "/_ready/" {
        debug!(path = %path, "Ready check request");
        return Ok(simple(StatusCode::OK, "READY"));
    }

    // Stats endpoint
    if path == "/_stats" || path == "/_stats/" {
        let body = serde_json::json!({
            "hits": stats.hits.load(Ordering::Relaxed),
            "stale_hits": stats.stale_hits.load(Ordering::Relaxed),
            "misses": stats.misses.load(Ordering::Relaxed),
            "not_modified": stats.not_modified.load(Ordering::Relaxed),
            "errors": stats.errors.load(Ordering::Relaxed),
        });
        let mut resp = Response::new(full(body.to_string()));
        *resp.status_mut() = StatusCode::OK;
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        return Ok(resp);
    }

    let is_head = method == http::Method::HEAD;
    if method != http::Method::GET && !is_head {
        warn!(method = %method, path = %path, "Method not allowed");
        return Ok(simple(
            StatusCode::METHOD_NOT_ALLOWED,
            "Only GET/HEAD supported",
        ));
    }

    // Path traversal protection
    if path.contains("..") || path.contains("//") || path.contains('\0') {
        warn!(path = %path, "Path traversal attempt blocked");
        return Ok(simple(StatusCode::BAD_REQUEST, "Invalid path"));
    }

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("");
    let upstream_url = config.resolve_upstream(path_and_query);
    let base_cache_key = upstream_url.clone();

    // Range request parsing
    let mut range_request: Option<(u64, Option<u64>)> = None;
    if let Some(range_val) = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        && let Some(rest) = range_val.strip_prefix("bytes=")
        && !rest.contains(',')
        && let Some((s, e)) = rest.split_once('-')
    {
        if !s.is_empty() {
            if let Ok(start) = s.parse::<u64>() {
                if e.is_empty() {
                    range_request = Some((start, None));
                } else if let Ok(end) = e.parse::<u64>()
                    && end >= start
                {
                    range_request = Some((start, Some(end)));
                }
            }
        } else {
            if let Ok(suffix) = e.parse::<u64>() {
                range_request = Some((u64::MAX, Some(suffix)));
            }
        }
    }

    // Vary processing
    let mut final_cache_key = base_cache_key.clone();
    if let Ok(Some(vary_names)) = cache.get_vary_header_names(&base_cache_key).await {
        let mut parts: Vec<String> = Vec::new();
        for name in vary_names.iter() {
            if let Some(val) = req
                .headers()
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
            {
                parts.push(format!("{}={}", name, val));
            } else {
                parts.push(format!("{}=", name));
            }
        }
        if !parts.is_empty() {
            final_cache_key = format!("{}||{}", base_cache_key, parts.join("&"));
        }
    }

    let if_none_match = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Cache lookup
    if let Ok(Some(entry)) = cache.get_file(&final_cache_key).await {
        // Conditional request: If-None-Match
        if let (Some(req_etag), Some(entry_etag)) = (&if_none_match, &entry.etag) {
            let req_etag_clean = req_etag.trim().trim_start_matches("W/");
            let entry_etag_clean = entry_etag.trim().trim_start_matches("W/");
            if req_etag_clean == entry_etag_clean || req_etag == "*" {
                let mut resp = Response::new(empty());
                *resp.status_mut() = StatusCode::NOT_MODIFIED;
                if let Some(ct) = &entry.content_type
                    && let Ok(hv) = ct.parse()
                {
                    resp.headers_mut().insert(header::CONTENT_TYPE, hv);
                }
                if let Ok(hv) = entry_etag.parse() {
                    resp.headers_mut().insert(header::ETAG, hv);
                }
                let cache_status = if entry.is_fresh { "HIT" } else { "STALE" };
                add_cache_headers(resp.headers_mut(), cache_status, entry.created_at);

                let is_fresh = entry.is_fresh;
                if is_fresh {
                    stats.hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.stale_hits.fetch_add(1, Ordering::Relaxed);
                }
                stats.not_modified.fetch_add(1, Ordering::Relaxed);
                info!(
                    method = %method,
                    path = %path,
                    status = 304,
                    cache = cache_status,
                    duration_ms = %start_time.elapsed().as_millis(),
                    "Request completed (304 Not Modified)"
                );

                if !is_fresh {
                    let bg_shared = shared.clone();
                    let bg_key = final_cache_key.clone();
                    let bg_base_key = base_cache_key.clone();
                    tokio::spawn(async move {
                        let _ = background_refresh(bg_key, bg_base_key, bg_shared).await;
                    });
                }
                return Ok(resp);
            }
        }

        // Serve cached content (with optional range slicing)
        let mut extra_headers: Vec<(http::HeaderName, http::HeaderValue)> = Vec::new();
        extra_headers.push((
            header::ACCEPT_RANGES,
            header::HeaderValue::from_static("bytes"),
        ));

        let range_result = resolve_range(entry.size, range_request);
        if let Some(cr) = &range_result.content_range
            && let Ok(hv) = header::HeaderValue::from_str(cr)
        {
            extra_headers.push((header::CONTENT_RANGE, hv));
        }

        let response_body = if is_head || range_result.len == 0 {
            empty()
        } else {
            stream_file(entry.path.clone(), range_result.start, range_result.len)
        };

        let mut resp = Response::new(response_body);
        *resp.status_mut() = range_result.status;
        if let Ok(hv) = header::HeaderValue::from_str(&range_result.len.to_string()) {
            resp.headers_mut().insert(header::CONTENT_LENGTH, hv);
        }
        if let Some(ct) = &entry.content_type {
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                ct.parse()
                    .unwrap_or_else(|_| "application/octet-stream".parse().unwrap()),
            );
        }
        if let Some(etag) = &entry.etag
            && let Ok(hv) = etag.parse()
        {
            resp.headers_mut().insert(header::ETAG, hv);
        }
        let cache_status = if entry.is_fresh { "HIT" } else { "STALE" };
        add_cache_headers(resp.headers_mut(), cache_status, entry.created_at);
        for (k, v) in extra_headers {
            resp.headers_mut().insert(k, v);
        }

        let is_fresh = entry.is_fresh;
        if is_fresh {
            stats.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.stale_hits.fetch_add(1, Ordering::Relaxed);
        }

        if !is_fresh {
            let bg_shared = shared.clone();
            let bg_key = final_cache_key.clone();
            let bg_base_key = base_cache_key.clone();
            tokio::spawn(async move {
                let _ = background_refresh(bg_key, bg_base_key, bg_shared).await;
            });
        }
        info!(
            method = %method,
            path = %path,
            status = %resp.status().as_u16(),
            cache = cache_status,
            duration_ms = %start_time.elapsed().as_millis(),
            "Request completed"
        );
        return Ok(resp);
    }

    stats.misses.fetch_add(1, Ordering::Relaxed);

    // MISS: Fetch from upstream
    let ua_header = http::HeaderValue::from_static(USER_AGENT_STR);

    let upstream_method = if is_head {
        http::Method::HEAD
    } else {
        http::Method::GET
    };
    let mut upstream_builder = Request::builder()
        .method(upstream_method)
        .uri(upstream_url.as_str())
        .header(header::USER_AGENT, ua_header);
    if range_request.is_some()
        && let Some(range_val) = req.headers().get(header::RANGE)
    {
        upstream_builder = upstream_builder.header(header::RANGE, range_val.clone());
    }
    let upstream_req = upstream_builder
        .body(Full::<Bytes>::new(Bytes::new()))
        .expect("build upstream request");

    match client.request(upstream_req).await {
        Ok(up_resp) => {
            let status = up_resp.status();
            let headers = up_resp.headers().clone();

            // Derive TTL for caching
            let decision = if status.is_success() {
                derive_ttl(&headers, std::time::SystemTime::now())
            } else {
                crate::http_cache::TtlDecision::not_cacheable()
            };

            // ── Vary variant key computation ──────────────────
            let mut variant_key = final_cache_key.clone();
            let mut vary_all = false;
            if let Some(vary_val) = headers.get(header::VARY).and_then(|v| v.to_str().ok()) {
                let names: Vec<String> = vary_val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if names.iter().any(|name| name == "*") {
                    vary_all = true;
                } else if !names.is_empty() && status == StatusCode::OK && decision.cacheable {
                    let _ = cache.set_vary_header_names(&base_cache_key, &names).await;
                    let mut parts: Vec<String> = Vec::new();
                    for name in names.iter() {
                        if let Some(val) = req
                            .headers()
                            .get(name.as_str())
                            .and_then(|v| v.to_str().ok())
                        {
                            parts.push(format!("{}={}", name, val));
                        } else {
                            parts.push(format!("{}=", name));
                        }
                    }
                    if !parts.is_empty() {
                        variant_key = format!("{}||{}", base_cache_key, parts.join("&"));
                    }
                }
            }

            let ct = headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let upstream_etag = headers
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            if is_head {
                let mut resp = Response::new(empty());
                *resp.status_mut() = status;
                resp.headers_mut()
                    .insert("X-YU-RI-Cache", header::HeaderValue::from_static("MISS"));
                resp.headers_mut().insert(
                    header::ACCEPT_RANGES,
                    header::HeaderValue::from_static("bytes"),
                );
                if let Some(ct_val) = headers.get(header::CONTENT_TYPE) {
                    resp.headers_mut()
                        .insert(header::CONTENT_TYPE, ct_val.clone());
                }
                if let Some(cl_val) = headers.get(header::CONTENT_LENGTH) {
                    resp.headers_mut()
                        .insert(header::CONTENT_LENGTH, cl_val.clone());
                }
                if let Some(cr_val) = headers.get(header::CONTENT_RANGE) {
                    resp.headers_mut()
                        .insert(header::CONTENT_RANGE, cr_val.clone());
                }
                if let Some(etag_val) = headers.get(header::ETAG) {
                    resp.headers_mut().insert(header::ETAG, etag_val.clone());
                }
                info!(
                    method = %method,
                    path = %path,
                    status = %resp.status().as_u16(),
                    cache = "MISS",
                    duration_ms = %start_time.elapsed().as_millis(),
                    "Request completed"
                );
                return Ok(resp);
            }

            // ── Streaming response with side-effect caching ───
            let (tx, body_stream) =
                tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, hyper::Error>>(32);

            let stream_body = http_body_util::StreamBody::new(
                tokio_stream::wrappers::ReceiverStream::new(body_stream),
            )
            .boxed();

            let mut resp = Response::new(stream_body);
            *resp.status_mut() = status;
            resp.headers_mut()
                .insert("X-YU-RI-Cache", header::HeaderValue::from_static("MISS"));
            resp.headers_mut().insert(
                header::ACCEPT_RANGES,
                header::HeaderValue::from_static("bytes"),
            );
            if let Some(ct_val) = headers.get(header::CONTENT_TYPE) {
                resp.headers_mut()
                    .insert(header::CONTENT_TYPE, ct_val.clone());
            }
            if let Some(cl_val) = headers.get(header::CONTENT_LENGTH) {
                resp.headers_mut()
                    .insert(header::CONTENT_LENGTH, cl_val.clone());
            }
            if let Some(cr_val) = headers.get(header::CONTENT_RANGE) {
                resp.headers_mut()
                    .insert(header::CONTENT_RANGE, cr_val.clone());
            }
            if let Some(etag_val) = headers.get(header::ETAG) {
                resp.headers_mut().insert(header::ETAG, etag_val.clone());
            }
            if let Some(vary_val) = headers.get(header::VARY) {
                resp.headers_mut().insert(header::VARY, vary_val.clone());
            }

            let cache_cloned = cache.clone();
            let etag_for_cache = upstream_etag.clone();
            let mut up_body = up_resp.into_body();
            let max_body_limit = max_cacheable_body_bytes(config) as u64;
            let should_cache_body = decision.cacheable
                && status == StatusCode::OK
                && range_request.is_none()
                && !vary_all;
            let method_for_log = method.clone();
            let path_for_log = path.clone();

            tokio::spawn(async move {
                let mut temp_path = None;
                let mut temp_file = None;
                let mut bytes_written = 0_u64;
                let mut cache_aborted = !should_cache_body;

                if should_cache_body {
                    match cache_cloned.temp_data_path(&variant_key).await {
                        Ok(path) => match tfs::File::create(&path).await {
                            Ok(file) => {
                                temp_path = Some(path);
                                temp_file = Some(file);
                            }
                            Err(e) => {
                                warn!(error=?e, "cache temp file create failed");
                                cache_aborted = true;
                            }
                        },
                        Err(e) => {
                            warn!(error=?e, "cache temp path create failed");
                            cache_aborted = true;
                        }
                    }
                }

                while let Some(frame_res) = up_body.frame().await {
                    match frame_res {
                        Ok(frame) => {
                            if let Ok(data) = frame.into_data()
                                && !data.is_empty()
                            {
                                if !cache_aborted {
                                    let next_size = bytes_written.saturating_add(data.len() as u64);
                                    if next_size <= max_body_limit {
                                        if let Some(file) = temp_file.as_mut()
                                            && let Err(e) = file.write_all(&data).await
                                        {
                                            warn!(error=?e, "cache temp file write failed");
                                            cache_aborted = true;
                                        }
                                        bytes_written = next_size;
                                    } else {
                                        warn!(
                                            limit = max_body_limit,
                                            current = next_size,
                                            "Body exceeds limit, aborting cache write"
                                        );
                                        cache_aborted = true;
                                    }

                                    if cache_aborted {
                                        drop(temp_file.take());
                                        if let Some(path) = temp_path.take() {
                                            let _ = tfs::remove_file(path).await;
                                        }
                                    }
                                }

                                if tx.send(Ok(hyper::body::Frame::data(data))).await.is_err() {
                                    cache_aborted = true;
                                    break;
                                }
                            }
                            // trailers: ignore
                        }
                        Err(e) => {
                            warn!("Upstream stream error: {}", e);
                            let _ = tx.send(Err(e)).await;
                            cache_aborted = true;
                            break;
                        }
                    }
                }

                // Drop tx to close stream
                drop(tx);

                if !cache_aborted && bytes_written > 0 {
                    if let (Some(file), Some(path)) = (temp_file.take(), temp_path.take()) {
                        if let Err(e) = file.sync_all().await {
                            warn!(error=?e, "cache temp file sync failed");
                            let _ = tfs::remove_file(path).await;
                        } else {
                            drop(file);
                            if let Err(e) = cache_cloned
                                .put_file(
                                    &variant_key,
                                    &path,
                                    bytes_written,
                                    CacheStoreOptions {
                                        content_type: ct,
                                        ttl: decision.ttl,
                                        swr: decision.stale_while_revalidate,
                                        etag: etag_for_cache,
                                    },
                                )
                                .await
                            {
                                warn!(error=?e, "cache file promote failed");
                                let _ = tfs::remove_file(path).await;
                            }
                        }
                    }
                } else {
                    drop(temp_file.take());
                    if let Some(path) = temp_path.take() {
                        let _ = tfs::remove_file(path).await;
                    }
                }

                info!(
                    method = %method_for_log,
                    path = %path_for_log,
                    status = %status.as_u16(),
                    cache = "MISS",
                    duration_ms = %start_time.elapsed().as_millis(),
                    "Request completed"
                );
            });

            Ok(resp)
        }
        Err(err) => {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            warn!(
                error = ?err,
                method = %method,
                path = %path,
                "Upstream fetch failed"
            );
            info!(
                method = %method,
                path = %path,
                status = 502,
                cache = "ERROR",
                duration_ms = %start_time.elapsed().as_millis(),
                "Request completed"
            );
            Ok(simple(StatusCode::BAD_GATEWAY, "Upstream error"))
        }
    }
}

async fn background_refresh(
    cache_key: String,
    base_cache_key: String,
    shared: SharedState,
) -> Result<(), anyhow::Error> {
    let (config, cache, client, _stats) = (&shared.0, &shared.1, &shared.2, &shared.3);
    let upstream_url = base_cache_key;

    let upstream_req = Request::builder()
        .method(http::Method::GET)
        .uri(upstream_url.as_str())
        .header(
            header::USER_AGENT,
            http::HeaderValue::from_static(USER_AGENT_STR),
        )
        .body(Full::<Bytes>::new(Bytes::new()))
        .expect("build upstream request");

    if let Ok(up_resp) = client.request(upstream_req).await
        && up_resp.status() == StatusCode::OK
    {
        let headers = up_resp.headers().clone();
        let decision = derive_ttl(&headers, std::time::SystemTime::now());
        if !decision.cacheable {
            return Ok(());
        }

        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let etag = headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let max_body_limit = max_cacheable_body_bytes(config) as u64;
        let temp_path = cache.temp_data_path(&cache_key).await?;
        let mut temp_file = tfs::File::create(&temp_path).await?;
        let mut written = 0_u64;
        let mut body = up_resp.into_body();
        let mut aborted = false;

        while let Some(frame_res) = body.frame().await {
            match frame_res {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        let next_size = written.saturating_add(data.len() as u64);
                        if next_size > max_body_limit {
                            warn!(
                                limit = max_body_limit,
                                current = next_size,
                                "Background refresh body exceeds limit, aborting cache write"
                            );
                            aborted = true;
                            break;
                        }
                        temp_file.write_all(&data).await?;
                        written = next_size;
                    }
                }
                Err(e) => {
                    warn!(error=?e, "background refresh upstream stream failed");
                    aborted = true;
                    break;
                }
            }
        }

        if !aborted && written > 0 {
            temp_file.sync_all().await?;
            drop(temp_file);
            if let Err(e) = cache
                .put_file(
                    &cache_key,
                    &temp_path,
                    written,
                    CacheStoreOptions {
                        content_type: ct,
                        ttl: decision.ttl,
                        swr: decision.stale_while_revalidate,
                        etag,
                    },
                )
                .await
            {
                let _ = tfs::remove_file(&temp_path).await;
                return Err(e);
            }
        } else {
            drop(temp_file);
            let _ = tfs::remove_file(temp_path).await;
        }
    }
    Ok(())
}

fn simple(code: StatusCode, msg: &str) -> Response<BoxBody<Bytes, hyper::Error>> {
    let mut r = Response::new(full(msg.to_string()));
    *r.status_mut() = code;
    r
}

struct ResolvedRange {
    start: u64,
    len: u64,
    status: StatusCode,
    content_range: Option<String>,
}

fn resolve_range(total_len: u64, range: Option<(u64, Option<u64>)>) -> ResolvedRange {
    let Some((start, end_opt)) = range else {
        return ResolvedRange {
            start: 0,
            len: total_len,
            status: StatusCode::OK,
            content_range: None,
        };
    };

    if total_len == 0 {
        return ResolvedRange {
            start: 0,
            len: 0,
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            content_range: Some(format!("bytes */{}", total_len)),
        };
    }

    let (range_start, range_end) = if start == u64::MAX {
        let suffix = end_opt.unwrap_or(0).min(total_len);
        (total_len.saturating_sub(suffix), total_len - 1)
    } else {
        let end = end_opt
            .unwrap_or_else(|| total_len.saturating_sub(1))
            .min(total_len.saturating_sub(1));
        (start.min(total_len), end)
    };

    if range_start <= range_end && range_start < total_len {
        ResolvedRange {
            start: range_start,
            len: range_end - range_start + 1,
            status: StatusCode::PARTIAL_CONTENT,
            content_range: Some(format!("bytes {}-{}/{}", range_start, range_end, total_len)),
        }
    } else {
        ResolvedRange {
            start: 0,
            len: 0,
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            content_range: Some(format!("bytes */{}", total_len)),
        }
    }
}
