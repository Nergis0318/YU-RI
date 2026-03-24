use crate::cache::DiskCache;
use crate::config::Config;
use crate::http_cache::derive_ttl;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use http::{Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

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

pub async fn run(config: Config) -> Result<()> {
    let cache = DiskCache::new(
        &config.cache_dir,
        config.max_cache_size_bytes,
        config.default_ttl,
        config.eviction_policy,
    )
    .await?;

    // Create the legacy client
    let builder = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2();

    let https = builder.build();
    let client: HttpClient = Client::builder(TokioExecutor::new()).build(https);

    let shared = Arc::new((config, cache, client));

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

    // Graceful shutdown signal
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
    shared: Arc<(Config, DiskCache, HttpClient)>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    let start_time = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let (config, cache, client) = (&shared.0, &shared.1, &shared.2);

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

    let is_head = method == http::Method::HEAD;
    if method != http::Method::GET && !is_head {
        warn!(method = %method, path = %path, "Method not allowed");
        return Ok(simple(
            StatusCode::METHOD_NOT_ALLOWED,
            "Only GET/HEAD supported",
        ));
    }

    // Path traversal protection
    if path.contains("..") || path.contains("//") || path.contains("\0") {
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
    {
        if let Some(rest) = range_val.strip_prefix("bytes=") {
            if !rest.contains(',') {
                if let Some((s, e)) = rest.split_once('-') {
                    if !s.is_empty() {
                        if let Ok(start) = s.parse::<u64>() {
                            if e.is_empty() {
                                range_request = Some((start, None));
                            } else if let Ok(end) = e.parse::<u64>() {
                                if end >= start {
                                    range_request = Some((start, Some(end)));
                                }
                            }
                        }
                    } else {
                        if let Ok(suffix) = e.parse::<u64>() {
                            range_request = Some((u64::MAX, Some(suffix)));
                        }
                    }
                }
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
    if let Ok(Some(entry)) = cache.get(&final_cache_key).await {
        if let (Some(req_etag), Some(entry_etag)) = (&if_none_match, &entry.etag) {
            let req_etag_clean = req_etag.trim().trim_start_matches("W/");
            let entry_etag_clean = entry_etag.trim().trim_start_matches("W/");
            if req_etag_clean == entry_etag_clean || req_etag == "*" {
                let mut resp = Response::new(empty());
                *resp.status_mut() = StatusCode::NOT_MODIFIED;
                if let Some(ct) = &entry.content_type {
                    if let Ok(hv) = ct.parse() {
                        resp.headers_mut().insert(header::CONTENT_TYPE, hv);
                    }
                }
                if let Ok(hv) = entry_etag.parse() {
                    resp.headers_mut().insert(header::ETAG, hv);
                }
                resp.headers_mut().insert(
                    "X-YU-RI-Cache",
                    header::HeaderValue::from_static(if entry.is_fresh { "HIT" } else { "STALE" }),
                );
                if let Ok(hv) = header::HeaderValue::from_str(&entry.created_at.to_string()) {
                    resp.headers_mut().insert("X-YU-RI-Time", hv);
                }
                let cache_status = if entry.is_fresh { "HIT" } else { "STALE" };
                info!(
                    method = %method,
                    path = %path,
                    status = 304,
                    cache = cache_status,
                    duration_ms = %start_time.elapsed().as_millis(),
                    "Request completed (304 Not Modified)"
                );
                if !entry.is_fresh {
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

        let mut extra_headers: Vec<(http::HeaderName, http::HeaderValue)> = Vec::new();
        extra_headers.push((
            header::ACCEPT_RANGES,
            header::HeaderValue::from_static("bytes"),
        ));

        let range_result = apply_range(&entry.bytes, range_request);
        if let Some(cr) = &range_result.content_range {
            if let Ok(hv) = header::HeaderValue::from_str(cr) {
                extra_headers.push((header::CONTENT_RANGE, hv));
            }
        }

        let response_body = if is_head {
            empty()
        } else {
            full(range_result.body.clone())
        };

        let mut resp = Response::new(response_body);
        *resp.status_mut() = range_result.status;
        if is_head {
            if let Ok(hv) = header::HeaderValue::from_str(&range_result.body.len().to_string()) {
                resp.headers_mut().insert(header::CONTENT_LENGTH, hv);
            }
        }
        if let Some(ct) = entry.content_type {
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                ct.parse()
                    .unwrap_or_else(|_| "application/octet-stream".parse().unwrap()),
            );
        }
        if let Some(etag) = &entry.etag {
            if let Ok(hv) = etag.parse() {
                resp.headers_mut().insert(header::ETAG, hv);
            }
        }
        resp.headers_mut().insert(
            "X-YU-RI-Cache",
            header::HeaderValue::from_static(if entry.is_fresh { "HIT" } else { "STALE" }),
        );
        if let Ok(hv) = header::HeaderValue::from_str(&entry.created_at.to_string()) {
            resp.headers_mut().insert("X-YU-RI-Time", hv);
        }
        for (k, v) in extra_headers {
            resp.headers_mut().insert(k, v);
        }

        if !entry.is_fresh {
            let bg_shared = shared.clone();
            let bg_key = final_cache_key.clone();
            let bg_base_key = base_cache_key.clone();
            tokio::spawn(async move {
                let _ = background_refresh(bg_key, bg_base_key, bg_shared).await;
            });
        }
        let cache_status = if entry.is_fresh { "HIT" } else { "STALE" };
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

    // MISS: Fetch from upstream
    let ua_header =
        http::HeaderValue::from_static("YU-RI/1.0.1 (https://github.com/DevNergis/YU-RI)");

    let upstream_req = Request::builder()
        .method(http::Method::GET)
        .uri(upstream_url.as_str())
        .header(header::USER_AGENT, ua_header.clone())
        .body(Full::<Bytes>::new(Bytes::new())) // Empty body for upstream GET
        .expect("build upstream request");

    match client.request(upstream_req).await {
        Ok(up_resp) => {
            let status = up_resp.status();
            let headers = up_resp.headers().clone();

            if range_request.is_some() {
                // Collect entire body to slice it
                let body_bytes = up_resp
                    .collect()
                    .await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();

                let mut extra_headers: Vec<(http::HeaderName, http::HeaderValue)> = Vec::new();
                extra_headers.push((
                    header::ACCEPT_RANGES,
                    header::HeaderValue::from_static("bytes"),
                ));

                let range_result = apply_range(&body_bytes, range_request);
                if let Some(cr) = &range_result.content_range {
                    if let Ok(hv) = header::HeaderValue::from_str(cr) {
                        extra_headers.push((header::CONTENT_RANGE, hv));
                    }
                }

                let mut resp = Response::new(if is_head {
                    empty()
                } else {
                    full(range_result.body.clone())
                });
                *resp.status_mut() = range_result.status;
                if is_head {
                    if let Ok(hv) =
                        header::HeaderValue::from_str(&range_result.body.len().to_string())
                    {
                        resp.headers_mut().insert(header::CONTENT_LENGTH, hv);
                    }
                }
                resp.headers_mut()
                    .insert("X-YU-RI-Cache", header::HeaderValue::from_static("MISS"));
                for (k, v) in extra_headers {
                    resp.headers_mut().insert(k, v);
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

            // Normal streaming & caching
            let decision = if status.is_success() {
                derive_ttl(&headers, std::time::SystemTime::now())
            } else {
                crate::http_cache::TtlDecision::not_cacheable()
            };

            let mut variant_key = final_cache_key.clone();
            if let Some(vary_val) = headers.get(header::VARY).and_then(|v| v.to_str().ok()) {
                let names: Vec<String> = vary_val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !names.is_empty() {
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

            // Streaming with side-effect caching
            // Use a channel-like structure or just collect for simplicity?
            // Since we need to cache AND stream, we can't just collect.
            // But implementing Stream body for Hyper 1.0 requires `Body` trait impl or `Stream` wrapper.
            // We'll use a simple `StreamBody` wrapper using `tokio::sync::mpsc` if available, or just
            // `http_body_util::StreamBody` with a channel.

            // However, Hyper 1.0 body is `Frame` based.
            // Let's use `http_body_util::combinators::BoxBody` which we alias as `BoxBody`.
            // We will spawn a task that reads upstream body, writes to cache buffer, and sends to a channel
            // that feeds the response body.

            // We can use a channel to stream data to the response.
            let (tx, body_stream) =
                tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, hyper::Error>>(32);

            // Convert receiver to Body
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
            if let Some(etag_val) = headers.get(header::ETAG) {
                resp.headers_mut().insert(header::ETAG, etag_val.clone());
            }
            if let Some(vary_val) = headers.get(header::VARY) {
                resp.headers_mut().insert(header::VARY, vary_val.clone());
            }

            let cache_cloned = cache.clone();
            let etag_for_cache = upstream_etag.clone();
            let mut up_body = up_resp.into_body();

            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                let mut cache_aborted = false;

                while let Some(frame_res) = up_body.frame().await {
                    match frame_res {
                        Ok(frame) => {
                            if let Ok(data) = frame.into_data() {
                                if !data.is_empty() {
                                    if !cache_aborted {
                                        buf.extend_from_slice(&data);
                                    }
                                    let _ = tx.send(Ok(hyper::body::Frame::data(data))).await;
                                }
                            } else {
                                // trailers? ignore for now or forward
                            }
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

                if !cache_aborted && decision.cacheable && status.is_success() && !buf.is_empty() {
                    let _ = cache_cloned
                        .put(
                            &variant_key,
                            &buf,
                            ct,
                            decision.ttl,
                            decision.stale_while_revalidate,
                            etag_for_cache,
                        )
                        .await;
                }
            });

            Ok(resp)
        }
        Err(err) => {
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
    shared: Arc<(Config, DiskCache, HttpClient)>,
) -> Result<(), anyhow::Error> {
    let (_config, cache, client) = (&shared.0, &shared.1, &shared.2);
    let upstream_url = base_cache_key;

    let upstream_req = Request::builder()
        .method(http::Method::GET)
        .uri(upstream_url.as_str())
        .header(
            header::USER_AGENT,
            http::HeaderValue::from_static("YU-RI/1.0.1 (https://github.com/DevNergis/YU-RI)"),
        )
        .body(Full::<Bytes>::new(Bytes::new()))
        .expect("build upstream request");

    if let Ok(up_resp) = client.request(upstream_req).await {
        if up_resp.status().is_success() {
            let headers = up_resp.headers().clone();
            if let Ok(collected) = up_resp.collect().await {
                let body_bytes = collected.to_bytes();

                let ct = headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let etag = headers
                    .get(header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let decision = derive_ttl(&headers, std::time::SystemTime::now());

                if decision.cacheable {
                    let _ = cache
                        .put(
                            &cache_key,
                            &body_bytes,
                            ct,
                            decision.ttl,
                            decision.stale_while_revalidate,
                            etag,
                        )
                        .await;
                }
            }
        }
    }
    Ok(())
}

fn simple(code: StatusCode, msg: &str) -> Response<BoxBody<Bytes, hyper::Error>> {
    let mut r = Response::new(full(msg.to_string()));
    *r.status_mut() = code;
    r
}

struct RangeResult {
    body: Bytes,
    status: StatusCode,
    content_range: Option<String>,
}

fn apply_range(data: &Bytes, range: Option<(u64, Option<u64>)>) -> RangeResult {
    let total_len = data.len() as u64;

    let Some((start, end_opt)) = range else {
        return RangeResult {
            body: data.clone(),
            status: StatusCode::OK,
            content_range: None,
        };
    };

    if total_len == 0 {
        return RangeResult {
            body: Bytes::new(),
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
        let start_usize = range_start as usize;
        let end_usize = (range_end as usize).min(data.len() - 1);
        RangeResult {
            body: data.slice(start_usize..=end_usize),
            status: StatusCode::PARTIAL_CONTENT,
            content_range: Some(format!("bytes {}-{}/{}", range_start, range_end, total_len)),
        }
    } else {
        RangeResult {
            body: Bytes::new(),
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            content_range: Some(format!("bytes */{}", total_len)),
        }
    }
}
