use crate::cache::{CacheStoreOptions, DiskCache};
use crate::config::Config;
use crate::http_cache::{TtlDecision, derive_ttl};
use bytes::Bytes;
use http::{Request, Response, StatusCode, header};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::broadcast;
use tracing::{info, warn};

use super::headers::{
    CacheStatus, add_cache_headers, apply_vary_from_response, copy_upstream_headers,
    extract_upstream_meta, vary_cache_key,
};
use super::response::{
    BoxedBody, build_cached_response, empty, full, not_modified_response, parse_range_header,
    simple,
};
use super::upstream::{build_upstream_request, max_cacheable_body_bytes};
use super::CacheStats;
use super::SharedState;

pub async fn handle(
    req: Request<Incoming>,
    shared: SharedState,
) -> Result<Response<BoxedBody>, hyper::Error> {
    let start_time = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let (config, cache, stats) = (&shared.config, &shared.cache, &shared.stats);

    if let Some(resp) = try_admin_endpoint(path.as_str(), cache, stats, config).await {
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
    let range_request = parse_range_header(req.headers());

    let final_cache_key = if let Ok(Some(vary_names)) = cache.get_vary_header_names(&base_cache_key).await {
        vary_cache_key(&base_cache_key, req.headers(), &vary_names)
    } else {
        base_cache_key.clone()
    };
    let if_none_match = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let if_modified_since = req
        .headers()
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| httpdate::parse_http_date(s).ok());

    if let Ok(Some(entry)) = cache.get_file(&final_cache_key).await {
        if is_not_modified(&entry, &if_none_match, if_modified_since) {
            let cache_status = if entry.is_fresh {
                CacheStatus::Hit
            } else {
                CacheStatus::Stale
            };
            stats.record_hit(entry.is_fresh);
            stats.not_modified.fetch_add(1, Ordering::Relaxed);
            log_request(&method, &path, 304, cache_status.as_str(), false, start_time);
            if !entry.is_fresh {
                let s = shared.clone();
                tokio::spawn(async move {
                    let _ = super::upstream::background_refresh(
                        final_cache_key,
                        base_cache_key,
                        &s.config,
                        &s.cache,
                        &s.client,
                    )
                    .await;
                });
            }
            return Ok(not_modified_response(&entry, cache_status));
        }

        let cache_status = if entry.is_fresh {
            CacheStatus::Hit
        } else {
            CacheStatus::Stale
        };
        stats.record_hit(entry.is_fresh);
        let resp = build_cached_response(&entry, range_request, cache_status, is_head);
        if !entry.is_fresh {
            let s = shared.clone();
            tokio::spawn(async move {
                let _ = super::upstream::background_refresh(
                    final_cache_key,
                    base_cache_key,
                    &s.config,
                    &s.cache,
                    &s.client,
                )
                .await;
            });
        }
        log_request(
            &method,
            &path,
            resp.status().as_u16(),
            cache_status.as_str(),
            false,
            start_time,
        );
        return Ok(resp);
    }

    stats.misses.fetch_add(1, Ordering::Relaxed);

    if !is_head && range_request.is_none()
        && let Some(resp) = try_coalesced_hit(
            &shared,
            &final_cache_key,
            range_request,
            &method,
            &path,
            stats,
            start_time,
        )
        .await
    {
        return Ok(resp);
    }

    fetch_from_upstream(
        req,
        shared,
        upstream_url,
        base_cache_key,
        final_cache_key,
        range_request,
        is_head,
        &method,
        &path,
        start_time,
    )
    .await
}

async fn try_admin_endpoint(
    path: &str,
    cache: &DiskCache,
    stats: &CacheStats,
    config: &Config,
) -> Option<Response<BoxedBody>> {
    match path {
        "/_health" | "/_health/" => Some(simple(StatusCode::OK, "OK")),
        "/_ready" | "/_ready/" => Some(simple(StatusCode::OK, "READY")),
        "/_stats" | "/_stats/" => {
            let (cache_bytes, cache_entries) = cache.size_info().await;
            let body = serde_json::json!({
                "hits": stats.hits.load(Ordering::Relaxed),
                "stale_hits": stats.stale_hits.load(Ordering::Relaxed),
                "misses": stats.misses.load(Ordering::Relaxed),
                "not_modified": stats.not_modified.load(Ordering::Relaxed),
                "errors": stats.errors.load(Ordering::Relaxed),
                "cache_bytes": cache_bytes,
                "cache_entries": cache_entries,
                "max_cache_bytes": config.max_cache_size_bytes,
            });
            let mut resp = Response::new(full(body.to_string()));
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json"),
            );
            Some(resp)
        }
        _ => None,
    }
}

fn is_not_modified(
    entry: &crate::cache::CacheFileEntry,
    if_none_match: &Option<String>,
    if_modified_since: Option<std::time::SystemTime>,
) -> bool {
    let etag_matches = match (if_none_match, &entry.etag) {
        (Some(req_etag), Some(entry_etag)) => {
            req_etag.trim().trim_start_matches("W/")
                == entry_etag.trim().trim_start_matches("W/")
        }
        _ => false,
    };
    if etag_matches {
        return true;
    }
    match (if_modified_since, &entry.last_modified) {
        (Some(ims), Some(lm_str)) => httpdate::parse_http_date(lm_str)
            .map(|lm| lm <= ims)
            .unwrap_or(false),
        _ => false,
    }
}

async fn try_coalesced_hit(
    shared: &SharedState,
    final_cache_key: &str,
    range_request: Option<(u64, Option<u64>)>,
    method: &http::Method,
    path: &str,
    stats: &CacheStats,
    start_time: Instant,
) -> Option<Response<BoxedBody>> {
    let mut rx = {
        if let Some(tx) = shared.inflight.get(final_cache_key) {
            Some(tx.subscribe())
        } else {
            let (tx, _) = broadcast::channel::<()>(1);
            shared.inflight.insert(final_cache_key.to_string(), tx);
            None
        }
    };

    let rx = rx.as_mut()?;
    let _ = rx.recv().await;
    let entry = shared.cache.get_file(final_cache_key).await.ok()??;
    stats.hits.fetch_add(1, Ordering::Relaxed);
    let resp = build_cached_response(&entry, range_request, CacheStatus::Hit, false);
    log_request(method, path, resp.status().as_u16(), "HIT", true, start_time);
    Some(resp)
}

async fn fetch_from_upstream(
    req: Request<Incoming>,
    shared: SharedState,
    upstream_url: String,
    base_cache_key: String,
    final_cache_key: String,
    range_request: Option<(u64, Option<u64>)>,
    is_head: bool,
    method: &http::Method,
    path: &str,
    start_time: Instant,
) -> Result<Response<BoxedBody>, hyper::Error> {
    let (config, cache, client, stats) =
        (&shared.config, &shared.cache, &shared.client, &shared.stats);

    let range_header = range_request
        .is_some()
        .then(|| req.headers().get(header::RANGE).cloned())
        .flatten();
    let upstream_method = if is_head {
        http::Method::HEAD
    } else {
        http::Method::GET
    };
    let upstream_req = build_upstream_request(
        upstream_method,
        upstream_url.as_str(),
        range_header.as_ref(),
    );

    match client.request(upstream_req).await {
        Ok(up_resp) => {
            let status = up_resp.status();
            let headers = up_resp.headers().clone();
            let decision = if status.is_success() {
                derive_ttl(&headers, std::time::SystemTime::now())
            } else {
                TtlDecision::not_cacheable()
            };

            let vary = apply_vary_from_response(
                &base_cache_key,
                &final_cache_key,
                req.headers(),
                &headers,
                decision.cacheable,
                status == StatusCode::OK,
            );
            if !vary.vary_all
                && status == StatusCode::OK
                && decision.cacheable
                && let Some(vary_val) = headers.get(header::VARY).and_then(|v| v.to_str().ok())
            {
                let names: Vec<String> = vary_val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                let _ = cache.set_vary_header_names(&base_cache_key, &names).await;
            }

            let meta = extract_upstream_meta(&headers);

            if is_head {
                let mut resp = Response::new(empty());
                *resp.status_mut() = status;
                add_cache_headers(resp.headers_mut(), CacheStatus::Miss, 0);
                copy_upstream_headers(resp.headers_mut(), &headers, false);
                log_request(method, path, resp.status().as_u16(), "MISS", false, start_time);
                return Ok(resp);
            }

            let (tx, body_stream) =
                tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, hyper::Error>>(64);
            let stream_body = http_body_util::StreamBody::new(
                tokio_stream::wrappers::ReceiverStream::new(body_stream),
            )
            .boxed();

            let mut resp = Response::new(stream_body);
            *resp.status_mut() = status;
            add_cache_headers(resp.headers_mut(), CacheStatus::Miss, 0);
            copy_upstream_headers(resp.headers_mut(), &headers, true);

            let should_cache_body = decision.cacheable
                && status == StatusCode::OK
                && range_request.is_none()
                && !vary.vary_all;
            let max_body_limit = max_cacheable_body_bytes(config) as u64;
            let inflight_key = should_cache_body.then(|| final_cache_key.clone());

            let cache_cloned = cache.clone();
            let mut up_body = up_resp.into_body();
            let variant_key = vary.variant_key.clone();
            let store_options = CacheStoreOptions {
                content_type: meta.content_type,
                ttl: decision.ttl,
                swr: decision.stale_while_revalidate,
                etag: meta.etag,
                last_modified: meta.last_modified,
            };
            let shared_for_inflight = shared.clone();
            let method_for_log = method.clone();
            let path_for_log = path.to_string();

            tokio::spawn(async move {
                super::upstream::relay_body_with_cache(
                    &mut up_body,
                    Some(&tx),
                    &cache_cloned,
                    &variant_key,
                    should_cache_body,
                    max_body_limit,
                    store_options,
                )
                .await;
                drop(tx);
                if let Some(ref key) = inflight_key
                    && let Some((_, done_tx)) = shared_for_inflight.inflight.remove(key)
                {
                    let _ = done_tx.send(());
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
            if let Some((_, tx)) = shared.inflight.remove(&final_cache_key) {
                let _ = tx.send(());
            }
            stats.errors.fetch_add(1, Ordering::Relaxed);
            warn!(error = ?err, method = %method, path = %path, "Upstream fetch failed");
            log_request(method, path, 502, "ERROR", false, start_time);
            Ok(simple(StatusCode::BAD_GATEWAY, "Upstream error"))
        }
    }
}

fn log_request(
    method: &http::Method,
    path: &str,
    status: u16,
    cache: &str,
    coalesced: bool,
    start_time: Instant,
) {
    info!(
        method = %method,
        path = %path,
        status = status,
        cache = cache,
        coalesced = coalesced,
        duration_ms = %start_time.elapsed().as_millis(),
        "Request completed"
    );
}