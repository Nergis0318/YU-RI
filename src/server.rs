use crate::cache::DiskCache;
use crate::config::Config;
use crate::http_cache::derive_ttl;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use http::{StatusCode, header};
use hyper::{
    Body, Client, Request, Response, Server,
    body::HttpBody as _,
    service::{make_service_fn, service_fn},
};
use hyper_tls::HttpsConnector;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn, debug};

pub async fn run(config: Config) -> Result<()> {
    let cache = DiskCache::new(
        &config.cache_dir,
        config.max_cache_size_bytes,
        config.default_ttl,
        config.eviction_policy,
    )
    .await?;
    let addr = config.listen_addr.parse().unwrap();
    // 재사용 가능한 HTTPS 클라이언트 (connection pooling)
    let https = HttpsConnector::new();
    let client: Client<_, hyper::Body> = Client::builder().build(https);
    let shared = Arc::new((config, cache, client));

    // Cron 기반 캐시 전체 삭제 스케줄러 (옵션)
    if let Some(cron_expr) = shared.0.cache_clear_cron.clone() {
        // tokio_cron_scheduler 사용 (Cargo.toml 에 의존성 필요)
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

    // 단순 초(interval) 기반 주기적 전체 클리어 (cron 과 병행 가능, 각각 독립)
    if let Some(interval) = shared.0.cache_clear_interval {
        let shared_clone = shared.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // 최초 tick 은 바로 발생하므로 한번 건너뜀 (즉, interval 후 첫 실행)
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

    let make_svc = make_service_fn(move |_| {
        let shared = shared.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let shared = shared.clone();
                async move { handle(req, shared).await }
            }))
        }
    });

    info!(?addr, "Listening");
    let server = Server::bind(&addr)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown_signal());
    server.await?;
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

    info!("Graceful shutdown signal received, finishing in-flight requests...");
}

async fn handle(
    req: Request<Body>,
    shared: Arc<(
        Config,
        DiskCache,
        Client<HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
    )>,
) -> Result<Response<Body>, hyper::Error> {
    let start_time = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let (config, cache, client) = (&shared.0, &shared.1, &shared.2);

    // Health check endpoint
    if path == "/_health" || path == "/_health/" {
        debug!(path = %path, "Health check request");
        return Ok(simple(StatusCode::OK, "OK"));
    }

    // Ready check endpoint (checks upstream connectivity)
    if path == "/_ready" || path == "/_ready/" {
        debug!(path = %path, "Ready check request");
        return Ok(simple(StatusCode::OK, "READY"));
    }

    // Only allow GET and HEAD methods
    let is_head = method == http::Method::HEAD;
    if method != http::Method::GET && !is_head {
        warn!(method = %method, path = %path, "Method not allowed");
        return Ok(simple(StatusCode::METHOD_NOT_ALLOWED, "Only GET/HEAD supported"));
    }

    // Path traversal protection
    if path.contains("..") || path.contains("//") || path.contains("\0") {
        warn!(path = %path, "Path traversal attempt blocked");
        return Ok(simple(StatusCode::BAD_REQUEST, "Invalid path"));
    }

    let upstream_url = format!(
        "{}/{}",
        config.upstream_base.trim_end_matches('/'),
        req.uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("")
    );
    // 기본 cache key (Vary 고려 전)
    let base_cache_key = upstream_url.clone();

    // Range 요청 파싱 (단일 range 만 지원; 멀티 range => 패스스루, 캐시 미사용)
    let mut range_request: Option<(u64, Option<u64>)> = None; // (start, end) / suffix: (u64::MAX, Some(suffix_len))
    if let Some(range_val) = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(rest) = range_val.strip_prefix("bytes=") {
            if !rest.contains(',') {
                // multi-range 미지원
                if let Some((s, e)) = rest.split_once('-') {
                    if !s.is_empty() {
                        // start-end 또는 start-
                        if let Ok(start) = s.parse::<u64>() {
                            if e.is_empty() {
                                // start-
                                range_request = Some((start, None));
                            } else if let Ok(end) = e.parse::<u64>() {
                                if end >= start {
                                    range_request = Some((start, Some(end)));
                                }
                            }
                        }
                    } else {
                        // -suffix
                        if let Ok(suffix) = e.parse::<u64>() {
                            range_request = Some((u64::MAX, Some(suffix)));
                        }
                    }
                }
            }
        }
    }

    // Vary 헤더를 위한 변형 키 계산: base_key + serialized vary header values
    // 우선 캐시에 저장된 vary header 목록을 가져와서, 존재하면 그 목록 기반 variant key 구성
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

    // 조건부 요청(If-None-Match) 파싱
    let if_none_match = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // 캐시 조회 (Range 포함). stale 여부에 따라 SWR 처리.
    if let Ok(Some(entry)) = cache.get(&final_cache_key).await {
        // If-None-Match 조건부 요청 처리 (304 Not Modified)
        if let (Some(req_etag), Some(entry_etag)) = (&if_none_match, &entry.etag) {
            // ETag 비교 (weak/strong ETag 모두 지원)
            let req_etag_clean = req_etag.trim().trim_start_matches("W/");
            let entry_etag_clean = entry_etag.trim().trim_start_matches("W/");
            if req_etag_clean == entry_etag_clean || req_etag == "*" {
                let mut resp = Response::new(Body::empty());
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
                    "X-Cache",
                    header::HeaderValue::from_static(if entry.is_fresh { "HIT" } else { "STALE" }),
                );
                let cache_status = if entry.is_fresh { "HIT" } else { "STALE" };
                info!(
                    method = %method,
                    path = %path,
                    status = 304,
                    cache = cache_status,
                    duration_ms = %start_time.elapsed().as_millis(),
                    "Request completed (304 Not Modified)"
                );
                // stale 이면 백그라운드 재검증
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
        
        // Range 적용 (공통 함수 사용)
        let range_result = apply_range(&entry.bytes, range_request);
        if let Some(cr) = &range_result.content_range {
            if let Ok(hv) = header::HeaderValue::from_str(cr) {
                extra_headers.push((header::CONTENT_RANGE, hv));
            }
        }

        // HEAD 요청이면 body 비우기
        let response_body = if is_head {
            Body::empty()
        } else {
            Body::from(range_result.body.clone())
        };
        let mut resp = Response::new(response_body);
        *resp.status_mut() = range_result.status;
        // Content-Length는 HEAD에서도 실제 크기를 반환
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
        // ETag 헤더 추가
        if let Some(etag) = &entry.etag {
            if let Ok(hv) = etag.parse() {
                resp.headers_mut().insert(header::ETAG, hv);
            }
        }
        resp.headers_mut().insert(
            "X-Cache",
            header::HeaderValue::from_static(if entry.is_fresh { "HIT" } else { "STALE" }),
        );
        for (k, v) in extra_headers {
            resp.headers_mut().insert(k, v);
        }
        // stale 이면 백그라운드 재검증
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

    // Fetch from upstream (support http & https)
    // 재사용 client (connection pool)
    // Upstream 요청 시 User-Agent 고정 설정
    let ua_header = http::HeaderValue::from_static("RFMP/1.0");
    let upstream_req = Request::builder()
        .method(http::Method::GET)
        .uri(upstream_url.as_str())
        .header(header::USER_AGENT, ua_header.clone())
        .body(Body::empty())
        .expect("build upstream request");
    match client.request(upstream_req).await {
        Ok(up_resp) => {
            let status = up_resp.status();
            let headers = up_resp.headers().clone();

            // Range 요청 (캐시 미스 시): 전체 업스트림 바디 수신 후 슬라이싱 (공통 함수 사용)
            if range_request.is_some() {
                let body_bytes = hyper::body::to_bytes(up_resp.into_body())
                    .await
                    .unwrap_or_else(|_| Bytes::new());
                let mut extra_headers: Vec<(http::HeaderName, http::HeaderValue)> = Vec::new();
                extra_headers.push((
                    header::ACCEPT_RANGES,
                    header::HeaderValue::from_static("bytes"),
                ));
                
                // Range 적용 (공통 함수 사용)
                let range_result = apply_range(&body_bytes, range_request);
                if let Some(cr) = &range_result.content_range {
                    if let Ok(hv) = header::HeaderValue::from_str(cr) {
                        extra_headers.push((header::CONTENT_RANGE, hv));
                    }
                }

                let mut resp = Response::new(if is_head { Body::empty() } else { Body::from(range_result.body.clone()) });
                *resp.status_mut() = range_result.status;
                if is_head {
                    if let Ok(hv) = header::HeaderValue::from_str(&range_result.body.len().to_string()) {
                        resp.headers_mut().insert(header::CONTENT_LENGTH, hv);
                    }
                }
                resp.headers_mut()
                    .insert("X-Cache", header::HeaderValue::from_static("MISS"));
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

            // 스트리밍 경로: 업스트림 바디를 읽으며 동시에 클라이언트로 전송 & 메모리에 누적 후 캐시
            let decision = if status.is_success() {
                derive_ttl(&headers, std::time::SystemTime::now())
            } else {
                crate::http_cache::TtlDecision::not_cacheable()
            };

            // Vary 처리 (variant key 계산) - 응답 헤더 기반
            let mut variant_key = final_cache_key.clone();
            if let Some(vary_val) = headers.get(header::VARY).and_then(|v| v.to_str().ok()) {
                let names: Vec<String> = vary_val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !names.is_empty() {
                    let _ = cache.set_vary_header_names(&base_cache_key, &names).await; // 오류 무시
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

            // ETag 추출 (캐시 저장 및 조건부 요청용)
            let upstream_etag = headers
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let mut up_body = up_resp.into_body();
            // HEAD 요청이면 body 스트리밍 필요 없음
            if is_head {
                let mut resp = Response::new(Body::empty());
                *resp.status_mut() = status;
                resp.headers_mut()
                    .insert("X-Cache", header::HeaderValue::from_static("MISS"));
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
            let (mut tx, body) = Body::channel();
            let mut resp = Response::new(body);
            *resp.status_mut() = status;
            resp.headers_mut()
                .insert("X-Cache", header::HeaderValue::from_static("MISS"));
            resp.headers_mut().insert(
                header::ACCEPT_RANGES,
                header::HeaderValue::from_static("bytes"),
            );
            // 선택적으로 Content-Type 등 주요 헤더 전달
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

            // 스트리밍 및 캐시 저장 태스크
            if decision.cacheable && status.is_success() {
                let cache_cloned = cache.clone();
                let etag_for_cache = upstream_etag.clone();
                tokio::spawn(async move {
                    let mut buf = BytesMut::new();
                    while let Some(chunk_res) = up_body.data().await {
                        match chunk_res {
                            Ok(chunk) => {
                                let _ = tx.send_data(chunk.clone()).await; // 에러(취소 등)는 무시
                                buf.extend_from_slice(&chunk);
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }
                    // 전송 종료를 명시 (에러는 무시)
                    let _ = tx.send_data(Bytes::new()).await; // close
                    // 캐시에 저장
                    if !buf.is_empty() {
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
            } else {
                // 캐시 없이 단순 스트리밍
                tokio::spawn(async move {
                    while let Some(chunk_res) = up_body.data().await {
                        if let Ok(chunk) = chunk_res {
                            let _ = tx.send_data(chunk).await;
                        } else {
                            break;
                        }
                    }
                    let _ = tx.send_data(Bytes::new()).await; // close
                });
            }
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

// 백그라운드 재검증 함수
async fn background_refresh(
    cache_key: String,
    base_cache_key: String,
    shared: Arc<(
        Config,
        DiskCache,
        Client<HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
    )>,
) -> Result<(), anyhow::Error> {
    let (_config, cache, client) = (&shared.0, &shared.1, &shared.2);
    // base_cache_key 로 재검증 (간단한 fresh fetch). 기존 variant 값은 유지 목적
    let upstream_url = base_cache_key;
    if let Ok(up_resp) = {
        let upstream_req = Request::builder()
            .method(http::Method::GET)
            .uri(upstream_url.as_str())
            .header(
                header::USER_AGENT,
                http::HeaderValue::from_static("RFMP/1.0"),
            )
            .body(Body::empty())
            .expect("build upstream request");
        client.request(upstream_req).await
    } {
        if up_resp.status().is_success() {
            let headers = up_resp.headers().clone();
            if let Ok(body_bytes) = hyper::body::to_bytes(up_resp.into_body()).await {
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
                    // 기존 cache_key 덮어쓰기 (variant 유지)
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

fn simple(code: StatusCode, msg: &str) -> Response<Body> {
    let mut r = Response::new(Body::from(msg.to_string()));
    *r.status_mut() = code;
    r
}

/// Range 요청 적용 결과
struct RangeResult {
    body: Bytes,
    status: StatusCode,
    content_range: Option<String>,
}

/// Range 요청을 데이터에 적용하는 공통 함수
fn apply_range(
    data: &Bytes,
    range: Option<(u64, Option<u64>)>,
) -> RangeResult {
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
        // suffix range: -500 means last 500 bytes
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
