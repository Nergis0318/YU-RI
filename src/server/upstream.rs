use crate::cache::{CacheStoreOptions, DiskCache};
use crate::config::Config;
use crate::http_cache::{TtlDecision, derive_ttl};
use anyhow::Result;
use bytes::Bytes;
use http::{HeaderMap, Request, StatusCode, header};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use std::path::PathBuf;
use tokio::fs as tfs;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::warn;

use super::headers::extract_upstream_meta;

pub type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub const USER_AGENT: &str = concat!(
    "YU-RI/",
    env!("CARGO_PKG_VERSION"),
    " (https://github.com/Xeon-Dot/YU-RI)"
);

pub fn max_cacheable_body_bytes(config: &Config) -> usize {
    config
        .max_body_bytes
        .unwrap_or(config.max_cache_size_bytes)
        .min(usize::MAX as u64) as usize
}

pub(crate) fn store_options_from_headers(
    headers: &HeaderMap,
    decision: &TtlDecision,
) -> CacheStoreOptions {
    let meta = extract_upstream_meta(headers);
    CacheStoreOptions {
        content_type: meta.content_type,
        ttl: decision.ttl,
        swr: decision.stale_while_revalidate,
        etag: meta.etag,
        last_modified: meta.last_modified,
    }
}

pub fn build_upstream_request(
    method: http::Method,
    url: &str,
    range_header: Option<&header::HeaderValue>,
) -> Request<Full<Bytes>> {
    let mut builder = Request::builder().method(method).uri(url).header(
        header::USER_AGENT,
        header::HeaderValue::from_static(USER_AGENT),
    );
    if let Some(range) = range_header {
        builder = builder.header(header::RANGE, range.clone());
    }
    builder
        .body(Full::<Bytes>::new(Bytes::new()))
        .expect("build upstream request")
}

const CACHE_WRITE_BUFFER: usize = 256 * 1024;

pub struct BodyCacheWriter {
    temp_path: Option<PathBuf>,
    temp_file: Option<BufWriter<tfs::File>>,
    bytes_written: u64,
    pub aborted: bool,
}

impl BodyCacheWriter {
    pub async fn new(cache: &DiskCache, key: &str, enabled: bool) -> Self {
        let mut writer = Self {
            temp_path: None,
            temp_file: None,
            bytes_written: 0,
            aborted: !enabled,
        };
        if !enabled {
            return writer;
        }
        match cache.temp_data_path(key).await {
            Ok(path) => match tfs::File::create(&path).await {
                Ok(file) => {
                    writer.temp_path = Some(path);
                    writer.temp_file = Some(BufWriter::with_capacity(CACHE_WRITE_BUFFER, file));
                }
                Err(e) => {
                    warn!(error=?e, "cache temp file create failed");
                    writer.aborted = true;
                }
            },
            Err(e) => {
                warn!(error=?e, "cache temp path create failed");
                writer.aborted = true;
            }
        }
        writer
    }

    pub async fn write_chunk(&mut self, data: &[u8], max_body_limit: u64) {
        if self.aborted || data.is_empty() {
            return;
        }
        let next_size = self.bytes_written.saturating_add(data.len() as u64);
        if next_size > max_body_limit {
            warn!(
                limit = max_body_limit,
                current = next_size,
                "Body exceeds limit, aborting cache write"
            );
            self.abort().await;
            return;
        }
        if let Some(file) = self.temp_file.as_mut()
            && let Err(e) = file.write_all(data).await
        {
            warn!(error=?e, "cache temp file write failed");
            self.abort().await;
            return;
        }
        self.bytes_written = next_size;
    }

    pub async fn abort(&mut self) {
        self.aborted = true;
        drop(self.temp_file.take());
        if let Some(path) = self.temp_path.take() {
            let _ = tfs::remove_file(path).await;
        }
    }

    pub async fn finalize(mut self, cache: &DiskCache, key: &str, options: CacheStoreOptions) {
        if self.aborted || self.bytes_written == 0 {
            self.abort().await;
            return;
        }
        if let (Some(mut buf), Some(path)) = (self.temp_file.take(), self.temp_path.take()) {
            if let Err(e) = buf.flush().await {
                warn!(error=?e, "cache temp file flush failed");
                drop(buf);
                let _ = tfs::remove_file(path).await;
                return;
            }
            if let Err(e) = buf.get_mut().sync_all().await {
                warn!(error=?e, "cache temp file sync failed");
                drop(buf);
                let _ = tfs::remove_file(path).await;
                return;
            }
            drop(buf);
            if let Err(e) = cache
                .put_file(key, &path, self.bytes_written, options)
                .await
            {
                warn!(error=?e, "cache file promote failed");
                let _ = tfs::remove_file(path).await;
            }
        }
    }
}

pub async fn relay_body_with_cache<B>(
    body: &mut B,
    tx: Option<&tokio::sync::mpsc::Sender<Result<hyper::body::Frame<Bytes>, hyper::Error>>>,
    cache: &DiskCache,
    variant_key: &str,
    should_cache: bool,
    max_body_limit: u64,
    options: CacheStoreOptions,
) where
    B: BodyExt<Data = Bytes, Error = hyper::Error> + Unpin,
{
    let mut writer = BodyCacheWriter::new(cache, variant_key, should_cache).await;
    while let Some(frame_res) = body.frame().await {
        match frame_res {
            Ok(frame) => {
                if let Ok(data) = frame.into_data()
                    && !data.is_empty()
                {
                    if let Some(tx) = tx
                        && tx
                            .send(Ok(hyper::body::Frame::data(data.clone())))
                            .await
                            .is_err()
                    {
                        writer.abort().await;
                        break;
                    }
                    writer.write_chunk(&data, max_body_limit).await;
                }
            }
            Err(e) => {
                warn!("Upstream stream error: {}", e);
                if let Some(tx) = tx {
                    let _ = tx.send(Err(e)).await;
                }
                writer.abort().await;
                break;
            }
        }
    }
    writer.finalize(cache, variant_key, options).await;
}

pub async fn background_refresh(
    cache_key: String,
    upstream_url: String,
    config: &Config,
    cache: &DiskCache,
    client: &HttpClient,
) -> Result<()> {
    let upstream_req = build_upstream_request(http::Method::GET, upstream_url.as_str(), None);
    let Ok(up_resp) = client.request(upstream_req).await else {
        return Ok(());
    };
    if up_resp.status() != StatusCode::OK {
        return Ok(());
    }

    let headers = up_resp.headers().clone();
    let decision = derive_ttl(&headers, std::time::SystemTime::now());
    if !decision.cacheable {
        return Ok(());
    }

    let mut body = up_resp.into_body();
    relay_body_with_cache(
        &mut body,
        None,
        cache,
        &cache_key,
        true,
        max_cacheable_body_bytes(config) as u64,
        store_options_from_headers(&headers, &decision),
    )
    .await;
    Ok(())
}
