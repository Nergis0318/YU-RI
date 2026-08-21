use crate::cache::CacheFileEntry;
use http::{HeaderMap, header};

#[derive(Clone, Copy)]
pub enum CacheStatus {
    Hit,
    Stale,
    Miss,
}

impl CacheStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "HIT",
            Self::Stale => "STALE",
            Self::Miss => "MISS",
        }
    }
}

pub fn add_cache_headers(headers: &mut HeaderMap, status: CacheStatus, created_at: u64) {
    headers.insert(
        "X-YU-RI-Cache",
        header::HeaderValue::from_static(status.as_str()),
    );
    if let Ok(hv) = header::HeaderValue::from_str(&created_at.to_string()) {
        headers.insert("X-YU-RI-Time", hv);
    }
}

pub fn copy_entry_headers(dst: &mut HeaderMap, entry: &CacheFileEntry) {
    if let Some(ct) = &entry.content_type {
        dst.insert(
            header::CONTENT_TYPE,
            ct.parse()
                .unwrap_or_else(|_| "application/octet-stream".parse().unwrap()),
        );
    }
    if let Some(etag) = &entry.etag
        && let Ok(hv) = etag.parse()
    {
        dst.insert(header::ETAG, hv);
    }
    if let Some(lm) = &entry.last_modified
        && let Ok(hv) = lm.parse()
    {
        dst.insert(header::LAST_MODIFIED, hv);
    }
}

pub fn copy_upstream_headers(dst: &mut HeaderMap, src: &HeaderMap, include_vary: bool) {
    dst.insert(
        header::ACCEPT_RANGES,
        header::HeaderValue::from_static("bytes"),
    );
    for hdr in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ETAG,
        header::LAST_MODIFIED,
    ] {
        if let Some(v) = src.get(&hdr) {
            dst.insert(hdr, v.clone());
        }
    }
    if include_vary && let Some(v) = src.get(header::VARY) {
        dst.insert(header::VARY, v.clone());
    }
}


