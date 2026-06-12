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

#[derive(Default)]
pub struct UpstreamMeta {
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub fn extract_upstream_meta(headers: &HeaderMap) -> UpstreamMeta {
    UpstreamMeta {
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        etag: headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        last_modified: headers
            .get(header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from),
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

pub fn vary_cache_key(base_key: &str, req_headers: &HeaderMap, vary_names: &[String]) -> String {
    let parts: Vec<String> = vary_names
        .iter()
        .map(|name| {
            if let Some(val) = req_headers.get(name.as_str()).and_then(|v| v.to_str().ok()) {
                format!("{name}={val}")
            } else {
                format!("{name}=")
            }
        })
        .collect();
    if parts.is_empty() {
        base_key.to_string()
    } else {
        format!("{}||{}", base_key, parts.join("&"))
    }
}

pub struct VaryResult {
    pub variant_key: String,
    pub vary_all: bool,
}

pub fn apply_vary_from_response(
    base_key: &str,
    current_key: &str,
    req_headers: &HeaderMap,
    resp_headers: &HeaderMap,
    cacheable: bool,
    is_ok: bool,
) -> VaryResult {
    let Some(vary_val) = resp_headers.get(header::VARY).and_then(|v| v.to_str().ok()) else {
        return VaryResult {
            variant_key: current_key.to_string(),
            vary_all: false,
        };
    };

    let names: Vec<String> = vary_val
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if names.iter().any(|name| name == "*") {
        return VaryResult {
            variant_key: current_key.to_string(),
            vary_all: true,
        };
    }

    if names.is_empty() || !is_ok || !cacheable {
        return VaryResult {
            variant_key: current_key.to_string(),
            vary_all: false,
        };
    }

    VaryResult {
        variant_key: vary_cache_key(base_key, req_headers, &names),
        vary_all: false,
    }
}
