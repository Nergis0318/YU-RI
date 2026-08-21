use http::HeaderMap;
use std::time::{Duration, SystemTime};
use tracing::debug;

/// TTL decision and cacheability result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlDecision {
    pub ttl: Option<Duration>, // None means use the default TTL when storing
    pub cacheable: bool,
    pub stale_while_revalidate: Option<Duration>, // stale-while-revalidate=N support
}

/// Parses Cache-Control / Expires headers to derive a TTL.
/// Simplified rules:
/// 1. If Cache-Control is present, it takes precedence.
///    - no-store | private | no-cache => non-cacheable
///    - max-age=N => N seconds (0 means non-cacheable)
///    - s-maxage=N => shared proxy TTL, used if present
/// 2. If no Cache-Control max-age/s-maxage, use Expires (HTTP-date) if it is in the future.
/// 3. Otherwise use the default TTL (cacheable with ttl=None).
pub fn derive_ttl(headers: &HeaderMap, now: SystemTime) -> TtlDecision {
    use http::header;
    if let Some(cc_val) = headers
        .get(header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
    {
        debug!(target: "http_cache", cache_control = cc_val, "Parsing Cache-Control header");
        let mut max_age: Option<i64> = None;
        let mut s_maxage: Option<i64> = None;
        let mut swr: Option<i64> = None;
        for part in cc_val.split(',') {
            let token = part.trim().to_ascii_lowercase();
            if token == "no-store" || token == "no-cache" || token == "private" {
                // "private" is treated as non-cacheable for a shared cache.
                debug!(target: "http_cache", directive = token, "Cache-Control directive forces non-cacheable");
                return NOT_CACHEABLE;
            }
            if let Some(rest) = token.strip_prefix("s-maxage=") {
                if let Ok(v) = rest.parse::<i64>() {
                    s_maxage = Some(v.max(0));
                }
            } else if let Some(rest) = token.strip_prefix("max-age=") {
                if let Ok(v) = rest.parse::<i64>() {
                    max_age = Some(v.max(0));
                }
            } else if let Some(rest) = token.strip_prefix("stale-while-revalidate=")
                && let Ok(v) = rest.parse::<i64>()
            {
                swr = Some(v.max(0));
            }
        }
        let chosen = s_maxage.or(max_age);
        if let Some(sec) = chosen {
            if sec == 0 {
                debug!(target: "http_cache", ttl = sec, "Chosen TTL is zero => non-cacheable");
                return NOT_CACHEABLE;
            }
            let mut d = TtlDecision {
                ttl: Some(Duration::from_secs(sec as u64)),
                cacheable: true,
                stale_while_revalidate: None,
            };
            if let Some(s) = swr
                && s > 0
            {
                d.stale_while_revalidate = Some(Duration::from_secs(s as u64));
                debug!(target: "http_cache", ttl_secs = sec, swr_secs = s, "Derived TTL with stale-while-revalidate");
            }
            if d.stale_while_revalidate.is_none() {
                debug!(target: "http_cache", ttl_secs = sec, "Derived TTL from Cache-Control");
            }
            return d;
        }
    }
    // Expires handling (only when Cache-Control max-age/s-maxage is absent)
    if let Some(exp_val) = headers.get("Expires").and_then(|v| v.to_str().ok())
        && let Ok(exp_time) = httpdate::parse_http_date(exp_val)
    {
        if let Ok(diff) = exp_time.duration_since(now) {
            if diff.as_secs() == 0 {
                debug!(target: "http_cache", expires = exp_val, "Expires header is now => non-cacheable");
                return NOT_CACHEABLE;
            }
            debug!(target: "http_cache", expires = exp_val, ttl_secs = diff.as_secs(), "Derived TTL from Expires header");
            return TtlDecision {
                ttl: Some(diff),
                cacheable: true,
                stale_while_revalidate: None,
            };
        } else {
            debug!(target: "http_cache", expires = exp_val, "Expires header is in the past => non-cacheable");
            return NOT_CACHEABLE;
        }
    }
    // Default cacheable policy
    debug!(target: "http_cache", "No explicit TTL headers => using default cacheable policy");
    TtlDecision {
        ttl: None,
        cacheable: true,
        stale_while_revalidate: None,
    }
}

pub const NOT_CACHEABLE: TtlDecision = TtlDecision {
    ttl: None,
    cacheable: false,
    stale_while_revalidate: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_max_age() {
        let mut h = HeaderMap::new();
        h.insert("Cache-Control", "max-age=120".parse().unwrap());
        let d = derive_ttl(&h, SystemTime::now());
        assert_eq!(d.ttl, Some(Duration::from_secs(120)));
        assert!(d.cacheable);
    }

    #[test]
    fn test_s_maxage_precedence() {
        let mut h = HeaderMap::new();
        h.insert("Cache-Control", "max-age=10, s-maxage=20".parse().unwrap());
        let d = derive_ttl(&h, SystemTime::now());
        assert_eq!(d.ttl, Some(Duration::from_secs(20)));
    }

    #[test]
    fn test_no_store() {
        let mut h = HeaderMap::new();
        h.insert("Cache-Control", "no-store".parse().unwrap());
        let d = derive_ttl(&h, SystemTime::now());
        assert!(!d.cacheable);
    }

    #[test]
    fn test_expires() {
        let mut h = HeaderMap::new();
        let now = SystemTime::now();
        let future = httpdate::fmt_http_date(now + Duration::from_secs(30));
        h.insert("Expires", future.parse().unwrap());
        let d = derive_ttl(&h, now);
        let secs = d.ttl.unwrap().as_secs();
        assert!(
            secs == 30 || secs == 29,
            "expected ~30 secs ttl, got {}",
            secs
        );
    }

    #[test]
    fn test_expired_expires() {
        let mut h = HeaderMap::new();
        let now = SystemTime::now();
        let past = httpdate::fmt_http_date(now - Duration::from_secs(5));
        h.insert("Expires", past.parse().unwrap());
        let d = derive_ttl(&h, now);
        assert!(!d.cacheable);
    }
}
