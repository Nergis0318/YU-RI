use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// Raw TOML structure matching config.toml layout
#[derive(Debug, Deserialize)]
struct TomlConfig {
    settings: TomlSettings,
    upstream: TomlUpstream,
}

#[derive(Debug, Deserialize)]
struct TomlSettings {
    host: Option<String>,
    port: Option<u16>,
    log: Option<String>,
    cache: Option<TomlCache>,
}

#[derive(Debug, Deserialize)]
struct TomlCache {
    dir: Option<String>,
    size: Option<u64>,
    ttl: Option<u64>,
    policy: Option<String>,
    interval: Option<TomlCacheInterval>,
    cron: Option<String>,
    body_limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TomlCacheInterval {
    enable: Option<bool>,
    time: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TomlUpstream {
    url: String,
    sub: Option<Vec<TomlUpstreamSub>>,
}

#[derive(Debug, Deserialize)]
struct TomlUpstreamSub {
    url: String,
    path: String,
}

// ── Public types ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UpstreamSub {
    /// Base URL for this sub-route (e.g. "https://example.com/asdadsad")
    pub url: String,
    /// Path prefix to match (e.g. "/path/to/subpath")
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub log_level: String,
    pub upstream_base: String,
    /// Sub-path upstream overrides, sorted longest-path-first for greedy matching
    pub upstream_subs: Vec<UpstreamSub>,
    pub cache_dir: String,
    pub max_cache_size_bytes: u64,
    pub default_ttl: Duration,
    pub eviction_policy: EvictionPolicy,
    pub cache_clear_cron: Option<String>,
    pub cache_clear_interval: Option<Duration>,
    pub max_body_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub enum EvictionPolicy {
    /// First-In-First-Out (created_at 기반)
    Fifo,
    /// Least Recently Used (last_access_at 오래된 것 먼저)
    Lru,
    /// Size 우선 (가장 큰 객체부터 제거)
    Size,
    /// LRU 우선, 동일/유사 접근시 큰 객체 우선 제거
    LruSize,
}

impl EvictionPolicy {
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "FIFO" => Self::Fifo,
            "SIZE" => Self::Size,
            "LRU_SIZE" | "LRUSIZE" | "LRU-SIZE" => Self::LruSize,
            _ => Self::Lru, // 기본 LRU
        }
    }
}

impl Config {
    /// Load configuration from `config.toml` located at `path`.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let raw: TomlConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        // ── settings ────────────────────────────────────────
        let host = raw.settings.host.unwrap_or_else(|| "127.0.0.1".into());
        let port = raw.settings.port.unwrap_or(8080);
        let listen_addr = format!("{}:{}", host, port);
        let log_level = raw.settings.log.unwrap_or_else(|| "info".into());

        // ── cache ───────────────────────────────────────────
        let (
            cache_dir,
            max_cache_size_bytes,
            default_ttl,
            eviction_policy,
            cache_clear_interval,
            cache_clear_cron,
            max_body_bytes,
        ) = if let Some(cache) = raw.settings.cache {
            let dir = cache.dir.unwrap_or_else(|| "cache".into());
            let size = cache.size.unwrap_or(5 * 1024 * 1024 * 1024); // 5 GB
            let ttl = Duration::from_secs(cache.ttl.unwrap_or(300));
            let policy =
                EvictionPolicy::from_str_loose(&cache.policy.unwrap_or_else(|| "lru".into()));

            let interval = cache.interval.and_then(|iv| {
                if iv.enable.unwrap_or(false) {
                    let secs = iv.time.unwrap_or(0);
                    if secs > 0 {
                        Some(Duration::from_secs(secs))
                    } else {
                        None
                    }
                } else {
                    None
                }
            });

            let cron = cache.cron;
            let body_limit = cache.body_limit;

            (dir, size, ttl, policy, interval, cron, body_limit)
        } else {
            (
                "cache".into(),
                5 * 1024 * 1024 * 1024,
                Duration::from_secs(300),
                EvictionPolicy::Lru,
                None,
                None,
                None,
            )
        };

        // ── upstream ────────────────────────────────────────
        let upstream_base = raw.upstream.url;

        let mut upstream_subs: Vec<UpstreamSub> = raw
            .upstream
            .sub
            .unwrap_or_default()
            .into_iter()
            .map(|s| {
                // 경로는 항상 '/' 로 시작하도록 정규화, 끝의 '/' 는 제거
                let path = if s.path.starts_with('/') {
                    s.path.trim_end_matches('/').to_string()
                } else {
                    format!("/{}", s.path.trim_end_matches('/'))
                };
                UpstreamSub { url: s.url, path }
            })
            .collect();

        // 긴 path 부터 매칭하도록 내림차순 정렬 (greedy matching)
        upstream_subs.sort_by_key(|b| std::cmp::Reverse(b.path.len()));

        Ok(Self {
            listen_addr,
            log_level,
            upstream_base,
            upstream_subs,
            cache_dir,
            max_cache_size_bytes,
            default_ttl,
            eviction_policy,
            cache_clear_cron,
            cache_clear_interval,
            max_body_bytes,
        })
    }

    /// Resolve the upstream URL for a given request path.
    ///
    /// If a sub-path matches, the sub.path prefix is stripped from `req_path`
    /// and the remainder is appended to `sub.url`.
    /// Otherwise the default `upstream_base` is used with the full `req_path`.
    ///
    /// Example:
    ///   sub.path = "/assets", sub.url = "https://cdn.example.com/static"
    ///   req_path = "/assets/img/logo.png"
    ///   → "https://cdn.example.com/static/img/logo.png"
    pub fn resolve_upstream(&self, req_path_and_query: &str) -> String {
        // req_path_and_query looks like "/some/path?query=1"
        // We only match against the path portion for sub routing.
        let path_only = req_path_and_query
            .split('?')
            .next()
            .unwrap_or(req_path_and_query);

        for sub in &self.upstream_subs {
            if path_only == sub.path || path_only.starts_with(&format!("{}/", sub.path)) {
                let remainder = &req_path_and_query[sub.path.len()..];
                return format!(
                    "{}{}{}",
                    sub.url.trim_end_matches('/'),
                    if remainder.is_empty()
                        || remainder.starts_with('/')
                        || remainder.starts_with('?')
                    {
                        ""
                    } else {
                        "/"
                    },
                    remainder
                );
            }
        }

        // Default upstream
        format!(
            "{}/{}",
            self.upstream_base.trim_end_matches('/'),
            req_path_and_query.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> Config {
        Config {
            listen_addr: "127.0.0.1:8080".into(),
            log_level: "info".into(),
            upstream_base: "https://example.kr".into(),
            upstream_subs: vec![
                UpstreamSub {
                    url: "https://cdn.example.com/static".into(),
                    path: "/assets/img".into(),
                },
                UpstreamSub {
                    url: "https://cdn.example.com/all".into(),
                    path: "/assets".into(),
                },
                UpstreamSub {
                    url: "https://api.example.com".into(),
                    path: "/api".into(),
                },
            ],
            cache_dir: "cache".into(),
            max_cache_size_bytes: 1_000_000,
            default_ttl: Duration::from_secs(300),
            eviction_policy: EvictionPolicy::Lru,
            cache_clear_cron: None,
            cache_clear_interval: None,
            max_body_bytes: None,
        }
    }

    #[test]
    fn test_default_upstream() {
        let cfg = make_config();
        assert_eq!(
            cfg.resolve_upstream("/some/file.txt"),
            "https://example.kr/some/file.txt"
        );
    }

    #[test]
    fn test_sub_upstream() {
        let cfg = make_config();
        assert_eq!(
            cfg.resolve_upstream("/api/v1/users"),
            "https://api.example.com/v1/users"
        );
    }

    #[test]
    fn test_sub_upstream_exact_path() {
        let cfg = make_config();
        assert_eq!(cfg.resolve_upstream("/api"), "https://api.example.com");
    }

    #[test]
    fn test_sub_upstream_with_query() {
        let cfg = make_config();
        assert_eq!(
            cfg.resolve_upstream("/api/v1/users?page=1"),
            "https://api.example.com/v1/users?page=1"
        );
    }

    #[test]
    fn test_longest_prefix_match() {
        let cfg = make_config();
        // /assets/img should match the longer prefix "/assets/img" not "/assets"
        assert_eq!(
            cfg.resolve_upstream("/assets/img/logo.png"),
            "https://cdn.example.com/static/logo.png"
        );
        // /assets/css should match "/assets"
        assert_eq!(
            cfg.resolve_upstream("/assets/css/style.css"),
            "https://cdn.example.com/all/css/style.css"
        );
    }
}
