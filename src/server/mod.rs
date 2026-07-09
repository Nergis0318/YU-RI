mod handler;
mod headers;
mod response;
mod upstream;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cache::DiskCache;
use crate::config::Config;
use anyhow::Result;
use handler::handle;
use hyper::service::service_fn;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use tokio::{net::TcpListener, sync::broadcast};
use tracing::{debug, info, warn};

pub use upstream::HttpClient;

#[derive(Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub stale_hits: AtomicU64,
    pub misses: AtomicU64,
    pub errors: AtomicU64,
    pub not_modified: AtomicU64,
}

impl CacheStats {
    pub fn record_hit(&self, is_fresh: bool) {
        let counter = if is_fresh {
            &self.hits
        } else {
            &self.stale_hits
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct AppState {
    config: Config,
    cache: DiskCache,
    client: HttpClient,
    stats: CacheStats,
    inflight: Arc<Mutex<HashMap<String, broadcast::Sender<()>>>>,
    refresh_inflight: Arc<Mutex<HashSet<String>>>,
}

pub(crate) type SharedState = Arc<AppState>;

pub async fn run(config: Config) -> Result<()> {
    let cache = DiskCache::new(
        &config.cache_dir,
        config.max_cache_size_bytes,
        config.default_ttl,
    )
    .await?;

    let https = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client: HttpClient = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        .pool_max_idle_per_host(64)
        .http2_keep_alive_interval(Some(Duration::from_secs(30)))
        .build(https);

    let shared: SharedState = Arc::new(AppState {
        config,
        cache,
        client,
        stats: CacheStats::default(),
        inflight: Arc::new(Mutex::new(HashMap::new())),
        refresh_inflight: Arc::new(Mutex::new(HashSet::new())),
    });

    spawn_cache_clear_tasks(&shared).await;

    let addr = shared.config.listen_addr.clone();
    let listener = TcpListener::bind(&addr).await?;
    info!(?addr, "Listening");

    let mut shutdown_signal = std::pin::pin!(shutdown_signal());
    let mut connections = tokio::task::JoinSet::new();
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
                let _ = stream.set_nodelay(true);
                debug!("Accepted connection from {}", remote_addr);

                let io = TokioIo::new(stream);
                let shared = shared.clone();
                connections.spawn(async move {
                    let service = service_fn(move |req| {
                        let shared = shared.clone();
                        async move { handle(req, shared).await }
                    });
                    let mut builder = Builder::new(TokioExecutor::new());
                    builder.http1().timer(TokioTimer::new());
                    builder.http2().timer(TokioTimer::new());
                    if let Err(err) = builder.serve_connection(io, service)
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

    drop(listener);
    info!("Draining active connections...");
    let drain_timeout = Duration::from_secs(30);
    let drain = async { while connections.join_next().await.is_some() {} };
    if tokio::time::timeout(drain_timeout, drain).await.is_err() {
        warn!("Graceful shutdown timed out; forcing remaining connections");
        connections.abort_all();
    }

    info!("Server shutdown complete");
    Ok(())
}

async fn clear_cache(shared: &SharedState, label: &str) {
    if let Err(e) = shared.cache.clear_all().await {
        warn!(error=?e, label, "cache clear failed");
    }
}

async fn spawn_cache_clear_tasks(shared: &SharedState) {
    if let Some(interval) = shared.config.cache_clear_interval {
        let shared_clone = shared.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                info!(?interval, "Interval cache clear running");
                clear_cache(&shared_clone, "interval").await;
            }
        });
        info!(every_secs=%interval.as_secs(), "Cache clear interval enabled");
    }
}

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
