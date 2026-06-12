mod handler;
mod headers;
mod response;
mod upstream;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::cache::DiskCache;
use crate::config::Config;
use anyhow::Result;
use dashmap::DashMap;
use handler::handle;
use hyper::service::service_fn;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::sync::Arc;
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
    stats: Arc<CacheStats>,
    inflight: Arc<DashMap<String, broadcast::Sender<()>>>,
}

pub(crate) type SharedState = Arc<AppState>;

pub async fn run(config: Config) -> Result<()> {
    let cache = DiskCache::new(
        &config.cache_dir,
        config.max_cache_size_bytes,
        config.default_ttl,
        config.eviction_policy,
    )
    .await?;

    let https = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client: HttpClient = Client::builder(TokioExecutor::new()).build(https);

    let shared: SharedState = Arc::new(AppState {
        config,
        cache,
        client,
        stats: Arc::new(CacheStats::default()),
        inflight: Arc::new(DashMap::new()),
    });

    spawn_cache_clear_tasks(&shared).await?;

    let addr = shared.config.listen_addr.clone();
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

async fn spawn_cache_clear_tasks(shared: &SharedState) -> Result<()> {
    if let Some(cron_expr) = shared.config.cache_clear_cron.clone() {
        let scheduler = tokio_cron_scheduler::JobScheduler::new().await?;
        let shared_clone = shared.clone();
        let job = tokio_cron_scheduler::Job::new_async(cron_expr.as_str(), move |_uuid, _l| {
            let shared_inner = shared_clone.clone();
            Box::pin(async move {
                info!("Running scheduled cache clear");
                if let Err(e) = shared_inner.cache.clear_all().await {
                    warn!(error=?e, "cache clear failed");
                }
            })
        })?;
        scheduler.add(job).await?;
        scheduler.start().await?;
        info!(cron=%cron_expr, "Cache clear cron enabled");
    }
    if let Some(interval) = shared.config.cache_clear_interval {
        let shared_clone = shared.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                info!(?interval, "Interval cache clear running");
                if let Err(e) = shared_clone.cache.clear_all().await {
                    warn!(error=?e, "interval cache clear failed");
                }
            }
        });
        info!(every_secs=%interval.as_secs(), "Cache clear interval enabled");
    }
    Ok(())
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
