mod cache;
mod config;
mod http_cache;
mod server;

use crate::config::Config;
use anyhow::Result;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    // Load config from config.toml first (before logging is initialized)
    let config = Config::from_file("config.toml")?;

    // Initialize logging with the log level from config
    // ENV_FILTER still takes precedence if RUST_LOG is set
    let default_filter = format!("yu_ri={}", config.log_level);
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&default_filter)))
        .init();

    tracing::info!(?config, "Starting caching server");

    server::run(config).await
}
