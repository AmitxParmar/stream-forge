//! Thin binary entry point. All logic lives in the library crate
//! ([`streamforge::run`]) so integration tests can exercise it.

use anyhow::Context;
use clap::Parser;
use streamforge::{Config, telemetry};

/// StreamForge Phase 1: single-node in-memory ingestion engine.
#[derive(Parser, Debug)]
#[command(name = "streamforge", version, about)]
struct Args {
    /// Address to bind the HTTP gateway to.
    #[arg(long, env = "STREAMFORGE_BIND", default_value = "127.0.0.1:8080")]
    bind: std::net::SocketAddr,

    /// Capacity of the bounded gateway-to-worker channel.
    #[arg(long, env = "STREAMFORGE_QUEUE_CAPACITY", default_value_t = 1024)]
    queue_capacity: usize,

    /// Max events per batch flushed to the sink.
    #[arg(long, env = "STREAMFORGE_BATCH_SIZE", default_value_t = 256)]
    batch_size: usize,

    /// Max time to wait before flushing a partial batch (ms).
    #[arg(long, env = "STREAMFORGE_BATCH_TIMEOUT_MS", default_value_t = 100)]
    batch_timeout_ms: u64,

    /// Postgres connection string. Omit to use the in-memory sink.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Maximum connections in the Postgres pool.
    #[arg(long, env = "STREAMFORGE_POOL_SIZE", default_value_t = 5)]
    pool_size: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    telemetry::init().context("failed to initialise telemetry")?;

    let config = Config {
        bind: args.bind,
        queue_capacity: args.queue_capacity,
        batch: streamforge::pipeline::BatchConfig {
            max_size: args.batch_size,
            timeout: std::time::Duration::from_millis(args.batch_timeout_ms),
        },
        database_url: args.database_url,
        pool_size: args.pool_size,
    };

    streamforge::run(config).await
}
