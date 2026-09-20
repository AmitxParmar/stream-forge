//! StreamForge: event ingestion and stream processing engine.
//!
//! Phase 1 exposes a single-node in-memory pipeline:
//! HTTP gateway -> bounded channel -> batch worker -> [`Sink`].
//!
//! # Examples
//!
//! ```no_run
//! use streamforge::{run, Config};
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let config = Config::default();
//! run(config).await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod event;
pub mod gateway;
pub mod pipeline;
pub mod sink;
pub mod telemetry;

pub use config::Config;
pub use error::AppError;
pub use event::{Event, EventId, UserId};
pub use sink::Sink;

use anyhow::Context;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Run the Phase 1 engine: channel, worker, and HTTP gateway.
///
/// When `config.database_url` is `Some`, events are persisted to Postgres;
/// otherwise an in-memory sink is used (suitable for tests and benchmarks).
///
/// # Errors
///
/// Returns an error if the gateway fails to bind or a spawned task joins with an error.
pub async fn run(config: Config) -> anyhow::Result<()> {
    config.validate()?;

    let shutdown = CancellationToken::new();
    let ctrl_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("received shutdown signal");
            ctrl_shutdown.cancel();
        }
    });

    let (tx, rx) = pipeline::channel(&config);

    match config.database_url {
        Some(ref url) => {
            let pg_sink = sink::PostgresSink::connect(url, config.pool_size)
                .await
                .context("failed to connect to postgres")?;
            info!("using postgres sink");
            run_with_sink(config, tx, rx, pg_sink, shutdown).await
        }
        None => {
            let mem_sink = sink::InMemorySink::default();
            info!("using in-memory sink");
            run_with_sink(config, tx, rx, mem_sink, shutdown).await
        }
    }
}

/// Inner orchestration loop generic over the sink implementation.
async fn run_with_sink<S: sink::Sink>(
    config: Config,
    tx: tokio::sync::mpsc::Sender<Event>,
    rx: tokio::sync::mpsc::Receiver<Event>,
    sink: S,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let worker = pipeline::spawn_worker(rx, sink, config.batch.clone(), shutdown.clone());

    gateway::serve(config.clone(), tx, shutdown.clone())
        .await
        .context("gateway failed")?;

    shutdown.cancel();
    worker
        .await
        .context("worker task panicked")?
        .context("worker failed")?;
    Ok(())
}

/// Re-exported for doctests and integration tests.
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
}
