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
pub mod worker;
pub mod workload;

pub use config::Config;
pub use error::AppError;
pub use event::{Event, EventId, UserId};
pub use sink::Sink;
pub use telemetry::Metrics;
pub use worker::WorkerModel;

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
    let metrics = telemetry::Metrics::new();

    match config.database_url {
        Some(ref url) => {
            let pg_sink = sink::PostgresSink::connect(url, config.pool_size)
                .await
                .context("failed to connect to postgres")?;
            info!("using postgres sink");
            let pg_sink = sink::SlowSink::new(
                pg_sink,
                std::time::Duration::from_millis(config.sink_delay_ms),
            );
            run_with_sink(config, tx, rx, pg_sink, shutdown, metrics).await
        }
        None => {
            let mem_sink = sink::InMemorySink::default();
            info!("using in-memory sink");
            let mem_sink = sink::SlowSink::new(
                mem_sink,
                std::time::Duration::from_millis(config.sink_delay_ms),
            );
            run_with_sink(config, tx, rx, mem_sink, shutdown, metrics).await
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
    metrics: telemetry::Metrics,
) -> anyhow::Result<()> {
    let plan = worker::WorkerPlan {
        batch: config.batch.clone(),
        model: config.worker_model,
        count: config.worker_count,
        workload: workload::Workload::new(config.io_work_us, config.cpu_iters),
    };
    let workers = worker::spawn_workers(rx, sink, plan, shutdown.clone(), metrics.clone());

    gateway::serve(config.clone(), tx, shutdown.clone(), metrics)
        .await
        .context("gateway failed")?;

    shutdown.cancel();
    workers.wait().await.context("worker group failed")?;
    Ok(())
}

/// Re-exported for doctests and integration tests.
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
}
