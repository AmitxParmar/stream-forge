//! Phase 4 sharded workers: N parallel batch consumers fed by a partitioner.
//!
//! The gateway's single bounded channel fans out to N per-shard channels via
//! `hash(user_id) % N` ([`Partitioner`]); each worker exclusively owns one
//! shard, so per-key ordering holds without cross-worker locks (ADR-004).
//! Each worker keeps the adaptive batching loop from Phase 2 and dispatches
//! the batch kernel to one of three execution models ([`WorkerModel`]).

use std::str::FromStr;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::error::AppError;
use crate::event::Event;
use crate::partitioner::Partitioner;
use crate::pipeline::{BatchConfig, effective_batch_size};
use crate::sink::Sink;
use crate::telemetry::Metrics;
use crate::workload::Workload;

/// Execution model for the per-flush batch kernel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorkerModel {
    /// Run the kernel inline on the worker's async task.
    #[default]
    Tokio,
    /// Run the kernel on the blocking OS-thread pool (`spawn_blocking`).
    Threads,
    /// Run the kernel on the rayon pool, await via oneshot.
    Rayon,
}

impl std::fmt::Display for WorkerModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerModel::Tokio => f.write_str("tokio"),
            WorkerModel::Threads => f.write_str("threads"),
            WorkerModel::Rayon => f.write_str("rayon"),
        }
    }
}

impl FromStr for WorkerModel {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, AppError> {
        match s {
            "tokio" => Ok(WorkerModel::Tokio),
            "threads" => Ok(WorkerModel::Threads),
            "rayon" => Ok(WorkerModel::Rayon),
            _ => Err(AppError::Config {
                reason: format!("unknown worker model '{s}' (tokio|threads|rayon)"),
            }),
        }
    }
}

/// Startup settings for the worker group: batch policy, model, count, kernel.
#[derive(Debug, Clone)]
pub struct WorkerPlan {
    /// Batching policy per worker.
    pub batch: BatchConfig,
    /// Execution model for the batch kernel.
    pub model: WorkerModel,
    /// Number of workers (1 = single consumer).
    pub count: usize,
    /// Workload applied to each batch.
    pub workload: Workload,
}

/// Handle set for the distributor plus all workers.
pub struct WorkerGroup {
    handles: Vec<JoinHandle<Result<(), AppError>>>,
}

impl WorkerGroup {
    /// Wait for every task; the first failure aborts.
    ///
    /// # Errors
    ///
    /// Returns the first [`AppError`] produced by any worker.
    pub async fn wait(self) -> Result<(), AppError> {
        for handle in self.handles {
            handle
                .await
                .map_err(|e| AppError::WorkerFailed {
                    reason: e.to_string(),
                })?
                .map_err(|e: AppError| AppError::WorkerFailed {
                    reason: e.to_string(),
                })?;
        }
        Ok(())
    }
}

/// Spawn the partitioner and `count` shard workers; `count` is at least 1.
///
/// The partitioner owns the gateway channel and routes each event to
/// `hash(user_id) % count`, so one worker exclusively owns each shard and
/// per-key order is preserved end to end (ADR-004).
pub fn spawn_workers<S>(
    mut rx: mpsc::Receiver<Event>,
    sink: S,
    plan: WorkerPlan,
    shutdown: CancellationToken,
    metrics: Metrics,
) -> WorkerGroup
where
    S: Sink,
{
    let count = plan.count.max(1);
    let batch = &plan.batch;
    let capacity = (batch.max_size.saturating_mul(2)).max(1);
    let (txs, rxs): (Vec<mpsc::Sender<Event>>, Vec<mpsc::Receiver<Event>>) =
        (0..count).map(|_| mpsc::channel(capacity)).unzip();

    metrics.init_shards(count);
    let dist_shutdown = shutdown.clone();
    let dist_metrics = metrics.clone();
    let mut handles: Vec<JoinHandle<Result<(), AppError>>> = Vec::with_capacity(count + 1);

    handles.push(tokio::spawn(async move {
        let partitioner = Partitioner::new(count)?;
        loop {
            tokio::select! {
                _ = dist_shutdown.cancelled() => {
                    info!("partitioner shutting down, closing shard channels");
                    drop(txs);
                    return Ok(());
                }
                received = rx.recv() => {
                    let Some(event) = received else {
                        info!("gateway channel closed, closing shard channels");
                        drop(txs);
                        return Ok(());
                    };
                    dist_metrics.set_depth(rx.len());
                    let shard = partitioner.route(&event);
                    let tx = txs.get(shard).ok_or(AppError::RoutingError {
                        index: shard,
                        count,
                    })?;
                    dist_metrics.set_shard_depth(shard, tx.max_capacity().saturating_sub(tx.capacity()));
                    if tx.send(event).await.is_err() {
                        return Err(AppError::WorkerFailed {
                            reason: "worker channel closed unexpectedly".to_owned(),
                        });
                    }
                }
            }
        }
    }));

    for (id, worker_rx) in rxs.into_iter().enumerate() {
        let sink = sink.clone();
        let plan = plan.clone();
        let metrics = metrics.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            worker_loop(id, worker_rx, sink, plan, metrics, shutdown).await
        }));
    }

    WorkerGroup { handles }
}

#[instrument(
    level = "debug",
    skip(rx, sink, plan, metrics, shutdown),
    fields(worker = id)
)]
async fn worker_loop<S>(
    id: usize,
    mut rx: mpsc::Receiver<Event>,
    sink: S,
    plan: WorkerPlan,
    metrics: Metrics,
    shutdown: CancellationToken,
) -> Result<(), AppError>
where
    S: Sink,
{
    let WorkerPlan {
        batch,
        model,
        workload,
        ..
    } = plan;
    let capacity = rx.max_capacity();
    metrics.set_shard_depth(id, rx.len());
    let mut pending: Vec<Event> = Vec::with_capacity(batch.max_size);
    loop {
        let effective = if batch.adaptive {
            effective_batch_size(batch.max_size, rx.len(), capacity)
        } else {
            batch.max_size
        };
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!(pending = pending.len(), "worker shutting down, flushing remainder");
                flush(model, &workload, &sink, &mut pending, &metrics).await?;
                metrics.set_shard_depth(id, 0);
                return Ok(());
            }
            received = rx.recv() => {
                let Some(event) = received else {
                    info!(pending = pending.len(), "worker channel closed, flushing remainder");
                    flush(model, &workload, &sink, &mut pending, &metrics).await?;
                    metrics.set_shard_depth(id, 0);
                    return Ok(());
                };
                pending.push(event);
                metrics.set_shard_depth(id, rx.len());
                if pending.len() >= effective {
                    flush(model, &workload, &sink, &mut pending, &metrics).await?;
                    continue;
                }
                if tokio::time::timeout(batch.timeout, drain(&mut rx, &mut pending, effective))
                    .await
                    .is_err()
                {
                    tracing::debug!(pending = pending.len(), "batch timeout elapsed");
                }
                flush(model, &workload, &sink, &mut pending, &metrics).await?;
            }
        }
    }
}

/// Flush the pending batch through the model's execution context, then the sink.
#[instrument(level = "debug", skip(model, workload, sink, pending, metrics), fields(batch_size = pending.len()))]
async fn flush<S>(
    model: WorkerModel,
    workload: &Workload,
    sink: &S,
    pending: &mut Vec<Event>,
    metrics: &Metrics,
) -> Result<(), AppError>
where
    S: Sink,
{
    let n = pending.len();
    if n == 0 {
        return Ok(());
    }
    metrics.set_batch_last(n);
    let batch = std::mem::take(pending);
    let batch = run_kernel(model, workload, batch).await?;
    sink.write_batch(&batch).await.map_err(AppError::Sink)?;
    info!(flushed = n, %model, "batch flushed");
    Ok(())
}

/// Execute the batch kernel on the chosen model.
async fn run_kernel(
    model: WorkerModel,
    workload: &Workload,
    batch: Vec<Event>,
) -> Result<Vec<Event>, AppError> {
    match model {
        WorkerModel::Tokio => workload.process(batch).await,
        WorkerModel::Threads => {
            let workload = workload.clone();
            Ok(tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(workload.process(batch))
            })
            .await
            .unwrap_or_else(|e| {
                Err(AppError::WorkerFailed {
                    reason: e.to_string(),
                })
            })?)
        }
        WorkerModel::Rayon => {
            let (tx, rx) = oneshot::channel();
            let workload = workload.clone();
            let handle = tokio::runtime::Handle::current();
            rayon::spawn(move || {
                let result = handle.block_on(workload.process(batch));
                let _ = tx.send(result);
            });
            Ok(rx.await.unwrap_or_else(|_| {
                Err(AppError::WorkerFailed {
                    reason: "rayon channel closed".to_owned(),
                })
            })?)
        }
    }
}

/// Drain up to `limit` additional events without blocking past channel state.
async fn drain(rx: &mut mpsc::Receiver<Event>, pending: &mut Vec<Event>, limit: usize) {
    while pending.len() < limit {
        match rx.try_recv() {
            Ok(event) => pending.push(event),
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::InMemorySink;
    use std::time::Duration;

    fn raw_event(id: u64) -> crate::event::Event {
        crate::event::Event::parse(crate::event::RawEvent {
            event_id: id,
            user_id: id % 10,
            kind: "click".to_owned(),
            payload: serde_json::json!({ "id": id }),
        })
        .unwrap()
    }

    async fn run_group(model: WorkerModel, workers: usize, total: usize) -> usize {
        let (tx_in, rx) = mpsc::channel(64);
        let sink = InMemorySink::default();
        let shutdown = CancellationToken::new();
        let batch = BatchConfig {
            max_size: 16,
            timeout: Duration::from_millis(20),
            adaptive: false,
        };
        let group = spawn_workers(
            rx,
            sink.clone(),
            WorkerPlan {
                batch,
                model,
                count: workers,
                workload: Workload::default(),
            },
            shutdown.clone(),
            Metrics::new(),
        );

        for id in 0..total {
            tx_in.send(raw_event(id as u64)).await.unwrap();
        }
        drop(tx_in);
        group.wait().await.unwrap();
        sink.len().await
    }

    #[tokio::test]
    async fn tokio_workers_flush_everything() {
        assert_eq!(run_group(WorkerModel::Tokio, 4, 40).await, 40);
    }

    #[tokio::test]
    async fn thread_workers_flush_everything() {
        assert_eq!(run_group(WorkerModel::Threads, 2, 40).await, 40);
    }

    #[tokio::test]
    async fn rayon_workers_flush_everything() {
        assert_eq!(run_group(WorkerModel::Rayon, 4, 40).await, 40);
    }

    #[tokio::test]
    async fn single_worker_flushes_everything() {
        assert_eq!(run_group(WorkerModel::Tokio, 1, 40).await, 40);
    }

    #[test]
    fn model_from_str_round_trip() {
        assert_eq!("tokio".parse::<WorkerModel>().unwrap(), WorkerModel::Tokio);
        assert_eq!("rayon".parse::<WorkerModel>().unwrap(), WorkerModel::Rayon);
        assert!("bogus".parse::<WorkerModel>().is_err());
    }
}
