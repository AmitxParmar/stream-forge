//! Batch pipeline: bounded channel plus a single batching worker.
//!
//! The channel is intentionally bounded so overload becomes backpressure
//! (a slow `send`) instead of unbounded memory growth. Phase 2 adds explicit
//! shedding on top of this mechanism.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

use crate::error::AppError;
use crate::event::Event;
use crate::sink::Sink;

/// Batching policy for the worker.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Flush when the batch reaches this many events.
    pub max_size: usize,
    /// Flush a partial batch after this long.
    pub timeout: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 256,
            timeout: Duration::from_millis(100),
        }
    }
}

impl BatchConfig {
    /// Validate batching policy.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if `max_size` is zero or `timeout` is zero.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.max_size == 0 {
            return Err(AppError::Config {
                reason: "batch max_size must be greater than zero".to_owned(),
            });
        }
        if self.timeout.is_zero() {
            return Err(AppError::Config {
                reason: "batch timeout must be greater than zero".to_owned(),
            });
        }
        Ok(())
    }
}

/// Create the bounded gateway-to-worker channel.
///
/// The sender is cloned per gateway handler; there is exactly one receiver.
pub fn channel(config: &crate::Config) -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
    mpsc::channel(config.queue_capacity)
}

/// Spawn the single Phase 1 batch worker.
///
/// The worker accumulates events until [`BatchConfig::max_size`] or
/// [`BatchConfig::timeout`], then flushes to `sink`. It exits when the
/// shutdown token fires or all senders are dropped.
pub fn spawn_worker<S>(
    mut rx: mpsc::Receiver<Event>,
    sink: S,
    batch: BatchConfig,
    shutdown: CancellationToken,
) -> JoinHandle<Result<(), AppError>>
where
    S: Sink,
{
    tokio::spawn(async move {
        let mut pending: Vec<Event> = Vec::with_capacity(batch.max_size);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!(pending = pending.len(), "worker shutting down, flushing remainder");
                    flush(&sink, &mut pending).await?;
                    return Ok(());
                }
                received = rx.recv() => {
                    let Some(event) = received else {
                        info!(pending = pending.len(), "channel closed, flushing remainder");
                        flush(&sink, &mut pending).await?;
                        return Ok(());
                    };
                    pending.push(event);
                    if pending.len() >= batch.max_size {
                        flush(&sink, &mut pending).await?;
                        continue;
                    }
                    // Wait briefly for more events before flushing a partial batch.
                    if tokio::time::timeout(batch.timeout, drain(&mut rx, &mut pending, batch.max_size))
                        .await
                        .is_err()
                    {
                        warn!(pending = pending.len(), "batch timeout elapsed, flushing partial batch");
                    }
                    flush(&sink, &mut pending).await?;
                }
            }
        }
    })
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

/// Flush the pending batch to the sink and reuse the allocation.
#[instrument(skip(sink, pending), fields(batch_size = pending.len()))]
async fn flush<S>(sink: &S, pending: &mut Vec<Event>) -> Result<(), AppError>
where
    S: Sink,
{
    if pending.is_empty() {
        return Ok(());
    }
    sink.write_batch(pending).await.map_err(AppError::Sink)?;
    info!(flushed = pending.len(), "batch flushed");
    pending.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::InMemorySink;

    #[tokio::test]
    async fn worker_flushes_on_channel_close() {
        // Arrange
        let (tx, rx) = mpsc::channel(16);
        let sink = InMemorySink::default();
        let shutdown = CancellationToken::new();
        let handle = spawn_worker(
            rx,
            sink.clone(),
            BatchConfig {
                max_size: 256,
                timeout: Duration::from_millis(10),
            },
            shutdown,
        );

        // Act
        let event = crate::event::Event::parse(crate::event::RawEvent {
            event_id: 1,
            user_id: 2,
            kind: "click".to_owned(),
            payload: serde_json::json!({}),
        })
        .unwrap();
        tx.send(event).await.unwrap();
        drop(tx);

        // Assert
        handle.await.unwrap().unwrap();
        assert_eq!(sink.len().await, 1);
    }
}
