//! Batch pipeline: bounded channel plus a single batching worker.
//!
//! The channel is intentionally bounded so overload becomes backpressure
//! (a slow `send`) instead of unbounded memory growth. Phase 2 adds explicit
//! shedding on top of this mechanism.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::error::AppError;
use crate::event::Event;

/// Batching policy for the worker.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Flush when the batch reaches this many events.
    pub max_size: usize,
    /// Flush a partial batch after this long.
    pub timeout: Duration,
    /// Resize batches from queue depth instead of always using `max_size`.
    pub adaptive: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 256,
            timeout: Duration::from_millis(100),
            adaptive: true,
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

/// Effective flush threshold for this batch.
///
/// A shallow queue means no backlog: flush small batches quickly to keep
/// per-event latency low. A deep queue means we are behind: grow the batch
/// to amortize per-flush cost (fsync, serialization, dispatch) and drain faster.
/// Always within `[max_size / 8, max_size]`, never below 1.
#[must_use]
pub fn effective_batch_size(max_size: usize, depth: usize, capacity: usize) -> usize {
    if capacity == 0 || max_size == 0 {
        return 1;
    }
    let floor = (max_size / 8).max(1);
    let headroom = max_size.saturating_sub(floor);
    let used = depth.min(capacity);
    floor + headroom.saturating_mul(used) / capacity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_batch_grows_with_backlog() {
        let shallow = effective_batch_size(256, 0, 1024);
        let mid = effective_batch_size(256, 512, 1024);
        let deep = effective_batch_size(256, 1024, 1024);
        assert!(shallow < mid, "shallow={shallow} mid={mid}");
        assert!(mid < deep, "mid={mid} deep={deep}");
        assert_eq!(deep, 256);
        assert_eq!(shallow, 32);
    }

    #[test]
    fn adaptive_batch_never_exceeds_floor() {
        assert!(effective_batch_size(8, usize::MAX, 16) >= 1);
    }
}
