//! Tracing subscriber setup. Only the binary calls [`init`]; library code
//! emits spans/events through the facade and never installs a subscriber.

use tracing_subscriber::EnvFilter;

/// Process-local pipeline metrics shared between gateway, worker, and the
/// `/metrics` endpoint. Prometheus export arrives in Phase 9 (ADR-010).
#[derive(Debug, Clone)]
pub struct Metrics {
    inner: std::sync::Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    depth: std::sync::atomic::AtomicUsize,
    batch_last: std::sync::atomic::AtomicUsize,
    accepted: std::sync::atomic::AtomicU64,
    rejected_full: std::sync::atomic::AtomicU64,
    rejected_invalid: std::sync::atomic::AtomicU64,
    started: std::time::Instant,
}

/// Point-in-time metrics view serialised at `/metrics`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    /// Events currently sitting in the bounded channel.
    pub queue_depth: usize,
    /// Last flushed batch size.
    pub batch_last: usize,
    /// Events accepted with 202.
    pub accepted: u64,
    /// Events shed with 429 because the channel was full.
    pub rejected_full: u64,
    /// Events rejected with 422 for failing validation.
    pub rejected_invalid: u64,
    /// Milliseconds since startup.
    pub uptime_ms: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a fresh metrics handle.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(MetricsInner {
                depth: std::sync::atomic::AtomicUsize::new(0),
                batch_last: std::sync::atomic::AtomicUsize::new(0),
                accepted: std::sync::atomic::AtomicU64::new(0),
                rejected_full: std::sync::atomic::AtomicU64::new(0),
                rejected_invalid: std::sync::atomic::AtomicU64::new(0),
                started: std::time::Instant::now(),
            }),
        }
    }

    /// Publish the current channel depth.
    pub fn set_depth(&self, depth: usize) {
        self.inner
            .depth
            .store(depth, std::sync::atomic::Ordering::Relaxed);
    }

    /// Publish the last flushed batch size.
    pub fn set_batch_last(&self, batch: usize) {
        self.inner
            .batch_last
            .store(batch, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record one accepted event.
    pub fn inc_accepted(&self) {
        self.inner
            .accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record one event shed because the channel was full.
    pub fn inc_rejected_full(&self) {
        self.inner
            .rejected_full
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record one event rejected for failing validation.
    pub fn inc_rejected_invalid(&self) {
        self.inner
            .rejected_invalid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Capture the current counters as a serialisable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            queue_depth: self.inner.depth.load(std::sync::atomic::Ordering::Relaxed),
            batch_last: self
                .inner
                .batch_last
                .load(std::sync::atomic::Ordering::Relaxed),
            accepted: self
                .inner
                .accepted
                .load(std::sync::atomic::Ordering::Relaxed),
            rejected_full: self
                .inner
                .rejected_full
                .load(std::sync::atomic::Ordering::Relaxed),
            rejected_invalid: self
                .inner
                .rejected_invalid
                .load(std::sync::atomic::Ordering::Relaxed),
            uptime_ms: u64::try_from(self.inner.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }
    }
}

/// Initialise the global tracing subscriber from `RUST_LOG` (default: `info`).
///
/// # Errors
///
/// Returns an error if the filter directive cannot be parsed or the global
/// subscriber is already set.
pub fn init() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing subscriber already initialised: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_counts_everything() {
        let m = Metrics::new();
        m.set_depth(7);
        m.set_batch_last(64);
        m.inc_accepted();
        m.inc_accepted();
        m.inc_rejected_full();
        m.inc_rejected_invalid();
        let s = m.snapshot();
        assert_eq!(s.queue_depth, 7);
        assert_eq!(s.batch_last, 64);
        assert_eq!(s.accepted, 2);
        assert_eq!(s.rejected_full, 1);
        assert_eq!(s.rejected_invalid, 1);
    }

    #[test]
    fn clone_shares_counters() {
        let m = Metrics::new();
        let m2 = m.clone();
        m.inc_accepted();
        assert_eq!(m2.snapshot().accepted, 1);
    }
}
