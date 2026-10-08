//! Synthetic batch kernels used to shape the Phase 3 concurrency comparison.
//!
//! The same batch either sleeps for `io` microseconds per event (IO-bound) or
//! burns deterministic CPU per event (`cpu` iterations). Phase 3 runs the
//! kernel on one of three execution models so the boundary where async stops
//! helping becomes measurable (see `docs/performance.md` §P3).

use std::hint::black_box;
use std::time::Duration;

use crate::error::AppError;
use crate::event::Event;

/// Work applied to each batch before it reaches the sink.
#[derive(Debug, Clone, Default)]
pub struct Workload {
    /// Simulated IO wait per event.
    pub io: Duration,
    /// CPU iterations per event.
    pub cpu: usize,
}

impl Workload {
    /// Build from CLI/config microsecond and iteration knobs.
    #[must_use]
    pub fn new(io_us: u64, cpu_iters: usize) -> Self {
        Self {
            io: Duration::from_micros(io_us),
            cpu: cpu_iters,
        }
    }

    /// Whether any work is configured (both knobs zero = pass-through).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.io.is_zero() && self.cpu == 0
    }

    /// Run the kernel over a batch, returning it unchanged.
    ///
    /// # Errors
    ///
    /// Never fails in Phase 3; the signature is future-proofing for Phase 5+.
    pub async fn process(&self, batch: Vec<Event>) -> Result<Vec<Event>, AppError> {
        if !self.io.is_zero() {
            tokio::time::sleep(self.io.saturating_mul(batch.len() as u32)).await;
        }
        for event in &batch {
            burn(self.cpu, event);
        }
        Ok(batch)
    }
}

/// Deterministic CPU burn over an event payload.
///
/// Mixes a stable seed with the payload bytes and iterates enough to be
/// measurable; `black_box` keeps the optimizer from deleting the loop.
fn burn(iters: usize, event: &Event) {
    let mut acc: u64 = 0x9E37_79B9_7F4A_7C15;
    for round in 0..iters {
        let payload = serde_json::to_vec(&event.payload).unwrap_or_default();
        for (i, byte) in payload.iter().enumerate() {
            acc ^= u64::from(*byte) << ((i + round) % 56);
        }
        acc = acc.wrapping_mul(6364136223846793005).wrapping_add(1);
        acc ^= acc >> 29;
        black_box(acc);
    }
    black_box(acc);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event() -> Event {
        Event::parse(crate::event::RawEvent {
            event_id: 1,
            user_id: 2,
            kind: "click".to_owned(),
            payload: json!({ "page": "/home", "ts": 123 }),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn empty_workload_passes_through() {
        let events = vec![event(), event()];
        let out = Workload::default().process(events).await.unwrap();
        assert_eq!(out.len(), 2);
    }

    #[tokio::test]
    async fn io_workload_waits_per_event() {
        let events = vec![event(), event()];
        let started = std::time::Instant::now();
        Workload::new(40_000, 0).process(events).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(80));
    }
}
