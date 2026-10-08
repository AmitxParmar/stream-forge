//! Phase 4 partitioning: `hash(user_id) % N` routing to per-shard channels.
//!
//! One consumer owns each shard channel, so every event for a given `user_id`
//! traverses exactly one FIFO path: same key, same shard, same order. No
//! cross-worker locks are needed for per-key ordering (ADR-004).
//!
//! The hash below is an Fx-inspired 64-bit mixer over the raw `u64` key. It is
//! deliberately hand-rolled: the plan's `rustc-hash` suggestion would add a
//! dependency for ~10 lines of arithmetic, and `AGENTS.md` forbids new crates
//! without explicit user confirmation. Deterministic across runs and platforms.

use crate::error::AppError;
use crate::event::{Event, UserId};

/// Monotonic offset assigned to an event in arrival order.
///
/// The partitioner reserves this vocabulary in Phase 4; the WAL (Phase 5)
/// becomes the authority that stamps offsets durably. Ordering verification
/// in this phase uses per-key event sequences through the sharded pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

/// Routes events to shards by `hash(user_id) % shard_count`.
///
/// Construct once per worker group; `route` is a pure function with no shared
/// state, so the single distributor task is the only fan-out point.
#[derive(Debug, Clone)]
pub struct Partitioner {
    shard_count: usize,
}

impl Partitioner {
    /// Build a router for `shard_count` shards (must be `1..=64`).
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] when `shard_count` is zero or exceeds 64
    /// (matching the `worker_count` bound in [`crate::Config::validate`]).
    pub fn new(shard_count: usize) -> Result<Self, AppError> {
        if shard_count == 0 || shard_count > 64 {
            return Err(AppError::Config {
                reason: format!("shard_count must be between 1 and 64, got {shard_count}"),
            });
        }
        Ok(Self { shard_count })
    }

    /// Number of shards this router distributes over.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Route an event to its shard index in `[0, shard_count)`.
    ///
    /// Deterministic: the same `user_id` always maps to the same shard for a
    /// fixed `shard_count`. Borrowed input, no allocation, no locks.
    #[must_use]
    pub fn route(&self, event: &Event) -> usize {
        Self::route_user_to(event.user_id, self.shard_count)
    }

    /// Route a bare user id to its shard index in `[0, shard_count)`.
    #[must_use]
    pub fn route_user(&self, user: UserId) -> usize {
        Self::route_user_to(user, self.shard_count)
    }

    /// Round-robin fallback for keyless events (ADR-004).
    ///
    /// All Phase 4 events carry a `user_id`, so the distributor never calls
    /// this; it exists so a future keyless event type degrades to even
    /// spreading instead of a hot shard. The caller owns `next` (usually the
    /// distributor task), keeping this method lock-free.
    #[must_use]
    pub fn route_keyless(&self, next: &mut usize) -> usize {
        let idx = *next % self.shard_count;
        *next = (*next + 1) % self.shard_count;
        idx
    }

    fn route_user_to(user: UserId, shard_count: usize) -> usize {
        (hash_user_id(user) % shard_count as u64) as usize
    }

    /// Fx-inspired 64-bit mixer over the raw key.
    ///
    /// Multiply-permute keeps the low bits (the ones `% N` observes)
    /// bijective; the xor-shifts avalanche the high bits down so sequential
    /// user ids still scatter across shards.
    #[must_use]
    pub fn hash_user(user: UserId) -> u64 {
        hash_user_id(user)
    }
}

/// Hash a user id to 64 bits. Free function so unit tests can pin the
/// avalanche behaviour without constructing a [`Partitioner`].
#[must_use]
pub fn hash_user_id(user: UserId) -> u64 {
    const K: u64 = 0x51_7c_c1_b7_27_22_0a_95;
    let mut h = user.0.wrapping_add(K);
    h ^= h >> 33;
    h = h.wrapping_mul(K);
    h ^= h >> 29;
    h = h.wrapping_mul(K);
    h ^= h >> 32;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(user_id: u64, event_id: u64) -> Event {
        Event::parse(crate::event::RawEvent {
            event_id,
            user_id,
            kind: "click".to_owned(),
            payload: serde_json::json!({}),
        })
        .unwrap()
    }

    #[test]
    fn rejects_empty_and_oversize_shard_counts() {
        assert!(Partitioner::new(0).is_err());
        assert!(Partitioner::new(65).is_err());
        assert!(Partitioner::new(1).is_ok());
        assert!(Partitioner::new(64).is_ok());
    }

    #[test]
    fn same_user_always_maps_to_same_shard() {
        let a = Partitioner::new(8).unwrap();
        let b = Partitioner::new(8).unwrap();
        for user_id in 0..10_000 {
            let e = event(user_id, user_id);
            assert_eq!(a.route(&e), b.route(&e));
            assert_eq!(a.route(&e), a.route_user(UserId(user_id)));
        }
    }

    #[test]
    fn single_shard_routes_everything_to_zero() {
        let p = Partitioner::new(1).unwrap();
        for user_id in 0..1_000 {
            assert_eq!(p.route(&event(user_id, user_id)), 0);
        }
    }

    #[test]
    fn uniform_keys_spread_evenly() {
        let shards = 8;
        let p = Partitioner::new(shards).unwrap();
        let mut hits = vec![0usize; shards];
        let total = 80_000u64;
        for user_id in 0..total {
            hits[p.route(&event(user_id, user_id))] += 1;
        }
        let mean = total as f64 / shards as f64;
        for (i, h) in hits.iter().enumerate() {
            let ratio = *h as f64 / mean;
            assert!(
                (0.8..1.2).contains(&ratio),
                "shard {i} holds {h} of {total} (ratio {ratio:.3})"
            );
        }
    }

    #[test]
    fn keyless_fallback_cycles_round_robin() {
        let p = Partitioner::new(3).unwrap();
        let mut next = 0usize;
        let seq: Vec<usize> = (0..7).map(|_| p.route_keyless(&mut next)).collect();
        assert_eq!(seq, vec![0, 1, 2, 0, 1, 2, 0]);
    }
}
