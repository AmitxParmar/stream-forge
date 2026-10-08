//! Phase 4 partitioning: per-key ordering through the sharded pipeline.
//!
//! Seam: the real `spawn_workers` distributor plus `InMemorySink`, driven
//! without HTTP so the test can push 10k+ sequenced events fast enough to
//! stress interleavings across shards.

use std::collections::HashMap;

use streamforge::event::{Event, RawEvent};
use streamforge::partitioner::Partitioner;
use streamforge::pipeline::BatchConfig;
use streamforge::sink::InMemorySink;
use streamforge::worker::{WorkerModel, WorkerPlan};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn event(user_id: u64, event_id: u64) -> Event {
    Event::parse(RawEvent {
        event_id,
        user_id,
        kind: "click".to_owned(),
        payload: serde_json::json!({ "seq": event_id }),
    })
    .unwrap()
}

/// Every user's events must land on exactly one shard (ADR-004).
#[test]
fn each_user_pins_to_exactly_one_shard() {
    let shards = 8;
    let partitioner = Partitioner::new(shards).unwrap();
    let mut seen: HashMap<u64, usize> = HashMap::new();
    for user_id in 0..50u64 {
        for event_id in 0..1_000u64 {
            let shard = partitioner.route(&event(user_id, event_id));
            assert_eq!(
                seen.insert(user_id, shard).unwrap_or(shard),
                shard,
                "user {user_id} moved shards"
            );
        }
    }
    // Routing is non-degenerate: every shard owns at least one user.
    let mut covered = vec![false; shards];
    for shard in seen.values() {
        covered[*shard] = true;
    }
    assert!(covered.iter().all(|c| *c), "empty shard: {covered:?}");
}

/// A hot key pins a single shard: the documented cost of modulo routing.
#[test]
fn single_hot_key_saturates_one_shard() {
    let shards = 8;
    let partitioner = Partitioner::new(shards).unwrap();
    let first = partitioner.route(&event(7, 0));
    for event_id in 1..10_000u64 {
        assert_eq!(partitioner.route(&event(7, event_id)), first);
    }
}

/// Per-key order survives the full fan-out under load: for every user, the
/// arrival subsequence in the sink must carry strictly increasing event ids.
#[tokio::test]
async fn per_key_order_holds_under_load() {
    const SHARDS: usize = 4;
    const USERS: u64 = 20;
    const PER_USER: u64 = 500;

    let (tx, rx) = mpsc::channel(1024);
    let sink = InMemorySink::default();
    let shutdown = CancellationToken::new();
    let group = streamforge::worker::spawn_workers(
        rx,
        sink.clone(),
        WorkerPlan {
            batch: BatchConfig {
                max_size: 64,
                timeout: std::time::Duration::from_millis(5),
                adaptive: true,
            },
            model: WorkerModel::Tokio,
            count: SHARDS,
            workload: streamforge::workload::Workload::default(),
        },
        shutdown,
        streamforge::Metrics::new(),
    );

    // Interleave users round-robin so consecutive same-user events are
    // separated by other keys — the shape that exposes cross-shard races.
    // Event ids increase per user: user u owns ids u*PER_USER..(u+1)*PER_USER.
    for round in 0..PER_USER {
        for user in 0..USERS {
            tx.send(event(user, user * PER_USER + round)).await.unwrap();
        }
    }
    drop(tx);
    group.wait().await.unwrap();

    let stored = sink.snapshot().await;
    assert_eq!(stored.len(), (USERS * PER_USER) as usize);

    let mut last_seen: HashMap<u64, u64> = HashMap::new();
    for e in &stored {
        let user = e.user_id.0;
        let id = e.id.0;
        if let Some(prev) = last_seen.insert(user, id) {
            assert!(
                id > prev,
                "user {user}: event {id} arrived after {prev} — order violated"
            );
        }
    }
    // Every user contributed its full sequence.
    for user in 0..USERS {
        assert_eq!(last_seen[&user], (user + 1) * PER_USER - 1);
    }
}
