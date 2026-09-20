# StreamForge — Phases 2–9 Architectural Specifications

Companion to [phase-01-plan.md](phase-01-plan.md) (v2.0). Each phase below follows the
same structure: architecture diagram, trait/type contracts, concurrency model, error
extensions, memory impact, work breakdown, and verification plan.

Phases build on each other — do not reorder. Cross-references to PRD, ADRs, and
`performance.md` are inline.

---
---

# Phase 2 — Backpressure & Load Shedding

**Lights up:** Gateway → Queue (deliberately overloaded)
**PRD:** FR-4 · **ADR:** ADR-007 · **Perf row:** P2

## P2.1 Architecture Overview

```
  ┌──────────┐      ┌──────────────────────────────────────────┐
  │  HTTP    │ POST │  Gateway (axum)                          │
  │  Client  │─────►│                                          │
  └──────────┘      │  try_send() ──ok──► mpsc(K) ──► Worker   │
                    │       │                                   │
                    │      full?                                │
                    │       │                                   │
                    │   timeout(T) ──► send().await ──ok──►     │
                    │       │                                   │
                    │   still full ──► 429 + Retry-After        │
                    │                  + rejected_total.inc()   │
                    │                                           │
                    │  Metrics handle ──► depth gauge           │
                    │                 ──► accepted/rejected     │
                    │                 ──► batch_size histogram  │
                    │                                           │
                    │  Worker: adaptive batch sizing            │
                    │    depth < 30%  → batch *= 2 (amortize)   │
                    │    depth > 70%  → batch /= 2 (drain fast) │
                    └───────────────────────────────────────────┘
```

## P2.2 Trait & Type Contracts

```rust
/// Lightweight metrics handle threaded through all stages.
/// Phase 2: in-memory counters via tracing events.
/// Phase 9: swapped to Prometheus exporter (backend change, not API change).
pub struct Metrics {
    accepted: AtomicU64,
    rejected: AtomicU64,
    queue_depth: fn() -> usize,  // closure over channel len
}

impl Metrics {
    pub fn record_accepted(&self);
    pub fn record_rejected(&self);
    pub fn queue_depth(&self) -> usize;
    pub fn queue_pressure(&self, capacity: usize) -> f64;  // depth / capacity
}
```

**No new traits.** The `Sink` trait is unchanged. `Metrics` is a concrete struct (not a trait)
because there is exactly one implementation until Phase 9; introducing a trait now would be
premature abstraction.

## P2.3 Concurrency Model Changes

| Component | Phase 1 | Phase 2 |
|---|---|---|
| Gateway send | `tx.send(event).await` (blocks forever) | `tx.try_send()` → timeout → 429 |
| Batch sizing | Fixed `max_size` from Config | Adaptive: `[min_batch..max_batch]` keyed on `Metrics::queue_pressure()` |
| Metrics | None | `AtomicU64` counters; no lock, no allocation |

**Backpressure stages (ordered):**
1. `try_send()` succeeds → 202 (happy path, zero overhead)
2. `try_send()` fails → `timeout(Duration, send())` → 202 if slot opens (natural backpressure)
3. Timeout expires → 429 + `Retry-After` header + `metrics.record_rejected()` (explicit shed)

## P2.4 Error Domain Extensions

No new `AppError` variants. `ChannelFull` already exists — it is now wired to the 429 path:

```rust
// gateway.rs — Phase 2 send path
match state.tx.try_send(event) {
    Ok(()) => { metrics.record_accepted(); 202 }
    Err(TrySendError::Full(_)) => {
        match tokio::time::timeout(shed_timeout, state.tx.send(event)).await {
            Ok(Ok(())) => { metrics.record_accepted(); 202 }
            Ok(Err(_)) => 503,         // ChannelClosed
            Err(_) => {                // timeout elapsed
                metrics.record_rejected();
                429 + Retry-After
            }
        }
    }
    Err(TrySendError::Closed(_)) => 503,  // ChannelClosed
}
```

## P2.5 Memory Impact

| Addition | Size | Bounded? |
|---|---|---|
| `Metrics` struct | 24 bytes (3 × AtomicU64) | Yes — fixed |
| Adaptive batch bounds | 2 × `usize` in BatchConfig | Yes — config |
| No new buffers | — | — |

Total hot-path delta: negligible.

## P2.6 Work Breakdown

- [ ] **P2-T01** — `src/metrics.rs` (new): depth gauge, accepted/rejected counters, batch histogram
  - **Files:** `src/metrics.rs` (new), `src/lib.rs` (add `pub mod metrics`)
  - **Acceptance:** Counters are `AtomicU64`; no locks; `queue_pressure()` returns `f64` in `[0.0, 1.0]`
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings`

- [ ] **P2-T02** — Gateway: `try_send` + timeout fallback → 429 + `Retry-After`
  - **Files:** `src/gateway.rs`, `src/config.rs` (add `shed_timeout: Duration`)
  - **Acceptance:** Full channel returns 429 with JSON body `{"error":"overloaded","retryAfter":N}`; `rejected_total` counter increments; no `unwrap()`
  - **Verify:** `cargo test --test backpressure`

- [ ] **P2-T03** — Pipeline: adaptive batch sizing keyed on queue depth
  - **Files:** `src/pipeline.rs`, `src/config.rs` (add `min_batch_size`, `max_batch_size`)
  - **Acceptance:** Batch size doubles when `pressure < 0.3`, halves when `pressure > 0.7`; clamped to `[min, max]`
  - **Verify:** Unit test asserting batch bounds under simulated pressure

- [ ] **P2-T04** — Loadgen: `--slow-sink 2x` injection + `--report queue-depth.csv`
  - **Files:** `src/bin/loadgen.rs`
  - **Acceptance:** Wraps sink with configurable latency injection; CSV with columns: `timestamp,depth,accepted,rejected`
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings`

- [ ] **P2-T05** — `tests/backpressure.rs` (new): shed semantics, bounded memory, batch bounds
  - **Files:** `tests/backpressure.rs` (new)
  - **Acceptance:** (a) Full channel → 429; (b) Memory stays bounded under stuck consumer; (c) Adaptive batch within `[min, max]`
  - **Verify:** `cargo test --test backpressure`

- [ ] **P2-T06** — 2× overload drill + results
  - **Files:** `docs/performance-results.md` (new or append)
  - **Acceptance:** Queue-depth trace before/after adaptive batching; accepted/rejected counts; P99 of accepted events bounded
  - **Verify:** Reproducible with `cargo run --release -- bench --rate 150000 --slow-sink 2x`

- [ ] **P2-T07** — Verification gate green
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 3 — Parallel Processing

**Lights up:** Queue → Workers 1..N (round-robin, model-swappable)
**PRD:** FR-10 · **ADR:** ADR-002 · **Perf row:** P3

## P3.1 Architecture Overview

```
  mpsc(K) ──► Dispatcher ──► Worker[0]  (tokio::spawn)
                          ──► Worker[1]  (std::thread)
                          ──► Worker[N]  (rayon::spawn)
                               │
                            ┌──┴──────────────────────┐
                            │ WorkerModel trait        │
                            │   fn spawn(task) → Join  │
                            │   fn name() → &str       │
                            └─────────────────────────┘

  --model tokio    → TokioModel  (async tasks, IO-bound sweet spot)
  --model threads  → ThreadModel (OS threads, CPU-bound sweet spot)
  --model rayon    → RayonModel  (work-stealing pool, parallel-for sweet spot)
```

## P3.2 Trait & Type Contracts

```rust
/// Concurrency model abstraction. Each model spawns workers differently.
/// Static dispatch: `run_pipeline::<M: WorkerModel>(...)`.
pub trait WorkerModel: Send + Sync + 'static {
    /// Opaque join handle returned by `spawn`.
    type JoinHandle: Send;

    /// Spawn a worker task/thread that processes batches from `rx`.
    fn spawn<S: Sink>(
        &self,
        id: usize,
        rx: mpsc::Receiver<Event>,
        sink: S,
        batch: BatchConfig,
        shutdown: CancellationToken,
    ) -> Self::JoinHandle;

    /// Wait for the worker to complete.
    fn join(handle: Self::JoinHandle) -> Result<(), AppError>;

    /// Human-readable name for benchmark tables.
    fn name(&self) -> &'static str;
}

pub struct TokioModel;    // impl WorkerModel — uses tokio::spawn
pub struct ThreadModel;   // impl WorkerModel — uses std::thread::spawn
pub struct RayonModel;    // impl WorkerModel — uses rayon::spawn

/// New config field
pub struct Config {
    // ... existing ...
    pub worker_count: usize,          // default: num_cpus::get()
    pub worker_model: WorkerModelKind, // enum { Tokio, Threads, Rayon }
}
```

**Dispatch:** Static (`M: WorkerModel`). The model is selected once at startup via CLI `--model`,
not at runtime per-request. No vtable overhead in the worker loop.

## P3.3 Concurrency Model Changes

| Model | Spawn | Bridge to async sink | CPU-bound work |
|---|---|---|---|
| `TokioModel` | `tokio::spawn` | Native — sink is async | Must not block the runtime; use `spawn_blocking` for CPU work |
| `ThreadModel` | `std::thread::spawn` | `tokio::runtime::Handle::block_on(sink.write_batch())` | Natural — OS thread |
| `RayonModel` | `rayon::spawn` | `Handle::block_on` or channel back to Tokio | Work-stealing for CPU-bound; avoid async inside pool |

**Dispatcher** (round-robin initially): reads from the main `mpsc(K)` channel and distributes
to `N` per-worker channels. Phase 4 replaces round-robin with `hash(user_id) % N`.

## P3.4 Error Domain Extensions

```rust
pub enum AppError {
    // ... existing ...

    /// A spawned worker task/thread panicked or returned an error.
    #[error("worker {id} failed: {reason}")]
    WorkerFailed { id: usize, reason: String },
}
```

## P3.5 Memory Impact

| Addition | Size | Bounded? |
|---|---|---|
| N per-worker channels | `N × cap × sizeof(Event)` | Yes — bounded per ADR-007 |
| N batch buffers | `N × max_size × sizeof(Event)` | Yes — one per worker |
| Rayon thread pool | Configurable thread count | Yes — `num_cpus` default |

Total: ~`N × 1.3 MB` (scales linearly with worker count). At N=8, ~10.4 MB.

## P3.6 Work Breakdown

- [ ] **P3-T01** — `WorkerModel` trait + `TokioModel` implementation
  - **Files:** `src/pipeline.rs` (refactor `spawn_worker` into trait)
  - **Acceptance:** Existing P1 tests pass with `TokioModel`; `spawn_worker` is now `WorkerModel::spawn`
  - **Verify:** `cargo test --test ingest`

- [ ] **P3-T02** — `ThreadModel` + `RayonModel` implementations
  - **Files:** `src/pipeline.rs`, `Cargo.toml` (add `rayon` — requires user confirmation)
  - **Acceptance:** All three models pass the same worker test; `name()` returns the model name
  - **Verify:** `cargo test`

- [ ] **P3-T03** — Round-robin dispatcher for N workers
  - **Files:** `src/pipeline.rs`, `src/config.rs` (add `worker_count`)
  - **Acceptance:** Events distributed evenly across N workers; all workers flush on shutdown
  - **Verify:** Integration test with N=4 workers, verify all receive events

- [ ] **P3-T04** — CLI `--model tokio|threads|rayon` + `--workers N`
  - **Files:** `src/main.rs`, `src/config.rs`
  - **Acceptance:** `clap` enum for model selection; `N` defaults to `num_cpus::get()`
  - **Verify:** `cargo run -- --model rayon --workers 4 --help`

- [ ] **P3-T05** — CPU-bound vs IO-bound workload toggles in loadgen
  - **Files:** `src/bin/loadgen.rs`
  - **Acceptance:** `--workload cpu` (simulated parse/hash) vs `--workload io` (simulated disk/sleep)
  - **Verify:** `cargo check`

- [ ] **P3-T06** — Benchmark: 6 combinations (2 workloads × 3 models) × worker sweep
  - **Files:** `docs/performance-results.md`
  - **Acceptance:** Table with throughput at workers=1,2,4,8,N_cores for each combo; crossover point documented
  - **Verify:** Reproducible commands in results doc

- [ ] **P3-T07** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 4 — Partitioning

**Lights up:** Dispatcher → Partitioner (`hash(key) % N`) → Sharded Workers
**PRD:** FR-9 · **ADR:** ADR-004 · **Perf row:** P4

## P4.1 Architecture Overview

```
  mpsc(K) ──► Partitioner ──hash(user_id) % N──► shard_channel[0] ──► Worker 0
                                                 ──► shard_channel[1] ──► Worker 1
                                                 ──► shard_channel[N] ──► Worker N

  Keyless events ──► round-robin fallback

  Per-shard guarantee:
    Same user_id → same shard → same worker → per-key ordering (no cross-shard locks)
```

## P4.2 Trait & Type Contracts

```rust
/// Event router. Determines which shard an event belongs to.
/// Static dispatch: single implementation in Phase 4.
pub struct Partitioner {
    shard_count: usize,
}

impl Partitioner {
    /// Route an event to a shard index in [0, shard_count).
    /// Uses FxHash for speed (no cryptographic requirement).
    #[must_use]
    pub fn route(&self, event: &Event) -> usize {
        // hash(event.user_id.0) % self.shard_count
    }
}

/// Monotonic offset assigned by the partitioner (or WAL in Phase 5).
/// Used for per-key ordering verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);
```

**No trait for Partitioner** in Phase 4 — there is one routing strategy. If consistent hashing
is ever needed (ADR-008 explicitly defers this), the struct can refactor into a trait.

## P4.3 Concurrency Model

- **Partitioner task**: Single Tokio task reading from `mpsc(K)`, computing shard index, sending
  to `shard_channels[i]`. This is the only point of fan-out.
- **Per-shard channels**: `mpsc::channel(cap=K/N)` — total channel memory stays ~constant vs Phase 1.
- **Workers**: One per shard, same `WorkerModel` from Phase 3. Each worker owns its shard channel
  receiver exclusively (shared-nothing).

## P4.4 Error Domain Extensions

```rust
pub enum AppError {
    // ... existing ...

    /// Partitioner detected an invalid shard index (defensive; should not happen).
    #[error("routing error: shard index {index} out of range [0, {count})")]
    RoutingError { index: usize, count: usize },
}
```

## P4.5 Work Breakdown

- [ ] **P4-T01** — `Partitioner` struct + `route()` with FxHash
  - **Files:** `src/pipeline.rs` (or new `src/partitioner.rs`), `Cargo.toml` (add `rustc-hash` — user confirmation)
  - **Acceptance:** Deterministic: same `user_id` always maps to same shard; unit test with 10k events
  - **Verify:** `cargo test`

- [ ] **P4-T02** — Replace round-robin dispatcher with partitioner
  - **Files:** `src/pipeline.rs`, `src/lib.rs`
  - **Acceptance:** Per-shard channels; partitioner routes by `hash(user_id) % N`; keyless events round-robin
  - **Verify:** Integration test: send 1000 events for 10 users, verify each user's events land on one shard

- [ ] **P4-T03** — Per-key ordering verification under load
  - **Files:** `tests/ordering.rs` (new)
  - **Acceptance:** Monotonic offset per user_id at 100k events/s; test fails if ordering violated
  - **Verify:** `cargo test --test ordering`

- [ ] **P4-T04** — Zipf skew test + per-shard lag metrics
  - **Files:** `src/bin/loadgen.rs` (add `--skew` flag), `src/metrics.rs`
  - **Acceptance:** Zipf-distributed keys with s=1.5; per-shard depth gauge shows hot-shard saturation
  - **Verify:** Drill produces per-shard lag CSV; hot shard documented

- [ ] **P4-T05** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 5 — Persistent Log (WAL)

**Lights up:** Queue → WAL (append, fsync, segment rotation)
**PRD:** FR-5..FR-7 · **ADR:** ADR-003 · **Perf row:** P5

## P5.1 Architecture Overview

```
  Gateway ──► mpsc(K) ──► WAL Writer (single task)
                              │
                              ▼
              ┌──────────────────────────────────┐
              │  wal/                            │
              │  ├── 00000000000001.log           │
              │  ├── 00000000000042.log           │
              │  └── active.log                   │
              │                                   │
              │  Record format:                   │
              │  [crc32:4][len:4][payload:len]    │
              │                                   │
              │  Offset = file_base + byte_pos   │
              │  fsync policy: record|batch|Nms  │
              │  Rotation: when segment > S MB   │
              └──────────────────────────────────┘
                              │
                              ▼
              Partitioner reads from WAL (live tail / replay)
```

## P5.2 Trait & Type Contracts

```rust
/// Write-ahead log for durable event storage.
/// Single writer, many readers. NOT a trait — one implementation, hand-rolled (ADR-003).
pub struct Wal {
    dir: PathBuf,
    active_segment: BufWriter<File>,
    active_base_offset: u64,
    segment_size_limit: u64,
    fsync_policy: FsyncPolicy,
    next_offset: AtomicU64,
}

/// Fsync policy — controls durability/throughput tradeoff.
#[derive(Debug, Clone)]
pub enum FsyncPolicy {
    /// fsync after every record (safest, slowest).
    PerRecord,
    /// fsync after every batch flush (balanced).
    PerBatch,
    /// fsync at most every N milliseconds (fastest, small loss window).
    Interval(Duration),
}

/// A single WAL record with its assigned offset.
pub struct WalRecord {
    pub offset: Offset,
    pub crc: u32,
    pub payload: Vec<u8>,  // serialized Event
}

impl Wal {
    /// Append a batch of events, assign offsets, fsync per policy.
    /// Returns the offset range [first, last] of the appended batch.
    pub fn append_batch(&mut self, events: &[Event]) -> Result<(Offset, Offset), WalError>;

    /// Read records starting from `offset` (for replay).
    pub fn read_from(&self, offset: Offset) -> Result<WalIterator, WalError>;

    /// Rotate the active segment if it exceeds the size limit.
    fn maybe_rotate(&mut self) -> Result<(), WalError>;

    /// Validate and recover: truncate trailing corrupt records.
    pub fn recover(dir: &Path) -> Result<Wal, WalError>;
}

/// WAL-specific errors (separate from SinkError).
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum WalError {
    #[error("wal io error")]
    Io(#[from] std::io::Error),

    #[error("crc mismatch at offset {offset}: expected {expected}, got {actual}")]
    CorruptRecord { offset: u64, expected: u32, actual: u32 },

    #[error("segment rotation failed")]
    RotationFailed(#[source] std::io::Error),
}
```

## P5.3 Concurrency Model

- **Single WAL writer task**: Receives batches from the pipeline channel, appends to disk,
  fsyncs per policy. This is the serialization point — all writes go through one task.
- **WAL reader (partitioner)**: Reads from the last committed offset. In steady state, this
  is a live tail (reads what the writer appends). On recovery, it replays from the checkpoint.
- **No cross-thread file access**: Writer owns the `File` handle; readers open read-only handles
  to sealed segments (no lock contention).

## P5.4 Error Domain Extensions

```rust
pub enum AppError {
    // ... existing ...

    /// WAL operation failed.
    #[error("wal error")]
    Wal(#[source] WalError),
}
```

## P5.5 Memory Impact

| Addition | Size | Bounded? |
|---|---|---|
| `BufWriter` buffer | 8 KB default | Yes |
| Segment read buffer | 8 KB per reader | Yes |
| Serialized event batch | `batch_size × ~1KB` | Yes — matches pipeline buffer |

WAL is disk-backed; memory footprint is I/O buffer only (~16 KB).

## P5.6 Work Breakdown

- [ ] **P5-T01** — `src/wal.rs` (new): record format, append, read, offset assignment
  - **Acceptance:** `[crc32:4][len:4][payload:len]` format; sequential append; monotonic offsets
  - **Verify:** Unit test: write 1000 records, read them back, verify offsets and CRC

- [ ] **P5-T02** — Segment rotation at size threshold
  - **Acceptance:** Active segment sealed when `> segment_size_limit`; new segment opened with next offset
  - **Verify:** Unit test: write past limit, verify two segment files exist

- [ ] **P5-T03** — Fsync policy (`record | batch | interval:Nms`)
  - **Files:** `src/wal.rs`, `src/config.rs`
  - **Acceptance:** Three policies; `PerBatch` is default; CLI `--fsync` flag
  - **Verify:** `cargo test`

- [ ] **P5-T04** — Torn-write recovery (truncate at last good CRC)
  - **Acceptance:** Simulated torn write (truncate file mid-record); `Wal::recover()` truncates to last good record
  - **Verify:** Unit test with injected corruption

- [ ] **P5-T05** — Integrate WAL into pipeline (write before process)
  - **Files:** `src/lib.rs`, `src/pipeline.rs`
  - **Acceptance:** Events written to WAL before partitioner reads them; pipeline reads from WAL, not channel directly
  - **Verify:** `cargo test --test ingest` (updated)

- [ ] **P5-T06** — fsync-ladder benchmark
  - **Acceptance:** events/s at `record` vs `batch` vs `interval:100ms`; throughput curve plotted
  - **Verify:** Reproducible with `cargo run --release -- bench --rate 100000 --fsync <policy>`

- [ ] **P5-T07** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 6 — Crash Recovery

**Lights up:** WAL ↔ Recovery ↔ Partitioner (replay path)
**PRD:** FR-8, FR-18, FR-19 · **ADR:** ADR-003, ADR-005 · **Perf row:** P6

## P6.1 Architecture Overview

```
  Process starts
       │
       ▼
  Wal::recover(dir)
  ├── list segments, validate CRC per record
  ├── truncate torn tail records
       │
       ▼
  Read checkpoints from PostgreSQL
  ├── HashMap<ShardId, Offset>  (last committed offset per shard)
       │
       ▼
  Replay: for each WAL record with offset > committed_offset[shard]
  ├── Partitioner routes to shard
  ├── Worker processes event
  ├── Dedup filter: skip if event_id already in seen_set
       │
       ▼
  Resume live tail → steady state
```

## P6.2 Trait & Type Contracts

```rust
/// Checkpoint: persisted processing state per shard.
pub struct Checkpoint {
    pub shard_id: usize,
    pub committed_offset: Offset,
    pub dedup_state: DedupSnapshot,  // serialized bloom/LRU
    pub timestamp: SystemTime,
}

/// Dedup filter — per-shard, bounded memory.
pub struct DedupFilter {
    seen: LruCache<EventId, ()>,  // bounded LRU; size from Config
}

impl DedupFilter {
    /// Returns `true` if this event_id has NOT been seen (i.e., should process).
    /// Returns `false` if duplicate (skip).
    pub fn check_and_insert(&mut self, id: EventId) -> bool;

    /// Serialize for checkpointing.
    pub fn snapshot(&self) -> DedupSnapshot;

    /// Restore from checkpoint.
    pub fn restore(snapshot: DedupSnapshot) -> Self;
}

/// Checkpoint manager — writes to PostgreSQL at configurable intervals.
pub struct CheckpointManager {
    pool: PgPool,
    interval: Duration,
}

impl CheckpointManager {
    /// Persist all shard checkpoints atomically (single transaction).
    pub async fn save(&self, checkpoints: &[Checkpoint]) -> Result<(), AppError>;

    /// Load the latest checkpoint for each shard.
    pub async fn load(&self) -> Result<Vec<Checkpoint>, AppError>;
}
```

## P6.3 Error Domain Extensions

```rust
pub enum AppError {
    // ... existing ...

    /// Checkpoint read/write failed.
    #[error("checkpoint error: {reason}")]
    Checkpoint { reason: String },

    /// WAL replay detected unrecoverable corruption.
    #[error("recovery failed: {reason}")]
    Recovery { reason: String },
}
```

## P6.4 Work Breakdown

- [ ] **P6-T01** — `DedupFilter` with bounded LRU cache
  - **Files:** `src/pipeline.rs` (or new `src/dedup.rs`)
  - **Acceptance:** LRU with configurable capacity; `check_and_insert` is O(1); snapshot/restore round-trips
  - **Verify:** Unit test: insert N+1 items with capacity N, verify oldest evicted

- [ ] **P6-T02** — `CheckpointManager` — Postgres read/write
  - **Files:** `src/checkpoint.rs` (new)
  - **Acceptance:** Atomic save of all shard checkpoints; load returns latest per shard; no `unwrap()`
  - **Verify:** `sqlx::test` with test database

- [ ] **P6-T03** — Recovery sequence: WAL scan → checkpoint load → replay
  - **Files:** `src/lib.rs` (startup path)
  - **Acceptance:** On startup, skips events before committed offset; dedup filters replayed duplicates
  - **Verify:** Integration test: write 1000 events, checkpoint at 500, restart, verify 501-1000 replayed

- [ ] **P6-T04** — `crash-test` harness: 10 consecutive `kill -9` runs
  - **Files:** `src/bin/crashtest.rs` (new) or `tests/crash.rs`
  - **Acceptance:** All 10 runs: zero loss (count parity), zero duplicate effects, recovery < 10s for 10M-event WAL
  - **Verify:** `cargo run --release -- crash-test --runs 10 --wal-events 10000000`

- [ ] **P6-T05** — Checkpoint overhead measurement
  - **Acceptance:** events/s with checkpointing on vs off; overhead documented
  - **Verify:** Results appended to `docs/performance-results.md`

- [ ] **P6-T06** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 7 — Real-Time Analytics (Windowing + Dedup)

**Lights up:** Workers → Windowing + Dedup aggregator
**PRD:** FR-11..FR-13 · **ADR:** ADR-005 · **Perf row:** P7

## P7.1 Architecture Overview

```
  Worker (per-shard)
       │
       ▼
  ┌───────────────────────────────────────────┐
  │  Windowing Engine (per-shard, local)      │
  │                                            │
  │  event ──► watermark check                 │
  │              │ yes             │ no         │
  │              ▼                ▼            │
  │         active windows    late-event       │
  │         ├── tumbling      policy:          │
  │         │   [start, end)  ├── drop         │
  │         ├── sliding       ├── side-output  │
  │         │   ring buffer   └── recompute    │
  │         │                                  │
  │         ▼                                  │
  │    aggregations:                           │
  │    ├── count (u64)                         │
  │    ├── sum (f64)                           │
  │    ├── avg (sum/count)                     │
  │    └── percentiles (P² sketch)             │
  │                                            │
  │  window closes ──► emit Aggregate          │
  └────────────────────┬──────────────────────┘
                       │
                       ▼
                  Sink Router
```

## P7.2 Trait & Type Contracts

```rust
/// Time boundary for windowed aggregations.
#[derive(Debug, Clone, Copy)]
pub struct WindowBounds {
    pub start: SystemTime,  // inclusive
    pub end: SystemTime,    // exclusive
}

/// Emitted when a window closes.
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub window: WindowBounds,
    pub shard_id: usize,
    pub kind: String,          // event kind this window covers
    pub count: u64,
    pub sum: f64,
    pub avg: f64,
    pub p50: f64,
    pub p99: f64,
}

/// Window type — tumbling (fixed, non-overlapping) or sliding (overlapping).
#[derive(Debug, Clone)]
pub enum WindowType {
    Tumbling { duration: Duration },
    Sliding { duration: Duration, slide: Duration },
}

/// Policy for events arriving after the watermark.
#[derive(Debug, Clone)]
pub enum LateEventPolicy {
    Drop,
    SideOutput,   // route to a separate sink/log
    Recompute,    // re-open the window and update
}

/// Streaming percentile estimator (P² algorithm).
/// Fixed memory: 5 markers per quantile tracked.
pub struct P2Sketch { /* 5 markers × sizeof(f64) = 40 bytes per quantile */ }

impl P2Sketch {
    pub fn observe(&mut self, value: f64);
    pub fn quantile(&self, q: f64) -> f64;  // q in [0.0, 1.0]
}

/// Per-shard windowing engine. Owns window state + watermark.
pub struct WindowEngine {
    window_type: WindowType,
    late_policy: LateEventPolicy,
    watermark: SystemTime,
    watermark_slack: Duration,  // configurable tolerance
    active_windows: BTreeMap<WindowBounds, WindowState>,
}

impl WindowEngine {
    /// Process one event: update watermark, route to window, check for closes.
    pub fn process(&mut self, event: &Event) -> Vec<Aggregate>;
}
```

## P7.3 Memory Impact

| Addition | Per-shard size | Bounded? |
|---|---|---|
| Active tumbling windows | 1–2 windows × ~100 bytes | Yes — windows close |
| Active sliding windows | `duration/slide` windows × ~100 bytes | Yes — bounded by config |
| P² sketch per window | 40 bytes per quantile | Yes — fixed |
| Dedup LRU (from P6) | Configurable capacity | Yes |

Per-shard overhead: < 10 KB for typical configurations. At N=8 shards: < 80 KB total.

## P7.4 Work Breakdown

- [ ] **P7-T01** — `src/window.rs` (new): `WindowEngine`, `WindowBounds`, `Aggregate`
  - **Acceptance:** Tumbling windows with count/sum/avg; emit on close; watermark advances monotonically
  - **Verify:** Unit test: 100 events in 10-second tumbling window, verify aggregate values

- [ ] **P7-T02** — `P2Sketch` for streaming percentiles
  - **Acceptance:** P50/P99 within 5% of exact sort-based computation on 10k samples
  - **Verify:** Property test comparing P² vs exact sort

- [ ] **P7-T03** — Sliding windows with ring buffer
  - **Acceptance:** Overlapping windows emit correctly; ring buffer bounds memory
  - **Verify:** Unit test: sliding(10s, 5s) — verify two windows cover the overlap

- [ ] **P7-T04** — Late-event policy (drop / side-output / recompute)
  - **Acceptance:** Configurable via `Config`; drop silently discards (counted in metrics); side-output routes to a separate channel
  - **Verify:** Unit test: inject late event, verify policy is applied

- [ ] **P7-T05** — Window state checkpointing (integrate with P6 checkpoints)
  - **Files:** `src/window.rs`, `src/checkpoint.rs`
  - **Acceptance:** Window state serialized alongside shard offset; recovery resumes mid-window
  - **Verify:** Integration test: checkpoint mid-window, restart, verify aggregate continuity

- [ ] **P7-T06** — Correctness oracle: windowed aggregate vs batch-computed reference
  - **Acceptance:** Property test: random event stream → windowed aggregate == batch aggregate
  - **Verify:** `cargo test`

- [ ] **P7-T07** — Benchmark: events/s with windowing on vs off
  - **Acceptance:** Overhead quantified; late-event rate vs watermark slack tunable
  - **Verify:** Results appended to `docs/performance-results.md`

- [ ] **P7-T08** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 8 — Storage Optimization

**Lights up:** Aggregator → Postgres / Parquet / Redis (tiered sinks)
**PRD:** FR-14..FR-17 · **ADR:** ADR-006, ADR-009 · **Perf row:** P8

## P8.1 Architecture Overview

```
  Aggregate / Event
       │
       ▼
  ┌──────────────┐
  │  Codec trait  │──► JSON (default, existing)
  │  encode()     │──► MessagePack (rmp-serde)
  │  decode()     │──► Protobuf (prost)
  └──────┬───────┘
         │ encoded bytes
         ▼
  ┌──────────────────────────────────────────┐
  │  SinkRouter                              │
  │  ├── PostgresSink  (state, checkpoints)  │
  │  ├── ParquetSink   (analytics, history)  │
  │  └── RedisSink     (hot cache, TTL)      │
  │       └── fallback → PostgresSink        │
  └──────────────────────────────────────────┘
```

## P8.2 Trait & Type Contracts

```rust
/// Codec abstraction — separates serialization from transport.
/// Static dispatch for the hot path; selected at startup via Config.
pub trait Codec: Send + Sync + 'static {
    /// Encode an event to bytes.
    fn encode(&self, event: &Event) -> Result<Vec<u8>, CodecError>;

    /// Decode bytes back to an event.
    fn decode(&self, bytes: &[u8]) -> Result<Event, CodecError>;

    /// Human-readable name for benchmark tables.
    fn name(&self) -> &'static str;
}

pub struct JsonCodec;       // impl Codec — serde_json
pub struct MsgPackCodec;    // impl Codec — rmp-serde
pub struct ProtobufCodec;   // impl Codec — prost

/// Codec-specific errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CodecError {
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}

/// PostgresSink — transactional state and checkpoints.
pub struct PostgresSink {
    pool: PgPool,
}

/// ParquetSink — columnar analytics storage.
pub struct ParquetSink {
    writer: ArrowWriter<File>,
    output_dir: PathBuf,
    row_group_size: usize,
}

/// RedisSink — hot cache with Postgres fall-through.
pub struct RedisSink {
    redis: redis::Client,
    fallback: PostgresSink,
    circuit_breaker: CircuitBreaker,
    ttl: Duration,
}

/// Circuit breaker for sink resilience.
pub struct CircuitBreaker {
    state: AtomicU8,  // Closed=0, Open=1, HalfOpen=2
    failure_count: AtomicU32,
    threshold: u32,
    reset_timeout: Duration,
}
```

**Dispatch decision for Phase 8:** The `Sink` trait transitions to `Box<dyn Sink>` for the
sink router (which dispatches to Postgres/Parquet/Redis at runtime based on event type).
The `Codec` trait stays static dispatch (selected once at startup). This is the Phase 8
`dyn` transition documented in Phase 1's plan.

## P8.3 Error Domain Extensions

```rust
pub enum AppError {
    // ... existing ...
    #[error("codec error")]
    Codec(#[source] CodecError),
}

pub enum SinkError {
    // ... existing ...
    #[error("redis error: {0}")]
    Redis(String),
    #[error("parquet error: {0}")]
    Parquet(String),
    #[error("circuit breaker open for sink: {0}")]
    CircuitOpen(String),
}
```

## P8.4 New Crate Dependencies (all require user confirmation)

| Crate | Purpose | Phase |
|---|---|---|
| `rmp-serde` | MessagePack codec | P8 |
| `prost` + `prost-build` | Protobuf codec | P8 |
| `arrow` + `parquet` | Parquet writer | P8 |
| `redis` | Redis client | P8 |

## P8.5 Work Breakdown

- [ ] **P8-T01** — `Codec` trait + `JsonCodec` (baseline)
  - **Files:** `src/codec.rs` (new)
  - **Acceptance:** Round-trip: `decode(encode(event)) == event`; JSON uses existing `serde_json`
  - **Verify:** `cargo test`

- [ ] **P8-T02** — `MsgPackCodec` + `ProtobufCodec`
  - **Files:** `src/codec.rs`, `Cargo.toml` (add `rmp-serde`, `prost`)
  - **Acceptance:** Round-trip parity with `JsonCodec`; Protobuf schema in `proto/event.proto`
  - **Verify:** `cargo test`

- [ ] **P8-T03** — `ParquetSink` implementation
  - **Files:** `src/sink.rs` (or `src/sink/parquet.rs`), `Cargo.toml` (add `arrow`, `parquet`)
  - **Acceptance:** Writes events + aggregates as row groups; columnar schema; file rotation by size
  - **Verify:** Read back with `arrow` and verify row count

- [ ] **P8-T04** — `RedisSink` with circuit breaker + Postgres fall-through
  - **Files:** `src/sink.rs` (or `src/sink/redis.rs`), `Cargo.toml` (add `redis`)
  - **Acceptance:** Write to Redis with TTL; on Redis failure, circuit breaker opens → fall through to Postgres
  - **Verify:** Integration test: kill Redis, verify Postgres receives writes

- [ ] **P8-T05** — Sink router: `Box<dyn Sink>` dispatch
  - **Files:** `src/sink.rs`
  - **Acceptance:** Route by event type: state→Postgres, analytics→Parquet, hot→Redis
  - **Verify:** `cargo test`

- [ ] **P8-T06** — Serialization benchmark matrix
  - **Acceptance:** bytes/event, encode ns/event, decode ns/event, 1M-row scan time, decode memory
  - **Verify:** Results + winning codec documented; ADR-009 amendment written

- [ ] **P8-T07** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Phase 9 — Production Infrastructure & Performance Lab

**Lights up:** All stages → Observability stack (Prometheus + Grafana + OTel)
**PRD:** FR-20..FR-23, all NFRs · **ADR:** ADR-008, ADR-010 · **Perf row:** P9

## P9.1 Architecture Overview

```
  ┌─────────────────── StreamForge Node ────────────────────────┐
  │                                                              │
  │  Every stage ──► metrics::Handle                            │
  │       │              │                                       │
  │       │         ┌────┴─────┐                                │
  │       │         │ Prom     │──► :9090/metrics ──► Prometheus │
  │       │         │ Exporter │                        │        │
  │       │         └──────────┘                        ▼        │
  │       │                                        Grafana       │
  │       │         ┌──────────┐                  Dashboard     │
  │       └────────►│  OTel    │──► Jaeger/OTLP                 │
  │                 │  Tracer  │                                 │
  │                 └──────────┘                                 │
  │                                                              │
  │  Live-tunable knobs (runtime):                              │
  │  ├── batch_size: AtomicUsize ──► watch::channel             │
  │  ├── worker_count: dynamic resizing                         │
  │  └── fsync_policy: swap via RwLock                          │
  │                                                              │
  │  Health: GET /health ──► readiness + liveness probes        │
  │  Shutdown: drain → flush WAL → close segments → exit 0      │
  └──────────────────────────────────────────────────────────────┘
```

## P9.2 Trait & Type Contracts

```rust
/// Prometheus metrics registry — replaces Phase 2's AtomicU64 counters.
pub struct MetricsRegistry {
    pub events_accepted: Counter,
    pub events_rejected: Counter,
    pub queue_depth: Gauge,
    pub batch_size: Histogram,
    pub wal_fsync_latency: Histogram,
    pub sink_write_latency: HistogramVec,  // label: sink_name
    pub per_shard_lag: GaugeVec,           // label: shard_id
    pub dedup_hit_rate: Counter,
    pub window_emit_rate: Counter,
}

/// Sink retry wrapper with exponential backoff + circuit breaker.
pub struct RetryingSink<S: Sink> {
    inner: S,
    max_retries: u32,
    base_delay: Duration,
    circuit_breaker: CircuitBreaker,
}

impl<S: Sink> Sink for RetryingSink<S> {
    async fn write_batch(&self, batch: &[Event]) -> Result<(), SinkError>;
    // Retries with exponential backoff; opens circuit after threshold failures
}

/// Runtime-tunable knobs.
pub struct RuntimeConfig {
    batch_size: watch::Sender<usize>,
    worker_count: watch::Sender<usize>,
    fsync_policy: watch::Sender<FsyncPolicy>,
}
```

## P9.3 New Crate Dependencies (require user confirmation)

| Crate | Purpose |
|---|---|
| `prometheus` or `metrics` + `metrics-exporter-prometheus` | Prometheus export |
| `opentelemetry` + `opentelemetry-otlp` | OTel trace export |
| `tracing-opentelemetry` | Bridge tracing → OTel |

## P9.4 Work Breakdown

- [ ] **P9-T01** — Prometheus metrics exporter (replace Phase 2 AtomicU64 counters)
  - **Files:** `src/metrics.rs` (rewrite), `src/telemetry.rs`, `Cargo.toml`
  - **Acceptance:** `/metrics` endpoint serves Prometheus text format; all counters from P2 preserved
  - **Verify:** `curl localhost:9090/metrics` returns valid exposition format

- [ ] **P9-T02** — OpenTelemetry tracing integration
  - **Files:** `src/telemetry.rs`, `Cargo.toml`
  - **Acceptance:** Per-batch spans through the full pipeline; trace_id propagated; visible in Jaeger
  - **Verify:** `cargo test`

- [ ] **P9-T03** — Grafana dashboard definition
  - **Files:** `deploy/grafana/dashboard.json` (new)
  - **Acceptance:** Panels: throughput, P99, memory, queue depth, per-shard lag, dedup hit rate, sink latencies
  - **Verify:** Import into Grafana, verify panels render

- [ ] **P9-T04** — Health check endpoint: readiness + liveness
  - **Files:** `src/gateway.rs`
  - **Acceptance:** `/health/ready` checks DB connectivity + WAL access; `/health/live` always 200
  - **Verify:** `cargo test`

- [ ] **P9-T05** — Graceful shutdown: drain → flush WAL → close segments
  - **Files:** `src/lib.rs`
  - **Acceptance:** Shutdown completes within 30s; zero in-flight events lost; WAL segments properly closed
  - **Verify:** Integration test: send events, trigger shutdown, verify all flushed

- [ ] **P9-T06** — Sink retries with exponential backoff + circuit breaker
  - **Files:** `src/sink.rs` (add `RetryingSink<S>`)
  - **Acceptance:** Retries up to `max_retries`; backoff doubles; circuit opens after threshold
  - **Verify:** Unit test with failing mock sink

- [ ] **P9-T07** — Live-tunable knobs via `watch::channel`
  - **Files:** `src/config.rs`, `src/pipeline.rs`, `src/gateway.rs`
  - **Acceptance:** Batch size, worker count, fsync policy changeable via API; workers pick up new values within 1 batch
  - **Verify:** Integration test: change batch size, verify next flush uses new value

- [ ] **P9-T08** — Final SLO gate: 182k/s for 300s
  - **Files:** `docs/performance-results.md`
  - **Acceptance:** All NFR-1..NFR-8 met; dashboard screenshots at load; 10/10 crash test
  - **Verify:** `cargo run --release -- bench --rate 182000 --duration 300 --assert-slo --dashboard`

- [ ] **P9-T09** — Verification gate
  - **Verify:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

---
---

# Cross-Phase Dependency Graph

```
  P1 (Engine)
   │
   ├──► P2 (Backpressure)     ← builds on bounded channel
   │     │
   │     └──► P3 (Parallel)   ← builds on metrics + adaptive batching
   │           │
   │           └──► P4 (Partition)  ← replaces round-robin with hash routing
   │
   └──► P5 (WAL)              ← independent of P2-P4 (can start after P1)
         │
         └──► P6 (Recovery)   ← requires WAL + checkpoints
               │
               └──► P7 (Analytics)  ← requires dedup from P6 + shards from P4
                     │
                     └──► P8 (Storage)  ← requires aggregates from P7
                           │
                           └──► P9 (Prod)  ← integrates everything
```

# Cumulative Error Hierarchy (Phase 9 final)

```
AppError
├── Config { reason }                    ← P1
├── Validation { reason }                ← P1
├── ChannelClosed                        ← P1
├── ChannelFull                          ← P1
├── Sink(SinkError)                      ← P1
│   ├── SinkError::Io                    ← P1
│   ├── SinkError::Backend               ← P1
│   ├── SinkError::Redis                 ← P8
│   ├── SinkError::Parquet               ← P8
│   └── SinkError::CircuitOpen           ← P8
├── Shutdown                             ← P1
├── WorkerFailed { id, reason }          ← P3
├── RoutingError { index, count }        ← P4
├── Wal(WalError)                        ← P5
│   ├── WalError::Io                     ← P5
│   ├── WalError::CorruptRecord          ← P5
│   └── WalError::RotationFailed         ← P5
├── Checkpoint { reason }                ← P6
├── Recovery { reason }                  ← P6
└── Codec(CodecError)                    ← P8
    ├── CodecError::Encode               ← P8
    └── CodecError::Decode               ← P8
```

# Cumulative Memory Budget (Phase 9 final)

| Component | Budget | Phase |
|---|---|---|
| Channel buffers (N shards) | `N × (K/N) × 1KB` ≈ 1 MB | P1→P4 |
| Batch buffers (N workers) | `N × 256 × 1KB` ≈ 2 MB | P1→P3 |
| WAL I/O buffers | ~16 KB | P5 |
| Dedup LRU caches (N shards) | `N × cache_size × 16B` | P6 |
| Window state (N shards) | < 80 KB | P7 |
| Postgres connection pool | ~5 MB (pool_size × conn) | P8 |
| Redis connection pool | ~2 MB | P8 |
| Prometheus registry | ~1 MB | P9 |
| **Total** | **< 15 MB** (excluding event payload) | **Well within 412 MB NFR-3** |

---

# Cumulative Crate Additions (requires user confirmation per AGENTS.md §3)

| Crate | Phase | Purpose |
|---|---|---|
| `sqlx` | P1 (T13) | PostgresSink |
| `rayon` | P3 | Worker model |
| `rustc-hash` | P4 | FxHash for partitioning |
| `bytes` | P5 | WAL serialization |
| `crc32fast` | P5 | WAL record checksums |
| `lru` | P6 | Dedup cache |
| `rmp-serde` | P8 | MessagePack codec |
| `prost` + `prost-build` | P8 | Protobuf codec |
| `arrow` + `parquet` | P8 | Parquet writer |
| `redis` | P8 | Redis client |
| `prometheus` / `metrics` | P9 | Metrics export |
| `opentelemetry` + bridge | P9 | Distributed tracing |
