# StreamForge — Phase 1 Architectural Specification

Status: **v2.0** · Scope: `docs/phases.md` Phase 1
Lights up: Clients → Gateway → Queue → Worker → Sink (in-memory; Postgres next)

---

## 1. Architecture Overview

```
                    ┌─────────── StreamForge Node ───────────────┐
                    │                                            │
  ┌──────────┐      │  ┌──────────┐  mpsc(K)  ┌──────────────┐  │
  │  HTTP    │ POST │  │ Gateway  │──Event──►│ Batch Worker │  │
  │  Client  │─────►│  │ (axum)   │  bounded  │ drain+flush  │  │
  └──────────┘      │  │          │  channel  │              │  │
                    │  │ validate │           │ &[Event]     │  │
                    │  │ parse    │           │     │        │  │
                    │  └──────────┘           │     ▼        │  │
                    │       │                 │ ┌─────────┐  │  │
                    │       │ 422 invalid     │ │  Sink   │  │  │
                    │       │ 503 pipe dead   │ │ (trait) │  │  │
                    │       │ 202 accepted    │ └─────────┘  │  │
                    │       │                 └──────────────┘  │
                    │       │                                   │
                    │  CancellationToken ──► cooperative stop   │
                    │  tracing ──► structured spans every stage │
                    └───────────────────────────────────────────┘

  Ownership transfer (single arrow = move, & = borrow):

  RawEvent ──parse()──► Event ──send()──► Event ──batch──► &[Event] ──► Sink
  (HTTP body)          (validated)       (channel)        (borrowed)    (stores)
       │                   │                 │                │
    heap alloc          move once         move once      zero-copy read
    (axum owns)        (gateway→ch)      (ch→worker)    (worker→sink)
```

### Data Flow Invariants

1. **Parse-don't-validate**: `RawEvent` (serde shape) converts once into `Event` via `Event::parse()`. No unvalidated data crosses the gateway boundary.
2. **Move-once ownership**: Each `Event` moves exactly once per stage. No clones in the hot path except the sink's intentional store.
3. **Borrow at the sink**: The worker passes `&[Event]` to `Sink::write_batch()`, not `Vec<Event>`. The sink reads; the worker retains and reuses the buffer.
4. **Single writer per channel**: One `mpsc::Receiver<Event>`, one worker task. The `Sender` is cloned per gateway handler (cheap `Arc` bump).

---

## 2. Trait Boundaries & Modularity

### 2.1 Dispatch Strategy Decision

**Decision: Static dispatch (generics + `impl Trait`) on all hot-path boundaries.**

| Boundary | Dispatch | Rationale |
|---|---|---|
| `Sink` trait | `S: Sink` (generic) | Zero-cost; monomorphised at compile time. Worker is generic over `S`. Hot path (batch flush) cannot afford vtable indirection. |
| `Pipeline::spawn_worker` | `S: Sink` (generic) | Propagates the static dispatch through the worker spawn. |
| `gateway::router` | Concrete `mpsc::Sender<Event>` | No trait needed; the channel type is fixed by `tokio::sync::mpsc`. |

**Rationale against `dyn Trait` for Phase 1:** The PRD targets 182k events/s at P99 ≤ 18ms. At that throughput, `write_batch` is called ~710 times/s (256-event batches). While vtable overhead is small per-call, the coding-standards mandate (`trait-dyn-vs-generic`, `anti-type-erasure`) and the performance budget (`docs/performance.md` §1) favour zero-cost abstraction. Dynamic dispatch (`Box<dyn Sink>`) is deferred to Phase 8 if runtime codec/sink selection requires it — a future ADR will record that decision.

### 2.2 Core Trait: `Sink`

```rust
/// Destination for flushed event batches.
///
/// # Contract
/// - `write_batch` receives a borrowed slice; implementations must not
///   assume ownership or that the slice persists after the call returns.
/// - Implementations must be idempotent-friendly: Phase 6 replays
///   batches after crashes, so writing the same batch twice must be safe.
/// - The future returned by `write_batch` must be `Send` (required by
///   `tokio::spawn`).
///
/// # Errors
/// Returns [`SinkError`] on I/O or backend failure.
pub trait Sink: Clone + Send + Sync + 'static {
    fn write_batch(
        &self,
        batch: &[Event],
    ) -> impl std::future::Future<Output = Result<(), SinkError>> + Send;
}
```

**Supertraits justified:**
- `Clone`: Worker needs to share the sink handle (e.g., `Arc<Mutex<Vec<Event>>>` for `InMemorySink`).
- `Send + Sync + 'static`: Required by `tokio::spawn` to move the sink into a task.

### 2.3 Planned Trait: `Gateway` (informational — Phase 2+)

Phase 1 does not abstract the gateway behind a trait (axum routes are concrete). Phase 2 will introduce protocol abstraction when WebSocket support lands (FR-1). The boundary is documented here for forward compatibility:

```rust
// Future: not implemented in Phase 1
pub trait Gateway: Send + Sync + 'static {
    async fn accept(&self, shutdown: CancellationToken) -> Result<(), AppError>;
}
```

### 2.4 Planned Trait: `Pipeline` (informational — Phase 3+)

Phase 3 introduces swappable concurrency models. The pipeline abstraction will emerge when `--model tokio|threads|rayon` ships. Phase 1's `spawn_worker` is a free function, not a trait method — it will refactor into a trait when the second implementation exists.

---

## 3. Zero-Copy & Memory Model

### 3.1 Event Representation (Current)

```rust
pub struct Event {
    pub id: EventId,        // Copy, 8 bytes
    pub user_id: UserId,    // Copy, 8 bytes
    pub kind: String,       // heap-allocated, ≤ 64 bytes (MAX_KIND_LEN)
    pub payload: serde_json::Value,  // heap-allocated, ≤ 64 KiB (MAX_PAYLOAD_BYTES)
}
```

**Phase 1 trade-off analysis:**

| Field | Current | Zero-copy alternative | Phase 1 decision |
|---|---|---|---|
| `kind` | `String` | `Cow<'a, str>` or `bytes::Bytes` | **Keep `String`.** `kind` is max 64 bytes; the allocation happens once at parse time (gateway boundary). Introducing a lifetime parameter (`'a`) would infect `Event`, the channel, and the sink trait with lifetime bounds, adding complexity disproportionate to the 64-byte saving. |
| `payload` | `serde_json::Value` | `bytes::Bytes` (raw JSON bytes) | **Keep `Value` in Phase 1.** Downstream consumers (Phase 7 windowing) need structured access to payload fields. Deferring to raw bytes now would require re-parsing in every worker. Phase 8's serialization benchmark will determine the hot-path format (ADR-009). |
| `id`, `user_id` | `Copy` newtypes | — | **Optimal.** No allocation, no clone cost. |

### 3.2 Batching Buffer Strategy

```
Worker lifecycle:
  let mut pending: Vec<Event> = Vec::with_capacity(batch.max_size);  // alloc once
  loop {
      // ... accumulate events ...
      sink.write_batch(&pending)?;   // borrow, zero-copy to sink
      pending.clear();               // reuse allocation, length → 0
  }
```

**Key properties:**
- **Single allocation**: `with_capacity(max_size)` allocates once at worker startup. `clear()` resets length without deallocating (`mem-reuse-collections`).
- **No `format!` on hot path**: Tracing uses structured fields (`info!(flushed = pending.len())`), not string interpolation (`anti-format-hot-path`).
- **Sink borrows**: `write_batch(&[Event])` — the worker retains ownership of the `Vec` for reuse; the sink reads the slice.

### 3.3 Memory Budget (Phase 1 Envelope)

| Component | Formula | Budget |
|---|---|---|
| Channel buffer | `K × sizeof(Event)` ≈ 1024 × ~1KB | ~1 MB |
| Batch buffer | `max_size × sizeof(Event)` ≈ 256 × ~1KB | ~256 KB |
| `InMemorySink` storage | Grows without bound (test-only) | Unbounded (acceptable: test sink, not production) |
| Gateway handler stacks | Tokio task stacks, ~8KB each | Bounded by connection count |
| **Total hot-path** | Channel + batch | **~1.3 MB** (well within 412 MB NFR-3) |

Phase 8 sinks (Postgres/Redis/Parquet) will have their own connection pool budgets. The in-memory sink is intentionally unbounded because it only exists for tests.

### 3.4 Future Zero-Copy Path (Phase 5+)

When the WAL lands (Phase 5), events will be serialized to `bytes::Bytes` for the append log. The zero-copy path will be:

```
Gateway → Event (validated) → serialize to Bytes → WAL append → deserialize in worker
```

This requires the `Event` ↔ `Bytes` round-trip to be lossless. Phase 1's `Serialize`/`Deserialize` derives on `Event` prepare for this. The `bytes` crate is *not* added to `Cargo.toml` in Phase 1 (no dependency creep — AGENTS.md §3).

---

## 4. Concurrency & Backpressure

### 4.1 Concurrency Topology

```
  ┌─────────────────────────────────────────────────────────────┐
  │                     Tokio Runtime                           │
  │                                                             │
  │  ┌──────────────┐                                          │
  │  │ ctrl_c task  │──► CancellationToken::cancel()           │
  │  └──────────────┘                                          │
  │                                                             │
  │  ┌──────────────┐   mpsc::Sender    ┌──────────────────┐   │
  │  │ axum::serve  │──────────────────►│ spawn_worker     │   │
  │  │ (N conn      │   (clone per      │ (single task)    │   │
  │  │  handler     │    handler)        │                  │   │
  │  │  tasks)      │                    │ select! {        │   │
  │  └──────────────┘                    │   shutdown,      │   │
  │       │                              │   rx.recv()      │   │
  │       │ graceful_shutdown            │ }                │   │
  │       └─────────────────────────────►│                  │   │
  │                                      └──────────────────┘   │
  └─────────────────────────────────────────────────────────────┘

  Task count: 1 (ctrl_c) + 1 (axum server) + N (conn handlers) + 1 (worker) = N+3
```

### 4.2 Channel Semantics & Backpressure

**Primitive:** `tokio::sync::mpsc::channel(cap=K)` where `K = Config::queue_capacity` (default 1024).

**Backpressure stages (Phase 1 → Phase 2 progression):**

| Stage | Phase 1 Behavior | Phase 2 Addition |
|---|---|---|
| Channel has capacity | `send(event).await` completes immediately | Same |
| Channel full | `send(event).await` **suspends** the handler task (natural async backpressure — the HTTP response is delayed) | `try_send()` + 429 load shedding |
| Channel closed (worker gone) | `send()` returns `Err` → handler replies 503 | Same |
| Slow sink | Worker blocks on `write_batch()`, drains channel slowly → channel fills → gateway handlers suspend | Adaptive batch sizing + queue-depth metrics |

**Phase 1 deliberately relies on implicit backpressure** (send-await suspension). This is correct but coarse — under sustained overload, all handler tasks block and the TCP accept queue fills. Phase 2 upgrades this to explicit shedding (429 + rejection counters).

### 4.3 Shutdown Protocol

```
Signal: ctrl_c
  │
  ▼
CancellationToken::cancel()
  │
  ├──► axum::serve graceful_shutdown ──► stop accepting, drain in-flight
  │
  └──► worker select! branch ──► flush pending batch ──► return Ok(())
```

**Cancellation safety:** Both `select!` branches in the worker are cancel-safe:
- `shutdown.cancelled()` is a `Future` that resolves once; dropping it is safe.
- `rx.recv()` is documented as cancel-safe by Tokio (no data loss on drop).

### 4.4 Concurrency Constraints

| Constraint | Enforcement | Source |
|---|---|---|
| No lock guard across `.await` | `InMemorySink` acquires mutex, extends, drops guard, then returns | `async-no-lock-await` |
| All channels bounded | `mpsc::channel(cap)` with `cap` from `Config`, never hardcoded | ADR-007 |
| No CPU-bound work on async runtime | Phase 1 has no CPU-bound stages; Phase 3 adds `spawn_blocking` | `async-spawn-blocking` |
| Cooperative shutdown | Every task races work vs `shutdown.cancelled()` | `async-cancellation-token` |

---

## 5. Error Domain & Failure Modes

### 5.1 Error Hierarchy

```
                    ┌──────────────┐
                    │   AppError   │  (library-level, thiserror)
                    │ #[non_exhaustive] │
                    └──────┬───────┘
          ┌────────┬───────┼────────┬──────────┬──────────┐
          ▼        ▼       ▼        ▼          ▼          ▼
       Config  Validation ChannelClosed ChannelFull  Sink(SinkError)  Shutdown
       (startup) (gateway)  (fatal)    (backpressure)  (egress)     (graceful)
          │        │          │           │              │             │
       unrecov.  drop-safe  unrecov.   recoverable   recoverable   expected
                (4xx resp)  (503 resp)  (429 / retry)  (retry/CB)
```

### 5.2 Error Classification

| Variant | Recoverable? | HTTP mapping | Action |
|---|---|---|---|
| `Config { reason }` | **No** — fail fast at startup | N/A (pre-listen) | Log, exit non-zero |
| `Validation { reason }` | **Yes** — per-event, drop-safe | 422 Unprocessable Entity | Return error body, do not retry |
| `ChannelClosed` | **No** — worker is gone | 503 Service Unavailable | Log critical, initiate shutdown |
| `ChannelFull` | **Yes** — transient overload | 429 Too Many Requests (Phase 2) | Phase 1: await; Phase 2: shed + retry guidance |
| `Sink(SinkError)` | **Depends** — I/O may be transient | N/A (internal) | Phase 1: propagate up, fail worker; Phase 9: retry + circuit breaker |
| `Shutdown` | **Expected** — not an error | N/A | Clean exit |

### 5.3 Contract Signatures

```rust
/// Top-level library error. Matches the error domain diagram above.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum AppError {
    #[error("invalid configuration: {reason}")]
    Config { reason: String },

    #[error("invalid event: {reason}")]
    Validation { reason: String },

    #[error("pipeline channel closed")]
    ChannelClosed,

    #[error("pipeline channel full")]
    ChannelFull,

    #[error("sink write failed")]
    Sink(#[source] SinkError),

    #[error("shutdown requested")]
    Shutdown,
}

/// Sink-specific errors. Separate enum so sink implementations
/// don't need to know about pipeline errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SinkError {
    #[error("sink io error")]
    Io(#[from] std::io::Error),

    #[error("sink backend error: {0}")]
    Backend(String),
}
```

**Key design decisions:**
1. **`#[non_exhaustive]`** on both enums: callers must have a wildcard arm, so adding variants (e.g., `SinkError::Timeout` in Phase 9) is non-breaking.
2. **`#[source]` chain**: `AppError::Sink` wraps `SinkError` with source preservation for `anyhow`'s error chain display.
3. **No `String` payloads in hot-path variants**: `ChannelClosed`, `ChannelFull`, and `Shutdown` carry no heap allocation. `Validation` and `Config` allocate a `String` but occur at the boundary (gateway or startup), never in the batch loop.

### 5.4 Error Flow Per Stage

```
Gateway handler:
  Event::parse(raw) → Err(Validation) → 422 response  (drop-safe, no propagation)
  tx.send(event)    → Err(SendError)  → 503 response  (maps to ChannelClosed)

Worker loop:
  rx.recv()         → None            → flush + Ok(()) (channel closed, clean exit)
  sink.write_batch  → Err(SinkError)  → Err(AppError::Sink) → task joins with Err

Orchestrator (lib.rs::run):
  worker.await      → Err(AppError)   → .context("worker failed") → anyhow::Error
  gateway.await     → Err(anyhow)     → .context("gateway failed") → propagate

Binary (main.rs):
  run(config).await  → Err(anyhow)    → process exit 1 with error chain display
```

---

## 6. Module Contracts

### 6.1 `src/event.rs` — Domain Model

| Symbol | Type | Contract |
|---|---|---|
| `EventId(u64)` | Newtype | `Copy + Eq + Hash`. Never bare `u64` in APIs. |
| `UserId(u64)` | Newtype | `Copy + Eq + Hash`. Partitioning key in Phase 4. |
| `RawEvent` | Serde input | `deny_unknown_fields`, `rename_all = "camelCase"`. Never crosses the gateway boundary. |
| `Event` | Validated domain type | Constructible only via `Event::parse(RawEvent) -> Result<Self, AppError>`. |
| `MAX_KIND_LEN` | `const usize` | 64 bytes. Enforced in `parse()`. |
| `MAX_PAYLOAD_BYTES` | `const usize` | 64 KiB. Enforced in `parse()`. |

### 6.2 `src/gateway.rs` — Ingestion Boundary

| Symbol | Type | Contract |
|---|---|---|
| `router(tx) -> Router` | Constructor | Builds axum router with shared state. Testable without binding a socket. |
| `serve(config, tx, shutdown)` | Async | Binds `config.bind`, serves until `shutdown` fires. Returns `anyhow::Result`. |
| `ingest_event` | Handler | 202 accepted / 422 validation error / 503 pipeline dead. Instrumented with `event_id` and `user_id` span fields. |
| `health` | Handler | Returns `{"status": "ok"}`. No auth, no state. |

### 6.3 `src/pipeline.rs` — Batch Worker

| Symbol | Type | Contract |
|---|---|---|
| `BatchConfig` | Config struct | `max_size > 0`, `timeout > 0`. Validated via `validate()`. |
| `channel(config)` | Factory | Returns `(Sender<Event>, Receiver<Event>)` with `config.queue_capacity`. |
| `spawn_worker<S: Sink>(rx, sink, batch, shutdown)` | Spawner | Returns `JoinHandle<Result<(), AppError>>`. Worker flushes on: (a) batch full, (b) timeout, (c) shutdown, (d) channel close. Buffer reused via `clear()`. |
| `flush<S: Sink>(sink, pending)` | Internal | Borrows `&[Event]` to sink, then `clear()`. No-op on empty batch. Instrumented with `batch_size`. |
| `drain(rx, pending, limit)` | Internal | Non-blocking `try_recv` loop up to `limit`. |

### 6.4 `src/sink.rs` — Egress Abstraction

| Symbol | Type | Contract |
|---|---|---|
| `Sink` | Trait | `Clone + Send + Sync + 'static`. Single method: `write_batch(&self, &[Event]) -> impl Future<..> + Send`. |
| `InMemorySink` | Test impl | `Arc<Mutex<Vec<Event>>>`. Lock held only during `extend()`, never across `.await`. |

### 6.5 `src/config.rs` — Configuration

| Symbol | Type | Contract |
|---|---|---|
| `Config` | Root config | `bind`, `queue_capacity`, `batch`. `Default` provides sane values. `validate()` fails fast. |

### 6.6 `src/error.rs` — Error Types

See §5.3 above. Both enums are `#[non_exhaustive]`, `Debug`, use `thiserror`.

### 6.7 `src/telemetry.rs` — Instrumentation

| Symbol | Type | Contract |
|---|---|---|
| `init()` | Setup | Installs `tracing_subscriber` with `EnvFilter` from `RUST_LOG`. Called only from binary. Library code emits only. |

### 6.8 `src/lib.rs` — Orchestrator

| Symbol | Type | Contract |
|---|---|---|
| `run(config)` | Entry | Validates config, spawns ctrl_c handler, creates channel, spawns worker, serves gateway, awaits shutdown. Returns `anyhow::Result<()>`. |

---

## 7. Decisions Made (with reasoning)

| # | Decision | Rationale | Standard |
|---|---|---|---|
| 1 | Static dispatch for `Sink` | Zero-cost on hot path; vtable deferred to Phase 8 if runtime flexibility needed | `trait-dyn-vs-generic` |
| 2 | `String`/`Value` in `Event`, not `Bytes` | Structured access needed in workers (Phase 7); lifetime infection disproportionate to saving | `own-borrow-over-clone` |
| 3 | `Vec::with_capacity` + `clear()` for batch buffer | Single allocation, reused across flushes; no per-batch heap churn | `mem-reuse-collections` |
| 4 | `send().await` for Phase 1 backpressure | Correct and simple; explicit shedding (429) deferred to Phase 2 | ADR-007 |
| 5 | `CancellationToken` for shutdown | Cooperative, composable; every `select!` races work vs cancel | `async-cancellation-token` |
| 6 | `thiserror` in lib, `anyhow` in binary | Typed errors for handler matching; context chains for diagnostics | `err-thiserror-lib`, `err-anyhow-app` |
| 7 | `Event::parse()` as sole constructor | Parse-don't-validate; invalid states unrepresentable downstream | `api-parse-dont-validate` |
| 8 | Bounded channel capacity from `Config` | No magic numbers; overload becomes signal, not OOM | ADR-007 |
| 9 | `#[non_exhaustive]` on error enums | Adding variants is non-breaking across phases | `api-non-exhaustive` |
| 10 | No new crate additions in Phase 1 | `bytes` deferred to Phase 5 (WAL); `sqlx` to next step | AGENTS.md §3 |

**Explicitly deferred:** Postgres sink, `bytes` crate, load generator/bench harness, WebSocket support, shedding/429s (Phase 2), worker models (Phase 3), WAL (Phase 5).

---

## 8. Work Breakdown Structure

### Completed Tasks

- [x] **T01** — Rename crate, dependencies, profiles, lints (`Cargo.toml`)
- [x] **T02** — `lib.rs`/`main.rs` split with `run(config)` orchestration
- [x] **T03** — `config.rs` validated configuration + unit test
- [x] **T04** — `error.rs` `AppError`/`SinkError` with `thiserror`
- [x] **T05** — `event.rs` `RawEvent` → `Event::parse()` boundary + unit tests
- [x] **T06** — `pipeline.rs` bounded channel + batch worker + unit test
- [x] **T07** — `sink.rs` `Sink` trait + `InMemorySink`
- [x] **T08** — `gateway.rs` axum routes + HTTP status contract
- [x] **T09** — `tests/ingest.rs` integration path (3 tests)
- [x] **T10** — `telemetry.rs` tracing subscriber init
- [x] **T11** — `docs/coding-standards.md`, this plan
- [x] **T12** — Full verification gate green (`cargo check` + `clippy` + `fmt` + `test`)

### Remaining Tasks (Phase 1 exit)

- [ ] **T13** — Postgres `Sink` implementation
  - **Files:** `src/sink.rs` (add `PostgresSink`), `Cargo.toml` (add `sqlx`)
  - **Acceptance criteria:**
    - `PostgresSink` implements `Sink` trait with `write_batch` inserting rows into an `events` table
    - Connection pool configured from `Config` (new fields: `database_url`, `pool_size`)
    - Idempotent writes (upsert on `event_id`)
    - `InMemorySink` remains for tests; no `unwrap()`/`expect()` in implementation
    - Unit test with `sqlx::test` attribute and test database
  - **Verification:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo test`

- [ ] **T14** — Load generator binary
  - **Files:** `src/bin/loadgen.rs` (new)
  - **Acceptance criteria:**
    - CLI with `--rate`, `--duration`, `--target` flags
    - Generates valid `RawEvent` payloads at configurable rate
    - Reports: events sent, accepted (2xx), rejected (4xx/5xx), P50/P99 latency
    - Does not add new dependencies beyond what exists in `Cargo.toml`
  - **Verification:** `cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check`

- [ ] **T15** — Throughput/latency benchmark (Phase 1 exit gate)
  - **Files:** `docs/performance.md` (append results)
  - **Acceptance criteria:**
    - Run load generator at 1k, 10k, 100k events/s for 60s each (release build)
    - Record throughput, P50, P99 latency per rate
    - Identify saturation knee in one paragraph
    - Results table includes: CPU model, Rust version, `Config` values used
  - **Verification:** `cargo run --release -- bench` produces reproducible numbers; results committed to `docs/performance.md`

---

## 9. Verification Plan

### Automated Tests (run after every change)

```bash
# 1. Fast compilation check
cargo check

# 2. Strict linting (warnings are errors)
cargo clippy --all-targets -- -D warnings

# 3. Code formatting
cargo fmt --check

# 4. Integration test suite
cargo test --test ingest

# 5. All tests including unit and doctests
cargo test
```

### Phase 1 Exit Criteria

1. All T01–T15 complete and checked
2. Latency-vs-rate curve at 1k → 10k → 100k events/s documented
3. Saturation knee identified and explained in one paragraph
4. Full verification gate (`cargo check` + `clippy` + `fmt` + `test`) passes
5. No `unwrap()`/`expect()` in `src/` (AGENTS.md §3)
6. No unbounded channels (ADR-007)
7. No new crate additions without explicit user confirmation (AGENTS.md §3)

---

## 10. Next Steps (in order)

1. **T13**: Add `sqlx` + `PostgresSink: Sink` (user confirmation required for `sqlx` dependency)
2. **T14**: Add `src/bin/loadgen.rs` driving the rate curve
3. **T15**: Record the throughput/latency knee; write the one-paragraph explanation
4. **Phase 2**: Shedding + adaptive batching on the bounded channel built here
