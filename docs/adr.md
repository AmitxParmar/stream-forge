# StreamForge — Architecture Decision Records

Format: lightweight ADR (Context → Decision → Reasoning → Alternatives → Consequences).
Statuses: Proposed / Accepted / Superseded.

---

## ADR-001: Implementation language is Rust

**Status:** Accepted

**Context:** We need a language for a high-throughput (182k events/s, 18ms P99) single-node streaming engine whose core learning goals include memory layout, backpressure, and concurrency tradeoffs.

**Decision:** Implement the entire engine in Rust (stable toolchain).

**Reasoning:**
- No GC pauses → predictable P99, which is the core NFR; JVM/Go GC pauses would complicate tail-latency reasoning.
- Ownership model enforces the single-writer / message-passing design at compile time (no accidental shared mutable state across shards).
- First-class async ecosystem (`tokio`) plus explicit choice to compare with OS threads and Rayon — Rust is one of few languages where all three models are idiomatic.
- `serde` + zero-cost abstractions make the serialization benchmark (Phase 8) meaningful rather than allocator-dominated.

**Alternatives:**
- Go: simpler concurrency, but GC pauses + weaker control over memory layout undercut the performance-lab goals.
- Java/Kafka Streams: battle-tested but the goal is to build, not use; too much magic between us and the fsync.

**Consequences:** Slower initial development; unsafe-free code with occasional `Arc`/`Channel` ceremony. Accepted — the point is systems learning.

---

## ADR-002: Tokio as the async runtime for the gateway

**Status:** Accepted

**Context:** The ingestion gateway must hold thousands of concurrent HTTP/WS connections cheaply; the rest of the pipeline is batch/queue-shaped.

**Decision:** Use `tokio` (multi-threaded scheduler) for the gateway, connection handling, and batching tasks.

**Reasoning:**
- Connection handling is IO-bound and highly concurrent — exactly the async sweet spot; a thread-per-connection model would cap at a few thousand connections.
- Multipart ecosystem: `axum`/`hyper` for HTTP, `tokio-tungstenite` for WS, `tokio::sync::mpsc` for bounded queues with natural backpressure (send awaits when full).
- Phase 3 explicitly benchmarks Tokio vs threads vs Rayon for the *worker* side; we only commit to Tokio where IO-bound wins are unambiguous.

**Alternatives:** `async-std` (stagnant), `smol` (fine but smaller ecosystem), pure `std::net` threads (fails the 10k-connections test).

**Consequences:** Two concurrency worlds (async gateway + configurable workers) coexist; bridge via `spawn_blocking`/channels. Documented and measured in Phase 3.

---

## ADR-003: Hand-rolled WAL instead of an embedded database

**Status:** Accepted

**Context:** We need durability before processing (zero acknowledged-event loss across kill -9) and replayable offsets.

**Decision:** Build a minimal Kafka-style append-only log ourselves: segment files, sequential appends, batch-level fsync, per-record CRC32, monotonic offsets.

**Reasoning:**
- The WAL is the pedagogical heart of the project ("where the project stops being 'a Rust project' and becomes systems engineering"): segment rotation, fsync economics, and torn-write recovery must be *felt*, not imported.
- Requirements are narrow (single writer, offset reads, replay); a general engine (RocksDB, SQLite, redb) adds opaque complexity that hides exactly the behaviors we want to observe.
- Hand-rolled ≈ 300–500 LOC; the failure modes (torn tail, crc mismatch) are testable with a kill -9 harness.

**Alternatives:** `redb`/`sled` (B-tree semantics, opaque fsync behavior), SQLite (great, but no sequential-log offsets semantics), Kafka itself (that would be *using* Kafka, not building the concept).

**Consequences:** We own corruption handling (truncate-at-last-good-CRC) and fsync tuning. Single-writer constraint documented in architecture.md §2.2.

---

## ADR-004: Partition by `hash(user_id) % N` with per-shard channels

**Status:** Accepted

**Context:** We need per-key ordering (same user → ordered events) while processing partitions concurrently.

**Decision:** Router computes `hash(user_id) % N` and dispatches to one of N shard channels, each with exactly one consumer worker. Keyless events round-robin.

**Reasoning:**
- Deterministic routing gives per-key ordering for free: one consumer per shard means no interleaving within a key.
- Shard-local state (windows, dedup caches) removes all cross-worker locks — the "shared nothing" pattern is why throughput scales near-linearly with cores.
- Modulo routing is trivially correct and easy to reason about; consistent hashing is only needed for *elastic* resharding, which is out of scope (see ADR-008).

**Alternatives:** Work-stealing queue (loses per-key ordering), Kafka-style consumer groups with external coordination (out of scope), consistent hashing (premature — no elasticity yet).

**Consequences:** Hot keys can saturate one shard (documented limitation, detectable via per-shard metrics); changing N re-maps all keys (requires drain-and-replay, acceptable for single node).

---

## ADR-005: At-least-once delivery + idempotent processing (not exactly-once)

**Status:** Accepted

**Context:** Crash recovery replays from the last checkpoint; some events will be reprocessed.

**Decision:** Guarantee at-least-once delivery; make processing idempotent via event_id dedup at the windowing/sink stage.

**Reasoning:**
- Exactly-once requires transactional coordination between WAL, processing, and sinks (two-phase commit / transactional offsets) — large complexity for a single-node system.
- Dedup-by-event_id converts at-least-once into *effectively-once* processing with a small bounded cache (per-shard LRU/bloom filter of recent event_ids).
- This is the industry-standard tradeoff (Kafka Streams' "exactly-once" is precisely idempotent processing + transactional offsets).

**Alternatives:** Exactly-once via 2PC with Postgres (correct but kills throughput and the learning focus), at-most-once (unacceptable data loss).

**Consequences:** Dedup state must be checkpointed with offsets (else duplicates slip through after recovery). Dedup hit rate is a first-class metric.

---

## ADR-006: Tiered storage — PostgreSQL (state), Parquet (analytics), Redis (hot cache)

**Status:** Accepted

**Context:** Aggregates and raw events have conflicting access patterns: transactional point lookups vs wide analytical scans vs ultra-low-latency repeated reads.

**Decision:** Route by access pattern: checkpoints + transactional state → PostgreSQL; historical events + windowed aggregates → Parquet; hot repeated lookups → Redis with Postgres fall-through.

**Reasoning:**
- Checkpoints need ACID (offset + dedup state must commit atomically) — Postgres.
- Analytics is columnar-scan dominated (sum/avg over millions of rows) — Parquet is 10–100x faster to scan and smaller on disk than row formats; this is also the Phase 8 benchmark subject.
- Repeated identical lookups (e.g., user-profile enrichment) would hammer Postgres; Redis absorbs them, and a circuit breaker degrades gracefully to Postgres.

**Alternatives:** Single store (Postgres for everything — scans too slow; or Parquet for everything — no transactions), ClickHouse (attractive but replaces the learning goal with an ops exercise).

**Consequences:** Three sinks to make consistent (retries + circuit breakers each, FR-22); sink write paths are serialized per shard so ordering holds per key.

---

## ADR-007: Bounded queues everywhere; backpressure is a feature

**Status:** Accepted

**Context:** Under overload the system must degrade predictably, never OOM via unbounded buffering.

**Decision:** Every internal queue is bounded (`tokio::sync::mpsc(cap=K)`). Overflow policy is staged: (1) adaptive batch growth/shrink, (2) producer throttling, (3) explicit load shedding (429/drop).

**Reasoning:**
- Unbounded queues convert overload into memory death; bounded queues convert overload into *signal* — the single most valuable concept in the project.
- mpsc's `send().await` gives natural async backpressure for free; explicit shedding keeps P99 for *accepted* events low instead of letting everything rot in a queue.
- Adaptive batching amortizes per-batch costs (fsync, serialization) when the system has headroom, and backs off when it doesn't.

**Alternatives:** Unbounded channel + reactive eviction (lag becomes unobservable), reactive streams everywhere (fine but reinvents mpsc).

**Consequences:** Clients must handle 429 with retry+jitter (documented contract FR-4); queue depth is the primary overload signal on the dashboard.

---

## ADR-008: Single-node scope; no clustering

**Status:** Accepted

**Context:** The phase map and WAL design could drift toward multi-node (replication, leader election, cross-node partition assignment).

**Decision:** StreamForge v1 is explicitly single-node. No replication, no consensus, no cross-node rebalancing.

**Reasoning:**
- The performance-lab goals (backpressure, fsync economics, concurrency models, serialization) are all single-node phenomena; clustering would consume the schedule before those are measured.
- Kafka/Redpanda already solve distributed logs; re-solving them without measuring the fundamentals first produces a worse Kafka, not a better engineer.
- Single binary + local SSD WAL keeps the perf lab reproducible on one machine.

**Alternatives:** Raft-replicated WAL (superceded-able later: the WAL's segment/offset abstraction leaves room for a replication role behind the same trait).

**Consequences:** Node failure = downtime until restart (recovery covers durability, not availability). Documented as a non-goal in the PRD; a future ADR may supersede.

---

## ADR-009: Serialization format chosen by measurement, with a documented default

**Status:** Proposed (resolved in Phase 8)

**Context:** The hot path needs a wire + storage format; candidates are JSON, MessagePack, Protobuf, and Parquet (storage).

**Decision:** Benchmark all four in Phase 8 (size, encode/decode speed, CPU, memory) and adopt the winner for the hot path; Parquet is fixed for the analytics sink regardless.

**Reasoning:**
- The architecture doc mandates measurement rather than assumption: the answer depends on payload shape (small user events vs wide aggregates), so the benchmark harness is the deliverable, and the default follows from its output.
- Protobuf is the expected winner for the hot path (schema-checked, compact, fast decode via `prost`); MessagePack is the fallback if schema friction dominates.

**Alternatives:** Committing now without data would violate the project's own methodology.

**Consequences:** The event schema (Phase 1, JSON-only) must be versioned so a format switch is a codec change, not a model change. Expect an ADR-009 amendment with the benchmark table after Phase 8.

---

## ADR-010: Observability stack — Prometheus + Grafana + OpenTelemetry

**Status:** Accepted

**Context:** Phase 9 requires a live dashboard with real numbers and per-stage regression visibility.

**Decision:** Export Prometheus metrics from every stage, emit OTel traces across the pipeline, and build the Grafana dashboard with live-tunable knobs.

**Reasoning:**
- Pull-based Prometheus counters/histograms are the de-facto standard, trivially self-hostable, and the dashboard doubles as the perf-lab UI.
- OTel spans per batch make P99 attribution possible (which *stage* is slow?) — histograms alone can't answer that.
- Tunable knobs (batch size, worker count, fsync policy) turn the dashboard from a monitor into an experiment bench.

**Alternatives:** InfluxDB/Timescale (fine, heavier), logs-only (can't compute percentiles), vendor APM (defeats self-hosted lab goal).

**Consequences:** Every stage must thread a metrics/tracing handle from day one (retrofitting instrumentation is the classic trap — Phase 1 starts with `tracing` wired in).
