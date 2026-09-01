# StreamForge — Build Phases

The 9-phase plan behind the architecture map (`docs/streamforge-architecture.html` — the phase chips light up the parts of the pipeline each phase exercises).
Each phase has scope, deliverables, exit criteria, and the primary concept it teaches. Phases build on each other; do not reorder.

---

## Phase 1 — Fast single-node engine

**Lights up:** Clients → Gateway → Queue → Worker 1 → Aggregator (in-memory only)

**Scope:** HTTP ingest via Tokio, a bounded channel, one batch worker, writes to Postgres. In-memory end-to-end — no WAL, no partitioning yet. `tracing` wired in from the first commit (ADR-010).

**Deliverables:**
- `axum`/`hyper` HTTP server with payload validation
- `tokio::sync::mpsc` bounded queue (gateway → worker)
- Single batch worker writing to Postgres
- Load generator + first throughput/latency measurement

**Exit criteria:** Latency-vs-rate curve from 1k → 10k → 100k events/s; the saturation knee identified and explained in one paragraph.

**Primary lesson:** async/await fundamentals, connection handling, structured logging, honest benchmarking.

---

## Phase 2 — Backpressure

**Lights up:** Gateway → Queue (deliberately overloaded) → WAL handoff point

**Scope:** Make the queue the bottleneck on purpose — slow the downstream and watch the bounded channel fill.

**Deliverables:**
- Configurable slow consumer (fault injection)
- Producer throttling + explicit load shedding (429/drop, FR-4)
- Adaptive batching: batch size grows/shrinks with queue depth
- Queue-depth metric + accepted/rejected counters

**Exit criteria:** At 2x overload: zero unbounded memory growth; P99 of *accepted* events stays bounded; queue-depth trace before/after adaptive batching documented (perf.md P2 row).

**Primary lesson:** backpressure, load shedding, adaptive batching — the single most valuable concept in the project.

---

## Phase 3 — Parallel processing

**Lights up:** WAL → Partitioner → Workers 1..N → Aggregator

**Scope:** Split the single worker across N workers; make the concurrency model swappable.

**Deliverables:**
- N worker tasks consuming from a shared queue (round-robin initially)
- `--model tokio|threads|rayon` runtime switch
- CPU-bound vs IO-bound workload toggles (simulated enrichment/parse vs sleep/disk)
- Throughput-vs-workers sweep for all 6 combinations (2 workloads × 3 models)

**Exit criteria:** Benchmark table showing where each model wins and the point where async stops helping; workers scaled to at least physical core count without regression.

**Primary lesson:** the real difference between async, OS threads, and Rayon — felt, not read.

---

## Phase 4 — Partitioning

**Lights up:** WAL → Partitioner → Workers 1..N (sharded channels)

**Scope:** Route by `hash(user_id) % N` to per-shard channels with exactly one consumer each.

**Deliverables:**
- Partitioner with stable fast hash (FxHash/xxhash)
- N shard channels, one worker each (ADR-004)
- Per-key ordering verification under load
- Zipf skew test exposing the hot-key limitation

**Exit criteria:** Ordering proof (monotonic per-key offsets) at 100k events/s; per-shard lag metrics on the dashboard; hot-shard skew documented.

**Primary lesson:** per-key ordering + horizontal scalability without cross-worker locks.

---

## Phase 5 — Persistent log (WAL)

**Lights up:** Queue → WAL (+ checkpoint/replay edge to Recovery)

**Scope:** Build the Kafka-style append-only log by hand (ADR-003).

**Deliverables:**
- Segment files with rotation at size threshold
- Sequential append writes, batch-level fsync (policy: `record|batch|interval:Nms`)
- Per-record CRC32 + offset assignment
- Torn-write recovery (truncate at last good CRC)
- fsync-ladder benchmark (perf.md §4)

**Exit criteria:** Zero acknowledged-event loss across kill -9 (single-run first); durability/throughput curve measured and plotted.

**Primary lesson:** where the project stops being "a Rust project" and becomes systems engineering.

---

## Phase 6 — Crash recovery

**Lights up:** WAL ↔ Recovery ↔ Partitioner (replay path)

**Scope:** Kill mid-processing, recover, resume.

**Deliverables:**
- Checkpoints (shard → committed offset) to Postgres, configurable interval
- Replay from last committed offset on startup
- event_id dedup making replay idempotent (ADR-005)
- `crash-test` harness: 10 consecutive kill -9 runs

**Exit criteria:** All 10 runs: zero loss, zero duplicate *effects* after dedup, recovery < 10s with a 10M-event WAL; checkpoint overhead measured (events/s with vs without).

**Primary lesson:** idempotency pays off; durability and processing consistency.

---

## Phase 7 — Real-time analytics

**Lights up:** Workers → Windowing + Dedup aggregator

**Scope:** The miniature stream-processing engine on top of the shards.

**Deliverables:**
- Streaming aggregations: count, sum, avg; P² sketch for percentiles
- Tumbling + sliding windows (ring buffer for sliding)
- Event-time processing with watermarks; late-event policy (drop/side-output/recompute)
- Window state checkpointed with shard offsets

**Exit criteria:** Correct windowed aggregates vs a batch-computed oracle on the same event stream (property test); late-event rate and watermark slack tunable and measured.

**Primary lesson:** event-time vs processing-time, watermarks, streaming aggregation state.

---

## Phase 8 — Storage optimization

**Lights up:** Aggregator → Postgres / Parquet / Redis

**Scope:** The serialization matrix and sink tiering.

**Deliverables:**
- Codecs behind a trait: JSON → MessagePack → Protobuf (`prost`), with versioned schema
- Parquet writer for events + aggregates (arrow-rs)
- Redis read-through cache for hot lookups with Postgres fall-through
- Benchmarks: bytes/event, encode/decode ns/event, 1M-row scan, decode memory (perf.md P8 row)

**Exit criteria:** Winning codec adopted for the hot path with an ADR-009 amendment containing the result table; scan-time comparison JSON vs Parquet ≥ 10x or explained.

**Primary lesson:** serialization economics; tiered storage by access pattern.

---

## Phase 9 — Production infra + perf lab

**Lights up:** Gateway/Workers/Aggregator → Observability stack (all three dashed edges)

**Scope:** Make it operable and prove the numbers, visibly.

**Deliverables:**
- Prometheus + Grafana + OTel wired through every stage
- Grafana dashboard: throughput, P99, memory, queue depth, per-shard lag, dedup hit rate, sink latencies
- Health checks; graceful shutdown (drain, flush WAL, close segments)
- Sink retries with backoff + circuit breakers
- Live-tunable knobs: batch size, worker count, fsync policy
- Final SLO gate run at 182k/s (perf.md §4) with results appended to `docs/performance-results.md`

**Exit criteria:** All PRD §7 release criteria met — full SLO dashboard evidence, 10/10 crash test, reproducible benchmark commands.

**Primary lesson:** observability as a feature; a system you can *prove* works, not one you believe works.

---

## Phase map → docs cross-reference

| Phase | PRD reqs | ADRs | perf.md row |
|---|---|---|---|
| 1 | FR-1..FR-3 | ADR-001, ADR-002, ADR-010 | P1 |
| 2 | FR-4 | ADR-007 | P2 |
| 3 | FR-10 | ADR-002 | P3 |
| 4 | FR-9 | ADR-004 | P4 |
| 5 | FR-5..FR-7 | ADR-003 | P5 |
| 6 | FR-8, FR-18, FR-19 | ADR-003, ADR-005 | P6 |
| 7 | FR-11..FR-13 | ADR-005 | P7 |
| 8 | FR-14..FR-17 | ADR-006, ADR-009 | P8 |
| 9 | FR-20..FR-23, all NFRs | ADR-008, ADR-010 | P9 |
