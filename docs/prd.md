# StreamForge — Product Requirements Document (PRD)

Status: Draft v1.0 · Owner: amitxparmar · Source: `docs/streamforge-architecture.html`

---

## 1. Summary

StreamForge is a high-throughput, single-node event ingestion and stream processing engine written in Rust. It ingests events from HTTP/WebSocket clients, durably logs them, processes them in parallel by key, runs streaming aggregations, and routes results into tiered storage (PostgreSQL / Parquet / Redis). The project is both a usable engine and a deliberate systems-engineering curriculum: every major distributed-systems concept is built and measured, not just read about.

## 2. Goals

### 2.1 Business / product goals

- G1: Deliver a working stream processing engine capable of **182k events/s sustained** with **P99 ≤ 18ms** ingestion-to-sink on commodity hardware.
- G2: Provide a **live benchmark dashboard** (throughput, P99, memory, queue depth) that demonstrates the system's performance under load with real numbers.
- G3: Survive process crashes with **zero data loss** (WAL + checkpoint/replay recovery).
- G4: Serve as a reference implementation that teaches backpressure, durability, partitioning, concurrency, and observability through measurable phases.

### 2.2 Non-goals (explicitly out of scope)

- Multi-node / distributed clustering (consensus, replication, cross-node rebalancing)
- SQL-style query interface over streams
- Exactly-once delivery guarantees (at-least-once + idempotency instead)
- Multi-tenant auth/billing infrastructure
- GPU acceleration

## 3. Users & personas

| Persona | Needs |
|---|---|
| **Backend event producers** (services emitting telemetry/activity events) | Low-latency, reliable ingestion endpoint (HTTP/WS) that doesn't drop events silently; clear backpressure signals (429 + retry guidance) |
| **Data engineers / analysts** | Aggregated, windowed output in Parquet; ability to query historical events efficiently |
| **Systems engineers (primary: the builder)** | Real numbers for each design choice (Tokio vs threads, JSON vs Protobuf), tunable knobs, visible queue/latency metrics |
| **Ops / SRE** | Prometheus/Grafana dashboards, health checks, graceful shutdown, crash recovery that "just works" |

## 4. Functional requirements

### 4.1 Ingestion

- FR-1: Accept events via HTTP POST and WebSocket.
- FR-2: Validate payloads (schema, size limits, well-formedness) before acceptance; reject invalid events with a 4xx and reason.
- FR-3: Batch events before downstream handoff (size- or time-triggered).
- FR-4: Under overload, apply **backpressure**: first throttle producers, then shed load (reject with 429 or drop, per policy). Clients must be able to distinguish "rejected" from "accepted".

### 4.2 Durability

- FR-5: Persist all accepted events to a WAL **before** processing (write-ahead).
- FR-6: WAL is append-only, segmented, with per-record checksums and monotonic offsets.
- FR-7: Support segment rotation and corruption detection (truncate torn tail records on recovery).
- FR-8: Support checkpointing processed offsets to PostgreSQL at configurable intervals.

### 4.3 Processing

- FR-9: Route events to N worker shards via `hash(user_id) % N`; the same key must always map to the same shard (per-key ordering).
- FR-10: Worker concurrency model is configurable: Tokio tasks, OS threads, or Rayon pool.
- FR-11: Streaming aggregations: count, sum, avg, percentiles over **tumbling** and **sliding** windows.
- FR-12: Event-time processing with watermarks; late events handled by explicit policy (drop / side-output / recompute), never silently lost.
- FR-13: Dedup by event_id so replayed events (at-least-once) are processed exactly once logically (idempotent processing).

### 4.4 Storage

- FR-14: Durable state, checkpoints, and transactional data → PostgreSQL.
- FR-15: Historical/aggregated events → Parquet (columnar).
- FR-16: Repeated hot lookups → Redis cache with fall-through to Postgres when Redis is down.
- FR-17: Support benchmarked serialization formats (JSON / MessagePack / Protobuf) with a documented winner for the hot path.

### 4.5 Recovery

- FR-18: On restart after crash, read the WAL, load the last checkpoint, and replay uncommitted events idempotently.
- FR-19: Recovery must complete within a bounded, measurable time (target: < 10s for 10M buffered events).

### 4.6 Operations

- FR-20: Prometheus metrics from every stage; OpenTelemetry traces across the pipeline; structured logs correlated by trace_id.
- FR-21: Health check endpoint; graceful shutdown (drain in-flight events, flush WAL, close segments).
- FR-22: Retries with backoff + circuit breakers on sink writes (Postgres/Redis).
- FR-23: Live-tunable knobs at runtime: batch size, worker count, fsync policy.

## 5. Non-functional requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-1 | Sustained throughput | ≥ 182,000 events/s (1KB avg event, single node) |
| NFR-2 | Tail latency | P99 ≤ 18ms ingest→sink, P50 ≤ 5ms |
| NFR-3 | Memory | Bounded, ≤ ~412MB RSS under target load; no unbounded queues/buffers |
| NFR-4 | Durability | Zero data loss for acknowledged events across process crash (kill -9) |
| NFR-5 | Recovery time | < 10s to resume processing after crash (10M-event WAL) |
| NFR-6 | CPU | Scales to N cores via sharding; documented diminishing-returns point |
| NFR-7 | Availability | Graceful degradation: shed load rather than collapse; sinks degrade, ingestion survives |
| NFR-8 | Observability | Every stage emits metrics + traces; regressions visible on dashboard |

## 6. Success metrics

| Metric | How measured | Phase gate |
|---|---|---|
| Throughput 1k → 10k → 100k+ events/s | load generator + Prometheus counter | P1, P3, P9 |
| P99 latency under load | histogram per stage + end-to-end | P9 |
| Zero-loss crash test | kill -9 during processing, verify count parity | P6 |
| Serialization benchmark | size/speed/CPU matrix for 4 formats | P8 |
| Backpressure demonstration | queue-depth vs. input-rate graph before/after adaptive batching | P2 |

## 7. Release criteria (Phase 9 exit)

1. All FR-1..FR-23 implemented and demonstrated.
2. Perf lab dashboard live with: throughput, P99, memory, queue depth, tunable knobs.
3. Crash-recovery test passes 10 consecutive kill -9 runs with zero acknowledged-event loss.
4. Benchmarks documented in `docs/performance.md` with reproducible commands.

## 8. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| fsync cost dominates at high rates | throughput floor | batch-level fsync policy; tunable durability/latency tradeoff |
| Hot key skews one shard | per-key lag, uneven throughput | detect via per-shard metrics; document resharding path |
| Async/runtime learning curve stalls P1 | schedule slip | Phase 1 scope is deliberately tiny (in-memory, one worker) |
| WAL bugs silently corrupt | data loss | per-record CRC + crash-test harness in CI from P5 onward |
| Scope creep toward distributed | never ships | Non-goals enforced; ADR-008 records the decision |
