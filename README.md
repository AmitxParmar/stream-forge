# StreamForge

A high-throughput event ingestion and stream processing engine in Rust — built phase by phase, from a single-node async engine to a durable, recoverable, observable production-grade system.

```
Event Clients → Ingestion Gateway → Bounded Queue → WAL → Partitioner → Workers → Windowing/Dedup → PostgreSQL / Parquet / Redis
```

## What it is

StreamForge ingests events from HTTP/WebSocket clients, persists them to an append-only log (WAL), routes them by key across N parallel worker shards, runs streaming aggregations (windowing + dedup), and sinks results into tiered storage:

- **PostgreSQL** — durable OLTP state, checkpoints, offsets
- **Parquet** — columnar analytics store
- **Redis** — hot-path cache for repeated lookups

The end goal is a production-grade system with real numbers behind it: ~182k events/s sustained throughput, ~18ms P99 latency, and a live benchmark dashboard (Prometheus + Grafana + OpenTelemetry).

## Why build it

Most "stream processing" tutorials stop at a toy. StreamForge is a learning vehicle for real systems engineering:

- **Backpressure** — bounded queues, load shedding, adaptive batching under overload
- **Durability** — a hand-rolled Kafka-style WAL: segments, fsync, offsets, checksums
- **Crash recovery** — checkpoint + replay, idempotent processing
- **Partitioning** — `hash(user_id) % N` for per-key ordering with horizontal scale
- **Concurrency tradeoffs** — Tokio tasks vs OS threads vs Rayon, IO-bound vs CPU-bound
- **Serialization economics** — JSON vs MessagePack vs Protobuf vs Parquet, measured
- **Observability** — every stage emits metrics and traces; regressions are visible, not guessed

## Documentation

| Doc | Contents |
|---|---|
| [docs/prd.md](docs/prd.md) | Product requirements: goals, users, functional & non-functional requirements, success metrics |
| [docs/architecture.md](docs/architecture.md) | High-level and low-level diagrams, data flow, component specs, failure modes |
| [docs/adr.md](docs/adr.md) | Architecture decision records with reasoning (language, runtime, WAL, partitioning, storage, serialization) |
| [docs/phases.md](docs/phases.md) | The 9-phase build plan: scope, deliverables, exit criteria per phase |
| [docs/phase-01-plan.md](docs/phase-01-plan.md) | Phase 1 plan: module map, decisions, task checklist |
| [docs/roadmap.md](docs/roadmap.md) | Remaining work: status, detailed P1-finish–P9 plans, folder evolution |
| [docs/phase-02-plan.md](docs/phase-02-plan.md) | Phase 2 plan: backpressure, shedding, adaptive batching |
| [docs/coding-standards.md](docs/coding-standards.md) | Project-wide Rust practices (rust-skills rule IDs) |
| [docs/performance.md](docs/performance.md) | SLOs, benchmark methodology, phase targets, profiling toolkit |
| [docs/streamforge-architecture.html](docs/streamforge-architecture.html) | Interactive clickable architecture map with phase overlays |

## Quick start

```bash
# Build and run
cargo run --release

# Run the performance lab (Phase 9)
cargo run --release -- bench --rate 100000

# View architecture
open docs/streamforge-architecture.html
```

## License

MIT
