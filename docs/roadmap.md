# StreamForge — Roadmap: remaining work and detailed phase plans

Status overview plus the detailed plan for every unfinished step, P1 leftovers
through Phase 9. Source requirements: `prd.md` (FR/NFR), `phases.md` (scope),
`performance.md` (benchmarks), `adr.md` (decisions), `coding-standards.md` (rules).

Related: `phase-01-plan.md` (done), `phase-02-plan.md` (next, executable detail).

---

## 1. Where we stand

Phase 1 scaffold is implemented and green (`fmt --check`, `clippy` zero warnings,
5 unit + 3 integration + 2 doctests pass) but **uncommitted**. Two boxes remain open:

| # | Item | Status | Blocks |
|---|---|---|---|
| P1-a | Commit Phase 1 scaffold (code + `coding-standards.md` + `phase-01-plan.md`) | Open | Clean baseline for all later work |
| P1-b | `PostgresSink: Sink` impl via `sqlx` | Open | Phase 1 "writes to Postgres"; P6 checkpoints need Postgres |
| P1-c | `src/bin/loadgen.rs` + 1k/10k/100k rate curve | Open | Phase 1 exit criteria; grows into the P2–P9 bench harness |

**Decision (recommended, adopted):** Postgres lands now with `infra/docker-compose.yml`
for local dev; `loadgen.rs` grows into the canonical `bench` binary from
`performance.md` §4 rather than adopting `criterion` (criterion suits micro-benches;
we need a system load driver).

---

## 2. P1 finish — Postgres sink + load generator (closes Phase 1)

**Goal:** satisfy the Phase 1 deliverables verbatim: batch worker writing to
Postgres, load generator, first throughput/latency curve.

- **Code:**
  - `Cargo.toml`: add `sqlx` (features `postgres`, `runtime-tokio`, `tls-native-tls`).
  - `src/sink.rs`: add `PostgresSink { pool: sqlx::PgPool }` implementing `Sink`
    (batch `INSERT … UNNEST`, no pipeline changes — proves the trait seam).
    Keep `InMemorySink` for tests.
  - `src/bin/loadgen.rs` (`proj-bin-dir`): clap-driven driver with
    `--rate/--duration/--size/--skew` (performance.md §3); fixed-rate control,
    reports accepted count + client-observed latency histogram.
  - `infra/docker-compose.yml`: Postgres (and Redis, commented for P8) for local dev.
  - `docs/performance-results.md` (new, append-only): env header + 1k/10k/100k table + knee paragraph.
- **Tests:** `PostgresSink` round-trip behind `#[ignore]` (needs DB;
  `test-fixture-raii` for table cleanup); loadgen dry-run test.
- **Exit:** knee identified in one paragraph in `performance-results.md`; P1 boxes ticked.

## 3. Phase 2 — Backpressure (FR-4, ADR-007; perf P2)

**Goal:** overload becomes signal, never memory death. Full executable detail lives in
`phase-02-plan.md`; summary here:

- **Code:** `src/metrics.rs` (queue-depth gauge, accepted/rejected counters, emitted via
  `tracing` for now); gateway `try_send` → `429` with `Retry-After` + JSON reason;
  adaptive batch sizing in `pipeline.rs` (grow/shrink with depth); loadgen
  `--slow-sink 2x` fault injection; queue-depth CSV report.
- **Measurement:** 2x-overload drill; queue-depth trace before/after adaptive batching;
  P99 of *accepted* events bounded; accepted-vs-rejected counts.
- **Exit:** zero unbounded growth at 2x overload; trace + paragraph appended to
  `performance-results.md` (PRD §6 backpressure row).

## 4. Phase 3 — Parallel processing (FR-10, ADR-002; perf P3)

**Goal:** feel the difference between Tokio tasks, OS threads, and Rayon.

- **Code:** split `src/worker.rs` out of `pipeline.rs`; `--model tokio|threads|rayon`
  runtime switch (+`rayon` dep); CPU-bound vs IO-bound workload toggle
  (parse/enrich vs sleep); shared-queue round-robin fan-out; CPU-bound work via
  `spawn_blocking`/Rayon, never the async worker (`async-spawn-blocking`).
- **Measurement:** throughput-vs-workers (1…N…2N) × 6 combos; crossover point where
  async stops helping (performance.md §6 envelope).
- **Tests:** each model drains N events; no-loss assertion per model.
- **Exit:** benchmark table in `performance-results.md`; workers scale to core count.

## 5. Phase 4 — Partitioning (FR-9, ADR-004; perf P4)

**Goal:** per-key ordering with horizontal scale, shared-nothing shards.

- **Code:** `src/partition.rs` (`hash(user_id) % N`, `fxhash`/`xxhash` — fast,
  non-DoS hasher per `perf-ahash`); N shard channels, one consumer each;
  keyless events round-robin; per-shard lag gauges in `metrics.rs`;
  loadgen `--skew` (Zipf) to expose hot-key behaviour.
- **Measurement:** ordering proof (monotonic per-key offsets) at 100k events/s;
  per-shard lag under skew; document hot-shard limitation (ADR-004).
- **Tests:** same-key ordering under concurrency; key distribution uniformity test.
- **Exit:** ordering proof + skew paragraph in `performance-results.md`.

## 6. Phase 5 — Persistent log / WAL (FR-5..FR-7, ADR-003; perf P5)

**Goal:** hand-rolled Kafka-style log; durability before processing.

- **Code:** `src/wal/` folder (first justified split, ~300–500 LOC):
  `segment.rs` (append writer, rotation at size threshold), `reader.rs`
  (offset reads), `record.rs` (`[crc32][len][payload]` framing, `crc32fast` dep),
  `fsync.rs` (policy `record|batch|interval:Nms`, `tokio::fs` per `async-tokio-fs`).
  Torn-write recovery: truncate at last good CRC. Single writer, many readers.
- **Measurement:** fsync ladder (record vs batch vs interval) — expect ~1–5k/s floor
  on per-record fsync (physics, performance.md §6); WAL write-latency histogram;
  rotation stall check.
- **Tests:** torn-tail truncation (crafted corrupt segment); rotation boundary;
  single-run kill -9 zero-loss.
- **Exit:** zero acknowledged-event loss across kill -9; durability/throughput curve plotted.

## 7. Phase 6 — Crash recovery (FR-8, FR-18, FR-19, ADR-005; perf P6)

**Goal:** kill mid-processing, recover, resume — idempotently.

- **Code:** `src/recovery.rs` (WAL scan → last checkpoint from Postgres → replay
  from committed offset); checkpoint writer (shard → offset, configurable interval);
  `src/dedup.rs` (per-shard bounded LRU/bloom of `event_id`s, checkpointed with
  offsets per ADR-005); `src/bin/crash-test.rs` harness
  (`--runs 10 --wal-events 10M`, performance.md §4).
- **Measurement:** recovery time vs backlog; duplicate *effects* = 0 after dedup;
  checkpoint overhead (events/s with vs without); 10 consecutive kill -9 runs.
- **Tests:** replay dedup correctness; checkpoint round-trip; crash-test in CI from here.
- **Exit:** 10/10 runs, zero loss, recovery < 10s on 10M-event WAL (NFR-4/5).

## 8. Phase 7 — Real-time analytics (FR-11..FR-13; perf P7)

**Goal:** miniature stream engine on top of the shards.

- **Code:** `src/aggregate/` folder: `window.rs` (tumbling + sliding via ring buffer;
  count/sum/avg), `percentile.rs` (P² sketch), `watermark.rs` (event-time tracking,
  watermark slack config), late-event policy (drop/side-output/recompute — never
  silent, FR-12); window state checkpointed with shard offsets; dedup feeds
  idempotent aggregate updates.
- **Measurement:** events/s with windowing on vs off; per-window emit latency;
  late-event rate vs slack; P² accuracy vs exact oracle.
- **Tests:** windowed aggregates vs batch-computed oracle (`proptest`,
  `test-proptest-properties`); watermark/late-event unit tests.
- **Exit:** oracle property test green; cost-of-windows paragraph in results.

## 9. Phase 8 — Storage optimization (FR-14..FR-17, ADR-006/009; perf P8)

**Goal:** serialization economics + tiered sinks, chosen by measurement.

- **Code:** `src/codec/` folder: `Codec` trait + `json`/`msgpack`/`protobuf`
  (`prost`, versioned schema — format switch is a codec change, not a model change);
  Parquet writer (`arrow`/`parquet` crates); Redis read-through cache with Postgres
  fall-through + circuit breaker (`redis`); sink retry/backoff scaffolding (FR-22,
  completed P9).
- **Measurement:** bytes/event, encode/decode ns/event, 1M-row scan, decode memory
  for all four formats; JSON-vs-Parquet scan ≥ 10x or explained.
- **Docs:** ADR-009 amendment with the winning-codec table; hot-path default switched.
- **Exit:** winner adopted; matrix in `performance-results.md`.

## 10. Phase 9 — Production infra + perf lab (FR-20..FR-23, all NFRs; perf P9)

**Goal:** operable system with visible proof of every SLO.

- **Code:** `metrics.rs` → Prometheus exporter (`metrics` + `metrics-exporter-prometheus`);
  OTel spans per batch (`tracing-opentelemetry`); retries + circuit breakers on all
  sinks; live knobs (batch size, worker count, fsync policy); `bench --assert-slo
  --dashboard` gate; `infra/grafana/` dashboard JSON (throughput, P99, memory,
  queue depth, per-shard lag, dedup hit rate, sink latencies).
- **Measurement:** full SLO gate at 182k/s, 300s run; knob sweeps plotted
  throughput/P99; dashboard evidence.
- **Docs:** PRD §7 release checklist signed off; final numbers appended to results.
- **Exit:** all FR-1..FR-23 demonstrated; 10/10 crash test; reproducible commands.

---

## 11. Folder structure evolution

Stay flat until a module passes ~300–500 lines (`proj-flat-small`). `wal/` is the
first justified folder split.

```
P1 now:     src/{main,lib,config,error,event,gateway,pipeline,sink,telemetry}.rs
            tests/ingest.rs
P1 finish:  + src/bin/loadgen.rs + infra/docker-compose.yml
P2:         + src/metrics.rs
P3:         + src/worker.rs              (split from pipeline.rs)
P4:         + src/partition.rs
P5:         + src/wal/{mod,segment,reader,record,fsync}.rs
P6:         + src/recovery.rs + src/dedup.rs + src/bin/crash-test.rs
P7:         + src/aggregate/{mod,window,percentile,watermark}.rs
P8:         + src/codec/{mod,json,msgpack,protobuf}.rs
P9:         + infra/grafana/  (metrics.rs gains exporter)
```

End state ≈ 25 files, still single crate, no workspace (workspace only if a second
binary crate or proc-macro forces it — `proj-workspace-large`).

## 12. Doc index for remaining work

| Doc | Purpose | Created |
|---|---|---|
| `docs/roadmap.md` (this file) | Status + detailed plan per phase + folder evolution | Now |
| `docs/phase-02-plan.md` | Executable P2 plan (module map, decisions, checklist) | Now |
| `docs/phase-0X-plan.md` (3–9) | Same template, one per phase | Just-in-time at phase start |
| `docs/performance-results.md` | Append-only benchmark history (never edited in place) | P1-c, appended every phase |
| `docs/adr.md` | Amend ADR-009 after P8; new ADRs only for reversals | As needed |
