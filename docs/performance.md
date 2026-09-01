# StreamForge — Performance Specification & Benchmark Plan

Companion to the interactive Perf Lab card in `docs/streamforge-architecture.html` (Phase 9).
All targets are single-node, commodity hardware (modern x86_64, NVMe SSD, ≥ 8 cores).

---

## 1. SLOs and current targets

| Metric | Lab target | Notes / source |
|---|---|---|
| Sustained throughput | **182,000 events/s** | 1KB avg event, accepted → WAL → processed → sinked |
| End-to-end P99 latency | **≤ 18 ms** | ingest accept → aggregate emitted; P50 target ≤ 5ms |
| Memory (RSS) | **≤ ~412 MB** under target load | bounded queues must keep this flat as rate rises |
| Queue depth (steady) | **≤ ~1,284 events** | primary overload canary; spikes precede P99 blowup |
| Data loss | **0** acknowledged events | across `kill -9` during processing |
| Recovery time | **< 10 s** | 10M-event WAL, checkpoint + replay to steady state |

Numbers in bold are the "lab targets" shown on the architecture dashboard; they are the Phase 9 exit bar, not marketing claims. Every number must be reproducible with a command (§4).

---

## 2. Benchmark matrix (what gets measured, per phase)

| Phase | Question the benchmark answers | Measurement |
|---|---|---|
| P1 — Engine | Where does a naive async pipeline saturate? | 1k → 10k → 100k events/s vs latency curve; find the knee |
| P2 — Backpressure | What happens when downstream is 2x slower than input? | queue-depth trace before/after adaptive batching + shedding; accepted vs rejected counts; P99 of *accepted* events stays bounded |
| P3 — Parallelism | Tokio tasks vs OS threads vs Rayon, IO-bound vs CPU-bound workload | throughput vs worker count (1..N..2N) per model; crossover point where async stops helping |
| P4 — Partitioning | Does per-key ordering hold at scale? | ordering verification (monotonic offset per key) under load; skew test with Zipf-distributed keys; per-shard lag |
| P5 — WAL | What does durability cost? | events/s at fsync-per-record vs fsync-per-batch vs fsync-per-N-ms; WAL write latency histogram; segment rotation stall check |
| P6 — Recovery | How long is the gap after a crash? | recovery time vs WAL backlog size; duplicate-processing count (must be 0 after dedup); 10 consecutive kill -9 runs, zero loss |
| P7 — Analytics | What do windows cost? | events/s with windowing+dédup on vs off; per-window emit latency; late-event rate vs watermark slack; P² percentile accuracy vs exact |
| P8 — Storage | Which serialization wins? | JSON vs MessagePack vs Protobuf vs Parquet: bytes/event, encode ns/event, decode ns/event, scan time for a 1M-row aggregate, memory during decode |
| P9 — Prod/Lab | Does the whole system hit SLOs, visibly? | dashboard screenshots at 182k/s; knob sweeps (batch size, workers, fsync policy) plotted against throughput/P99 |

---

## 3. Methodology rules

1. **Release builds only** (`cargo run --release`); no debug-profile numbers ever enter docs.
2. **Warm-up**: discard the first 30s of each run (page cache, JIT-ish effects, connection ramp).
3. **Duration**: each point ≥ 60s steady state; each config re-run ≥ 3 times, report median + min/max.
3. **Synthetic load generator**: in-process or separate host, with fixed rate control (`--rate`), Zipf key skew option (`--skew`), and size-configurable payloads (`--size`).
4. **Record environment**: CPU model, governor (must be `performance`), Rust version, sync mode, disk model — top of every results table.
5. **Attribution before optimization**: P99 must be decomposed per stage via OTel spans before any tuning commit; no changes without a before/after number.
6. **Every result is a command**: benches are committed binaries (`cargo run --release -- bench ...`) so numbers are reproducible.

---

## 4. Benchmark commands (canonical)

```bash
# Throughput sweep (Phase 1/3/9)
cargo run --release -- bench --rate 1000   --duration 60
cargo run --release -- bench --rate 10000  --duration 60
cargo run --release -- bench --rate 100000 --duration 60
cargo run --release -- bench --rate 182000 --duration 60 --assert-slo

# Backpressure drill (Phase 2): slow sink injected at t=20s
cargo run --release -- bench --rate 150000 --slow-sink 2x --report queue-depth.csv

# Concurrency model shootout (Phase 3)
cargo run --release -- bench --rate 200000 --workers 8 --model tokio
cargo run --release -- bench --rate 200000 --workers 8 --model threads
cargo run --release -- bench --rate 200000 --workers 8 --model rayon

# WAL durability ladder (Phase 5)
cargo run --release -- bench --rate 100000 --fsync record
cargo run --release -- bench --rate 100000 --fsync batch
cargo run --release -- bench --rate 100000 --fsync interval:100ms

# Crash-recovery suite (Phase 6): 10 runs must all pass
cargo run --release -- crash-test --runs 10 --wal-events 10M

# Serialization matrix (Phase 8)
cargo run --release -- bench --codec json --codec msgpack --codec protobuf --store parquet

# Serialization shootout → full SLO gate (Phase 9)
cargo run --release -- bench --rate 182000 --duration 300 --assert-slo --dashboard
```

---

## 5. Profiling toolkit

| Tool | Used for |
|---|---|
| `tokio-console` | task scheduling stalls, channel contention, await-point hotspots |
| `perf` + `flamegraph` (cargo-flamegraph) | CPU-bound phases (workers, serialization, windowing) |
| `strace` / `io_uring` visibility | syscall counts per event (goal: amortized O(1/batch)) |
| `heaptrack` / `valgrind massif` | heap growth, per-batch allocations that break the 412MB bar |
| Prometheus histograms | production-shape P99 attribution per stage |
| `iostat -x` / `fio` baseline | separate disk saturation from engine saturation |

---

## 6. Expected failure envelopes (what "bending" should look like)

- **fsync per record**: throughput floors around 1/(fsync-latency) ≈ 1–5k events/s on NVMe — this is physics, not a bug; the ladder in §4 quantifies the durability/throughput curve.
- **Async on CPU-bound work**: Tokio ≈ threads ≈ Rayon at low core counts, Rayon pulls ahead as CPU share dominates; async stops helping when the CPU is the bottleneck — document the crossover.
- **Skewed keys (Zipf s>1)**: aggregate throughput holds, but the hot shard's lag grows unboundedly while others idle — the documented hot-key limitation (ADR-004).
- **Queue depth → P99**: P99 degrades *exponentially* once queue depth crosses ~70% of capacity; this co-linearity is the dashboard's early-warning panel.

## 7. Regression policy

- Benchmarks in §4 wired into `just bench` / CI on PRs touching gateway, WAL, partitioner, workers, or sinks.
- A 10%+ throughput or P99 regression on the canonical 182k/s run blocks merge until explained or fixed.
- New numbers append to `docs/performance-results.md` (created in Phase 9) — tables are append-only history, never edited in place.
