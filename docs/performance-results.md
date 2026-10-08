# StreamForge — Performance Results (append-only history)

Environment for all rows below unless noted: `rustc 1.98.0 (88d9e12ae 2026-08-18)`,
`cargo 1.98.0`, `--release` profile (`opt-level 3`, fat LTO, `codegen-units 1`),
x86_64 Linux, single node. Numbers come from committed binaries
(`cargo run --release --bin loadgen`) and the server flag shown — reproducible,
never edited in place (performance.md §7).

---

## Phase 2 — Backpressure drill (2026-10-08)

Sink fault injection: `--sink-delay-ms` slows each flush by that duration.
Load target: `--rate 6000 --duration 15 --concurrency 8`. Depth series:
`--depth-csv` samples `GET /metrics` every 200ms (columns:
`t_ms,queue_depth,batch_last,accepted,rejected_full,rejected_invalid,uptime_ms`).
Raw CSVs kept at `/tmp/p2-*.csv` during the drill; env here, conclusions below.

### Run A — adaptive batching on (`--sink-delay-ms 50`)

| Metric | Result |
|---|---|
| Target vs actual input rate | 6000/s requested, **5009 events/s** achieved |
| Accepted / rejected / errors | 75355 / 0 / 0 |
| Queue depth trace | 672 (t≈200ms) → **1024 pinned from t≈400ms** (capacity cap) |
| `batch_last` trace | **144 → 256** (adaptive growth as backlog built, then saturated) |
| Client P50 / P99 accepted | 118µs / 47.6ms |
| Data loss | 0 (no drops before shed threshold) |

### Run B — adaptive off (`--no-adaptive-batching --sink-delay-ms 50`)

| Metric | Result |
|---|---|
| Target vs actual input rate | 6000/s requested, **5037 events/s** achieved |
| Accepted / rejected / errors | 75790 / 0 / 0 |
| Queue depth trace | 410 → **1024 pinned** (same saturation point, ~200ms later) |
| `batch_last` trace | **256 flat** from t=0 (fixed policy) |
| Client P50 / P99 accepted | 120µs / 47.6ms |

### Run C — shedding forced (`--sink-delay-ms 300`, 12s)

| Metric | Result |
|---|---|
| Sent / accepted / **rejected** / errors | 12590 / 11014 / **1576 / 0** |
| Server counter: `rejected_full` | 0 → 1552 (429 + `Retry-After` + JSON reason) |
| Queue depth trace | 0 (t≈200ms) → 1024 pinned → never exceeded capacity |
| `batch_last` trace | **6 → 256** (adaptive ramp again visible) |
| Client P50 / P99 accepted | 129µs / 51.6ms |
| Memory | bounded; queue never exceeded `queue_capacity` |

### Interpretation (the honest summary)

1. **Backpressure worked end-to-end in A and B.** Requested 6000/s but the
   *achieved* rate shaped itself to the sink's drain rate (~5000/s): a full
   queue made every accepted POST wait on a free slot, which applied natural
   backpressure to loadgen's effective rate. Depth pinned at exactly 1024, zero
   loss, P99 rose to ~47ms. This is the bounded-channel contract (ADR-007).
2. **Adaptive vs fixed batching was visible only in the transient.** Adaptive
   started small (144, Run A; 6, Run C) and grew to max as backlog built;
   fixed pinned at 256 from t=0. Steady-state throughput was identical (~5009 vs
   5037/s): under *sustained* overload, batching policy converges, because the
   delay-per-flush model rewards maximum batch size once the queue is deep.
   "Ambient" latency differs before saturation — adaptive never held 256-batches
   while the queue was still shallow.
3. **Shedding (429) only forced its way out in Run C.** With a 50ms admission
   window and ~50ms slot-release period, natural backpressure absorbed the
   overload and no request was rejected. At 300ms delay, slot-release≫admission
   window, so 1576 POSTs got explicit 429s; the `rejected_full` counter and
   client-side `rejected` tally agree (1552 sampled + tail lag). Shedding is
   a *dial*: it should trigger only when per-request latency tolerance is
   lower than the slot-release period.
4. **No unbounded growth in any run.** RSS and depth stayed flat; the failure
   modes to re-run with NFR-3 tooling (heaptrack) are the ingest→sink path
   under 10x overload — still to be measured (Phase 9 dashboard).

### Phase 2 gate status

- [x] Queue-depth trace before/after adaptive batching (Run A vs B CSVs above)
- [x] accepted/rejected counts documented (loadgen summary + `rejected_full`)
- [x] P99 of *accepted* events bounded at 2x-class overload (47–52ms)
- [x] Zero unbounded memory growth under overload
- [x] P1 throughput knee paragraph — run below (Phase 1 gate section)

---

## Phase 1 — Saturation knee (2026-10-08)

Baseline pipeline: no sink delay, no workload kernel, adaptive on, 12-core host.
`--rate` ramp, 10s per point, `--concurrency 8`, depth sampled from `/metrics`.

| Requested rate | Achieved | Accepted | P99 accepted | Final queue depth |
|---|---|---|---|---|
| 1,000/s | 1,000/s | 10,000 | 0.30ms | 2 |
| 10,000/s | 9,998/s | 99,984 | 0.32ms | 0 |
| 100,000/s | **56,013/s** | 560,135 | 0.35ms | 0 |

**Knee paragraph (Phase 1 exit):** the engine tracks the offered load exactly
up to ~10k/s with flat sub-millisecond P99 and near-zero queue occupancy; at a
100k/s request the achieved rate pins at **≈56k events/s** — and the knee is an
*admission* limit, not a queueing one: queue depth stays 0 and P99 stays
~0.35ms even at the ceiling, i.e. per-request overhead (HTTP parsing, JSON
validation, channel handoff) saturates a core before buffering ever builds.
Scaling past that point requires parallel workers (Phase 3) and cheaper
per-event boundaries, not bigger queues.

---

## Phase 3 — Parallel processing: model shootout (2026-10-08)

3 models × 2 workloads × workers {1,4,8}, 12s per cell, `--concurrency 8`.
Workloads: `io` = 500µs/event sleep (`--batch-size 32`, rate 1500/s);
`cpu` = 5000-iter deterministic burn per event (rate 8000/s). Raw CSV:
`/tmp/p3-results.csv` (columns `model,workers,workload,throughput,accepted,rejected,p50_ms,p99_ms`).

| Model | Workers | Workload | Throughput | Rejected | P50 | P99 |
|---|---|---|---|---|---|---|
| tokio | 1 | io | 1,496/s | 0 | 134µs | 299µs |
| threads | 1 | io | 1,496/s | 0 | 132µs | 282µs |
| rayon | 1 | io | 1,496/s | 0 | 139µs | 302µs |
| tokio | 4 | io | 1,496/s | 0 | 146µs | 351µs |
| threads | 4 | io | 1,496/s | 0 | 144µs | 306µs |
| rayon | 4 | io | 1,496/s | 0 | 152µs | 324µs |
| tokio | 8 | io | 1,496/s | 0 | 139µs | 296µs |
| threads | 8 | io | 1,496/s | 0 | 151µs | 325µs |
| rayon | 8 | io | 1,496/s | 0 | 149µs | 322µs |
| tokio | 1 | cpu | 2,940/s | 1,328 | 124µs | 51.5ms |
| threads | 1 | cpu | 2,896/s | 984 | 128µs | 51.4ms |
| rayon | 1 | cpu | 2,969/s | 1,008 | 126µs | 51.4ms |
| tokio | 4 | cpu | 4,910/s | **1,320** | 122µs | **51.4ms** |
| threads | 4 | cpu | 7,999/s | 0 | 154µs | **0.87ms** |
| rayon | 4 | cpu | 7,999/s | 0 | 150µs | **0.62ms** |
| tokio | 8 | cpu | 4,677/s | **1,224** | 123µs | **51.4ms** |
| threads | 8 | cpu | 7,999/s | 0 | 164µs | 1.09ms |
| rayon | 8 | cpu | 7,999/s | 0 | 164µs | 0.90ms |

### Interpretation (the Phase 3 findings)

1. **IO-bound work: model irrelevant.** All three models deliver the offered
   rate (1,500/s) with ~1.5% spread at every worker count. Sleep is async-native;
   the concurrency model only decides who waits, and waiting costs nothing.
2. **CPU-bound work: async stops helping — measured, not argued.** With a
   blocking kernel inline on the tokio worker tasks, the async runtime itself
   becomes the bottleneck: 8 tokio workers pin the same pool that serves the
   gateway, so achieved rate sticks at ~4.7–4.9k/s, ~1.3k requests queue past
   the 50ms admission window and get 429s, and P99 of accepted events sits at
   the 51ms shedding boundary. Move the identical kernel to OS threads
   (`spawn_blocking`) or rayon and the same 4 workers absorb the full 8k/s
   with **zero** rejections and sub-millisecond P99 — a 1.6x throughput and
   ~60x tail-latency gap from the choice of executor alone.
3. **Single worker: the kernel's core cost dominates.** At workers=1 all models
   converge (~2.9k/s) because one core burns regardless of which pool it lives
   in; P99 ≈51ms is pure queueing at the admission window (offered rate 3x
   exceeds drain). This matches the Phase 1 finding: one consumer caps at
   admission speed.
4. **threads ≈ rayon at batch granularity.** Both are pools executing the whole
   batch kernel; rayon wins slightly on P99 at workers=4 (0.62 vs 0.87ms). The
   expected rayon advantage — `par_iter` *inside* a batch — is not exercised
   yet, because the kernel runs per batch as one job. Intra-batch parallelism
   is the future refinement that would separate them.

### Phase 3 gate status

- [x] `--model tokio|threads|rayon` switch (src/worker.rs, FromStr + Config)
- [x] Worker count scaling + round-robin distributor; per-worker adaptive batching
- [x] Benchmark matrix (18 runs) recorded above
- [ ] Persist matrix to CI (regression policy: performance.md §7) — with Phase 9
- [ ] Rayon intra-batch `par_iter` split — refinement after Phase 8 codec work
