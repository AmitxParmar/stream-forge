# StreamForge — Phase 2 Plan: backpressure

Scope: `docs/phases.md` Phase 2 (FR-4, ADR-007; perf P2). Builds on the bounded
`mpsc` channel from Phase 1. No WAL, no partitioning, no new sinks — those are
later phases and must not leak into this one.

## 1. Goal and exit criteria

Make overload observable and survivable: when the downstream is slower than input,
the system throttles, then sheds load explicitly — never OOMs, never silently drops.

Exit: at 2x overload (input 2x sink rate) — zero unbounded memory growth, P99 of
*accepted* events stays bounded, queue-depth trace before/after adaptive batching
documented in `docs/performance-results.md` (PRD §6 backpressure row).

**Primary lesson:** backpressure, load shedding, adaptive batching.

## 2. Module map (changes on top of Phase 1)

| Module | Change | Key decision |
|---|---|---|
| `src/metrics.rs` (new) | Queue-depth gauge, accepted/rejected counters, batch-size histogram; emitted via `tracing` fields/events for now, Prometheus exporter in P9 | Metrics handle threaded from day one so P9 is a backend swap, not a retrofit (ADR-010) |
| `src/gateway.rs` | `send().await` → `try_send` fast path with timeout fallback; full channel returns `429` + `Retry-After` + JSON reason (FR-4 client contract) | Shedding is explicit and countable, never a hung connection |
| `src/pipeline.rs` | Adaptive batching: batch size grows when queue is shallow, shrinks when deep; expose current depth to metrics | Amortize per-batch cost with headroom, back off without it (ADR-007) |
| `src/bin/loadgen.rs` | `--slow-sink 2x` fault injection (wrap sink with injected latency at t=20s); `--report queue-depth.csv` depth sampler | Reproducible overload drill per performance.md §4 |
| `src/error.rs` | `AppError::ChannelFull` already exists — wire it to the 429 path | No new error variants needed |
| `tests/backpressure.rs` (new) | Full channel → 429 semantics; bounded memory under stuck consumer; adaptive batch bounds | Integration over unit where path matters |
| `docs/performance-results.md` | Append P2 section: drill config, depth trace, accepted/rejected counts, P99 accepted | Append-only history, never edited in place |

Data flow unchanged from P1: `gateway --Event--> mpsc(cap) --batch--> worker
--&[Event]--> Sink`. Only the full-channel behaviour and batch sizing change.

## 3. Decisions made (with reasoning)

1. **`try_send` + timeout, not `send().await` forever.** Indefinite await converts
   overload into client timeouts; `try_send` converts it into a countable 429 the
   client can retry with jitter. Natural backpressure (P1) remains the first stage;
   shedding is the second. (`async-bounded-channel`, ADR-007.)
2. **429 contract: status + `Retry-After` + JSON reason.** Clients must distinguish
   "rejected" from "accepted" (FR-4); a bare dropped connection is a protocol bug.
3. **Adaptive batching keyed on queue depth.** Depth is already the overload canary
   (performance.md §6: P99 degrades past ~70% capacity); batch policy reads the same
   signal instead of inventing a new one. Shallow queue → small batches (low per-event
   latency); deep queue → grow toward `max_size` (amortize per-flush cost and drain
   faster). Measured result in `performance-results.md` §P2: the effect shows in the
 transient (`batch_last` ramps 6→256 in Run C); steady state converges to max batch
   under sustained overload.
4. **Metrics via `tracing` now, Prometheus in P9.** Avoids a second observability
   migration; `metrics.rs` owns the handle so the exporter swap is localised.
   (`obs-library-facade`.)
5. **Fault injection in loadgen, not in the engine.** `--slow-sink` keeps production
   code free of test hooks (`test-mock-traits` spirit: test through the real path).
6. **No dynamic capacity changes.** Queue capacity stays a startup `Config` value;
   live-tunable knobs arrive in P9 (FR-23). Adaptive *batching* is enough for P2.

Explicitly deferred: WAL (P5), partitioning (P4), worker models (P3), Prometheus
exporter + dashboards (P9).

## 4. Task checklist

- [x] Metrics handles: depth, accepted/rejected counters, batch-last (landed in `src/telemetry.rs` — metrics live with telemetry per AGENTS.md, not a separate `metrics.rs`)
- [x] Gateway: 50ms `send` deadline → 429 + `Retry-After` + JSON reason (see `performance-results.md` P2: natural backpressure absorbs until slot-release > admission window)
- [x] Pipeline: adaptive batch sizing keyed on queue depth (`effective_batch_size`, floor max/8, grows under backlog)
- [x] Loadgen: metrics poll + `--depth-csv` series; slow-sink injection lives server-side as `--sink-delay-ms` (fault injection stays out of the load binary)
- [x] `tests/backpressure.rs`: 429+Retry-After at the router seam, metrics counter contract
- [x] 2x-class overload drill (A/B/C); trace + counts + P99-accepted in `docs/performance-results.md`
- [x] `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green

## 5. Next steps (in order)

1. Finish P1 leftovers first (`PostgresSink`, loadgen base, commit) — P2 builds on them.
2. Implement §4 top to bottom; metrics handle first (everything else emits through it).
3. Run the drill, record the trace, tick the exit boxes.
4. Phase 3: worker split + `--model` switch on the measured P2 baseline.
