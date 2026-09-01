# StreamForge — Architecture

System: high-throughput event ingestion & stream processing engine (Rust)
Source of truth for the interactive map: `docs/streamforge-architecture.html`

---

## 1. High-level architecture

```
                            ┌─────────────────────────────────────────────────────────┐
                            │                      StreamForge Node                     │
                            │                                                          │
 ┌──────────┐   HTTP/WS     │  ┌─────────────┐    ┌───────────┐    ┌─────────────┐   │
 │  Event   │ ─────────────►│  │  Ingestion  │ ─► │  Bounded  │ ─► │ Append Log  │   │
 │  Clients │               │  │   Gateway   │    │   Queue   │    │  (WAL)       │   │
 └──────────┘               │  │ accept,     │    │ back-     │    │ segments,   │   │
                            │  │ batch,      │    │ pressure, │    │ offsets,    │   │
                            │  │ validate    │    │ shed load │    │ checksums   │   │
                            │  └─────────────┘    └───────────┘    └──────┬──────┘   │
                            │                                           │          │
                            │                          checkpoint /    │ replay    │
                            │                          ┌───────────────┤          │
                            │                          ▼               ▼          │
                            │                    ┌─────────────┐  ┌─────────────┐  │
                            │                    │   Crash     │  │ Partitioner │  │
                            │                    │  Recovery   │  │ hash(key)%N│  │
                            │                    │ checkpoint, │  └──────┬──────┘  │
                            │                    │  replay     │         │         │
                            │                    └─────────────┘         ▼         │
                            │                            │     ┌─────────────────┐   │
                            │                            │     │  ┌───────────┐  │   │
                            │                            │     │  │ Worker 1  │  │   │
                            │                            │     │  │ (shard 0) │  │   │
                            │                            │     │  ├───────────┤  │   │
                            │                            │     │  │ Worker 2  │  │   │
                            │                            │     │  │ (shard 1) │  │   │
                            │                            │     │  ├───────────┤  │   │
                            │                            │     │  │ Worker N  │  │   │
                            │                            │     │  │ (shard N) │  │   │
                            │                            │     │  └───────────┘  │   │
                            │                            │     └───────┬─────────┘   │
                            │                            │             ▼             │
                            │                            │     ┌─────────────┐       │
                            │                            │     │  Windowing  │       │
                            │                            │     │  + Dedup    │       │
                            │                            │     └──────┬──────┘       │
                            │                            │            │ serialize    │
                            │                            ▼            ▼              │
                            │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
                            │  │ PostgreSQL  │  │   Parquet   │  │    Redis    │    │
                            │  │ OLTP state  │  │  analytics  │  │  hot cache  │    │
                            │  └─────────────┘  └─────────────┘  └─────────────┘    │
                            │                                                                  │
                            │  Cross-cutting: Prometheus · Grafana · OpenTelemetry           │
                            └─────────────────────────────────────────────────────────┘
```

**Zones (left → right):** Clients → Ingestion → Durable Log → Router → Workers → Processing → Storage.
Dashed edges below the pipeline are cross-cutting observability flows.

### Happy path (end-to-end)

1. Client pushes an event over HTTP or WebSocket.
2. Gateway accepts the connection, validates the payload, and batches events.
3. Batch goes through a **bounded channel** — when full, the gateway throttles or sheds load (backpressure).
4. Batch is **appended to the WAL** and fsynced (durable before processing).
5. Partitioner routes each event by `hash(user_id) % N` — same key always lands on the same shard.
6. Worker for that shard processes the event (Tokio task / OS thread / Rayon — configurable).
7. Windowing + dedup stage aggregates (count, sum, percentiles) over tumbling/sliding windows.
8. Results are serialized and routed: durable state → PostgreSQL, analytics → Parquet, hot lookups → Redis.
9. Checkpoints are periodically written to Postgres; on crash, the process replays from the last checkpoint.

---

## 2. Low-level component diagrams

### 2.1 Ingestion Gateway (Rust · Tokio)

```
 TCP accept loop (tokio)
 │
 ├── conn handler task ──► parse request (HTTP/WS)
 │                          │
 │                          ▼
 │                    validate payload
 │                    (schema, bounds, UTF-8, size cap)
 │                          │
 │                          ▼
 │                    push Event into mpsc::channel(cap=K)
 │                          │
 │                    ┌─────┴─────────────┐
 │                    │ channel full?     │
 │                    ├───────────────────┤
 │                    │ K-1 slots: await  │──► natural async backpressure
 │                    │ full + high:      │
 │                    │  - adaptive batch │
 │                    │  - 429 / drop     │──► load shedding
 │                    └───────────────────┘
 ▼
 batching task: drain up to B events or T ms, whichever first
 │
 ▼
 WAL append
```

Key details:

- One task per connection; no thread per connection.
- **Adaptive batching**: batch size grows when the queue is shallow, shrinks when it's deep.
- Every stage logs structured events with a trace ID (OTel span per event batch).

### 2.2 Append Log (WAL) — hand-rolled, Kafka-style

```
 wal/                                  on-disk layout
 ├── 00000000000000000001.log          ┌───────────────────────────┐
 ├── 00000000000000000042.log          │ [crc32][len][payload]     │ record
 ├── 00000000000000000088.log          ├───────────────────────────┤
 └── 00000000000000000099.index        │ sequential append-only    │
                                       │ offset = byte position    │
 active segment                        │ fsync policy: batch-level │
 (single writer) ──► write() ──►       │ segment rotation at S MB  │
                                       │ checksum per record       │
                                       └───────────────────────────┘
 Rotation: when active segment exceeds S MB → seal it, open a new one.
 Corruption: trailing partial record (torn write) → truncate at last good crc.
```

- **Single writer, many readers** — appends are serialized through one task; readers fetch by offset.
- **Offsets** are monotonically increasing; the partitioner/reader resumes from the last committed offset.
- **Checkpoint** = (worker shard id → committed offset) persisted to PostgreSQL.

### 2.3 Partitioner & workers

```
                  events from WAL replay / live tail
                              │
                              ▼
                     ┌────────────────┐
        key present? │ hash(key) % N  │  FxHash / xxhash — fast, stable
        └───────────►│                │
           else      └───────┬────────┘
             │              │ round-robin / least-loaded
             └──────────────►│
                             ▼
       ┌─────────────────────────────────────────┐
       │  shard channel[0] ──► Worker task 0      │  each shard:
       │  shard channel[1] ──► Worker task 1      │   - single consumer
       │       ...             ...                │   - per-key ordering
       │  shard channel[N] ──► Worker task N      │   - no cross-shard locks
       └─────────────────────────────────────────┘
                             │
                             ▼
                     windowing + dedup
                     (per shard, local state)
```

- Same key → same shard → **per-key ordering** while shards run concurrently.
- Worker concurrency model is swappable (Tokio task / OS thread / Rayon pool) for benchmarking (Phase 3).
- Shard state (windows, dedup cache) lives in worker-local memory — no shared mutable state, no lock contention.

### 2.4 Windowing + Dedup (the mini stream engine)

```
 event ──► is event_time within watermark? ──no──► late-event policy
             │ yes                                (drop / side-output / recompute)
             ▼
      ┌────────────────────────────────────────┐
      │ active windows (tumbling or sliding)    │
      │  - count, sum, avg                      │
      │  - ring buffer for sliding windows      │
      │  - P² sketch for percentiles            │
      └───────────────┬────────────────────────┘
                      │ window closes (event-time boundary passes watermark)
                      ▼
              emit aggregate ──► dedup (event_id seen?) ──► sink router
                                                                │
                                                ┌───────────────┼───────────────┐
                                                ▼               ▼               ▼
                                           PostgreSQL        Parquet         Redis
                                           (state, cp)      (analytics)     (hot cache)
```

- **Event-time** processing with watermarks; late events handled by policy, not silent loss.
- **Dedup** makes processing idempotent — required for at-least-once WAL replay (Phase 6).
- Window state is checkpointed with the offset so recovery resumes mid-window.

### 2.5 Crash recovery sequence

```
 process starts
      │
      ▼
 read WAL directory ──► list segments, validate crc per record
      │
      ▼
 read checkpoints from PostgreSQL (last committed offset per shard)
      │
      ▼
 tail-replay: for each event with offset > committed offset
      │           │
      │           └──► idempotent apply (dedup drops already-processed event_ids)
      ▼
 resume live tail ──► steady state
```

---

## 3. Deployment topology (single node → lab)

```
┌────────────── host ──────────────┐
│  streamforge (single binary)     │
│    - gateway (HTTP :8080, WS)    │
│    - WAL (local SSD path)        │
│    - workers (N = cpu_cores)     │
└───────────┬──────────────────────┘
            │
   ┌────────┼────────────────┐
   ▼        ▼                ▼
:5432     :6379          :9090
Postgres  Redis     Prometheus ──► Grafana dashboard
                                  (throughput, P99, queue depth,
                                   memory, live-tunable knobs)
```

Single-node by design; multi-node is explicitly out of scope (see ADR-008).

---

## 4. Failure modes & responses

| Failure | Detection | Response |
|---|---|---|
| Downstream slow (workers/backlog) | bounded queue depth | backpressure → throttle producers → shed load (429/drop) |
| Queue full | channel send fails/times out | adaptive batching + load shedding; client retry w/ jitter |
| Process crash | supervisor (systemd) restarts | WAL replay from last checkpoint; dedup makes it safe |
| Torn write (partial record) | crc mismatch on read | truncate segment at last good record |
| Postgres down | connection error + circuit breaker | buffer to WAL, sink retries; circuit breaker halts sink writes |
| Redis down | timeout + circuit breaker | fall through to Postgres lookups (slower, correct) |
| Late events | watermark check | policy: drop / side-output / trigger recompute |
| Hot key (one shard saturated) | per-shard queue metrics | alert + manual reshard (known limitation) |

---

## 5. Observability

Every stage (gateway, queue, WAL, partitioner, workers, aggregator, sinks) exports:

- **Metrics** (Prometheus): events/s in & out per stage, queue depth, batch size histogram, WAL fsync latency, per-shard lag, window emit rate, sink write latency, dedup hit rate
- **Traces** (OpenTelemetry): one span per batch through the whole pipeline; event_id/trace_id propagated
- **Logs**: structured (tracing crate), correlated by trace_id

Grafana dashboard panels: throughput, P99, memory, queue depth — plus live-tunable knobs (batch size, worker count, fsync policy) for the Phase 9 perf lab.
