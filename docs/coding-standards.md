# StreamForge — Coding Standards

Project-wide Rust practices for all 9 phases. Rule IDs refer to the
`rust-skills` catalogue; the CI gate (`fmt --check`, `clippy`, `cargo test`)
enforces the mechanically checkable subset.

## 1. Project structure

- `src/main.rs` stays thin: CLI parsing, telemetry init, `run(config).await`
  (`proj-lib-main-split`). All logic lives in `src/lib.rs` so integration
  tests in `tests/` can exercise it (`test-integration-dir`).
- Modules are organised by pipeline stage, flat in `src/` while under ~10 files
  (`proj-flat-small`, `proj-mod-by-feature`). Split a module into a folder only
  past ~300–500 lines.
- Internal APIs default to private; `pub(crate)` for cross-module use,
  `pub` only for the integeration-test surface (`proj-pub-crate-internal`).
- `unsafe` is forbidden project-wide (`#![forbid(unsafe_code)]`, `unsafe-*`).
  If a later phase ever needs it: `// SAFETY:` comment, minimal scope, Miri in CI.

```
src/
├── main.rs      # thin entry: args, telemetry, run
├── lib.rs       # run() orchestration, re-exports
├── config.rs    # validated runtime configuration
├── error.rs     # thiserror AppError / SinkError
├── event.rs     # RawEvent -> Event validation boundary
├── gateway.rs   # axum routes + handlers
├── pipeline.rs  # bounded channel + batch worker
├── sink.rs      # Sink trait + InMemorySink
└── telemetry.rs # subscriber init (binary only)
tests/
└── ingest.rs    # end-to-end ingest path
```

## 2. Ownership and memory

- Borrow (`&T`, `&str`, `&[T]`) over `.clone()` on every boundary
  (`own-borrow-over-clone`, `own-slice-over-vec`). Handlers take `Json<RawEvent>`
  by value (axum owns the body); everything after validation borrows or moves once.
- `Event` moves through the channel exactly once: gateway validates → `send(event)`
  → worker owns → sink borrows `&[Event]`. No stage clones except the sink's
  intentional store (`own-move-large`).
- Small ID types (`EventId`, `UserId`) are `Copy` newtypes, never bare `u64`
  (`own-copy-small`, `type-newtype-ids`). No stringly-typed APIs (`type-no-stringly`).
- Batch buffer is allocated once with `with_capacity(max_size)` and reused via
  `clear()` (`mem-with-capacity`, `mem-reuse-collections`). No `format!` on hot
  paths (`anti-format-hot-path`); prefer iterators over indexing (`perf-iter-over-index`,
  `opt-bounds-check`).

## 3. Errors

- Library errors are typed with `thiserror` (`err-thiserror-lib`): `AppError`
  for pipeline failures, `SinkError` for sink backends. Messages are lowercase,
  no trailing punctuation (`err-lowercase-msg`); sources preserved with
  `#[source]`/`#[from]` (`err-source-chain`, `err-from-impl`).
- Binary/orchestration code uses `anyhow` with `.context()` at each layer
  (`err-anyhow-app`, `err-context-chain`); `main` returns `anyhow::Result<()>`.
- Fallible conversions use `TryFrom`-style constructors (`Event::parse`)
  (`conv-tryfrom-fallible`); fallible public functions carry `# Errors` docs
  (`err-doc-errors`, `doc-errors-section`).
- Never `unwrap()`/`expect()` in production paths (`err-no-unwrap-prod`,
  `anti-unwrap-abuse`); `expect()` only for true invariants
  (`err-expect-bugs-only`). Never silently ignore errors (`anti-empty-catch`).

## 4. Async and concurrency

- Tokio multi-thread runtime, one task per concern; `JoinSet`-style ownership
  where tasks are dynamic (`async-tokio-runtime`, `async-joinset-structured`).
- All channels bounded (`mpsc::channel(cap)`); capacity comes from `Config`,
  never a magic number (`async-bounded-channel`, ADR-007). Unbounded channels
  are a review-blocking finding.
- Never hold a lock guard across `.await` (`async-no-lock-await`,
  `anti-lock-across-await`): lock, extend/clone, drop, then await.
- Shutdown is cooperative via `CancellationToken` (`async-cancellation-token`):
  every `select!` loop races work against `shutdown.cancelled()`, flushes state,
  then returns. Dropping a `JoinHandle` is never the shutdown plan.
- CPU-bound work goes to `spawn_blocking`/Rayon, never the async worker
  (`async-spawn-blocking`) — enforced from Phase 3's model comparison.
- Cancellation safety: futures in `select!` branches must tolerate being
  dropped (`async-cancel-safety`); `recv()` and `timeout()` are safe.

## 5. API and types

- Parse, don't validate: `RawEvent` (serde shape) converts once into `Event`
  (validated domain type); downstream functions take `Event`, making invalid
  states unrepresentable (`api-parse-dont-validate`).
- Serde attributes carry the wire contract: `rename_all = "camelCase"`,
  `deny_unknown_fields` on ingress, `default` for forward-compatible fields
  (`serde-rename-all`, `serde-deny-unknown-fields`, `serde-default-compat`).
- Traits use native `async fn` (no `async_trait` macro) (`async-fn-in-trait`);
  prefer generics/`impl Trait` over `dyn Trait` on hot paths (`trait-dyn-vs-generic`,
  `anti-type-erasure`); `From` impls instead of `Into` (`api-from-not-into`).
- Public types derive the standard set (`Debug, Clone`, plus `Copy/PartialEq/Eq/Hash`
  where valid) (`api-common-traits`); `#[non_exhaustive]` on public error enums
  (`api-non-exhaustive`); `#[must_use]` on pure accessors (`api-must-use`).
- Naming: `UpperCamelCase` types, `snake_case` fns/modules, `SCREAMING_SNAKE_CASE`
  consts, `is_/`has_` booleans, no `get_` on plain getters, acronyms as words
  (`HttpServer`, `EventId`) (`name-*`).

## 6. Observability

- `tracing` everywhere, never `println!` (`obs-tracing-over-log`). Library code
  emits only; the binary installs the subscriber (`obs-library-facade`).
- Structured fields, not interpolated strings: `info!(user.id = %id, …)`
  (`obs-structured-fields`); `#[instrument]` on stage boundaries with `skip`
  for heavy args (`obs-instrument-spans`).
- Levels via `RUST_LOG`/`EnvFilter` (`obs-levels-filter`); log each error once
  with its chain (`obs-error-chain`); never log payloads containing PII
  (`obs-no-sensitive-data`).

## 7. Docs, tests, lints

- `///` on all public items, `//!` per module, `# Examples` doctests that compile,
  `# Errors` on fallible fns, `?` (never `unwrap`) in examples (`doc-*`).
- Unit tests live with the module (`#[cfg(test)]`, `use super::*`),
  Arrange/Act/Assert structure, descriptive names; async tests use `#[tokio::test]`;
  cross-module paths go in `tests/` (`test-*`).
- Enforced in CI: `cargo fmt --check`, `clippy` with
  `correctness = deny` + `suspicious/style/complexity/perf = warn`
  (`lint-*`), `cargo test` incl. doctests. Profile before optimising
  (`perf-profile-first`, `anti-premature-optimize`).
