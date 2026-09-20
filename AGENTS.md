# AGENTS.md - StreamForge Engine Guidelines

## 1. Architecture & Boundaries
Event ingestion and processing engine written in Rust. Keep modules single-purpose:
- `src/gateway.rs`: Inbound protocol handling and connection lifecycle.
- `src/event.rs`: Core domain models, schemas, and serialization formats.
- `src/pipeline.rs`: Event transformation, batching, routing, and filtering stages.
- `src/sink.rs`: Outbound adapters and delivery mechanisms.
- `src/telemetry.rs`: Tracing, metrics, and structured log initialization.
- `src/config.rs`: Environment and configuration loading.
- `src/error.rs`: Central error types (`thiserror`/enum-based).
- `tests/`: Integration tests only (e.g., `tests/ingest.rs`). Unit tests go inline with `#[cfg(test)]`.

## 2. Docs Routing (Progressive Context)
Do not load all docs at once. Consult only when explicitly requested or relevant to the subtask:
- Architectural decisions: `docs/adr.md`
- Performance benchmarks & memory budgets: `docs/performance.md`
- Coding conventions: `docs/coding-standards.md`
- Current task scope: Check `docs/phases.md` or the active `docs/phase-0X-plan.md`

## 3. Strict Negative Constraints
- **NO PANICS:** Never use `unwrap()` or `expect()` in `src/`. Map errors explicitly into `crate::error::*`.
- **NO SILENT ALLOCATIONS:** Favor borrowed types (`&str`, `&[u8]`, `Cow`) over heap allocations (`String`, `Vec`) in high-throughput hot paths (`event.rs`, `pipeline.rs`).
- **NO DEPENDENCY CREEP:** Do not add new crates to `Cargo.toml` without explicit user confirmation.
- **NO DRIFT:** Do not refactor files outside the immediate scope of the requested change.

## 4. Verification & Feedback Loop
Run these commands in order after EVERY change. Do not declare a task complete if any command fails:

```bash
# 1. Fast compilation check
cargo check

# 2. Strict linting (warnings are errors)
cargo clippy --all-targets -- -D warnings

# 3. Code formatting
cargo fmt --check

# 4. Integration test suite
cargo test --test ingest

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**This project has a knowledge graph. Start with the code-review-graph
MCP tools to narrow scope, then read the source.** The graph is cheaper than scanning files and
gives you structural context (callers, dependents, test coverage) that file search cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes_tool` or `query_graph_tool` instead of Grep
- **Understanding impact**: `get_impact_radius_tool` instead of manually tracing imports
- **Code review**: `detect_changes_tool` + `get_review_context_tool` instead of reading entire files
- **Finding relationships**: `query_graph_tool` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview_tool` + `list_communities_tool`

### Verify in the source

- Narrow scope with the graph, then read the source. Do not change code from graph output alone.
- For any non-trivial change, read the implementation and the relevant tests before concluding.
- Verify the exact source when touching behavior, database logic, migrations, retries, fallbacks,
  recovery, or compatibility code.
- When the graph and the source disagree, the source wins. The graph may be stale or may not
  model that relationship.
- An empty graph result can mean "not indexed" or "not statically visible", not "does not exist".

### Key Tools

| Tool | Use when |
| ------ | ---------- |
| `detect_changes_tool` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context_tool` | Need source snippets for review — token-efficient |
| `get_impact_radius_tool` | Understanding blast radius of a change |
| `get_affected_flows_tool` | Finding which execution paths are impacted |
| `query_graph_tool` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes_tool` | Finding functions/classes by name or keyword |
| `get_architecture_overview_tool` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes_tool` for code review.
3. Use `get_affected_flows_tool` to understand impact.
4. Use `query_graph_tool` pattern="tests_for" to check coverage.
<!-- /code-review-graph MCP tools -->
