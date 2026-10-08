//! Batch sinks. The worker programs to the [`Sink`] trait so Phase 8 can swap
//! in Postgres/Parquet/Redis implementations without touching the pipeline.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::SinkError;
use crate::event::Event;

/// Destination for flushed event batches.
///
/// Implemented with native `async fn` (no `async_trait` macro needed since Rust 1.75).
pub trait Sink: Clone + Send + Sync + 'static {
    /// Persist a batch of events.
    ///
    /// Implementations must be idempotent-friendly: Phase 6 replays batches after
    /// crashes, so writing the same batch twice must be safe.
    fn write_batch(
        &self,
        batch: &[Event],
    ) -> impl std::future::Future<Output = Result<(), SinkError>> + Send;
}

/// In-memory sink used for Phase 1 development and tests.
///
/// Stores batches behind an async mutex; never hold the guard across `.await`.
#[derive(Debug, Clone, Default)]
pub struct InMemorySink {
    inner: Arc<Mutex<Vec<Event>>>,
}

impl InMemorySink {
    /// Number of events stored so far.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// Whether any events have been stored.
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }

    /// Ordered snapshot of everything stored so far.
    ///
    /// Test-only helper for the Phase 4 ordering proof: per-shard appends
    /// preserve arrival order, so each key's subsequence must be monotonic.
    pub async fn snapshot(&self) -> Vec<Event> {
        self.inner.lock().await.clone()
    }
}

impl Sink for InMemorySink {
    async fn write_batch(&self, batch: &[Event]) -> Result<(), SinkError> {
        // Short critical section: extend then drop the guard before returning.
        self.inner.lock().await.extend(batch.iter().cloned());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_sink_delays_each_flush() {
        let sink = SlowSink::new(
            InMemorySink::default(),
            std::time::Duration::from_millis(40),
        );
        let batch = vec![
            crate::event::Event::parse(crate::event::RawEvent {
                event_id: 1,
                user_id: 1,
                kind: "click".to_owned(),
                payload: serde_json::json!({}),
            })
            .unwrap(),
        ];
        let started = std::time::Instant::now();
        sink.write_batch(&batch).await.unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(40));
        assert_eq!(sink.inner.len().await, 1);
    }
}

/// Fault-injection wrapper that sleeps before every batch flush.
///
/// Phase 2 uses this to deliberately slow the downstream so the bounded
/// channel fills; a `delay` of zero is a pass-through.
#[derive(Debug, Clone)]
pub struct SlowSink<S> {
    inner: S,
    delay: std::time::Duration,
}

impl<S> SlowSink<S> {
    /// Wrap `sink` with a fixed per-flush delay.
    #[must_use]
    pub fn new(inner: S, delay: std::time::Duration) -> Self {
        Self { inner, delay }
    }
}

impl<S: Sink> Sink for SlowSink<S> {
    async fn write_batch(&self, batch: &[Event]) -> Result<(), SinkError> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.write_batch(batch).await
    }
}

/// Postgres sink backed by a connection pool.
///
/// Writes batches via multi-row `INSERT ... ON CONFLICT DO NOTHING` for
/// idempotency (Phase 6 replays). The pool handle is cheap to clone (`Arc`
/// internally).
#[derive(Clone)]
pub struct PostgresSink {
    pool: sqlx::PgPool,
}

impl PostgresSink {
    /// Connect to the database and return a ready sink.
    ///
    /// Creates the `events` table if it does not already exist.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::Database`] if the connection or migration fails.
    pub async fn connect(database_url: &str, pool_size: u32) -> Result<Self, SinkError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(pool_size)
            .connect(database_url)
            .await
            .map_err(|e| SinkError::Database(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (
                event_id   BIGINT PRIMARY KEY,
                user_id    BIGINT NOT NULL,
                kind       TEXT NOT NULL,
                payload    JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .execute(&pool)
        .await
        .map_err(|e| SinkError::Database(e.to_string()))?;

        Ok(Self { pool })
    }
}

impl Sink for PostgresSink {
    async fn write_batch(&self, batch: &[Event]) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }

        // Build a multi-row INSERT with ON CONFLICT DO NOTHING for idempotency.
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new("INSERT INTO events (event_id, user_id, kind, payload) ");

        query_builder.push_values(batch, |mut b, event| {
            b.push_bind(event.id.0 as i64)
                .push_bind(event.user_id.0 as i64)
                .push_bind(&event.kind)
                .push_bind(&event.payload);
        });

        query_builder.push(" ON CONFLICT (event_id) DO NOTHING");

        query_builder
            .build()
            .execute(&self.pool)
            .await
            .map_err(|e| SinkError::Database(e.to_string()))?;

        Ok(())
    }
}
