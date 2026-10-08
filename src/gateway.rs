//! HTTP ingestion gateway (axum). Validates at the boundary, then hands
//! validated events to the bounded pipeline channel.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::error::AppError;
use crate::event::{Event, RawEvent};

/// Shared gateway state. The sender and metrics are cheap to clone per request.
#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<Event>,
    metrics: crate::telemetry::Metrics,
}

/// Build the axum router. Split from [`serve`] so tests can call handlers
/// without binding a socket.
pub fn router(tx: mpsc::Sender<Event>, metrics: crate::telemetry::Metrics) -> Router {
    let state = AppState { tx, metrics };
    Router::new()
        .route("/events", post(ingest_event))
        .route("/health", get(health))
        .route("/metrics", get(metrics_snapshot))
        .with_state(Arc::new(state))
}

/// Serve the gateway until the shutdown token fires.
pub async fn serve(
    config: crate::Config,
    tx: mpsc::Sender<Event>,
    shutdown: CancellationToken,
    metrics: crate::telemetry::Metrics,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "gateway listening");
    axum::serve(listener, router(tx, metrics))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

/// Health probe for orchestrators and the Phase 9 health checks.
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Live pipeline counters for the perf-lab CSV poll and dashboards.
async fn metrics_snapshot(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.metrics.snapshot())
}

/// Accept one event, validate it, and enqueue it.
///
/// A full channel sheds load explicitly: 429 + `Retry-After` + JSON reason,
/// counted in metrics. A closed channel is a 503 because the worker is gone.
#[instrument(skip(state), fields(event_id, user_id))]
async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Json(raw): Json<RawEvent>,
) -> impl IntoResponse {
    let event = match Event::parse(raw) {
        Ok(event) => event,
        Err(AppError::Validation { reason }) => {
            state.metrics.inc_rejected_invalid();
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": reason })),
            )
                .into_response();
        }
        Err(_) => {
            state.metrics.inc_rejected_invalid();
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid event" })),
            )
                .into_response();
        }
    };

    tracing::Span::current().record("event_id", tracing::field::debug(event.id));
    tracing::Span::current().record("user_id", tracing::field::debug(event.user_id));

    match tokio::time::timeout(std::time::Duration::from_millis(50), state.tx.send(event)).await {
        Ok(Ok(())) => {
            state.metrics.inc_accepted();
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "status": "accepted" })),
            )
                .into_response()
        }
        Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "pipeline unavailable" })),
        )
            .into_response(),
        Err(_) => {
            state.metrics.inc_rejected_full();
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "1")],
                Json(serde_json::json!({
                    "error": "backpressure: queue full, retry with jitter"
                })),
            )
                .into_response()
        }
    }
}
