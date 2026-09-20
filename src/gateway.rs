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

/// Shared gateway state. The sender is cheap to clone per request.
#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<Event>,
}

/// Build the axum router. Split from [`serve`] so tests can call handlers
/// without binding a socket.
pub fn router(tx: mpsc::Sender<Event>) -> Router {
    let state = AppState { tx };
    Router::new()
        .route("/events", post(ingest_event))
        .route("/health", get(health))
        .with_state(Arc::new(state))
}

/// Serve the gateway until the shutdown token fires.
pub async fn serve(
    config: crate::Config,
    tx: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "gateway listening");
    axum::serve(listener, router(tx))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

/// Health probe for orchestrators and the Phase 9 health checks.
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Accept one event, validate it, and enqueue it.
///
/// A full channel applies backpressure by awaiting; a closed channel is a
/// 503 because the worker is gone and the event cannot be processed.
#[instrument(skip(state), fields(event_id, user_id))]
async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Json(raw): Json<RawEvent>,
) -> impl IntoResponse {
    let event = match Event::parse(raw) {
        Ok(event) => event,
        Err(AppError::Validation { reason }) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": reason })),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid event" })),
            )
                .into_response();
        }
    };

    tracing::Span::current().record("event_id", tracing::field::debug(event.id));
    tracing::Span::current().record("user_id", tracing::field::debug(event.user_id));

    match state.tx.send(event).await {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "status": "accepted" })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "pipeline unavailable" })),
        )
            .into_response(),
    }
}
