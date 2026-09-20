//! Typed library errors. Binary code wraps these in [`anyhow::Error`] with context.

use thiserror::Error;

/// Top-level error for the StreamForge library.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum AppError {
    /// Invalid configuration detected at startup.
    #[error("invalid configuration: {reason}")]
    Config {
        /// Human-readable reason the configuration was rejected.
        reason: String,
    },

    /// Event payload failed validation at the gateway boundary.
    #[error("invalid event: {reason}")]
    Validation {
        /// Human-readable reason the event was rejected.
        reason: String,
    },

    /// Bounded channel closed because the receiver was dropped.
    #[error("pipeline channel closed")]
    ChannelClosed,

    /// Bounded channel full; producer must back off (shed in Phase 2).
    #[error("pipeline channel full")]
    ChannelFull,

    /// Batch sink failed to persist events.
    #[error("sink write failed")]
    Sink(#[source] SinkError),

    /// Graceful shutdown requested or completed.
    #[error("shutdown requested")]
    Shutdown,
}

/// Errors produced by [`crate::Sink`] implementations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SinkError {
    /// Generic I/O failure writing a batch.
    #[error("sink io error")]
    Io(#[from] std::io::Error),

    /// Backend-specific failure with a message.
    #[error("sink backend error: {0}")]
    Backend(String),

    /// Database error from Postgres or another SQL backend.
    #[error("database error: {0}")]
    Database(String),
}
