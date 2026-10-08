//! Runtime configuration, parsed at the process boundary and validated once.

use crate::error::AppError;
use crate::pipeline::BatchConfig;

/// Full runtime configuration for the Phase 1 engine.
#[derive(Debug, Clone)]
pub struct Config {
    /// Socket address the HTTP gateway binds to.
    pub bind: std::net::SocketAddr,
    /// Capacity of the bounded gateway-to-worker channel.
    pub queue_capacity: usize,
    /// Batching policy for the worker.
    pub batch: BatchConfig,
    /// Postgres connection string. When `None`, the in-memory sink is used.
    pub database_url: Option<String>,
    /// Maximum number of connections in the Postgres pool.
    pub pool_size: u32,
    /// Per-batch artificial delay injected into the sink (fault injection, Phase 2).
    pub sink_delay_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: std::net::SocketAddr::from(([127, 0, 0, 1], 8080)),
            queue_capacity: 1024,
            batch: BatchConfig::default(),
            database_url: None,
            pool_size: 5,
            sink_delay_ms: 0,
        }
    }
}

impl Config {
    /// Validate configuration values before startup.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if any value is out of range.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.queue_capacity == 0 {
            return Err(AppError::Config {
                reason: "queue_capacity must be greater than zero".to_owned(),
            });
        }
        self.batch.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_queue_capacity() {
        let config = Config {
            queue_capacity: 0,
            ..Config::default()
        };
        assert!(matches!(config.validate(), Err(AppError::Config { .. })));
    }
}
