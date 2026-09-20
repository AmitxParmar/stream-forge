//! Tracing subscriber setup. Only the binary calls [`init`]; library code
//! emits spans/events through the facade and never installs a subscriber.

use tracing_subscriber::EnvFilter;

/// Initialise the global tracing subscriber from `RUST_LOG` (default: `info`).
///
/// # Errors
///
/// Returns an error if the filter directive cannot be parsed or the global
/// subscriber is already set.
pub fn init() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing subscriber already initialised: {e}"))?;
    Ok(())
}
