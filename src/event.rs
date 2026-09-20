//! Event model. Raw JSON is parsed at the HTTP boundary into validated [`Event`]s;
//! nothing downstream accepts unvalidated data.

use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// Maximum accepted event kind length in bytes.
pub const MAX_KIND_LEN: usize = 64;
/// Maximum accepted serialised payload size in bytes.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// Strongly-typed event identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub u64);

/// Strongly-typed user identifier used for partitioning in Phase 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserId(pub u64);

/// Raw event as received over HTTP. Never used past the gateway boundary.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawEvent {
    /// Client-supplied event identifier.
    pub event_id: u64,
    /// User the event belongs to.
    pub user_id: u64,
    /// Event kind, e.g. `click`.
    #[serde(default)]
    pub kind: String,
    /// Arbitrary JSON payload.
    #[serde(default = "serde_json::Value::default")]
    pub payload: serde_json::Value,
}

/// Validated domain event. Construction is only possible via [`Event::parse`].
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// Validated event identifier.
    pub id: EventId,
    /// Validated user identifier.
    pub user_id: UserId,
    /// Validated non-empty event kind.
    pub kind: String,
    /// Payload within the accepted size bound.
    pub payload: serde_json::Value,
}

impl Event {
    /// Parse and validate a raw event.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Validation`] if the kind is empty/too long or the
    /// payload exceeds [`MAX_PAYLOAD_BYTES`].
    ///
    /// # Examples
    ///
    /// ```
    /// use streamforge::event::{Event, RawEvent};
    ///
    /// let raw = RawEvent {
    ///     event_id: 1,
    ///     user_id: 7,
    ///     kind: "click".to_owned(),
    ///     payload: serde_json::json!({}),
    /// };
    /// assert!(Event::parse(raw).is_ok());
    /// ```
    pub fn parse(raw: RawEvent) -> Result<Self, AppError> {
        if raw.kind.is_empty() {
            return Err(AppError::Validation {
                reason: "event kind must not be empty".to_owned(),
            });
        }
        if raw.kind.len() > MAX_KIND_LEN {
            return Err(AppError::Validation {
                reason: "event kind exceeds 64 bytes".to_owned(),
            });
        }
        let payload_bytes = serde_json::to_vec(&raw.payload).map_err(|_| AppError::Validation {
            reason: "event payload is not serialisable".to_owned(),
        })?;
        if payload_bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(AppError::Validation {
                reason: "event payload exceeds 64 KiB".to_owned(),
            });
        }
        Ok(Self {
            id: EventId(raw.event_id),
            user_id: UserId(raw.user_id),
            kind: raw.kind,
            payload: raw.payload,
        })
    }

    /// Borrow the event kind without cloning.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn raw(kind: &str) -> RawEvent {
        RawEvent {
            event_id: 1,
            user_id: 7,
            kind: kind.to_owned(),
            payload: json!({}),
        }
    }

    #[test]
    fn accepts_valid_event() {
        assert!(Event::parse(raw("click")).is_ok());
    }

    #[test]
    fn rejects_empty_kind() {
        assert!(matches!(
            Event::parse(raw("")),
            Err(AppError::Validation { .. })
        ));
    }

    #[test]
    fn rejects_oversize_kind() {
        let kind = "k".repeat(MAX_KIND_LEN + 1);
        assert!(matches!(
            Event::parse(raw(&kind)),
            Err(AppError::Validation { .. })
        ));
    }
}
