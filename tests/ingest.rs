//! Integration tests for the Phase 1 ingest path.

use streamforge::Config;
use streamforge::event::{Event, RawEvent};
use streamforge::pipeline::{self, BatchConfig};
use streamforge::sink::{InMemorySink, Sink};
use tokio_util::sync::CancellationToken;

fn raw_event(id: u64) -> RawEvent {
    RawEvent {
        event_id: id,
        user_id: 42,
        kind: "click".to_owned(),
        payload: serde_json::json!({ "page": "/home" }),
    }
}

#[tokio::test]
async fn validated_event_travels_gateway_channel() {
    // Arrange: the same bounded channel the gateway and worker share.
    let config = Config::default();
    let (tx, mut rx) = pipeline::channel(&config);
    let event = Event::parse(raw_event(1)).unwrap();

    // Act: handler validates, then sends (mirrors `gateway::router` behaviour).
    tx.send(event.clone()).await.unwrap();

    // Assert
    let received = rx.recv().await.unwrap();
    assert_eq!(received, event);
    assert_eq!(received.kind(), "click");
}

#[tokio::test]
async fn worker_batches_events_to_sink() {
    // Arrange
    let config = Config::default();
    let (tx, rx) = pipeline::channel(&config);
    let sink = InMemorySink::default();
    let shutdown = CancellationToken::new();
    let handle = pipeline::spawn_worker(rx, sink.clone(), BatchConfig::default(), shutdown);

    // Act
    for id in 0..10 {
        tx.send(Event::parse(raw_event(id)).unwrap()).await.unwrap();
    }
    drop(tx);

    // Assert
    handle.await.unwrap().unwrap();
    assert_eq!(sink.len().await, 10);
}

#[tokio::test]
async fn sink_stores_batch_contents() {
    // Arrange
    let sink = InMemorySink::default();
    let batch = vec![Event::parse(raw_event(9)).unwrap()];

    // Act
    sink.write_batch(&batch).await.unwrap();

    // Assert
    assert!(!sink.is_empty().await);
    assert_eq!(sink.len().await, 1);
}
