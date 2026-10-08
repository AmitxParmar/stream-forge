//! Phase 2 backpressure: explicit shedding at the gateway boundary.
//!
//! Seam: the HTTP surface of the real router, with no worker attached so the
//! bounded channel stays full.

use streamforge::event::{Event, RawEvent};
use streamforge::pipeline::{self, BatchConfig};
use streamforge::{Config, gateway};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn serve_test_app(
    config: &Config,
) -> (
    std::net::SocketAddr,
    CancellationToken,
    tokio::sync::mpsc::Receiver<streamforge::Event>,
) {
    let (tx, rx) = pipeline::channel(config);
    let shutdown = CancellationToken::new();
    let metrics = streamforge::Metrics::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serve_shutdown = shutdown.clone();
    tokio::spawn(async move {
        axum::serve(listener, gateway::router(tx, metrics))
            .with_graceful_shutdown(async move { serve_shutdown.cancelled().await })
            .await
            .unwrap();
    });
    (addr, shutdown, rx)
}

fn event_body(id: u64, kind: &str) -> serde_json::Value {
    serde_json::json!({
        "eventId": id,
        "userId": 42,
        "kind": kind,
        "payload": { "page": "/home" }
    })
}

#[tokio::test]
async fn full_channel_sheds_with_429_and_retry_after() {
    // Arrange: capacity 1, no worker draining, gateway timeout is 50ms.
    let config = Config {
        queue_capacity: 1,
        batch: BatchConfig {
            max_size: 256,
            timeout: std::time::Duration::from_millis(50),
            adaptive: false,
        },
        ..Config::default()
    };
    let (addr, shutdown, _rx_guard) = serve_test_app(&config).await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/events");

    // Act: the first event fits in the channel, the second cannot be accepted.
    let first = client
        .post(&url)
        .json(&event_body(1, "click"))
        .send()
        .await
        .unwrap();
    let second = client
        .post(&url)
        .json(&event_body(2, "click"))
        .send()
        .await
        .unwrap();

    // Assert
    assert_eq!(first.status(), 202);
    assert_eq!(second.status(), 429);
    assert!(second.headers().contains_key("retry-after"));
    let body: serde_json::Value = second.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("backpressure"));

    shutdown.cancel();
}

#[tokio::test]
async fn metrics_endpoint_reports_counters() {
    let config = Config {
        queue_capacity: 1,
        batch: BatchConfig {
            max_size: 256,
            timeout: std::time::Duration::from_millis(50),
            adaptive: false,
        },
        ..Config::default()
    };
    let (addr, shutdown, _rx_guard) = serve_test_app(&config).await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("http://{addr}/events"))
        .json(&event_body(1, "click"))
        .send()
        .await
        .unwrap();
    let _ = client
        .post(format!("http://{addr}/events"))
        .json(&event_body(2, "click"))
        .send()
        .await
        .unwrap();
    let _ = client
        .post(format!("http://{addr}/events"))
        .json(&event_body(3, ""))
        .send()
        .await
        .unwrap();

    let metrics: serde_json::Value = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(metrics["accepted"], 1);
    assert_eq!(metrics["rejected_full"], 1);
    assert_eq!(metrics["rejected_invalid"], 1);

    shutdown.cancel();
}

#[tokio::test]
async fn raw_event_json_shape_matches_gateway_contract() {
    let parsed = RawEvent {
        event_id: 1,
        user_id: 2,
        kind: "click".to_owned(),
        payload: serde_json::json!({}),
    };
    let event = Event::parse(parsed).unwrap();
    assert_eq!(event.kind(), "click");
}
