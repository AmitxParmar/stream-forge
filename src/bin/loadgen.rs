//! Load generator for benchmarking the StreamForge ingestion pipeline.
//!
//! Drives configurable-rate HTTP POST traffic against the `/events` endpoint
//! and reports throughput, acceptance rate, and latency percentiles.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::sync::Mutex;

/// StreamForge load generator.
#[derive(Parser, Debug)]
#[command(name = "loadgen", version, about = "Drive synthetic event load")]
struct Args {
    /// Target events per second.
    #[arg(long, default_value_t = 1000)]
    rate: u64,

    /// Duration of the test in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Base URL of the StreamForge server.
    #[arg(long, default_value = "http://127.0.0.1:8080/events")]
    target: String,

    /// Number of concurrent sender tasks.
    #[arg(long, default_value_t = 4)]
    concurrency: u64,

    /// Base URL of the server's `/metrics` endpoint (optional).
    #[arg(long)]
    metrics_url: Option<String>,

    /// CSV file path to append sampled metrics to (requires `--metrics-url`).
    #[arg(long)]
    depth_csv: Option<std::path::PathBuf>,
}

/// Shared counters across sender tasks.
struct Stats {
    sent: AtomicU64,
    accepted: AtomicU64,
    rejected: AtomicU64,
    errors: AtomicU64,
    latencies: Mutex<Vec<Duration>>,
}

impl Stats {
    fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latencies: Mutex::new(Vec::new()),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    eprintln!(
        "loadgen: rate={}/s duration={}s target={} concurrency={}",
        args.rate, args.duration, args.target, args.concurrency
    );

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency as usize)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;

    let stats = Arc::new(Stats::new());

    if let (Some(metrics_url), Some(depth_csv)) = (args.metrics_url.clone(), args.depth_csv.clone())
    {
        let sampler = client.clone();
        tokio::spawn(async move {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&depth_csv)
                .expect("open depth csv");
            use std::io::Write;
            writeln!(
                file,
                "t_ms,queue_depth,batch_last,accepted,rejected_full,rejected_invalid,uptime_ms"
            )
            .expect("write csv header");
            let started = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if started.elapsed() > Duration::from_secs(args.duration) + Duration::from_secs(5) {
                    break;
                }
                match sampler.get(&metrics_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(m) = resp.json::<serde_json::Value>().await {
                            let _ = writeln!(
                                file,
                                "{},{},{},{},{},{},{}",
                                started.elapsed().as_millis(),
                                m["queue_depth"].as_u64().unwrap_or(0),
                                m["batch_last"].as_u64().unwrap_or(0),
                                m["accepted"].as_u64().unwrap_or(0),
                                m["rejected_full"].as_u64().unwrap_or(0),
                                m["rejected_invalid"].as_u64().unwrap_or(0),
                                m["uptime_ms"].as_u64().unwrap_or(0),
                            );
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    let rate_per_task = args.rate / args.concurrency.max(1);
    let test_duration = Duration::from_secs(args.duration);
    let start = Instant::now();

    let mut handles = Vec::with_capacity(args.concurrency as usize);

    for task_id in 0..args.concurrency {
        let client = client.clone();
        let stats = Arc::clone(&stats);
        let target = args.target.clone();
        let interval = 1_000_000_000_u64
            .checked_div(rate_per_task)
            .map_or(Duration::from_secs(1), Duration::from_nanos);

        handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let mut seq: u64 = task_id * 1_000_000_000;

            loop {
                ticker.tick().await;

                if start.elapsed() >= test_duration {
                    break;
                }

                seq += 1;
                let user_id = seq % 10_000;
                let body = serde_json::json!({
                    "eventId": seq,
                    "userId": user_id,
                    "kind": "loadgen",
                    "payload": { "seq": seq, "task": task_id }
                });

                let req_start = Instant::now();
                stats.sent.fetch_add(1, Ordering::Relaxed);

                match client.post(&target).json(&body).send().await {
                    Ok(resp) => {
                        let elapsed = req_start.elapsed();
                        if resp.status().is_success() {
                            stats.accepted.fetch_add(1, Ordering::Relaxed);
                        } else {
                            stats.rejected.fetch_add(1, Ordering::Relaxed);
                        }
                        stats.latencies.lock().await.push(elapsed);
                    }
                    Err(_) => {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for handle in handles {
        // Task panics are not expected; log and continue if they occur.
        if let Err(e) = handle.await {
            eprintln!("sender task failed: {e}");
        }
    }

    let wall = start.elapsed();
    let sent = stats.sent.load(Ordering::Relaxed);
    let accepted = stats.accepted.load(Ordering::Relaxed);
    let rejected = stats.rejected.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);

    // Compute percentiles.
    let mut latencies = stats.latencies.lock().await;
    latencies.sort();
    let count = latencies.len();
    let (p50, p99) = if count > 0 {
        let p50_idx = count * 50 / 100;
        let p99_idx = (count * 99 / 100).min(count - 1);
        (latencies[p50_idx], latencies[p99_idx])
    } else {
        (Duration::ZERO, Duration::ZERO)
    };

    let throughput = if wall.as_secs_f64() > 0.0 {
        sent as f64 / wall.as_secs_f64()
    } else {
        0.0
    };

    eprintln!();
    eprintln!("=== Load Test Results ===");
    eprintln!("Duration:     {wall:.2?}");
    eprintln!("Sent:         {sent}");
    eprintln!("Accepted:     {accepted}");
    eprintln!("Rejected:     {rejected}");
    eprintln!("Errors:       {errors}");
    eprintln!("Throughput:   {throughput:.0} events/s");
    eprintln!("P50 latency:  {p50:.2?}");
    eprintln!("P99 latency:  {p99:.2?}");

    Ok(())
}
