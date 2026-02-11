//! Metrics and tracing setup for dynamo-kad.
//!
//! Provides a global [`NodeMetrics`] singleton backed by the `prometheus`
//! crate, plus an optional lightweight HTTP server for Prometheus scraping.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::net::SocketAddr;
use std::sync::OnceLock;

// ────────────────────────── Tracing ──────────────────────────

/// Initialize the tracing subscriber with env-filter.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

// ────────────────────────── Prometheus metrics ──────────────────────────

/// Global metrics instance.
static METRICS: OnceLock<NodeMetrics> = OnceLock::new();

/// Retrieve (or lazily create) the global metrics singleton.
pub fn metrics() -> &'static NodeMetrics {
    METRICS.get_or_init(NodeMetrics::new)
}

/// All Prometheus metrics for a dynamo-kad node.
pub struct NodeMetrics {
    pub registry: Registry,

    // ── RPC counters ──
    pub rpcs_sent: IntCounter,
    pub rpcs_received: IntCounter,
    pub rpcs_sent_by_type: IntCounterVec,
    pub rpcs_received_by_type: IntCounterVec,

    // ── RPC latency ──
    pub rpc_latency_secs: HistogramVec,

    // ── KV operation counters ──
    pub kv_puts: IntCounter,
    pub kv_gets: IntCounter,
    pub kv_deletes: IntCounter,

    // ── KV operation latency ──
    pub kv_latency_secs: HistogramVec,

    // ── Hints ──
    pub hints_stored: IntCounter,
    pub hints_delivered: IntCounter,

    // ── Read repair ──
    pub read_repairs: IntCounter,
}

// Manual Debug impl because prometheus types don't derive Debug.
impl std::fmt::Debug for NodeMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeMetrics").finish_non_exhaustive()
    }
}

/// Default histogram buckets (seconds) for RPC/KV latency.
const LATENCY_BUCKETS: &[f64] = &[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

impl NodeMetrics {
    fn new() -> Self {
        let registry = Registry::new();

        let rpcs_sent = IntCounter::with_opts(Opts::new(
            "dynamo_rpcs_sent_total",
            "Total outbound RPCs sent",
        ))
        .expect("rpcs_sent counter");
        let rpcs_received = IntCounter::with_opts(Opts::new(
            "dynamo_rpcs_received_total",
            "Total inbound RPCs received",
        ))
        .expect("rpcs_received counter");

        let rpcs_sent_by_type = IntCounterVec::new(
            Opts::new(
                "dynamo_rpcs_sent_by_type_total",
                "Outbound RPCs sent, by type",
            ),
            &["rpc_type"],
        )
        .expect("rpcs_sent_by_type counter vec");
        let rpcs_received_by_type = IntCounterVec::new(
            Opts::new(
                "dynamo_rpcs_received_by_type_total",
                "Inbound RPCs received, by type",
            ),
            &["rpc_type"],
        )
        .expect("rpcs_received_by_type counter vec");

        let rpc_latency_secs = HistogramVec::new(
            HistogramOpts::new("dynamo_rpc_latency_seconds", "RPC latency in seconds")
                .buckets(LATENCY_BUCKETS.to_vec()),
            &["rpc_type", "direction"],
        )
        .expect("rpc_latency_secs histogram");

        let kv_puts = IntCounter::with_opts(Opts::new("dynamo_kv_puts_total", "KV PUT operations"))
            .expect("kv_puts counter");
        let kv_gets = IntCounter::with_opts(Opts::new("dynamo_kv_gets_total", "KV GET operations"))
            .expect("kv_gets counter");
        let kv_deletes =
            IntCounter::with_opts(Opts::new("dynamo_kv_deletes_total", "KV DELETE operations"))
                .expect("kv_deletes counter");

        let kv_latency_secs = HistogramVec::new(
            HistogramOpts::new(
                "dynamo_kv_latency_seconds",
                "KV operation latency in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["op_type"],
        )
        .expect("kv_latency_secs histogram");

        let hints_stored = IntCounter::with_opts(Opts::new(
            "dynamo_hints_stored_total",
            "Hints stored for hinted handoff",
        ))
        .expect("hints_stored counter");
        let hints_delivered = IntCounter::with_opts(Opts::new(
            "dynamo_hints_delivered_total",
            "Hints successfully delivered",
        ))
        .expect("hints_delivered counter");

        let read_repairs = IntCounter::with_opts(Opts::new(
            "dynamo_read_repairs_total",
            "Read repair operations triggered",
        ))
        .expect("read_repairs counter");

        // Register all metrics
        registry
            .register(Box::new(rpcs_sent.clone()))
            .expect("register rpcs_sent");
        registry
            .register(Box::new(rpcs_received.clone()))
            .expect("register rpcs_received");
        registry
            .register(Box::new(rpcs_sent_by_type.clone()))
            .expect("register rpcs_sent_by_type");
        registry
            .register(Box::new(rpcs_received_by_type.clone()))
            .expect("register rpcs_received_by_type");
        registry
            .register(Box::new(rpc_latency_secs.clone()))
            .expect("register rpc_latency_secs");
        registry
            .register(Box::new(kv_puts.clone()))
            .expect("register kv_puts");
        registry
            .register(Box::new(kv_gets.clone()))
            .expect("register kv_gets");
        registry
            .register(Box::new(kv_deletes.clone()))
            .expect("register kv_deletes");
        registry
            .register(Box::new(kv_latency_secs.clone()))
            .expect("register kv_latency_secs");
        registry
            .register(Box::new(hints_stored.clone()))
            .expect("register hints_stored");
        registry
            .register(Box::new(hints_delivered.clone()))
            .expect("register hints_delivered");
        registry
            .register(Box::new(read_repairs.clone()))
            .expect("register read_repairs");

        Self {
            registry,
            rpcs_sent,
            rpcs_received,
            rpcs_sent_by_type,
            rpcs_received_by_type,
            rpc_latency_secs,
            kv_puts,
            kv_gets,
            kv_deletes,
            kv_latency_secs,
            hints_stored,
            hints_delivered,
            read_repairs,
        }
    }
}

/// Encode all registered metrics in Prometheus text exposition format.
pub fn encode_metrics() -> String {
    let m = metrics();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&m.registry.gather(), &mut buf)
        .expect("prometheus text encoding");
    String::from_utf8(buf).expect("prometheus output is valid UTF-8")
}

/// Helper: start an RPC latency timer. Returns a guard that records
/// elapsed time on drop.
pub fn start_rpc_timer(rpc_type: &str, direction: &str) -> prometheus::HistogramTimer {
    metrics()
        .rpc_latency_secs
        .with_label_values(&[rpc_type, direction])
        .start_timer()
}

/// Helper: start a KV operation latency timer.
pub fn start_kv_timer(op_type: &str) -> prometheus::HistogramTimer {
    metrics()
        .kv_latency_secs
        .with_label_values(&[op_type])
        .start_timer()
}

// ────────────────────────── Metrics HTTP server ──────────────────────────

use bytes::Bytes;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

async fn metrics_handler(
    _req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let body = encode_metrics();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .expect("valid HTTP response"))
}

/// Serve Prometheus metrics on the given address (`GET /metrics`).
///
/// This spawns a lightweight HTTP/1.1 server. Call from a `tokio::spawn`.
pub async fn serve_metrics(
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("metrics server listening on http://{}/metrics", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service_fn(metrics_handler))
                .await
            {
                tracing::debug!("metrics connection error: {}", e);
            }
        });
    }
}

// ────────────────────────── Tests ──────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Histogram;

    #[test]
    fn test_metrics_init_and_increment() {
        let m = metrics();

        let before_sent = m.rpcs_sent.get();
        m.rpcs_sent.inc();
        m.rpcs_sent.inc();
        assert_eq!(m.rpcs_sent.get(), before_sent + 2);

        let before_recv = m.rpcs_received.get();
        m.rpcs_received.inc();
        assert_eq!(m.rpcs_received.get(), before_recv + 1);

        m.kv_puts.inc();
        m.kv_gets.inc();
        m.kv_deletes.inc();

        m.rpcs_sent_by_type.with_label_values(&["ping"]).inc();
        m.rpcs_sent_by_type.with_label_values(&["find_node"]).inc();
        m.rpcs_sent_by_type.with_label_values(&["ping"]).inc();
    }

    #[test]
    fn test_encode_metrics_format() {
        // Ensure at least one counter is incremented
        metrics().hints_stored.inc();

        let output = encode_metrics();
        assert!(output.contains("dynamo_rpcs_sent_total"));
        assert!(output.contains("dynamo_hints_stored_total"));
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
    }

    #[test]
    fn test_histogram_records() {
        let m = metrics();

        m.rpc_latency_secs
            .with_label_values(&["test_rpc", "outbound"])
            .observe(0.005);
        m.rpc_latency_secs
            .with_label_values(&["test_rpc", "outbound"])
            .observe(0.010);

        let h: Histogram = m
            .rpc_latency_secs
            .with_label_values(&["test_rpc", "outbound"]);
        assert_eq!(h.get_sample_count(), 2);
        assert!((h.get_sample_sum() - 0.015).abs() < 1e-9);
    }
}
