# dynamo-metrics

Metrics and tracing setup for dynamo-kad.

Currently provides `init_tracing()` which configures a `tracing-subscriber` with env-filter support.

## Usage

```rust
dynamo_metrics::init_tracing();
// Set RUST_LOG=debug for verbose output
```

## Planned

- Prometheus metrics export
- RPC latency histograms
- Routing table size gauge
- Record store size gauge
