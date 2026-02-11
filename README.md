# dynamo-kad

A distributed key-value store combining **Kademlia DHT** routing with **Amazon Dynamo**-style replication semantics, implemented in Rust.

Kademlia provides the peer-to-peer overlay — every node maintains a routing table of peers organised by XOR distance, and iterative parallel lookups converge on the k closest nodes for any key. Dynamo supplies the replication model — writes fan out to N replicas, reads contact N replicas and reconcile via vector clocks, and configurable quorums (R, W) let operators trade consistency for availability.

~9 850 lines of Rust across 10 crates, 152 tests, 12 Criterion benchmarks, and a GitHub Actions CI pipeline.

---

## Architecture

```
                         ┌──────────────────────────────────────┐
                         │             node (bin)               │
                         │  config loading · bootstrap · serve  │
                         │         graceful shutdown            │
                         └──────────┬──────────┬──────────────-─┘
                                    │          │
               ┌────────────────────┘          └──────────────────┐
               ▼                                                  ▼
  ┌────────────────────────┐                        ┌──────────────────────┐
  │       net (lib)        │                        │    metrics (lib)     │
  │  tonic gRPC transport  │                        │  Prometheus counters │
  │  4 services · 13 RPCs  │                        │  + HTTP /metrics     │
  └───┬────────────────────┘                        └──────────────────────┘
      │         │    │
      │         │    └──────────────────────┐
      ▼         ▼                           ▼
  ┌────────┐ ┌────────────────┐   ┌──────────────────┐
  │  kad   │ │   kv (lib)     │   │   admin (lib)    │
  │ (lib)  │ │  coordinator   │   │   re-exports     │
  │        │ │  vclock·merkle │   │   AdminService   │
  │ routing│ │  read repair   │   └──────────────────┘
  │ lookup │ │  hinted handoff│
  │ store  │ │  placement     │
  └───┬────┘ └──────┬─────────┘
      │             │
      ▼             ▼
  ┌──────────────────────┐
  │    storage (lib)     │
  │  WAL + memtable      │
  │  versioned records   │
  └──────────────────────┘
      │
      ▼
  ┌──────────────────────┐      ┌──────────────────────┐
  │    common (lib)      │      │    config (lib)      │
  │  NodeId · Distance   │      │  YAML schema         │
  │  XOR metric · errors │      │  19 config params    │
  └──────────────────────┘      └──────────────────────┘
      │
      ▼
  ┌──────────────────────┐
  │    proto (lib)       │
  │  tonic-build codegen │
  │  specs/v1/*.proto    │
  └──────────────────────┘
```

---

## Key Features

- **Kademlia DHT**: 160-bit XOR metric, k-buckets, iterative parallel lookup (alpha=3), periodic bucket refresh
- **Dynamo replication**: configurable N/R/W quorums with per-request overrides
- **Vector clocks**: causal versioning with sibling detection for concurrent writes
- **Read repair**: async background repair of stale replicas on the read path
- **Hinted handoff**: local hint storage with periodic delivery to recovered nodes
- **Merkle trees**: binary hash tree for efficient anti-entropy key-range diffing
- **Storage engine**: write-ahead log (CRC32 framed) + in-memory memtable
- **gRPC API**: 4 services, 13 RPCs (Kademlia, KV, Replica, Admin)
- **Prometheus metrics**: 13 metrics (RPC counters, KV latency histograms, hints, read repairs)
- **Chaos testing**: fault injection for network partitions, random failures, latency
- **Config validation**: rejects impossible values (R > N, W > N, zero parameters)

---

## Milestones

| Milestone                    | Status | Scope                                                                                        |
| ---------------------------- | ------ | -------------------------------------------------------------------------------------------- |
| **M0** — Scaffolding         | Done   | Workspace, common crate, protobuf definitions                                                |
| **M1** — Kademlia Core       | Done   | K-buckets, iterative lookup, record store, SimTransport tests                                |
| **M2** — KV MVP + Networking | Done   | Storage engine, KV coordinator, gRPC transport, integration tests                            |
| **M3** — Dynamo Semantics    | Done   | Vector clocks, quorum R/W, read repair, hinted handoff                                       |
| **M4** — Repair & Handoff    | Done   | Hint delivery, Merkle tree anti-entropy                                                      |
| **M5** — Bench & Chaos       | Done   | Prometheus metrics, Criterion benchmarks, chaos transport, multi-node chaos integration      |
| **M6** — Polish              | Done   | Workspace lints, rustfmt, graceful shutdown, configurable parameters, Justfile, docs         |
| **M7** — Wire Loose Ends     | Done   | Periodic bucket refresh, metrics wiring, per-request quorum overrides, config validation, CI |

---

## Project Layout

```
dynamo-kad/
├── Cargo.toml             # Workspace manifest
├── Cargo.lock
├── Justfile               # Task runner (just check, just test, etc.)
├── config.example.yaml    # Annotated example config
├── crates/
│   ├── common/            # NodeId, Distance, XOR metric (425 lines)
│   ├── proto/             # Generated gRPC code (21 lines)
│   ├── config/            # YAML config schema + validation (368 lines)
│   ├── storage/           # WAL, memtable, persistence (914 lines)
│   ├── kad/               # Kademlia DHT: routing, lookup, store (2 806 lines)
│   ├── kv/                # Dynamo KV: quorum, vclock, repair (2 441 lines)
│   ├── net/               # gRPC transport (tonic) (981 lines)
│   ├── admin/             # Admin service facade (14 lines)
│   ├── metrics/           # Prometheus + tracing (333 lines)
│   └── node/              # Binary entry point (180 lines)
├── specs/                 # Protobuf definitions (4 files, 306 lines)
│   └── v1/
│       ├── common.proto
│       ├── kad.proto
│       ├── kv.proto
│       └── admin.proto
├── .github/workflows/     # CI pipeline
├── REPORT.md              # Comprehensive technical report
└── .gitignore
```

---

## Quick Start

```bash
# Build
cargo build --workspace

# Test (152 tests)
cargo test --workspace

# Lint (zero warnings)
cargo clippy --workspace --all-targets -- -D warnings

# Format check
cargo fmt --all -- --check

# Benchmarks
cargo bench --workspace

# Full CI check (fmt + clippy + test)
just check

# Run a node
cargo run -p dynamo-node -- config.example.yaml

# Run with defaults (127.0.0.1:7000, standalone)
cargo run -p dynamo-node
```

Requires [just](https://github.com/casey/just) for task runner recipes.

---

## Testing

**152 tests** across 7 crates using three layers of test infrastructure:

| Layer       | Transport                                   | Purpose                                                |
| ----------- | ------------------------------------------- | ------------------------------------------------------ |
| Unit        | `SimulatedTransport` / `InMemReplicaClient` | Routing, lookup, vclock, merkle, quorum logic          |
| Chaos       | `ChaosTransport` / `ChaosReplicaClient`     | Network partitions, random failures, latency injection |
| Integration | `GrpcTransport` / `GrpcReplicaClient`       | End-to-end gRPC with real tonic servers                |

| Crate     | Tests | Highlights                                                             |
| --------- | ----: | ---------------------------------------------------------------------- |
| `common`  |    13 | XOR distance properties, bucket indexing                               |
| `config`  |     7 | YAML parsing, validation (R>N, W>N, zero values)                       |
| `storage` |    25 | WAL CRC integrity, crash replay, memtable versioning                   |
| `kad`     |    43 | K-bucket LRU, lookup convergence, bootstrap, refresh, chaos            |
| `kv`      |    53 | Quorum R/W, vclock ordering, read repair, hinted handoff, 5-node chaos |
| `net`     |     8 | gRPC health, PUT/GET round-trip, multi-node bootstrap                  |
| `metrics` |     3 | Counter increments, histogram recording, Prometheus encoding           |

---

## Configuration

19 parameters configurable via YAML. See [`config.example.yaml`](config.example.yaml).

| Section      | Key parameters                                          | Defaults                              |
| ------------ | ------------------------------------------------------- | ------------------------------------- |
| **Kademlia** | `k`, `alpha`, `rpc_timeout_ms`, `refresh_interval_secs` | k=20, alpha=3, 5s timeout, 1h refresh |
| **KV**       | `n`, `r`, `w`, `read_repair`, `hinted_handoff`          | N=3, R=2, W=2, both enabled           |
| **Storage**  | `data_dir`, `fsync`                                     | `data/`, batch fsync                  |
| **Node**     | `listen`, `seeds`, `metrics_port`                       | Required listen address               |

---

## References

- **Dynamo** — Amazon's Highly Available Key-Value Store (DeCandia et al., 2007)
- **Kademlia** — A P2P Information System Based on the XOR Metric (Maymounkov & Mazieres, 2002)
- **Time, Clocks, and the Ordering of Events** — Lamport, 1978
- **Epidemic Algorithms for Replicated Database Maintenance** — Demers et al., 1987

---

## License

MIT OR Apache-2.0
