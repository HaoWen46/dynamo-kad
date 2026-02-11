# dynamo-proto

Generated gRPC/protobuf bindings for the dynamo-kad project.

## Proto Definitions

Source `.proto` files live in `specs/v1/`:

| File | Package | Services |
|------|---------|----------|
| `common.proto` | `dynamo.common` | Shared types: `NodeId`, `NodeInfo`, `VectorClock`, `VersionedValue` |
| `kad.proto` | `dynamo.kad` | `Kademlia` — Ping, FindNode, FindValue, Store |
| `kv.proto` | `dynamo.kv` | `KvService` (client-facing), `ReplicaService` (internal) |
| `admin.proto` | `dynamo.admin` | `AdminService` — Health, Stats, ClusterView |

## Usage

```rust
use dynamo_proto::kad::kademlia_client::KademliaClient;
use dynamo_proto::kv::kv_service_server::KvServiceServer;
use dynamo_proto::common::NodeId;
```

Code is generated at build time by `tonic-build` via `build.rs`.
