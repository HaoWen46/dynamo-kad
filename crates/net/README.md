# dynamo-net

gRPC networking layer — bridges proto definitions to domain logic.

## Components

| Module | Description |
|--------|-------------|
| `convert` | Proto <-> domain type conversions (NodeId, NodeInfo, VClock, etc.) |
| `server` | gRPC service implementations: `KademliaService`, `KvService`, `AdminService` |
| `client` | `GrpcTransport` — implements `KadTransport` over tonic with connection pooling |
| `lib` | `build_server()` — assembles all services into a tonic `Router` |

## Server Services

- **Kademlia** (`/dynamo.kad.Kademlia/*`): Ping, FindNode, FindValue, Store
- **KV** (`/dynamo.kv.KvService/*`): Put, Get, Delete
- **Admin** (`/dynamo.admin.AdminService/*`): Health, GetStats, ClusterView

## Client

`GrpcTransport` implements `KadTransport` for real network I/O:
- Lazy connection creation (connects on first RPC to a peer)
- Connection caching (reuses channels to the same node)
- Full proto <-> domain conversion for all Kademlia RPC types
