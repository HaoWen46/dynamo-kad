# dynamo-kad

Kademlia DHT implementation with iterative parallel lookups.

## Modules

| Module | Description |
|--------|-------------|
| `routing_table` | 160 k-buckets with LRU eviction. `update()` returns `Pending` when full, letting the caller async-ping before deciding. |
| `lookup` | Iterative parallel lookup with configurable `k` and `alpha`. Uses `FuturesUnordered` for concurrent queries. |
| `store` | In-memory key-value record store with optional TTL expiry. |
| `refresh` | Bucket refresh — generates random IDs in stale bucket ranges and performs lookups to repopulate them. |
| `rpc` | RPC request/response types and the `KadTransport` trait. |
| `node_info` | `NodeInfo` peer descriptor (ID + address + last_seen). |
| `key` | Re-exports from `common` plus key helpers. |

## `KadTransport` Trait

The core abstraction enabling testability:

```rust
#[async_trait]
pub trait KadTransport: Send + Sync + 'static {
    async fn send_request(
        &self,
        target: &NodeInfo,
        request: KadRequest,
    ) -> Result<KadResponse, KadError>;
}
```

In tests, `SimTransportInner` routes RPCs between in-process `Kad` instances. In production, a gRPC transport will implement this trait.

## `Kad<T>` Coordinator

Top-level API:

- `bootstrap(seeds)` — join the network via seed nodes + self-lookup
- `find_node(target)` — iterative lookup for k closest nodes
- `store(key, value)` — store on k closest nodes
- `find_value(key)` — retrieve a value (short-circuits on first found)
- `handle_request(req)` — process incoming PING / FIND_NODE / FIND_VALUE / STORE

## Tests

50 tests covering:
- XOR distance properties, bucket indexing
- Routing table insert/update/eviction/closest_nodes
- Iterative lookup convergence (brute-force verified on 50-node networks)
- Lookup with failures (30% failure rate), sparse networks, termination
- Store/retrieve, record expiry
- Bootstrap (single-seed, incremental 20-node)
- Cross-network find_node and store+retrieve
- Node departure resilience
