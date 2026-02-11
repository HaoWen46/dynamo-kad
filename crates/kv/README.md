# dynamo-kv

Dynamo-style KV layer on top of Kademlia.

## Features

- **Vector clocks** (`VClock`): causal versioning with increment, merge, and dominance comparison
- **Conflict detection**: concurrent writes produce siblings (multiple versions)
- **KV coordinator**: handles PUT/GET/DELETE with context passthrough
- **Key placement**: maps string keys to 160-bit Kademlia ID space via SHA-1

## Modules

| Module | Description |
|--------|-------------|
| `vclock` | Vector clock implementation (increment, merge, compare) |
| `coordinator` | `KvCoordinator` — local PUT/GET/DELETE with vclock versioning |
| `placement` | Maps string keys to `NodeId` for DHT routing |

## Usage Flow

1. Client PUTs `("mykey", value)` with optional context from a prior GET
2. Coordinator increments the vector clock for this node
3. Value is stored in the storage engine
4. On GET, all non-dominated versions are returned (siblings if concurrent)
5. Client merges/resolves and PUTs with the merged context
