# dynamo-node

Binary entry point for a dynamo-kad node.

Loads YAML config, initialises Kademlia DHT with gRPC transport, wires the KV coordinator and storage engine, bootstraps from seed nodes, and serves all three gRPC services.

## Running

```bash
# With config file
cargo run -p dynamo-node -- config.yaml

# Defaults to 127.0.0.1:7000 if no config found
cargo run -p dynamo-node
```

## Config Example

```yaml
listen: "127.0.0.1:7000"
seeds:
  - "127.0.0.1:7001"
storage:
  data_dir: /tmp/dynamo-node-0
```

## Services Exposed

- Kademlia RPCs (Ping, FindNode, FindValue, Store)
- KV RPCs (Put, Get, Delete)
- Admin RPCs (Health, GetStats, ClusterView)
