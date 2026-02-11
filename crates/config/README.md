# dynamo-config

Configuration schema and YAML loader for dynamo-kad nodes.

## Config Structure

```yaml
listen: "127.0.0.1:7000"
seeds:
  - "127.0.0.1:7001"

kademlia:
  k: 20           # bucket size
  alpha: 3         # lookup parallelism
  rpc_timeout_ms: 5000
  refresh_interval_secs: 3600

kv:
  n: 3             # replication factor
  r: 2             # read quorum
  w: 2             # write quorum
  read_repair: true
  hinted_handoff: true

storage:
  data_dir: data
  fsync: batch     # "always" | "batch" | "none"
```

All fields except `listen` have sensible defaults. Seeds can be empty for a standalone node.

## Usage

```rust
let config = dynamo_config::load_from_file(Path::new("config.yaml"))?;
// or
let config = dynamo_config::load_from_str(yaml_string)?;
```
