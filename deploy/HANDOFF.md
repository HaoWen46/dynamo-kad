# CSIE Deployment Handoff

This document is for the Claude Code instance running on the CSIE workstation pool.
It tells you what's been built, what to test, and what to watch for.

---

## What's in this branch (`deploy/csie-cluster`)

Three new files under `deploy/`:

| File | Purpose |
|------|---------|
| `cluster.conf` | Cluster definition: 10 node IPs, ports, paths, tunables |
| `dynamoctl.sh` | Orchestrator script: build, distribute, config, start/stop, health, logs |
| `workload.sh` | Workload driver: PUT/GET/sequential/mixed via grpcurl |

**No Rust code was changed.** The existing binary + config schema handles everything.

---

## Network rules (critical)

- **USE ONLY** `140.112.30.x` for all TCP services. The `10.217.44.x` interface is broken for TCP.
- Ports `51200` (gRPC) and `51201` (metrics) are used. If either conflicts, edit `GRPC_PORT` / `METRICS_PORT` in `cluster.conf`.
- SSH on port 22 works on `140.112.30.x`.
- Nodes: `140.112.30.182` through `140.112.30.191` (10 machines).

---

## Step-by-step deployment

All commands run from the project root on **any one** of the 10 machines (or any machine with SSH access to all 10).

### 0. Prerequisites check

```bash
# Verify you're on the right branch
git branch --show-current  # should say deploy/csie-cluster

# Rust toolchain available?
cargo --version

# grpcurl available? (optional but strongly recommended)
grpcurl --version  # if missing: install via https://github.com/fullstorydev/grpcurl/releases

# SSH access works? Pick any node:
ssh -o ConnectTimeout=5 140.112.30.185 hostname
```

### 1. Build

**If running on a CSIE node:**
```bash
./deploy/dynamoctl.sh build
```

**If running from a remote machine (e.g., macOS laptop):**
```bash
./deploy/dynamoctl.sh build-remote
# Builds on the first CSIE node via SSH, using the NFS-shared project dir.
# Or specify a node: ./deploy/dynamoctl.sh build-remote 140.112.30.183
```

There is also `deploy-remote` which does the full pipeline using remote build:
```bash
./deploy/dynamoctl.sh deploy-remote   # build-remote → distribute → config → start → health
```

This runs `cargo build --release -p dynamo-node`. Takes a few minutes first time.
The binary lands at `target/release/dynamo-node` (on NFS, visible from all nodes).

### 2. Check ports are free

```bash
./deploy/dynamoctl.sh check-ports
```

If any port is in use, either:
- Change `GRPC_PORT`/`METRICS_PORT` in `deploy/cluster.conf`
- Or figure out what's using it: `ssh <ip> "ss -tlnp | grep 51200"`

### 3. Distribute binary

```bash
./deploy/dynamoctl.sh distribute
```

Copies the binary to `/tmp2/dynamo-kad/bin/dynamo-node` on all 10 nodes via scp (runs in parallel).

### 4. Generate & distribute configs

```bash
./deploy/dynamoctl.sh gen-config
```

Creates `/tmp2/dynamo-kad/config/node.yaml` on each node with the correct `listen` address, seeds, and paths.

To preview configs without SSH (just writes to `deploy/generated/`):
```bash
./deploy/dynamoctl.sh gen-config-local
cat deploy/generated/node-185.yaml
```

### 5. Start the cluster

```bash
./deploy/dynamoctl.sh start
```

This starts in two waves:
1. Seed nodes (182, 183, 184) — wait 2s
2. Remaining 7 nodes

Then waits 5s for Kademlia bootstrap.

### 6. Verify

```bash
# Process check
./deploy/dynamoctl.sh status

# gRPC health (needs grpcurl)
./deploy/dynamoctl.sh health

# Routing table should show 9 peers
./deploy/dynamoctl.sh cluster-view

# Functional test
./deploy/workload.sh basic

# Cross-node replication
./deploy/workload.sh cross
```

### 7. Stopping

```bash
./deploy/dynamoctl.sh stop         # all nodes
./deploy/dynamoctl.sh stop 140.112.30.185  # one node
```

### 8. Cleanup

```bash
./deploy/dynamoctl.sh clean   # removes data/logs (must stop first)
./deploy/dynamoctl.sh nuke    # removes entire /tmp2/dynamo-kad (asks YES)
```

---

## Troubleshooting guide

### Node won't start
```bash
./deploy/dynamoctl.sh logs <ip>
# Check for: "Address already in use" → port conflict
# Check for: config parse errors → bad YAML
```

### Bootstrap fails (0 peers in routing table)
```bash
./deploy/dynamoctl.sh cluster-view <ip>
# If known_nodes is empty:
# 1. Are seed nodes running?  ./deploy/dynamoctl.sh status
# 2. Can this node reach seeds?
ssh <ip> "timeout 3 bash -c 'echo >/dev/tcp/140.112.30.182/51200' && echo OK"
# 3. Check seed node logs for incoming connection attempts
./deploy/dynamoctl.sh logs 140.112.30.182
```

### Health says FAIL but status says UP
The process is running but gRPC isn't responding. Check:
```bash
# Is it actually listening?
ssh <ip> "ss -tlnp | grep 51200"
# Read the log for panics or errors:
./deploy/dynamoctl.sh logs <ip> 100
```

### Node 140.112.30.190 issues
This node was flagged as "uncertain" for connectivity. If it fails:
```bash
# Test raw TCP from another node:
ssh 140.112.30.182 "timeout 3 bash -c 'echo >/dev/tcp/140.112.30.190/51200' && echo OK || echo FAIL"
```
If 190 consistently fails, remove it from `NODES` in `cluster.conf` and redeploy.

### "Connection refused" everywhere
Verify you're not accidentally using `10.217.44.x`:
```bash
# On the node, check what address the process is bound to:
ssh <ip> "ss -tlnp | grep dynamo"
# Should show 140.112.30.xxx:51200, NOT 10.xxx or 127.xxx
```

### grpcurl proto path issues
If grpcurl can't find protos, the import hierarchy is:
```
specs/v1/
├── admin.proto    (imports common.proto)
├── common.proto   (no imports)
├── kad.proto      (imports common.proto)
└── kv.proto       (imports common.proto)
```
All grpcurl commands use `-import-path specs/v1`. Make sure you run from the project root or that `PROTO_DIR` resolves correctly.

---

## Architecture quick reference

### Config fields that matter for deployment
```yaml
listen: "140.112.30.X:51200"   # gRPC bind address (specific interface!)
seeds: [...]                    # Bootstrap peers (first 3 nodes)
metrics_port: 51201             # Prometheus HTTP (binds 0.0.0.0)
storage.data_dir: "/tmp2/..."   # Absolute path, local disk
```

### gRPC services (all on same port)
| Service | Key RPCs | Use |
|---------|----------|-----|
| `AdminService` | Health, GetStats, ClusterView | Operational |
| `KvService` | Put, Get, Delete | Client API |
| `Kademlia` | Ping, FindNode, FindValue, Store | DHT internal |
| `ReplicaService` | ReplicaPut, ReplicaGet, HintDeliver | Replication internal |

### Process lifecycle
- Binary: `/tmp2/dynamo-kad/bin/dynamo-node`
- Config: `/tmp2/dynamo-kad/config/node.yaml`
- Data: `/tmp2/dynamo-kad/data/` (WAL + hints)
- Logs: `/tmp2/dynamo-kad/logs/dynamo-node.log`
- PID: `/tmp2/dynamo-kad/dynamo-node.pid`
- Shutdown: `kill -INT` (SIGINT → graceful tokio shutdown)
- Node ID: randomly generated each start (no persistent identity)

### What "bootstrap" does
1. Node starts gRPC server on its `listen` address
2. Background task sends `Ping` + `FindNode(self)` to each seed
3. Discovered nodes are added to the Kademlia routing table
4. All routing table buckets are refreshed
5. Result: every node knows about every other node (for 10 nodes, ~9 entries)

---

## What to report back

After testing, it would be useful to know:

1. **Which nodes are reachable** — did all 10 work, or did any fail (especially 190)?
2. **Bootstrap convergence** — does `cluster-view` show all 9 peers on each node?
3. **Port conflicts** — did 51200/51201 work, or did you need to change them?
4. **KV operations** — did `workload.sh basic` and `workload.sh cross` succeed?
5. **Any script bugs** — SSH quoting issues, path problems, missing utilities?
6. **Performance notes** — build time, startup time, rough ops/sec from sequential workload?

---

## Quick command cheat sheet

```bash
./deploy/dynamoctl.sh deploy        # do everything
./deploy/dynamoctl.sh status        # are nodes running?
./deploy/dynamoctl.sh health        # are gRPC endpoints healthy?
./deploy/dynamoctl.sh cluster-view  # does the DHT see all peers?
./deploy/dynamoctl.sh logs <ip>     # what's in the log?
./deploy/dynamoctl.sh stop          # shut it all down
./deploy/dynamoctl.sh clean         # wipe data for fresh start
./deploy/workload.sh basic          # quick PUT+GET test
./deploy/workload.sh sequential 50  # bulk write+verify
```
