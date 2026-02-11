# dynamo-admin

Admin/ops service facade for dynamo-kad nodes.

Re-exports `AdminService` from `dynamo-net` for convenience. The actual gRPC
service implementation lives in `dynamo-net::server::AdminService`.

## Endpoints

| RPC | Description |
|-----|-------------|
| `Health` | Returns healthy status, node ID, uptime |
| `GetStats` | Routing table size, record count, RPC counters |
| `ClusterView` | All known nodes in the routing table |
