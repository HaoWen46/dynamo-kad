//! Admin service facade for dynamo-kad nodes.
//!
//! The [`AdminService`] gRPC implementation lives in the `dynamo-net` crate.
//! This module re-exports it for convenience.
//!
//! ## Endpoints
//!
//! | RPC | Description |
//! |-----|-------------|
//! | `Health` | Returns healthy status, node ID, uptime |
//! | `GetStats` | Routing table size, record count, RPC counters |
//! | `ClusterView` | All known nodes in the routing table |

pub use dynamo_net::AdminService;
