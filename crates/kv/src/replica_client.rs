//! Abstraction over replica-to-replica RPCs.
//!
//! Concrete implementation lives in `dynamo-net::replica_client`.

use crate::coordinator::VersionedValue;
use dynamo_kad::node_info::NodeInfo;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("RPC failed: {0}")]
    RpcFailed(String),
    #[error("timeout")]
    Timeout,
}

/// Transport for replica RPCs (ReplicaPut, ReplicaGet).
///
/// Same pattern as `KadTransport` — a trait in the domain crate,
/// with a gRPC implementation in the `net` crate.
#[async_trait::async_trait]
pub trait ReplicaClient: Send + Sync + 'static {
    /// Send a versioned write to a remote replica node.
    async fn replica_put(
        &self,
        target: &NodeInfo,
        key: &str,
        versioned: &VersionedValue,
        write_id: &str,
    ) -> Result<(), ReplicaError>;

    /// Fetch all versions of a key from a remote replica node.
    async fn replica_get(
        &self,
        target: &NodeInfo,
        key: &str,
    ) -> Result<Vec<VersionedValue>, ReplicaError>;
}
