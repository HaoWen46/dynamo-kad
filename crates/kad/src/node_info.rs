//! Information about a known peer in the network.

use dynamo_common::NodeId;
use std::net::SocketAddr;
use tokio::time::Instant;

/// Descriptor for a peer node in the Kademlia network.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// The node's 160-bit Kademlia identifier.
    pub id: NodeId,
    /// Network address (used by real transports; placeholder in simulated tests).
    pub addr: SocketAddr,
    /// When we last successfully communicated with this node.
    pub last_seen: Instant,
}

impl NodeInfo {
    /// Create a new `NodeInfo` with `last_seen` set to now.
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            last_seen: Instant::now(),
        }
    }

    /// Create a `NodeInfo` with a dummy address (useful for testing).
    pub fn with_dummy_addr(id: NodeId) -> Self {
        Self::new(id, "127.0.0.1:0".parse().unwrap())
    }

    /// Touch the last_seen timestamp to now.
    pub fn touch(&mut self) {
        self.last_seen = Instant::now();
    }
}

impl PartialEq for NodeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for NodeInfo {}

impl std::hash::Hash for NodeInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
