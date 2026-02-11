//! Kademlia RPC types and the transport abstraction trait.

use crate::node_info::NodeInfo;
use dynamo_common::{KadError, NodeId};

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// A Kademlia RPC request.
#[derive(Debug, Clone)]
pub enum KadRequest {
    Ping {
        sender: NodeInfo,
    },
    FindNode {
        sender: NodeInfo,
        target: NodeId,
    },
    FindValue {
        sender: NodeInfo,
        key: NodeId,
    },
    Store {
        sender: NodeInfo,
        key: NodeId,
        value: Vec<u8>,
    },
}

impl KadRequest {
    /// Return the sender info from any request variant.
    pub fn sender(&self) -> &NodeInfo {
        match self {
            KadRequest::Ping { sender } => sender,
            KadRequest::FindNode { sender, .. } => sender,
            KadRequest::FindValue { sender, .. } => sender,
            KadRequest::Store { sender, .. } => sender,
        }
    }
}

/// A Kademlia RPC response.
#[derive(Debug, Clone)]
pub enum KadResponse {
    Pong {
        responder: NodeInfo,
    },
    FindNodeResult {
        responder: NodeInfo,
        closest: Vec<NodeInfo>,
    },
    FindValueResult {
        responder: NodeInfo,
        result: FindValueOutcome,
    },
    StoreOk {
        responder: NodeInfo,
    },
}

/// The result of a FIND_VALUE RPC.
#[derive(Debug, Clone)]
pub enum FindValueOutcome {
    /// The value was found on this node.
    Found(Vec<u8>),
    /// The value was not found; here are the closest known nodes.
    NotFound { closest: Vec<NodeInfo> },
}

// ---------------------------------------------------------------------------
// Transport trait
// ---------------------------------------------------------------------------

/// Abstraction over the network transport.
///
/// Implementations can be:
/// - `SimulatedTransport` for unit/integration tests (in-process, no real I/O)
/// - `GrpcTransport` for real deployment (wired up later)
#[async_trait::async_trait]
pub trait KadTransport: Send + Sync + 'static {
    /// Send a request to a target node and await the response.
    async fn send_request(
        &self,
        target: &NodeInfo,
        request: KadRequest,
    ) -> Result<KadResponse, KadError>;
}
