//! Generated gRPC code for dynamo-kad protobuf definitions.

/// Common types (NodeId, NodeInfo, VectorClock, VersionedValue).
pub mod common {
    tonic::include_proto!("dynamo.common");
}

/// Kademlia RPC service (Ping, FindNode, FindValue, Store).
pub mod kad {
    tonic::include_proto!("dynamo.kad");
}

/// KV service (Put, Get, Delete) and Replica service.
pub mod kv {
    tonic::include_proto!("dynamo.kv");
}

/// Admin service (Health, Stats, ClusterView).
pub mod admin {
    tonic::include_proto!("dynamo.admin");
}
