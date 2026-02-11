//! Conversions between proto types and domain types.

use dynamo_common::{NodeId, ID_BYTES};
use dynamo_kad::node_info::NodeInfo;
use dynamo_proto::common as pb;
use std::net::SocketAddr;
use tokio::time::Instant;

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

pub fn node_id_to_proto(id: &NodeId) -> pb::NodeId {
    pb::NodeId {
        raw: id.as_bytes().to_vec(),
    }
}

pub fn node_id_from_proto(proto: &pb::NodeId) -> Result<NodeId, tonic::Status> {
    if proto.raw.len() != ID_BYTES {
        return Err(tonic::Status::invalid_argument(format!(
            "NodeId must be {} bytes, got {}",
            ID_BYTES,
            proto.raw.len()
        )));
    }
    let mut bytes = [0u8; ID_BYTES];
    bytes.copy_from_slice(&proto.raw);
    Ok(NodeId::from_bytes(bytes))
}

// ---------------------------------------------------------------------------
// NodeInfo
// ---------------------------------------------------------------------------

pub fn node_info_to_proto(info: &NodeInfo) -> pb::NodeInfo {
    pb::NodeInfo {
        id: Some(node_id_to_proto(&info.id)),
        address: info.addr.to_string(),
    }
}

pub fn node_info_from_proto(proto: &pb::NodeInfo) -> Result<NodeInfo, tonic::Status> {
    let id = node_id_from_proto(
        proto
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing node id"))?,
    )?;
    let addr: SocketAddr = proto
        .address
        .parse()
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid address: {}", e)))?;
    Ok(NodeInfo {
        id,
        addr,
        last_seen: Instant::now(),
    })
}

// ---------------------------------------------------------------------------
// VectorClock
// ---------------------------------------------------------------------------

pub fn vclock_to_proto(entries: &std::collections::HashMap<String, u64>) -> pb::VectorClock {
    pb::VectorClock {
        entries: entries.clone(),
    }
}

pub fn vclock_from_proto(proto: &pb::VectorClock) -> std::collections::HashMap<String, u64> {
    proto.entries.clone()
}

// ---------------------------------------------------------------------------
// VersionedValue
// ---------------------------------------------------------------------------

pub fn versioned_value_to_proto(vv: &dynamo_kv::coordinator::VersionedValue) -> pb::VersionedValue {
    pb::VersionedValue {
        value: vv.value.clone(),
        vclock: Some(vclock_to_proto(vv.vclock.entries())),
        timestamp_ms: 0,
        tombstone: vv.tombstone,
    }
}

pub fn versioned_value_from_proto(
    proto: &pb::VersionedValue,
) -> dynamo_kv::coordinator::VersionedValue {
    let vclock = proto
        .vclock
        .as_ref()
        .map(|vc| dynamo_kv::vclock::VClock::from_map(vclock_from_proto(vc)))
        .unwrap_or_default();

    dynamo_kv::coordinator::VersionedValue {
        value: proto.value.clone(),
        vclock,
        tombstone: proto.tombstone,
    }
}
