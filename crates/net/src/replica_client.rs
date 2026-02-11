//! gRPC client implementing `ReplicaClient`.
//!
//! `GrpcReplicaClient` connects to remote nodes via tonic and translates
//! between proto types and the domain `VersionedValue` / `ReplicaError` types.

use crate::convert;
use dynamo_common::NodeId;
use dynamo_kad::node_info::NodeInfo;
use dynamo_kv::coordinator::VersionedValue;
use dynamo_kv::replica_client::{ReplicaClient, ReplicaError};
use dynamo_proto::kv::replica_service_client::ReplicaServiceClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

/// A gRPC-based replica transport.
///
/// Maintains a pool of tonic channels to remote nodes, reusing
/// the same connection caching strategy as `GrpcTransport`.
#[derive(Debug)]
pub struct GrpcReplicaClient {
    channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
}

impl GrpcReplicaClient {
    pub fn new(channels: Arc<RwLock<HashMap<NodeId, Channel>>>) -> Self {
        Self { channels }
    }

    async fn get_channel(&self, target: &NodeInfo) -> Result<Channel, ReplicaError> {
        // Check cache first
        {
            let cache = self.channels.read().await;
            if let Some(channel) = cache.get(&target.id) {
                return Ok(channel.clone());
            }
        }

        // Create new connection
        let endpoint = format!("http://{}", target.addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| ReplicaError::RpcFailed(format!("invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| ReplicaError::RpcFailed(format!("connect failed: {}", e)))?;

        // Cache it
        {
            let mut cache = self.channels.write().await;
            cache.insert(target.id, channel.clone());
        }

        Ok(channel)
    }
}

#[async_trait::async_trait]
impl ReplicaClient for GrpcReplicaClient {
    async fn replica_put(
        &self,
        target: &NodeInfo,
        key: &str,
        versioned: &VersionedValue,
        write_id: &str,
    ) -> Result<(), ReplicaError> {
        let m = dynamo_metrics::metrics();
        m.rpcs_sent.inc();
        m.rpcs_sent_by_type
            .with_label_values(&["replica_put"])
            .inc();
        let _timer = dynamo_metrics::start_rpc_timer("replica_put", "outbound");

        let channel = self.get_channel(target).await?;
        let mut client = ReplicaServiceClient::new(channel);

        let proto_vv = convert::versioned_value_to_proto(versioned);

        client
            .replica_put(dynamo_proto::kv::ReplicaPutRequest {
                key: key.to_string(),
                versioned: Some(proto_vv),
                write_id: write_id.to_string(),
            })
            .await
            .map_err(|e| ReplicaError::RpcFailed(format!("replica_put RPC failed: {}", e)))?;

        Ok(())
    }

    async fn replica_get(
        &self,
        target: &NodeInfo,
        key: &str,
    ) -> Result<Vec<VersionedValue>, ReplicaError> {
        let m = dynamo_metrics::metrics();
        m.rpcs_sent.inc();
        m.rpcs_sent_by_type
            .with_label_values(&["replica_get"])
            .inc();
        let _timer = dynamo_metrics::start_rpc_timer("replica_get", "outbound");

        let channel = self.get_channel(target).await?;
        let mut client = ReplicaServiceClient::new(channel);

        let resp = client
            .replica_get(dynamo_proto::kv::ReplicaGetRequest {
                key: key.to_string(),
            })
            .await
            .map_err(|e| ReplicaError::RpcFailed(format!("replica_get RPC failed: {}", e)))?;

        let inner = resp.into_inner();
        let versions = inner
            .versions
            .iter()
            .map(convert::versioned_value_from_proto)
            .collect();

        Ok(versions)
    }
}
