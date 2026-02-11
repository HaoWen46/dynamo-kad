//! gRPC client implementing `KadTransport`.
//!
//! `GrpcTransport` connects to remote nodes via tonic and translates
//! between proto types and the `KadRequest`/`KadResponse` domain types.

use crate::convert;
use dynamo_common::{KadError, NodeId};
use dynamo_kad::node_info::NodeInfo;
use dynamo_kad::rpc::{FindValueOutcome, KadRequest, KadResponse, KadTransport};
use dynamo_proto::kad::kademlia_client::KademliaClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

/// A gRPC-based Kademlia transport.
///
/// Maintains a pool of tonic channels to remote nodes, creating new
/// connections on demand.
#[derive(Debug)]
pub struct GrpcTransport {
    local_info: NodeInfo,
    channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
}

impl GrpcTransport {
    pub fn new(local_info: NodeInfo) -> Self {
        Self {
            local_info,
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_channel(&self, target: &NodeInfo) -> Result<Channel, KadError> {
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
            .map_err(|e| KadError::Internal(format!("invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| KadError::Internal(format!("connect failed: {}", e)))?;

        // Cache it
        {
            let mut cache = self.channels.write().await;
            cache.insert(target.id, channel.clone());
        }

        Ok(channel)
    }

    /// Remove a cached channel (e.g., on connection failure).
    pub async fn invalidate(&self, node_id: &NodeId) {
        let mut cache = self.channels.write().await;
        cache.remove(node_id);
    }
}

#[async_trait::async_trait]
impl KadTransport for GrpcTransport {
    async fn send_request(
        &self,
        target: &NodeInfo,
        request: KadRequest,
    ) -> Result<KadResponse, KadError> {
        let channel = self.get_channel(target).await?;
        let mut client = KademliaClient::new(channel);

        let sender_proto = convert::node_info_to_proto(&self.local_info);

        let rpc_type = match &request {
            KadRequest::Ping { .. } => "ping",
            KadRequest::FindNode { .. } => "find_node",
            KadRequest::FindValue { .. } => "find_value",
            KadRequest::Store { .. } => "store",
        };
        let m = dynamo_metrics::metrics();
        m.rpcs_sent.inc();
        m.rpcs_sent_by_type.with_label_values(&[rpc_type]).inc();
        let _timer = dynamo_metrics::start_rpc_timer(rpc_type, "outbound");

        match request {
            KadRequest::Ping { .. } => {
                let resp = client
                    .ping(dynamo_proto::kad::PingRequest {
                        sender: Some(sender_proto),
                    })
                    .await
                    .map_err(|e| KadError::Internal(format!("ping RPC failed: {}", e)))?;

                let inner = resp.into_inner();
                let responder = convert::node_info_from_proto(
                    inner
                        .responder
                        .as_ref()
                        .ok_or_else(|| KadError::Internal("missing responder".into()))?,
                )
                .map_err(|e| KadError::Internal(format!("invalid responder: {}", e)))?;

                Ok(KadResponse::Pong { responder })
            }

            KadRequest::FindNode { target: tgt, .. } => {
                let resp = client
                    .find_node(dynamo_proto::kad::FindNodeRequest {
                        sender: Some(sender_proto),
                        target: Some(convert::node_id_to_proto(&tgt)),
                    })
                    .await
                    .map_err(|e| KadError::Internal(format!("find_node RPC failed: {}", e)))?;

                let inner = resp.into_inner();
                let responder = convert::node_info_from_proto(
                    inner
                        .responder
                        .as_ref()
                        .ok_or_else(|| KadError::Internal("missing responder".into()))?,
                )
                .map_err(|e| KadError::Internal(format!("invalid responder: {}", e)))?;

                let closest: Result<Vec<NodeInfo>, _> = inner
                    .closest
                    .iter()
                    .map(convert::node_info_from_proto)
                    .collect();
                let closest = closest
                    .map_err(|e| KadError::Internal(format!("invalid closest node: {}", e)))?;

                Ok(KadResponse::FindNodeResult { responder, closest })
            }

            KadRequest::FindValue { key, .. } => {
                let resp = client
                    .find_value(dynamo_proto::kad::FindValueRequest {
                        sender: Some(sender_proto),
                        key: Some(convert::node_id_to_proto(&key)),
                    })
                    .await
                    .map_err(|e| KadError::Internal(format!("find_value RPC failed: {}", e)))?;

                let inner = resp.into_inner();
                let responder = convert::node_info_from_proto(
                    inner
                        .responder
                        .as_ref()
                        .ok_or_else(|| KadError::Internal("missing responder".into()))?,
                )
                .map_err(|e| KadError::Internal(format!("invalid responder: {}", e)))?;

                let result = match inner.result {
                    Some(dynamo_proto::kad::find_value_response::Result::Value(v)) => {
                        FindValueOutcome::Found(v)
                    }
                    Some(dynamo_proto::kad::find_value_response::Result::Closest(find_resp)) => {
                        let closest: Result<Vec<NodeInfo>, _> = find_resp
                            .closest
                            .iter()
                            .map(convert::node_info_from_proto)
                            .collect();
                        let closest = closest.map_err(|e| {
                            KadError::Internal(format!("invalid closest node: {}", e))
                        })?;
                        FindValueOutcome::NotFound { closest }
                    }
                    None => FindValueOutcome::NotFound { closest: vec![] },
                };

                Ok(KadResponse::FindValueResult { responder, result })
            }

            KadRequest::Store { key, value, .. } => {
                let resp = client
                    .store(dynamo_proto::kad::StoreRequest {
                        sender: Some(sender_proto),
                        key: Some(convert::node_id_to_proto(&key)),
                        value,
                    })
                    .await
                    .map_err(|e| KadError::Internal(format!("store RPC failed: {}", e)))?;

                let inner = resp.into_inner();
                let responder = convert::node_info_from_proto(
                    inner
                        .responder
                        .as_ref()
                        .ok_or_else(|| KadError::Internal("missing responder".into()))?,
                )
                .map_err(|e| KadError::Internal(format!("invalid responder: {}", e)))?;

                Ok(KadResponse::StoreOk { responder })
            }
        }
    }
}
