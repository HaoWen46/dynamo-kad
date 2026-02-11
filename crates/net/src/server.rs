//! gRPC service implementations.
//!
//! Bridges tonic-generated service traits to the domain logic
//! in the `kad` and `kv` crates.

use crate::convert;
use dynamo_kad::rpc::{FindValueOutcome, KadRequest, KadResponse, KadTransport};
use dynamo_kad::Kad;
use dynamo_kv::coordinator::KvCoordinator;
use dynamo_kv::replica_client::ReplicaClient;
use dynamo_kv::vclock::VClock;
use dynamo_storage::engine::StorageEngine;
use dynamo_storage::record::StorageRecord;
use std::sync::Arc;
use std::time::Instant as StdInstant;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Kademlia gRPC service
// ---------------------------------------------------------------------------

pub struct KademliaService<T: KadTransport> {
    pub kad: Arc<Kad<T>>,
}

impl<T: KadTransport> std::fmt::Debug for KademliaService<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KademliaService").finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl<T: KadTransport> dynamo_proto::kad::kademlia_server::Kademlia for KademliaService<T> {
    async fn ping(
        &self,
        request: tonic::Request<dynamo_proto::kad::PingRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kad::PingResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type.with_label_values(&["ping"]).inc();
        let _timer = dynamo_metrics::start_rpc_timer("ping", "inbound");
        let req = request.into_inner();
        let sender_proto = req
            .sender
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing sender"))?;
        let sender = convert::node_info_from_proto(sender_proto)?;

        let response = self.kad.handle_request(KadRequest::Ping { sender }).await;

        match response {
            KadResponse::Pong { responder } => {
                Ok(tonic::Response::new(dynamo_proto::kad::PingResponse {
                    responder: Some(convert::node_info_to_proto(&responder)),
                }))
            }
            _ => Err(tonic::Status::internal("unexpected response type")),
        }
    }

    async fn find_node(
        &self,
        request: tonic::Request<dynamo_proto::kad::FindNodeRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kad::FindNodeResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type
            .with_label_values(&["find_node"])
            .inc();
        let _timer = dynamo_metrics::start_rpc_timer("find_node", "inbound");
        let req = request.into_inner();
        let sender_proto = req
            .sender
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing sender"))?;
        let sender = convert::node_info_from_proto(sender_proto)?;
        let target = convert::node_id_from_proto(
            req.target
                .as_ref()
                .ok_or_else(|| tonic::Status::invalid_argument("missing target"))?,
        )?;

        let response = self
            .kad
            .handle_request(KadRequest::FindNode { sender, target })
            .await;

        match response {
            KadResponse::FindNodeResult { responder, closest } => {
                Ok(tonic::Response::new(dynamo_proto::kad::FindNodeResponse {
                    responder: Some(convert::node_info_to_proto(&responder)),
                    closest: closest.iter().map(convert::node_info_to_proto).collect(),
                }))
            }
            _ => Err(tonic::Status::internal("unexpected response type")),
        }
    }

    async fn find_value(
        &self,
        request: tonic::Request<dynamo_proto::kad::FindValueRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kad::FindValueResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type
            .with_label_values(&["find_value"])
            .inc();
        let _timer = dynamo_metrics::start_rpc_timer("find_value", "inbound");
        let req = request.into_inner();
        let sender_proto = req
            .sender
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing sender"))?;
        let sender = convert::node_info_from_proto(sender_proto)?;
        let key = convert::node_id_from_proto(
            req.key
                .as_ref()
                .ok_or_else(|| tonic::Status::invalid_argument("missing key"))?,
        )?;

        let response = self
            .kad
            .handle_request(KadRequest::FindValue { sender, key })
            .await;

        match response {
            KadResponse::FindValueResult { responder, result } => {
                let proto_result = match result {
                    FindValueOutcome::Found(value) => {
                        Some(dynamo_proto::kad::find_value_response::Result::Value(value))
                    }
                    FindValueOutcome::NotFound { closest } => {
                        Some(dynamo_proto::kad::find_value_response::Result::Closest(
                            dynamo_proto::kad::FindNodeResponse {
                                responder: Some(convert::node_info_to_proto(&responder)),
                                closest: closest.iter().map(convert::node_info_to_proto).collect(),
                            },
                        ))
                    }
                };

                Ok(tonic::Response::new(dynamo_proto::kad::FindValueResponse {
                    responder: Some(convert::node_info_to_proto(&responder)),
                    result: proto_result,
                }))
            }
            _ => Err(tonic::Status::internal("unexpected response type")),
        }
    }

    async fn store(
        &self,
        request: tonic::Request<dynamo_proto::kad::StoreRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kad::StoreResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type.with_label_values(&["store"]).inc();
        let _timer = dynamo_metrics::start_rpc_timer("store", "inbound");
        let req = request.into_inner();
        let sender_proto = req
            .sender
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing sender"))?;
        let sender = convert::node_info_from_proto(sender_proto)?;
        let key = convert::node_id_from_proto(
            req.key
                .as_ref()
                .ok_or_else(|| tonic::Status::invalid_argument("missing key"))?,
        )?;

        let response = self
            .kad
            .handle_request(KadRequest::Store {
                sender,
                key,
                value: req.value,
            })
            .await;

        match response {
            KadResponse::StoreOk { responder } => {
                Ok(tonic::Response::new(dynamo_proto::kad::StoreResponse {
                    responder: Some(convert::node_info_to_proto(&responder)),
                }))
            }
            _ => Err(tonic::Status::internal("unexpected response type")),
        }
    }
}

// ---------------------------------------------------------------------------
// KV gRPC service
// ---------------------------------------------------------------------------

pub struct KvService<R: ReplicaClient> {
    pub coordinator: Arc<KvCoordinator<R>>,
}

impl<R: ReplicaClient> std::fmt::Debug for KvService<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvService").finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl<R: ReplicaClient> dynamo_proto::kv::kv_service_server::KvService for KvService<R> {
    async fn put(
        &self,
        request: tonic::Request<dynamo_proto::kv::PutRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kv::PutResponse>, tonic::Status> {
        dynamo_metrics::metrics().kv_puts.inc();
        let _timer = dynamo_metrics::start_kv_timer("put");
        let req = request.into_inner();

        let context = req
            .context
            .map(|vc| VClock::from_map(convert::vclock_from_proto(&vc)));

        let w_override = if req.w > 0 {
            Some(req.w as usize)
        } else {
            None
        };
        let timeout_override = if req.timeout_ms > 0 {
            Some(std::time::Duration::from_millis(u64::from(req.timeout_ms)))
        } else {
            None
        };

        let new_clock = self
            .coordinator
            .put(&req.key, req.value, context, w_override, timeout_override)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(dynamo_proto::kv::PutResponse {
            context: Some(convert::vclock_to_proto(new_clock.entries())),
            success: true,
        }))
    }

    async fn get(
        &self,
        request: tonic::Request<dynamo_proto::kv::GetRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kv::GetResponse>, tonic::Status> {
        dynamo_metrics::metrics().kv_gets.inc();
        let _timer = dynamo_metrics::start_kv_timer("get");
        let req = request.into_inner();

        let r_override = if req.r > 0 {
            Some(req.r as usize)
        } else {
            None
        };
        let timeout_override = if req.timeout_ms > 0 {
            Some(std::time::Duration::from_millis(u64::from(req.timeout_ms)))
        } else {
            None
        };

        match self
            .coordinator
            .get(&req.key, r_override, timeout_override)
            .await
        {
            Ok(result) => {
                let versions: Vec<dynamo_proto::common::VersionedValue> = result
                    .versions
                    .iter()
                    .map(convert::versioned_value_to_proto)
                    .collect();

                Ok(tonic::Response::new(dynamo_proto::kv::GetResponse {
                    versions,
                    context: Some(convert::vclock_to_proto(result.context.entries())),
                    success: true,
                }))
            }
            Err(dynamo_kv::coordinator::KvError::NotFound) => {
                Ok(tonic::Response::new(dynamo_proto::kv::GetResponse {
                    versions: vec![],
                    context: None,
                    success: false,
                }))
            }
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn delete(
        &self,
        request: tonic::Request<dynamo_proto::kv::DeleteRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kv::DeleteResponse>, tonic::Status> {
        dynamo_metrics::metrics().kv_deletes.inc();
        let _timer = dynamo_metrics::start_kv_timer("delete");
        let req = request.into_inner();

        let context = req
            .context
            .map(|vc| VClock::from_map(convert::vclock_from_proto(&vc)));

        let w_override = if req.w > 0 {
            Some(req.w as usize)
        } else {
            None
        };
        let timeout_override = if req.timeout_ms > 0 {
            Some(std::time::Duration::from_millis(u64::from(req.timeout_ms)))
        } else {
            None
        };

        let new_clock = self
            .coordinator
            .delete(&req.key, context, w_override, timeout_override)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(dynamo_proto::kv::DeleteResponse {
            context: Some(convert::vclock_to_proto(new_clock.entries())),
            success: true,
        }))
    }
}

// ---------------------------------------------------------------------------
// Replica gRPC service (node-to-node internal)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ReplicaServiceImpl {
    pub storage: Arc<RwLock<StorageEngine>>,
}

#[tonic::async_trait]
impl dynamo_proto::kv::replica_service_server::ReplicaService for ReplicaServiceImpl {
    async fn replica_put(
        &self,
        request: tonic::Request<dynamo_proto::kv::ReplicaPutRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kv::ReplicaPutResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type
            .with_label_values(&["replica_put"])
            .inc();
        let req = request.into_inner();

        let vv = req
            .versioned
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing versioned value"))?;

        let domain_vv = convert::versioned_value_from_proto(vv);

        let record = if domain_vv.tombstone {
            StorageRecord::tombstone(req.key, domain_vv.vclock.clone().into_map())
        } else {
            StorageRecord::new(
                req.key,
                domain_vv.value,
                domain_vv.vclock.clone().into_map(),
            )
        };

        let mut s = self.storage.write().await;
        s.put(record)
            .map_err(|e| tonic::Status::internal(format!("storage error: {}", e)))?;

        Ok(tonic::Response::new(dynamo_proto::kv::ReplicaPutResponse {
            stored_vclock: Some(convert::vclock_to_proto(domain_vv.vclock.entries())),
            success: true,
        }))
    }

    async fn replica_get(
        &self,
        request: tonic::Request<dynamo_proto::kv::ReplicaGetRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kv::ReplicaGetResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type
            .with_label_values(&["replica_get"])
            .inc();
        let req = request.into_inner();

        let s = self.storage.read().await;
        let records = s.get(&req.key);

        let versions: Vec<dynamo_proto::common::VersionedValue> = records
            .into_iter()
            .map(|rec| {
                let vv = dynamo_kv::coordinator::VersionedValue {
                    value: rec.value,
                    vclock: VClock::from_map(rec.vclock),
                    tombstone: rec.tombstone,
                };
                convert::versioned_value_to_proto(&vv)
            })
            .collect();

        Ok(tonic::Response::new(dynamo_proto::kv::ReplicaGetResponse {
            versions,
        }))
    }

    async fn hint_deliver(
        &self,
        request: tonic::Request<dynamo_proto::kv::HintDeliverRequest>,
    ) -> Result<tonic::Response<dynamo_proto::kv::HintDeliverResponse>, tonic::Status> {
        let m = dynamo_metrics::metrics();
        m.rpcs_received.inc();
        m.rpcs_received_by_type
            .with_label_values(&["hint_deliver"])
            .inc();
        let req = request.into_inner();

        let vv = req
            .versioned
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing versioned value"))?;

        let domain_vv = convert::versioned_value_from_proto(vv);

        let record = if domain_vv.tombstone {
            StorageRecord::tombstone(req.key, domain_vv.vclock.clone().into_map())
        } else {
            StorageRecord::new(
                req.key,
                domain_vv.value,
                domain_vv.vclock.clone().into_map(),
            )
        };

        let mut s = self.storage.write().await;
        s.put(record)
            .map_err(|e| tonic::Status::internal(format!("storage error: {}", e)))?;

        Ok(tonic::Response::new(
            dynamo_proto::kv::HintDeliverResponse { success: true },
        ))
    }
}

// ---------------------------------------------------------------------------
// Admin gRPC service
// ---------------------------------------------------------------------------

pub struct AdminService<T: KadTransport> {
    pub kad: Arc<Kad<T>>,
    pub start_time: StdInstant,
}

impl<T: KadTransport> std::fmt::Debug for AdminService<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminService").finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl<T: KadTransport> dynamo_proto::admin::admin_service_server::AdminService for AdminService<T> {
    async fn health(
        &self,
        _request: tonic::Request<dynamo_proto::admin::HealthRequest>,
    ) -> Result<tonic::Response<dynamo_proto::admin::HealthResponse>, tonic::Status> {
        Ok(tonic::Response::new(dynamo_proto::admin::HealthResponse {
            healthy: true,
            node_id: format!("{}", self.kad.local_id()),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }))
    }

    async fn get_stats(
        &self,
        _request: tonic::Request<dynamo_proto::admin::StatsRequest>,
    ) -> Result<tonic::Response<dynamo_proto::admin::StatsResponse>, tonic::Status> {
        let rt = self.kad.routing_table().read().await;
        let rs = self.kad.record_store().read().await;

        Ok(tonic::Response::new(dynamo_proto::admin::StatsResponse {
            routing_table_size: rt.len() as u64,
            record_count: rs.len() as u64,
            total_rpcs_sent: dynamo_metrics::metrics().rpcs_sent.get() as u64,
            total_rpcs_received: dynamo_metrics::metrics().rpcs_received.get() as u64,
        }))
    }

    async fn cluster_view(
        &self,
        _request: tonic::Request<dynamo_proto::admin::ClusterViewRequest>,
    ) -> Result<tonic::Response<dynamo_proto::admin::ClusterViewResponse>, tonic::Status> {
        let rt = self.kad.routing_table().read().await;
        let all_nodes = rt.all_nodes();

        Ok(tonic::Response::new(
            dynamo_proto::admin::ClusterViewResponse {
                known_nodes: all_nodes.iter().map(convert::node_info_to_proto).collect(),
            },
        ))
    }
}
