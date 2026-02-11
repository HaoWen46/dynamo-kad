//! gRPC networking layer for dynamo-kad.
//!
//! Provides:
//! - `GrpcTransport`: implements `KadTransport` over tonic for real network I/O
//! - `GrpcReplicaClient`: implements `ReplicaClient` over tonic
//! - `KademliaService`: bridges the Kademlia proto to `Kad::handle_request`
//! - `KvService`: bridges the KV proto to `KvCoordinator`
//! - `ReplicaServiceImpl`: bridges the Replica proto to direct storage access
//! - `AdminService`: health checks, stats, cluster view
//! - `build_server`: assembles all services into a tonic `Router`

#![allow(clippy::result_large_err)]

pub mod client;
pub mod convert;
pub mod replica_client;
pub mod server;

pub use client::GrpcTransport;
pub use replica_client::GrpcReplicaClient;
pub use server::{AdminService, KademliaService, KvService, ReplicaServiceImpl};

use dynamo_kad::Kad;
use dynamo_kv::coordinator::KvCoordinator;
use dynamo_storage::engine::StorageEngine;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Build a tonic `Router` with all gRPC services.
pub fn build_server(
    kad: Arc<Kad<GrpcTransport>>,
    kv: Arc<KvCoordinator<GrpcReplicaClient>>,
    storage: Arc<RwLock<StorageEngine>>,
) -> tonic::transport::server::Router {
    let kad_svc = KademliaService { kad: kad.clone() };
    let kv_svc = KvService { coordinator: kv };
    let replica_svc = ReplicaServiceImpl { storage };
    let admin_svc = AdminService {
        kad,
        start_time: Instant::now(),
    };

    tonic::transport::Server::builder()
        .add_service(dynamo_proto::kad::kademlia_server::KademliaServer::new(
            kad_svc,
        ))
        .add_service(dynamo_proto::kv::kv_service_server::KvServiceServer::new(
            kv_svc,
        ))
        .add_service(
            dynamo_proto::kv::replica_service_server::ReplicaServiceServer::new(replica_svc),
        )
        .add_service(dynamo_proto::admin::admin_service_server::AdminServiceServer::new(admin_svc))
}
