//! dynamo-node: entry point for a Kademlia + Dynamo KV node.
//!
//! Loads config, initialises the Kad DHT with gRPC transport,
//! wires the KV coordinator and storage engine, then serves
//! all gRPC services on the configured listen address.

use dynamo_common::NodeId;
use dynamo_kad::node_info::NodeInfo;
use dynamo_kad::{Kad, KadConfig};
use dynamo_kv::coordinator::{KvCoordinator, QuorumConfig};
use dynamo_kv::hint_store::HintStore;
use dynamo_net::{GrpcReplicaClient, GrpcTransport};
use dynamo_storage::engine::StorageEngine;
use dynamo_storage::wal::FsyncPolicy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dynamo_metrics::init_tracing();

    // Load config: first CLI arg is the YAML config path
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yaml".to_string());

    let config =
        dynamo_config::load_from_file(std::path::Path::new(&config_path)).unwrap_or_else(|e| {
            tracing::warn!(
                "failed to load config from {}: {}, using defaults",
                config_path,
                e
            );
            // Minimal default: listen on 127.0.0.1:7000
            dynamo_config::load_from_str("listen: \"127.0.0.1:7000\"\nseeds: []\n")
                .expect("hardcoded default config must parse")
        });

    // Generate a random NodeId for this node
    let node_id = NodeId::random();
    let local_info = NodeInfo::new(node_id, config.listen);
    tracing::info!("node {} listening on {}", node_id, config.listen);

    // Build KadConfig from config
    let kad_config = KadConfig {
        k: config.kademlia.k,
        alpha: config.kademlia.alpha,
        rpc_timeout: Duration::from_millis(config.kademlia.rpc_timeout_ms),
        refresh_interval: Duration::from_secs(config.kademlia.refresh_interval_secs),
        max_records: config.kademlia.max_records,
    };

    // Create gRPC transport and Kad instance
    let transport = GrpcTransport::new(local_info.clone());
    let kad = Arc::new(Kad::new(local_info.clone(), transport, kad_config));

    // Open storage engine
    let fsync = match config.storage.fsync.as_str() {
        "always" => FsyncPolicy::Always,
        "none" => FsyncPolicy::None,
        _ => FsyncPolicy::Batch,
    };
    let engine = StorageEngine::open(&config.storage.data_dir, fsync)?;
    let storage = Arc::new(RwLock::new(engine));

    // Create replica client (shares channel pool for efficiency)
    let channels = Arc::new(RwLock::new(HashMap::new()));
    let replica_client = Arc::new(GrpcReplicaClient::new(channels));

    // Build quorum config
    let quorum_config = QuorumConfig {
        n: config.kv.n,
        default_w: config.kv.w,
        default_r: config.kv.r,
        write_timeout: Duration::from_millis(config.kv.write_timeout_ms),
        read_timeout: Duration::from_millis(config.kv.read_timeout_ms),
        read_repair: config.kv.read_repair,
        hinted_handoff: config.kv.hinted_handoff,
    };

    // Create hint store (if hinted handoff is enabled)
    let hint_store = if config.kv.hinted_handoff {
        let hint_dir = config.storage.data_dir.join("hints");
        let hs = HintStore::open(&hint_dir)?;
        Some(Arc::new(RwLock::new(hs)))
    } else {
        None
    };

    // Create KV coordinator
    let mut kv_builder = KvCoordinator::new(
        local_info.clone(),
        storage.clone(),
        kad.routing_table().clone(),
        replica_client.clone(),
        quorum_config,
    );
    if let Some(ref hs) = hint_store {
        kv_builder = kv_builder.with_hint_store(hs.clone());
    }
    let kv = Arc::new(kv_builder);

    // Spawn periodic bucket refresh task
    kad.spawn_refresh_task();

    // Build gRPC server
    let router = dynamo_net::build_server(kad.clone(), kv, storage);

    // Spawn hint delivery task
    if let Some(ref hs) = hint_store {
        let delivery_config = dynamo_kv::hint_delivery::HintDeliveryConfig {
            check_interval: Duration::from_secs(config.kv.hint_delivery_interval_secs),
            max_hints_per_cycle: config.kv.max_hints_per_cycle,
        };
        dynamo_kv::hint_delivery::spawn_hint_delivery_task(
            hs.clone(),
            kad.routing_table().clone(),
            replica_client,
            delivery_config,
        );
    }

    // Spawn metrics HTTP server if configured
    if let Some(metrics_port) = config.metrics_port {
        let metrics_addr: std::net::SocketAddr = format!("0.0.0.0:{}", metrics_port)
            .parse()
            .expect("valid metrics address");
        tokio::spawn(async move {
            if let Err(e) = dynamo_metrics::serve_metrics(metrics_addr).await {
                tracing::warn!("metrics server failed: {}", e);
            }
        });
    }

    // Bootstrap from seeds (in background after server starts)
    let kad_bootstrap = kad.clone();
    let seeds = config.seeds.clone();
    tokio::spawn(async move {
        if seeds.is_empty() {
            tracing::info!("no seeds configured, running as standalone node");
            return;
        }

        tracing::info!("bootstrapping from {} seed(s)...", seeds.len());
        let seed_infos: Vec<NodeInfo> = seeds
            .iter()
            .filter_map(|addr_str| {
                let addr = addr_str.parse().ok()?;
                // We don't know the seed's NodeId — use a placeholder.
                // The real ID will be learned during the PING/FindNode exchange.
                Some(NodeInfo::new(NodeId::from_sha1(addr_str.as_bytes()), addr))
            })
            .collect();

        match kad_bootstrap.bootstrap(seed_infos).await {
            Ok(()) => {
                let rt = kad_bootstrap.routing_table().read().await;
                tracing::info!("bootstrap complete, routing table has {} peer(s)", rt.len());
            }
            Err(e) => {
                tracing::warn!("bootstrap failed: {}", e);
            }
        }
    });

    // Serve with graceful shutdown on Ctrl+C
    tracing::info!("serving gRPC on {}", config.listen);
    tokio::select! {
        result = router.serve(config.listen) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received Ctrl+C, shutting down");
        }
    }

    Ok(())
}
