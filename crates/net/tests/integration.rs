//! Integration test: spin up multiple nodes with real gRPC,
//! bootstrap them, then PUT/GET key-value pairs across the cluster.

use dynamo_common::NodeId;
use dynamo_kad::node_info::NodeInfo;
use dynamo_kad::{Kad, KadConfig};
use dynamo_kv::coordinator::{KvCoordinator, QuorumConfig};
use dynamo_net::{build_server, GrpcReplicaClient, GrpcTransport};
use dynamo_proto::admin::admin_service_client::AdminServiceClient;
use dynamo_proto::kv::kv_service_client::KvServiceClient;
use dynamo_storage::engine::StorageEngine;
use dynamo_storage::wal::FsyncPolicy;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

struct TestNode {
    addr: SocketAddr,
    kad: Arc<Kad<GrpcTransport>>,
    _kv: Arc<KvCoordinator<GrpcReplicaClient>>,
}

async fn spawn_node(port: u16) -> TestNode {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let node_id = NodeId::random();
    let local_info = NodeInfo::new(node_id, addr);

    let transport = GrpcTransport::new(local_info.clone());
    let kad_config = KadConfig {
        k: 20,
        alpha: 3,
        rpc_timeout: Duration::from_secs(5),
        refresh_interval: Duration::from_secs(3600),
        max_records: 1024,
    };
    let kad = Arc::new(Kad::new(local_info.clone(), transport, kad_config));

    let dir = tempfile::TempDir::new().unwrap();
    let engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();
    let storage = Arc::new(RwLock::new(engine));

    let channels = Arc::new(RwLock::new(HashMap::new()));
    let replica_client = Arc::new(GrpcReplicaClient::new(channels));

    let quorum_config = QuorumConfig {
        n: 1,
        default_w: 1,
        default_r: 1,
        write_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        read_repair: false,
        hinted_handoff: false,
    };

    let kv = Arc::new(KvCoordinator::new(
        local_info,
        storage.clone(),
        kad.routing_table().clone(),
        replica_client,
        quorum_config,
    ));

    let router = build_server(kad.clone(), kv.clone(), storage);

    // Spawn the gRPC server
    tokio::spawn(async move {
        router.serve(addr).await.unwrap();
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestNode { addr, kad, _kv: kv }
}

/// Find a free port range. Uses OS-assigned port 0 trick — but for simplicity
/// we use high ephemeral ports and hope they're free.
fn test_ports(base: u16, count: u16) -> Vec<u16> {
    (base..base + count).collect()
}

#[tokio::test]
async fn test_single_node_health() {
    let ports = test_ports(17100, 1);
    let node = spawn_node(ports[0]).await;

    let mut client = AdminServiceClient::connect(format!("http://{}", node.addr))
        .await
        .unwrap();

    let resp = client
        .health(dynamo_proto::admin::HealthRequest {})
        .await
        .unwrap();

    let health = resp.into_inner();
    assert!(health.healthy);
    assert!(!health.node_id.is_empty());
}

#[tokio::test]
async fn test_single_node_put_get() {
    let ports = test_ports(17110, 1);
    let node = spawn_node(ports[0]).await;

    let mut kv_client = KvServiceClient::connect(format!("http://{}", node.addr))
        .await
        .unwrap();

    // PUT
    let put_resp = kv_client
        .put(dynamo_proto::kv::PutRequest {
            key: "hello".to_string(),
            value: b"world".to_vec(),
            context: None,
            w: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap();

    assert!(put_resp.into_inner().success);

    // GET
    let get_resp = kv_client
        .get(dynamo_proto::kv::GetRequest {
            key: "hello".to_string(),
            r: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap();

    let get = get_resp.into_inner();
    assert!(get.success);
    assert_eq!(get.versions.len(), 1);
    assert_eq!(get.versions[0].value, b"world");
}

#[tokio::test]
async fn test_single_node_put_update_get() {
    let ports = test_ports(17120, 1);
    let node = spawn_node(ports[0]).await;

    let mut kv_client = KvServiceClient::connect(format!("http://{}", node.addr))
        .await
        .unwrap();

    // PUT v1
    let put1 = kv_client
        .put(dynamo_proto::kv::PutRequest {
            key: "counter".to_string(),
            value: b"1".to_vec(),
            context: None,
            w: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(put1.success);

    // GET — grab the context
    let get1 = kv_client
        .get(dynamo_proto::kv::GetRequest {
            key: "counter".to_string(),
            r: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(get1.success);
    assert_eq!(get1.versions[0].value, b"1");

    // PUT v2 with context from GET
    let put2 = kv_client
        .put(dynamo_proto::kv::PutRequest {
            key: "counter".to_string(),
            value: b"2".to_vec(),
            context: get1.context.clone(),
            w: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(put2.success);

    // GET v2
    let get2 = kv_client
        .get(dynamo_proto::kv::GetRequest {
            key: "counter".to_string(),
            r: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(get2.success);
    assert_eq!(get2.versions.len(), 1);
    assert_eq!(get2.versions[0].value, b"2");
}

#[tokio::test]
async fn test_single_node_delete() {
    let ports = test_ports(17130, 1);
    let node = spawn_node(ports[0]).await;

    let mut kv_client = KvServiceClient::connect(format!("http://{}", node.addr))
        .await
        .unwrap();

    // PUT
    kv_client
        .put(dynamo_proto::kv::PutRequest {
            key: "ephemeral".to_string(),
            value: b"temp".to_vec(),
            context: None,
            w: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap();

    // GET context
    let get1 = kv_client
        .get(dynamo_proto::kv::GetRequest {
            key: "ephemeral".to_string(),
            r: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();

    // DELETE
    let del = kv_client
        .delete(dynamo_proto::kv::DeleteRequest {
            key: "ephemeral".to_string(),
            context: get1.context,
            w: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(del.success);

    // GET after delete — should return tombstone
    let get2 = kv_client
        .get(dynamo_proto::kv::GetRequest {
            key: "ephemeral".to_string(),
            r: 0,
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(get2.success);
    assert!(get2.versions[0].tombstone);
}

#[tokio::test]
async fn test_multi_node_bootstrap_and_cluster_view() {
    let ports = test_ports(17200, 3);
    let mut nodes = Vec::new();

    // Spawn 3 nodes
    for &port in &ports {
        nodes.push(spawn_node(port).await);
    }

    // Bootstrap node 1 and 2 with node 0 as seed
    let seed_addr = nodes[0].addr;
    let seed_info = NodeInfo::new(
        NodeId::from_sha1(seed_addr.to_string().as_bytes()),
        seed_addr,
    );

    for node in &nodes[1..] {
        node.kad.bootstrap(vec![seed_info.clone()]).await.unwrap();
    }

    // Bootstrap node 0 with node 1
    let seed1_addr = nodes[1].addr;
    let seed1_info = NodeInfo::new(
        NodeId::from_sha1(seed1_addr.to_string().as_bytes()),
        seed1_addr,
    );
    nodes[0].kad.bootstrap(vec![seed1_info]).await.unwrap();

    // Give time for routing tables to settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check cluster view from node 0
    let mut admin_client = AdminServiceClient::connect(format!("http://{}", nodes[0].addr))
        .await
        .unwrap();

    let cluster = admin_client
        .cluster_view(dynamo_proto::admin::ClusterViewRequest {})
        .await
        .unwrap()
        .into_inner();

    // Node 0 should know about at least 1 other node
    assert!(
        !cluster.known_nodes.is_empty(),
        "node 0 should know about peers after bootstrap"
    );
}

#[tokio::test]
async fn test_stats_endpoint() {
    let ports = test_ports(17300, 1);
    let node = spawn_node(ports[0]).await;

    let mut admin_client = AdminServiceClient::connect(format!("http://{}", node.addr))
        .await
        .unwrap();

    let stats = admin_client
        .get_stats(dynamo_proto::admin::StatsRequest {})
        .await
        .unwrap()
        .into_inner();

    // Fresh node should have 0 entries
    assert_eq!(stats.routing_table_size, 0);
    assert_eq!(stats.record_count, 0);
}

#[tokio::test]
async fn test_replica_put_get_direct() {
    use dynamo_proto::kv::replica_service_client::ReplicaServiceClient;

    let ports = test_ports(17400, 1);
    let node = spawn_node(ports[0]).await;

    let mut client = ReplicaServiceClient::connect(format!("http://{}", node.addr))
        .await
        .unwrap();

    // ReplicaPut
    let put_resp = client
        .replica_put(dynamo_proto::kv::ReplicaPutRequest {
            key: "rkey".to_string(),
            versioned: Some(dynamo_proto::common::VersionedValue {
                value: b"rval".to_vec(),
                vclock: Some(dynamo_proto::common::VectorClock {
                    entries: [("node1".to_string(), 1)].into(),
                }),
                timestamp_ms: 0,
                tombstone: false,
            }),
            write_id: "w1".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(put_resp.success);

    // ReplicaGet
    let get_resp = client
        .replica_get(dynamo_proto::kv::ReplicaGetRequest {
            key: "rkey".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.versions.len(), 1);
    assert_eq!(get_resp.versions[0].value, b"rval");
}

#[tokio::test]
async fn test_hint_deliver() {
    use dynamo_proto::kv::replica_service_client::ReplicaServiceClient;

    let ports = test_ports(17410, 1);
    let _node = spawn_node(ports[0]).await;

    let mut client = ReplicaServiceClient::connect(format!("http://127.0.0.1:{}", ports[0]))
        .await
        .unwrap();

    // HintDeliver (writes to storage like ReplicaPut)
    let resp = client
        .hint_deliver(dynamo_proto::kv::HintDeliverRequest {
            target_node: Some(dynamo_proto::common::NodeId { raw: vec![0u8; 20] }),
            key: "hintkey".to_string(),
            versioned: Some(dynamo_proto::common::VersionedValue {
                value: b"hintval".to_vec(),
                vclock: Some(dynamo_proto::common::VectorClock {
                    entries: [("nodeX".to_string(), 5)].into(),
                }),
                timestamp_ms: 0,
                tombstone: false,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);

    // Verify via ReplicaGet
    let get_resp = client
        .replica_get(dynamo_proto::kv::ReplicaGetRequest {
            key: "hintkey".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_resp.versions.len(), 1);
    assert_eq!(get_resp.versions[0].value, b"hintval");
}
