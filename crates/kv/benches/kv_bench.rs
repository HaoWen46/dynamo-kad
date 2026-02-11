//! Benchmarks for the KV layer: vector clocks, Merkle tree, coordinator.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;

// ────────────────────────── VClock benchmarks ──────────────────────────

fn bench_vclock_increment(c: &mut Criterion) {
    use dynamo_kv::vclock::VClock;

    let mut group = c.benchmark_group("vclock_increment");
    for n_entries in [1, 5, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_entries),
            &n_entries,
            |b, &n| {
                let mut entries = HashMap::new();
                for i in 0..n {
                    entries.insert(format!("node_{}", i), i as u64);
                }
                let clock = VClock::from_map(entries);
                b.iter(|| black_box(clock.increment("node_0")));
            },
        );
    }
    group.finish();
}

fn bench_vclock_merge(c: &mut Criterion) {
    use dynamo_kv::vclock::VClock;

    let mut group = c.benchmark_group("vclock_merge");
    for n_entries in [5, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_entries),
            &n_entries,
            |b, &n| {
                let mut e1 = HashMap::new();
                let mut e2 = HashMap::new();
                for i in 0..n {
                    e1.insert(format!("node_{}", i), i as u64);
                    e2.insert(format!("node_{}", i), (n - i) as u64);
                }
                let c1 = VClock::from_map(e1);
                let c2 = VClock::from_map(e2);
                b.iter(|| black_box(c1.merge(&c2)));
            },
        );
    }
    group.finish();
}

fn bench_vclock_compare(c: &mut Criterion) {
    use dynamo_kv::vclock::VClock;

    let mut group = c.benchmark_group("vclock_compare");
    for n_entries in [1, 5, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_entries),
            &n_entries,
            |b, &n| {
                let mut e1 = HashMap::new();
                let mut e2 = HashMap::new();
                for i in 0..n {
                    e1.insert(format!("node_{}", i), i as u64);
                    e2.insert(format!("node_{}", i), i as u64 + 1);
                }
                let c1 = VClock::from_map(e1);
                let c2 = VClock::from_map(e2);
                b.iter(|| black_box(c1.compare(&c2)));
            },
        );
    }
    group.finish();
}

// ────────────────────────── Merkle tree benchmarks ──────────────────────────

fn bench_merkle_build(c: &mut Criterion) {
    use dynamo_kv::merkle::MerkleTree;

    let mut group = c.benchmark_group("merkle_build");
    for n in [100, 1000, 10000] {
        let entries: Vec<(String, Vec<u8>)> = (0..n)
            .map(|i| (format!("key_{:06}", i), format!("val_{}", i).into_bytes()))
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &entries, |b, entries| {
            b.iter(|| black_box(MerkleTree::build(entries)));
        });
    }
    group.finish();
}

fn bench_merkle_diff_identical(c: &mut Criterion) {
    use dynamo_kv::merkle::MerkleTree;

    let entries: Vec<(String, Vec<u8>)> = (0..1000)
        .map(|i| (format!("key_{:06}", i), format!("val_{}", i).into_bytes()))
        .collect();
    let t1 = MerkleTree::build(&entries);
    let t2 = MerkleTree::build(&entries);

    c.bench_function("merkle_diff_identical_1000", |b| {
        b.iter(|| black_box(t1.diff(&t2)));
    });
}

fn bench_merkle_diff_1pct(c: &mut Criterion) {
    use dynamo_kv::merkle::MerkleTree;

    let entries: Vec<(String, Vec<u8>)> = (0..1000)
        .map(|i| (format!("key_{:06}", i), format!("val_{}", i).into_bytes()))
        .collect();
    let t1 = MerkleTree::build(&entries);

    // Change 1% (10 keys)
    let mut entries2 = entries.clone();
    for i in (0..1000).step_by(100) {
        entries2[i].1 = b"CHANGED".to_vec();
    }
    let t2 = MerkleTree::build(&entries2);

    c.bench_function("merkle_diff_1pct_1000", |b| {
        b.iter(|| black_box(t1.diff(&t2)));
    });
}

// ────────────────────────── Coordinator benchmarks ──────────────────────────

fn bench_coordinator_put(c: &mut Criterion) {
    use dynamo_common::NodeId;
    use dynamo_kad::node_info::NodeInfo;
    use dynamo_kad::routing_table::RoutingTable;
    use dynamo_kv::coordinator::{KvCoordinator, QuorumConfig, VersionedValue};
    use dynamo_kv::replica_client::{ReplicaClient, ReplicaError};
    use dynamo_storage::engine::StorageEngine;
    use dynamo_storage::wal::FsyncPolicy;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio::time::Duration;

    struct NoopReplicaClient;

    #[async_trait::async_trait]
    impl ReplicaClient for NoopReplicaClient {
        async fn replica_put(
            &self,
            _target: &NodeInfo,
            _key: &str,
            _versioned: &VersionedValue,
            _write_id: &str,
        ) -> Result<(), ReplicaError> {
            Ok(())
        }
        async fn replica_get(
            &self,
            _target: &NodeInfo,
            _key: &str,
        ) -> Result<Vec<VersionedValue>, ReplicaError> {
            Ok(vec![])
        }
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("coordinator_put", |b| {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();
        let storage = Arc::new(RwLock::new(engine));
        let node_id = NodeId::from_sha1(b"bench-node");
        let local_info = NodeInfo::with_dummy_addr(node_id);
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(node_id, 20)));
        let replica_client = Arc::new(NoopReplicaClient);
        let quorum_config = QuorumConfig {
            n: 1,
            default_w: 1,
            default_r: 1,
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            read_repair: false,
            hinted_handoff: false,
        };
        let kv = KvCoordinator::new(
            local_info,
            storage,
            routing_table,
            replica_client,
            quorum_config,
        );

        let mut i = 0u64;
        b.iter(|| {
            rt.block_on(async {
                let key = format!("key_{}", i);
                kv.put(&key, b"value".to_vec(), None, None, None)
                    .await
                    .unwrap();
            });
            i += 1;
        });
    });
}

fn bench_coordinator_get(c: &mut Criterion) {
    use dynamo_common::NodeId;
    use dynamo_kad::node_info::NodeInfo;
    use dynamo_kad::routing_table::RoutingTable;
    use dynamo_kv::coordinator::{KvCoordinator, QuorumConfig, VersionedValue};
    use dynamo_kv::replica_client::{ReplicaClient, ReplicaError};
    use dynamo_storage::engine::StorageEngine;
    use dynamo_storage::wal::FsyncPolicy;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio::time::Duration;

    struct NoopReplicaClient;

    #[async_trait::async_trait]
    impl ReplicaClient for NoopReplicaClient {
        async fn replica_put(
            &self,
            _target: &NodeInfo,
            _key: &str,
            _versioned: &VersionedValue,
            _write_id: &str,
        ) -> Result<(), ReplicaError> {
            Ok(())
        }
        async fn replica_get(
            &self,
            _target: &NodeInfo,
            _key: &str,
        ) -> Result<Vec<VersionedValue>, ReplicaError> {
            Ok(vec![])
        }
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();
    let storage = Arc::new(RwLock::new(engine));
    let node_id = NodeId::from_sha1(b"bench-node");
    let local_info = NodeInfo::with_dummy_addr(node_id);
    let routing_table = Arc::new(RwLock::new(RoutingTable::new(node_id, 20)));
    let replica_client = Arc::new(NoopReplicaClient);
    let quorum_config = QuorumConfig {
        n: 1,
        default_w: 1,
        default_r: 1,
        write_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        read_repair: false,
        hinted_handoff: false,
    };
    let kv = KvCoordinator::new(
        local_info,
        storage,
        routing_table,
        replica_client,
        quorum_config,
    );

    // Pre-populate 1000 keys
    rt.block_on(async {
        for i in 0..1000 {
            kv.put(
                &format!("key_{:04}", i),
                b"value".to_vec(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }
    });

    c.bench_function("coordinator_get", |b| {
        let mut i = 0u64;
        b.iter(|| {
            rt.block_on(async {
                let key = format!("key_{:04}", i % 1000);
                black_box(kv.get(&key, None, None).await.unwrap());
            });
            i += 1;
        });
    });
}

criterion_group!(
    benches,
    bench_vclock_increment,
    bench_vclock_merge,
    bench_vclock_compare,
    bench_merkle_build,
    bench_merkle_diff_identical,
    bench_merkle_diff_1pct,
    bench_coordinator_put,
    bench_coordinator_get,
);
criterion_main!(benches);
