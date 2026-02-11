//! KV coordinator: handles client PUT/GET by fanning out to replicas.
//!
//! The coordinator:
//! 1. Maps a key to its N closest nodes (via placement)
//! 2. Sends replica RPCs in parallel
//! 3. Waits for W acks (write) or R responses (read)
//! 4. Reconciles responses (vclock comparison) and returns
//! 5. Optionally performs async read repair
//! 6. Stores hints for failed replicas (hinted handoff)

use crate::hint_store::HintStore;
use crate::placement;
use crate::replica_client::{ReplicaClient, ReplicaError};
use crate::vclock::{VClock, VClockOrder};
use dynamo_common::NodeId;
use dynamo_kad::node_info::NodeInfo;
use dynamo_kad::routing_table::RoutingTable;
use dynamo_storage::engine::StorageEngine;
use dynamo_storage::record::StorageRecord;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

/// Result of a GET operation.
#[derive(Debug, Clone)]
pub struct GetResult {
    /// All non-dominated versions (siblings if concurrent).
    pub versions: Vec<VersionedValue>,
    /// Merged context to echo back on subsequent PUT.
    pub context: VClock,
}

/// A single versioned value returned from GET.
#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub vclock: VClock,
    pub tombstone: bool,
}

/// Quorum and replication configuration.
#[derive(Debug, Clone)]
pub struct QuorumConfig {
    /// Replication factor (N).
    pub n: usize,
    /// Default write quorum (W).
    pub default_w: usize,
    /// Default read quorum (R).
    pub default_r: usize,
    /// Write deadline.
    pub write_timeout: Duration,
    /// Read deadline.
    pub read_timeout: Duration,
    /// Whether to perform async read repair.
    pub read_repair: bool,
    /// Whether to store hints for failed replicas.
    pub hinted_handoff: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("not enough replicas: need {needed}, got {got}")]
    InsufficientReplicas { needed: usize, got: usize },
    #[error("quorum not reached: need {needed}, got {got}")]
    QuorumNotReached { needed: usize, got: usize },
    #[error("storage error: {0}")]
    Storage(#[from] dynamo_storage::engine::StorageError),
    #[error("key not found")]
    NotFound,
}

/// Distributed KV coordinator.
///
/// Generic over `R: ReplicaClient` for testability — real deployment uses
/// `GrpcReplicaClient`; unit tests use a mock.
pub struct KvCoordinator<R: ReplicaClient> {
    local_info: NodeInfo,
    node_id_hex: String,
    storage: Arc<RwLock<StorageEngine>>,
    routing_table: Arc<RwLock<RoutingTable>>,
    replica_client: Arc<R>,
    config: QuorumConfig,
    hint_store: Option<Arc<RwLock<HintStore>>>,
}

impl<R: ReplicaClient> std::fmt::Debug for KvCoordinator<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvCoordinator")
            .field("local_info", &self.local_info)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<R: ReplicaClient> KvCoordinator<R> {
    pub fn new(
        local_info: NodeInfo,
        storage: Arc<RwLock<StorageEngine>>,
        routing_table: Arc<RwLock<RoutingTable>>,
        replica_client: Arc<R>,
        config: QuorumConfig,
    ) -> Self {
        let node_id_hex = hex::encode(local_info.id.as_bytes());
        Self {
            local_info,
            node_id_hex,
            storage,
            routing_table,
            replica_client,
            config,
            hint_store: None,
        }
    }

    /// Attach a hint store for hinted handoff.
    pub fn with_hint_store(mut self, hint_store: Arc<RwLock<HintStore>>) -> Self {
        self.hint_store = Some(hint_store);
        self
    }

    /// The hex-encoded node ID (used as vclock key).
    pub fn node_id_hex(&self) -> &str {
        &self.node_id_hex
    }

    // -----------------------------------------------------------------------
    // PUT
    // -----------------------------------------------------------------------

    /// PUT a value. Increments the vector clock, fans out to N replicas,
    /// waits for W acks.
    ///
    /// Optional `w_override` and `timeout_override` let callers tune quorum
    /// and timeout on a per-request basis (0 / None = use defaults).
    pub async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        context: Option<VClock>,
        w_override: Option<usize>,
        timeout_override: Option<Duration>,
    ) -> Result<VClock, KvError> {
        let base_clock = context.unwrap_or_default();
        let new_clock = base_clock.increment(&self.node_id_hex);

        let versioned = VersionedValue {
            value,
            vclock: new_clock.clone(),
            tombstone: false,
        };

        self.fan_out_put(key, &versioned, w_override, timeout_override)
            .await?;
        Ok(new_clock)
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    /// DELETE a key (writes a tombstone with incremented vclock).
    ///
    /// Optional `w_override` and `timeout_override` for per-request tuning.
    pub async fn delete(
        &self,
        key: &str,
        context: Option<VClock>,
        w_override: Option<usize>,
        timeout_override: Option<Duration>,
    ) -> Result<VClock, KvError> {
        let base_clock = context.unwrap_or_default();
        let new_clock = base_clock.increment(&self.node_id_hex);

        let versioned = VersionedValue {
            value: Vec::new(),
            vclock: new_clock.clone(),
            tombstone: true,
        };

        self.fan_out_put(key, &versioned, w_override, timeout_override)
            .await?;
        Ok(new_clock)
    }

    // -----------------------------------------------------------------------
    // GET
    // -----------------------------------------------------------------------

    /// GET a key. Fans out to N replicas, waits for R responses,
    /// reconciles versions, optionally triggers read repair.
    ///
    /// Optional `r_override` and `timeout_override` for per-request tuning.
    pub async fn get(
        &self,
        key: &str,
        r_override: Option<usize>,
        timeout_override: Option<Duration>,
    ) -> Result<GetResult, KvError> {
        let replicas = {
            let rt = self.routing_table.read().await;
            placement::replica_nodes(key, self.config.n, &rt, &self.local_info)
        };

        let r = r_override
            .unwrap_or(self.config.default_r)
            .min(replicas.len());
        if replicas.is_empty() {
            return Err(KvError::InsufficientReplicas {
                needed: self.config.default_r,
                got: 0,
            });
        }

        let deadline =
            tokio::time::Instant::now() + timeout_override.unwrap_or(self.config.read_timeout);
        let mut futs = FuturesUnordered::new();

        for replica in &replicas {
            if replica.id == self.local_info.id {
                // Local read
                let storage = self.storage.clone();
                let key = key.to_string();
                let local_id = self.local_info.id;
                futs.push(tokio::spawn(async move {
                    let s = storage.read().await;
                    let records = s.get(&key);
                    let versions: Vec<VersionedValue> = records
                        .into_iter()
                        .map(|rec| VersionedValue {
                            value: rec.value,
                            vclock: VClock::from_map(rec.vclock),
                            tombstone: rec.tombstone,
                        })
                        .collect();
                    Ok::<(NodeId, Vec<VersionedValue>), ReplicaError>((local_id, versions))
                }));
            } else {
                // Remote read
                let client = self.replica_client.clone();
                let target = replica.clone();
                let key = key.to_string();
                futs.push(tokio::spawn(async move {
                    let versions = client.replica_get(&target, &key).await?;
                    Ok::<(NodeId, Vec<VersionedValue>), ReplicaError>((target.id, versions))
                }));
            }
        }

        // Collect R responses
        let mut all_versions: Vec<VersionedValue> = Vec::new();
        let mut replica_versions: Vec<(NodeId, Vec<VersionedValue>)> = Vec::new();
        let mut successes = 0usize;

        while let Some(result) = tokio::time::timeout_at(deadline, futs.next())
            .await
            .ok()
            .flatten()
        {
            if let Ok(Ok((node_id, versions))) = result {
                all_versions.extend(versions.clone());
                replica_versions.push((node_id, versions));
                successes += 1;
                if successes >= r {
                    break;
                }
            }
        }

        if successes < r {
            return Err(KvError::QuorumNotReached {
                needed: r,
                got: successes,
            });
        }

        // Reconcile: filter to non-dominated versions
        let reconciled = reconcile_versions(all_versions);

        if reconciled.is_empty() {
            return Err(KvError::NotFound);
        }

        let context = reconciled
            .iter()
            .fold(VClock::new(), |acc, v| acc.merge(&v.vclock));

        // Read repair (async, fire-and-forget)
        if self.config.read_repair && !replica_versions.is_empty() {
            self.spawn_read_repair(key.to_string(), reconciled.clone(), replica_versions);
        }

        Ok(GetResult {
            versions: reconciled,
            context,
        })
    }

    // -----------------------------------------------------------------------
    // Internal: fan-out put
    // -----------------------------------------------------------------------

    /// Fan out a versioned write to all N replicas. Waits for W acks.
    /// On success, stores hints for any failed remote replicas (hinted handoff).
    async fn fan_out_put(
        &self,
        key: &str,
        versioned: &VersionedValue,
        w_override: Option<usize>,
        timeout_override: Option<Duration>,
    ) -> Result<(), KvError> {
        let replicas = {
            let rt = self.routing_table.read().await;
            placement::replica_nodes(key, self.config.n, &rt, &self.local_info)
        };

        let w = w_override
            .unwrap_or(self.config.default_w)
            .min(replicas.len());
        if replicas.is_empty() {
            return Err(KvError::InsufficientReplicas {
                needed: self.config.default_w,
                got: 0,
            });
        }

        let deadline =
            tokio::time::Instant::now() + timeout_override.unwrap_or(self.config.write_timeout);
        let write_id = uuid::Uuid::new_v4().to_string();
        let mut futs = FuturesUnordered::new();

        for replica in &replicas {
            if replica.id == self.local_info.id {
                // Local write
                let storage = self.storage.clone();
                let record = if versioned.tombstone {
                    StorageRecord::tombstone(key.to_string(), versioned.vclock.clone().into_map())
                } else {
                    StorageRecord::new(
                        key.to_string(),
                        versioned.value.clone(),
                        versioned.vclock.clone().into_map(),
                    )
                };
                let local_id = self.local_info.id;
                futs.push(tokio::spawn(async move {
                    let result = {
                        let mut s = storage.write().await;
                        s.put(record)
                            .map_err(|e| ReplicaError::RpcFailed(e.to_string()))
                    };
                    (local_id, result)
                }));
            } else {
                // Remote write
                let client = self.replica_client.clone();
                let target = replica.clone();
                let key = key.to_string();
                let versioned = versioned.clone();
                let write_id = write_id.clone();
                let node_id = replica.id;
                futs.push(tokio::spawn(async move {
                    let result = client
                        .replica_put(&target, &key, &versioned, &write_id)
                        .await;
                    (node_id, result)
                }));
            }
        }

        // Wait for W acks with deadline, tracking which nodes succeeded
        let mut acks = 0usize;
        let mut succeeded_nodes: HashSet<NodeId> = HashSet::new();

        while let Some(result) = tokio::time::timeout_at(deadline, futs.next())
            .await
            .ok()
            .flatten()
        {
            if let Ok((node_id, Ok(()))) = result {
                acks += 1;
                succeeded_nodes.insert(node_id);
                if acks >= w {
                    break;
                }
            }
        }

        if acks < w {
            return Err(KvError::QuorumNotReached {
                needed: w,
                got: acks,
            });
        }

        // Hinted handoff: store hints for any remote replica that did NOT ack
        if self.config.hinted_handoff {
            if let Some(ref hint_store) = self.hint_store {
                let failed_replicas: Vec<NodeId> = replicas
                    .iter()
                    .filter(|r| r.id != self.local_info.id && !succeeded_nodes.contains(&r.id))
                    .map(|r| r.id)
                    .collect();

                if !failed_replicas.is_empty() {
                    let mut hs = hint_store.write().await;
                    for failed_id in &failed_replicas {
                        if let Err(e) = hs.store_hint(failed_id, key, versioned) {
                            tracing::warn!("failed to store hint for node {}: {}", failed_id, e);
                        }
                    }
                    dynamo_metrics::metrics()
                        .hints_stored
                        .inc_by(failed_replicas.len() as u64);
                    tracing::debug!("stored {} hint(s) for key '{}'", failed_replicas.len(), key);
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal: read repair
    // -----------------------------------------------------------------------

    /// Spawn a background task that pushes the reconciled versions to any
    /// stale replica.
    fn spawn_read_repair(
        &self,
        key: String,
        reconciled: Vec<VersionedValue>,
        replica_versions: Vec<(NodeId, Vec<VersionedValue>)>,
    ) {
        let client = self.replica_client.clone();
        let local_id = self.local_info.id;
        let storage = self.storage.clone();
        let routing_table = self.routing_table.clone();

        tokio::spawn(async move {
            dynamo_metrics::metrics().read_repairs.inc();
            let write_id = uuid::Uuid::new_v4().to_string();

            for (node_id, their_versions) in &replica_versions {
                if !is_stale(their_versions, &reconciled) {
                    continue;
                }

                for version in &reconciled {
                    if *node_id == local_id {
                        // Local repair
                        let record = if version.tombstone {
                            StorageRecord::tombstone(key.clone(), version.vclock.clone().into_map())
                        } else {
                            StorageRecord::new(
                                key.clone(),
                                version.value.clone(),
                                version.vclock.clone().into_map(),
                            )
                        };
                        let mut s = storage.write().await;
                        let _ = s.put(record);
                    } else {
                        // Remote repair: find the NodeInfo for this node_id
                        let rt = routing_table.read().await;
                        if let Some(target) = rt.get(node_id).cloned() {
                            drop(rt);
                            let _ = client.replica_put(&target, &key, version, &write_id).await;
                        }
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Filter a set of VersionedValues to only non-dominated versions.
///
/// Versions that are dominated by another version are removed.
/// Concurrent versions (siblings) are all kept.
pub fn reconcile_versions(versions: Vec<VersionedValue>) -> Vec<VersionedValue> {
    if versions.len() <= 1 {
        return versions;
    }

    let mut result: Vec<VersionedValue> = Vec::new();

    for candidate in versions {
        let mut dominated = false;
        let mut to_remove = Vec::new();

        for (i, existing) in result.iter().enumerate() {
            match candidate.vclock.compare(&existing.vclock) {
                VClockOrder::DominatedBy | VClockOrder::Equal => {
                    dominated = true;
                    break;
                }
                VClockOrder::Dominates => {
                    to_remove.push(i);
                }
                VClockOrder::Concurrent => {
                    // keep both
                }
            }
        }

        if !dominated {
            // Remove in reverse order to preserve indices
            for i in to_remove.into_iter().rev() {
                result.remove(i);
            }
            result.push(candidate);
        }
    }

    result
}

/// Check if a replica's versions are stale compared to the reconciled set.
fn is_stale(their_versions: &[VersionedValue], reconciled: &[VersionedValue]) -> bool {
    if their_versions.len() != reconciled.len() {
        return true;
    }
    for r in reconciled {
        let found = their_versions.iter().any(|t| t.vclock == r.vclock);
        if !found {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hint_store::HintStore;
    use crate::replica_client::ReplicaClient;
    use dynamo_storage::wal::FsyncPolicy;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // Mock ReplicaClient for single-node tests
    // -----------------------------------------------------------------------

    struct MockReplicaClient;

    #[async_trait::async_trait]
    impl ReplicaClient for MockReplicaClient {
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

    async fn setup() -> (KvCoordinator<MockReplicaClient>, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();
        let storage = Arc::new(RwLock::new(engine));
        let node_id = NodeId::from_sha1(b"test-node");
        let local_info = NodeInfo::with_dummy_addr(node_id);
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(node_id, 20)));

        let config = QuorumConfig {
            n: 1,
            default_w: 1,
            default_r: 1,
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            read_repair: false,
            hinted_handoff: false,
        };

        let coord = KvCoordinator::new(
            local_info,
            storage,
            routing_table,
            Arc::new(MockReplicaClient),
            config,
        );
        (coord, dir)
    }

    // -----------------------------------------------------------------------
    // Existing tests (updated for new API)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_put_and_get() {
        let (coord, _dir) = setup().await;

        let context = coord
            .put("k1", b"v1".to_vec(), None, None, None)
            .await
            .unwrap();
        assert!(context.get(coord.node_id_hex()) >= 1);

        let result = coord.get("k1", None, None).await.unwrap();
        assert_eq!(result.versions.len(), 1);
        assert_eq!(result.versions[0].value, b"v1");
    }

    #[tokio::test]
    async fn test_put_with_context() {
        let (coord, _dir) = setup().await;

        let ctx1 = coord
            .put("k1", b"v1".to_vec(), None, None, None)
            .await
            .unwrap();
        let ctx2 = coord
            .put("k1", b"v2".to_vec(), Some(ctx1.clone()), None, None)
            .await
            .unwrap();

        assert!(ctx2.get(coord.node_id_hex()) > ctx1.get(coord.node_id_hex()));

        let result = coord.get("k1", None, None).await.unwrap();
        assert_eq!(result.versions.len(), 1);
        assert_eq!(result.versions[0].value, b"v2");
    }

    #[tokio::test]
    async fn test_delete() {
        let (coord, _dir) = setup().await;

        let ctx = coord
            .put("k1", b"v1".to_vec(), None, None, None)
            .await
            .unwrap();
        let _del_ctx = coord.delete("k1", Some(ctx), None, None).await.unwrap();

        let result = coord.get("k1", None, None).await.unwrap();
        assert!(result.versions[0].tombstone);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (coord, _dir) = setup().await;
        let result = coord.get("nonexistent", None, None).await;
        assert!(matches!(result, Err(KvError::NotFound)));
    }

    #[tokio::test]
    async fn test_context_passthrough() {
        let (coord, _dir) = setup().await;

        coord
            .put("k1", b"v1".to_vec(), None, None, None)
            .await
            .unwrap();
        let get_result = coord.get("k1", None, None).await.unwrap();

        // Use the context from GET in a subsequent PUT
        let ctx = coord
            .put("k1", b"v2".to_vec(), Some(get_result.context), None, None)
            .await
            .unwrap();
        assert!(ctx.get(coord.node_id_hex()) >= 2);
    }

    // -----------------------------------------------------------------------
    // Reconciliation tests
    // -----------------------------------------------------------------------

    fn vc(entries: &[(&str, u64)]) -> VClock {
        VClock::from_map(entries.iter().map(|(k, v)| (k.to_string(), *v)).collect())
    }

    fn vv(value: &[u8], entries: &[(&str, u64)]) -> VersionedValue {
        VersionedValue {
            value: value.to_vec(),
            vclock: vc(entries),
            tombstone: false,
        }
    }

    #[test]
    fn test_reconcile_filters_dominated() {
        let versions = vec![vv(b"old", &[("a", 1)]), vv(b"new", &[("a", 2)])];
        let result = reconcile_versions(versions);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, b"new");
    }

    #[test]
    fn test_reconcile_keeps_concurrent() {
        let versions = vec![
            vv(b"v1", &[("a", 2), ("b", 1)]),
            vv(b"v2", &[("a", 1), ("b", 2)]),
        ];
        let result = reconcile_versions(versions);
        assert_eq!(
            result.len(),
            2,
            "concurrent versions should be kept as siblings"
        );
    }

    #[test]
    fn test_reconcile_deduplicates_equal() {
        let versions = vec![
            vv(b"same", &[("a", 1)]),
            vv(b"same", &[("a", 1)]),
            vv(b"same", &[("a", 1)]),
        ];
        let result = reconcile_versions(versions);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_reconcile_complex_scenario() {
        // Three replicas: one has old, two have newer but concurrent versions
        let versions = vec![
            vv(b"old", &[("a", 1)]),               // dominated by both others
            vv(b"branch1", &[("a", 2), ("b", 1)]), // concurrent with branch2
            vv(b"branch2", &[("a", 1), ("b", 2)]), // concurrent with branch1
        ];
        let result = reconcile_versions(versions);
        assert_eq!(
            result.len(),
            2,
            "should keep only the two concurrent versions"
        );
        // The old version should be gone
        assert!(!result.iter().any(|v| v.value == b"old"));
    }

    #[test]
    fn test_is_stale_different_lengths() {
        let theirs = vec![vv(b"v1", &[("a", 1)])];
        let reconciled = vec![
            vv(b"v1", &[("a", 2), ("b", 1)]),
            vv(b"v2", &[("a", 1), ("b", 2)]),
        ];
        assert!(is_stale(&theirs, &reconciled));
    }

    #[test]
    fn test_is_stale_same_content() {
        let v = vec![vv(b"v1", &[("a", 1)])];
        assert!(!is_stale(&v, &v));
    }

    #[test]
    fn test_is_stale_missing_version() {
        let theirs = vec![vv(b"v1", &[("a", 1)])];
        let reconciled = vec![vv(b"v1", &[("a", 2)])];
        assert!(is_stale(&theirs, &reconciled));
    }

    // -----------------------------------------------------------------------
    // Hinted handoff tests
    // -----------------------------------------------------------------------

    /// A mock ReplicaClient that fails for specific node IDs.
    struct FailingMockReplicaClient {
        fail_nodes: HashSet<NodeId>,
    }

    #[async_trait::async_trait]
    impl ReplicaClient for FailingMockReplicaClient {
        async fn replica_put(
            &self,
            target: &NodeInfo,
            _key: &str,
            _versioned: &VersionedValue,
            _write_id: &str,
        ) -> Result<(), ReplicaError> {
            if self.fail_nodes.contains(&target.id) {
                Err(ReplicaError::RpcFailed("simulated failure".into()))
            } else {
                Ok(())
            }
        }

        async fn replica_get(
            &self,
            _target: &NodeInfo,
            _key: &str,
        ) -> Result<Vec<VersionedValue>, ReplicaError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_hinted_handoff_stores_hints() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        let hint_dir = dir.path().join("hints");

        let engine = StorageEngine::open(&data_dir, FsyncPolicy::None).unwrap();
        let storage = Arc::new(RwLock::new(engine));

        let local_id = NodeId::from_sha1(b"local-node");
        let local_info = NodeInfo::with_dummy_addr(local_id);

        // Create a remote node that will fail
        let remote_id = NodeId::from_sha1(b"remote-node");
        let remote_info = NodeInfo::new(remote_id, "127.0.0.1:9999".parse().unwrap());

        let mut routing_table = RoutingTable::new(local_id, 20);
        routing_table.update(remote_info);
        let routing_table = Arc::new(RwLock::new(routing_table));

        let mut fail_set = HashSet::new();
        fail_set.insert(remote_id);
        let replica_client = Arc::new(FailingMockReplicaClient {
            fail_nodes: fail_set,
        });

        let hint_store = Arc::new(RwLock::new(HintStore::open(&hint_dir).unwrap()));

        let config = QuorumConfig {
            n: 2,
            default_w: 1, // Only need 1 ack (local succeeds)
            default_r: 1,
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            read_repair: false,
            hinted_handoff: true,
        };

        let coord = KvCoordinator::new(local_info, storage, routing_table, replica_client, config)
            .with_hint_store(hint_store.clone());

        // PUT should succeed (W=1, local acks)
        coord
            .put("mykey", b"myval".to_vec(), None, None, None)
            .await
            .unwrap();

        // A hint should be stored for the failed remote node
        // Give a moment for the async fan-out to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let hs = hint_store.read().await;
        assert_eq!(
            hs.hint_count(),
            1,
            "should have 1 hint for the failed replica"
        );
        let hints = hs.hints_for_node(&remote_id);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].key, "mykey");
        assert_eq!(hints[0].versioned.value, b"myval");
    }

    #[tokio::test]
    async fn test_no_hints_when_disabled() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        let hint_dir = dir.path().join("hints");

        let engine = StorageEngine::open(&data_dir, FsyncPolicy::None).unwrap();
        let storage = Arc::new(RwLock::new(engine));

        let local_id = NodeId::from_sha1(b"local-node");
        let local_info = NodeInfo::with_dummy_addr(local_id);

        let remote_id = NodeId::from_sha1(b"remote-node");
        let remote_info = NodeInfo::new(remote_id, "127.0.0.1:9999".parse().unwrap());

        let mut routing_table = RoutingTable::new(local_id, 20);
        routing_table.update(remote_info);
        let routing_table = Arc::new(RwLock::new(routing_table));

        let mut fail_set = HashSet::new();
        fail_set.insert(remote_id);
        let replica_client = Arc::new(FailingMockReplicaClient {
            fail_nodes: fail_set,
        });

        let hint_store = Arc::new(RwLock::new(HintStore::open(&hint_dir).unwrap()));

        let config = QuorumConfig {
            n: 2,
            default_w: 1,
            default_r: 1,
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            read_repair: false,
            hinted_handoff: false, // DISABLED
        };

        let coord = KvCoordinator::new(local_info, storage, routing_table, replica_client, config)
            .with_hint_store(hint_store.clone());

        coord
            .put("mykey", b"myval".to_vec(), None, None, None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let hs = hint_store.read().await;
        assert_eq!(hs.hint_count(), 0, "should have no hints when disabled");
    }

    #[tokio::test]
    async fn test_no_hints_when_all_succeed() {
        let (coord, _dir) = setup().await;

        // N=1 with local node only — no remote replicas to fail
        coord
            .put("k1", b"v1".to_vec(), None, None, None)
            .await
            .unwrap();
        // No hint store attached, and no remote nodes → no hints to store
    }
}
