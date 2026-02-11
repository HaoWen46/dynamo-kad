//! Background task for delivering hinted-handoff hints.
//!
//! Periodically checks the hint store for pending hints and attempts
//! to deliver them to recovered target nodes via `replica_put`.

use crate::hint_store::HintStore;
use crate::replica_client::ReplicaClient;
use dynamo_kad::routing_table::RoutingTable;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// Configuration for the hint delivery background task.
#[derive(Debug)]
pub struct HintDeliveryConfig {
    /// How often to check for undelivered hints.
    pub check_interval: Duration,
    /// Maximum hints to attempt per delivery cycle.
    pub max_hints_per_cycle: usize,
}

impl Default for HintDeliveryConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            max_hints_per_cycle: 100,
        }
    }
}

/// Spawn the background hint delivery task.
///
/// Returns a `JoinHandle` for the spawned task.
#[allow(clippy::needless_pass_by_value)] // config is moved into the spawned task
pub fn spawn_hint_delivery_task<R: ReplicaClient>(
    hint_store: Arc<RwLock<HintStore>>,
    routing_table: Arc<RwLock<RoutingTable>>,
    replica_client: Arc<R>,
    config: HintDeliveryConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = interval(config.check_interval);
        loop {
            tick.tick().await;
            deliver_hints(
                &hint_store,
                &routing_table,
                &replica_client,
                config.max_hints_per_cycle,
            )
            .await;
        }
    })
}

/// One cycle of hint delivery.
pub async fn deliver_hints<R: ReplicaClient>(
    hint_store: &Arc<RwLock<HintStore>>,
    routing_table: &Arc<RwLock<RoutingTable>>,
    replica_client: &Arc<R>,
    max_hints: usize,
) {
    let target_nodes = {
        let hs = hint_store.read().await;
        hs.all_target_nodes()
    };

    if target_nodes.is_empty() {
        return;
    }

    let mut delivered = 0usize;

    for target_id in target_nodes {
        if delivered >= max_hints {
            break;
        }

        // Check if the target is reachable (present in routing table)
        let target_info = {
            let rt = routing_table.read().await;
            rt.get(&target_id).cloned()
        };

        let target_info = match target_info {
            Some(info) => info,
            None => {
                tracing::debug!("hint target {} not in routing table, skipping", target_id);
                continue;
            }
        };

        // Get hints for this target
        let hints = {
            let hs = hint_store.read().await;
            hs.hints_for_node(&target_id)
        };

        for hint in hints {
            if delivered >= max_hints {
                break;
            }

            let write_id = uuid::Uuid::new_v4().to_string();
            match replica_client
                .replica_put(&target_info, &hint.key, &hint.versioned, &write_id)
                .await
            {
                Ok(()) => {
                    let mut hs = hint_store.write().await;
                    if let Err(e) = hs.delete_hint(&target_id, &hint.key) {
                        tracing::warn!("failed to delete delivered hint: {}", e);
                    }
                    delivered += 1;
                    dynamo_metrics::metrics().hints_delivered.inc();
                    tracing::debug!(
                        "delivered hint for key '{}' to node {}",
                        hint.key,
                        target_id
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        "hint delivery to {} failed: {}, will retry later",
                        target_id,
                        e
                    );
                    break; // Skip remaining hints for this target
                }
            }
        }
    }

    if delivered > 0 {
        tracing::info!("hint delivery cycle: delivered {} hint(s)", delivered);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::VersionedValue;
    use crate::hint_store::HintStore;
    use crate::replica_client::{ReplicaClient, ReplicaError};
    use crate::vclock::VClock;
    use dynamo_common::NodeId;
    use dynamo_kad::node_info::NodeInfo;
    use dynamo_kad::routing_table::RoutingTable;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    fn make_versioned(value: &[u8]) -> VersionedValue {
        VersionedValue {
            value: value.to_vec(),
            vclock: VClock::from_map([("n1".to_string(), 1)].into()),
            tombstone: false,
        }
    }

    /// Mock that tracks successful deliveries.
    struct TrackingMockClient {
        delivered_count: AtomicUsize,
        fail_nodes: HashSet<NodeId>,
    }

    impl TrackingMockClient {
        fn new() -> Self {
            Self {
                delivered_count: AtomicUsize::new(0),
                fail_nodes: HashSet::new(),
            }
        }

        fn with_failures(fail_nodes: HashSet<NodeId>) -> Self {
            Self {
                delivered_count: AtomicUsize::new(0),
                fail_nodes,
            }
        }

        fn delivered(&self) -> usize {
            self.delivered_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ReplicaClient for TrackingMockClient {
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
                self.delivered_count.fetch_add(1, Ordering::SeqCst);
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
    async fn test_deliver_to_reachable() {
        let dir = TempDir::new().unwrap();
        let target_id = NodeId::from_sha1(b"target-node");
        let target_info = NodeInfo::new(target_id, "127.0.0.1:9999".parse().unwrap());

        let mut hs = HintStore::open(dir.path()).unwrap();
        hs.store_hint(&target_id, "k1", &make_versioned(b"v1"))
            .unwrap();
        hs.store_hint(&target_id, "k2", &make_versioned(b"v2"))
            .unwrap();
        let hint_store = Arc::new(RwLock::new(hs));

        let local_id = NodeId::from_sha1(b"local-node");
        let mut rt = RoutingTable::new(local_id, 20);
        rt.update(target_info);
        let routing_table = Arc::new(RwLock::new(rt));

        let client = Arc::new(TrackingMockClient::new());

        deliver_hints(&hint_store, &routing_table, &client, 100).await;

        assert_eq!(client.delivered(), 2);
        let hs = hint_store.read().await;
        assert_eq!(
            hs.hint_count(),
            0,
            "all hints should be deleted after delivery"
        );
    }

    #[tokio::test]
    async fn test_skip_unreachable() {
        let dir = TempDir::new().unwrap();
        let target_id = NodeId::from_sha1(b"unreachable-node");

        let mut hs = HintStore::open(dir.path()).unwrap();
        hs.store_hint(&target_id, "k1", &make_versioned(b"v1"))
            .unwrap();
        let hint_store = Arc::new(RwLock::new(hs));

        // Routing table does NOT contain the target
        let local_id = NodeId::from_sha1(b"local-node");
        let rt = RoutingTable::new(local_id, 20);
        let routing_table = Arc::new(RwLock::new(rt));

        let client = Arc::new(TrackingMockClient::new());

        deliver_hints(&hint_store, &routing_table, &client, 100).await;

        assert_eq!(client.delivered(), 0);
        let hs = hint_store.read().await;
        assert_eq!(
            hs.hint_count(),
            1,
            "hints should remain when target is unreachable"
        );
    }

    #[tokio::test]
    async fn test_failure_keeps_hints() {
        let dir = TempDir::new().unwrap();
        let target_id = NodeId::from_sha1(b"fail-node");
        let target_info = NodeInfo::new(target_id, "127.0.0.1:9999".parse().unwrap());

        let mut hs = HintStore::open(dir.path()).unwrap();
        hs.store_hint(&target_id, "k1", &make_versioned(b"v1"))
            .unwrap();
        let hint_store = Arc::new(RwLock::new(hs));

        let local_id = NodeId::from_sha1(b"local-node");
        let mut rt = RoutingTable::new(local_id, 20);
        rt.update(target_info);
        let routing_table = Arc::new(RwLock::new(rt));

        let mut fail_set = HashSet::new();
        fail_set.insert(target_id);
        let client = Arc::new(TrackingMockClient::with_failures(fail_set));

        deliver_hints(&hint_store, &routing_table, &client, 100).await;

        assert_eq!(client.delivered(), 0);
        let hs = hint_store.read().await;
        assert_eq!(
            hs.hint_count(),
            1,
            "hints should remain when delivery fails"
        );
    }
}
