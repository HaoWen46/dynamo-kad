//! Chaos injection wrapper for [`ReplicaClient`].
//!
//! [`ChaosReplicaClient`] wraps any `R: ReplicaClient` and injects
//! configurable failures: random errors, per-node failures, and latency.

use crate::coordinator::VersionedValue;
use crate::replica_client::{ReplicaClient, ReplicaError};
use dynamo_common::NodeId;
use dynamo_kad::node_info::NodeInfo;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

/// Configuration for replica chaos injection.
#[derive(Debug, Clone)]
pub struct ChaosReplicaConfig {
    /// Probability of returning an error \[0.0, 1.0\].
    pub failure_rate: f64,
    /// Fixed latency injected before forwarding.
    pub latency: Duration,
    /// Random additional latency in \[0, jitter\].
    pub jitter: Duration,
}

impl Default for ChaosReplicaConfig {
    fn default() -> Self {
        Self {
            failure_rate: 0.0,
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
        }
    }
}

/// A [`ReplicaClient`] wrapper that injects chaos (failures, latency, per-node blocks).
pub struct ChaosReplicaClient<R: ReplicaClient> {
    inner: Arc<R>,
    config: Arc<RwLock<ChaosReplicaConfig>>,
    /// Nodes that are explicitly marked as failed.
    failed_nodes: Arc<RwLock<HashSet<NodeId>>>,
}

impl<R: ReplicaClient> std::fmt::Debug for ChaosReplicaClient<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChaosReplicaClient").finish_non_exhaustive()
    }
}

impl<R: ReplicaClient> ChaosReplicaClient<R> {
    pub fn new(inner: R, config: ChaosReplicaConfig) -> Self {
        Self {
            inner: Arc::new(inner),
            config: Arc::new(RwLock::new(config)),
            failed_nodes: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Mark a node as permanently failed (until recovered).
    pub async fn fail_node(&self, node_id: NodeId) {
        self.failed_nodes.write().await.insert(node_id);
    }

    /// Remove a node from the failed set.
    pub async fn recover_node(&self, node_id: NodeId) {
        self.failed_nodes.write().await.remove(&node_id);
    }

    /// Dynamically update the random failure rate.
    pub async fn set_failure_rate(&self, rate: f64) {
        self.config.write().await.failure_rate = rate;
    }

    /// Apply chaos checks: returns Err if the request should fail.
    async fn maybe_fail(&self, target: &NodeInfo) -> Result<(), ReplicaError> {
        // Check explicit node failures
        {
            let failed = self.failed_nodes.read().await;
            if failed.contains(&target.id) {
                return Err(ReplicaError::RpcFailed(
                    "chaos: node marked as failed".into(),
                ));
            }
        }

        // Read config
        let (delay, failure_rate) = {
            let config = self.config.read().await;
            let jitter_ms = if config.jitter.is_zero() {
                0
            } else {
                rand::thread_rng().gen_range(0..=config.jitter.as_millis() as u64)
            };
            let delay = config.latency + Duration::from_millis(jitter_ms);
            (delay, config.failure_rate)
        };

        // Inject latency
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        // Random failure
        if failure_rate > 0.0 && rand::thread_rng().gen_bool(failure_rate.min(1.0)) {
            return Err(ReplicaError::RpcFailed("chaos: random failure".into()));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<R: ReplicaClient> ReplicaClient for ChaosReplicaClient<R> {
    async fn replica_put(
        &self,
        target: &NodeInfo,
        key: &str,
        versioned: &VersionedValue,
        write_id: &str,
    ) -> Result<(), ReplicaError> {
        self.maybe_fail(target).await?;
        self.inner
            .replica_put(target, key, versioned, write_id)
            .await
    }

    async fn replica_get(
        &self,
        target: &NodeInfo,
        key: &str,
    ) -> Result<Vec<VersionedValue>, ReplicaError> {
        self.maybe_fail(target).await?;
        self.inner.replica_get(target, key).await
    }
}

// ────────────────────────── Tests ──────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    struct OkReplicaClient;

    #[async_trait::async_trait]
    impl ReplicaClient for OkReplicaClient {
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

    fn make_vv() -> VersionedValue {
        VersionedValue {
            value: b"val".to_vec(),
            vclock: crate::vclock::VClock::new().increment("n1"),
            tombstone: false,
        }
    }

    fn target(name: &[u8]) -> NodeInfo {
        NodeInfo::with_dummy_addr(NodeId::from_sha1(name))
    }

    #[tokio::test]
    async fn test_chaos_replica_passthrough() {
        let chaos = ChaosReplicaClient::new(OkReplicaClient, ChaosReplicaConfig::default());
        let t = target(b"node1");
        let vv = make_vv();
        assert!(chaos.replica_put(&t, "key", &vv, "w1").await.is_ok());
        assert!(chaos.replica_get(&t, "key").await.is_ok());
    }

    #[tokio::test]
    async fn test_chaos_replica_fail_node() {
        let chaos = ChaosReplicaClient::new(OkReplicaClient, ChaosReplicaConfig::default());
        let t = target(b"node1");
        let vv = make_vv();

        chaos.fail_node(t.id).await;
        assert!(chaos.replica_put(&t, "key", &vv, "w1").await.is_err());
        assert!(chaos.replica_get(&t, "key").await.is_err());

        // Other nodes still work
        let t2 = target(b"node2");
        assert!(chaos.replica_put(&t2, "key", &vv, "w1").await.is_ok());
    }

    #[tokio::test]
    async fn test_chaos_replica_recover_node() {
        let chaos = ChaosReplicaClient::new(OkReplicaClient, ChaosReplicaConfig::default());
        let t = target(b"node1");
        let vv = make_vv();

        chaos.fail_node(t.id).await;
        assert!(chaos.replica_put(&t, "key", &vv, "w1").await.is_err());

        chaos.recover_node(t.id).await;
        assert!(chaos.replica_put(&t, "key", &vv, "w1").await.is_ok());
    }

    #[tokio::test]
    async fn test_chaos_replica_random_failure() {
        let config = ChaosReplicaConfig {
            failure_rate: 1.0,
            ..Default::default()
        };
        let chaos = ChaosReplicaClient::new(OkReplicaClient, config);
        let t = target(b"node1");
        let vv = make_vv();

        for _ in 0..10 {
            assert!(chaos.replica_put(&t, "key", &vv, "w1").await.is_err());
        }

        // Set back to 0
        chaos.set_failure_rate(0.0).await;
        assert!(chaos.replica_put(&t, "key", &vv, "w1").await.is_ok());
    }
}
