//! Chaos injection wrappers for testing fault tolerance.
//!
//! [`ChaosTransport`] wraps any [`KadTransport`] implementation and injects
//! configurable failures: random errors, latency, and network partitions.

use crate::node_info::NodeInfo;
use crate::rpc::{KadRequest, KadResponse, KadTransport};
use dynamo_common::{KadError, NodeId};
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

/// Configuration for chaos injection.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of returning an error \[0.0, 1.0\].
    pub failure_rate: f64,
    /// Fixed latency injected before forwarding.
    pub latency: Duration,
    /// Random additional latency in \[0, jitter\].
    pub jitter: Duration,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            failure_rate: 0.0,
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
        }
    }
}

/// A transport wrapper that injects chaos (latency, failures, partitions).
///
/// Wraps any `T: KadTransport` and is itself a `KadTransport`.
pub struct ChaosTransport<T: KadTransport> {
    inner: Arc<T>,
    config: Arc<RwLock<ChaosConfig>>,
    /// Set of (from, to) pairs that are partitioned.
    partitions: Arc<RwLock<HashSet<(NodeId, NodeId)>>>,
    local_id: NodeId,
}

impl<T: KadTransport> std::fmt::Debug for ChaosTransport<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChaosTransport")
            .field("local_id", &self.local_id)
            .finish_non_exhaustive()
    }
}

impl<T: KadTransport> ChaosTransport<T> {
    pub fn new(inner: T, local_id: NodeId, config: ChaosConfig) -> Self {
        Self {
            inner: Arc::new(inner),
            config: Arc::new(RwLock::new(config)),
            partitions: Arc::new(RwLock::new(HashSet::new())),
            local_id,
        }
    }

    /// Dynamically update the failure rate.
    pub async fn set_failure_rate(&self, rate: f64) {
        self.config.write().await.failure_rate = rate;
    }

    /// Dynamically update the injected latency.
    pub async fn set_latency(&self, latency: Duration) {
        self.config.write().await.latency = latency;
    }

    /// Block all requests from `from` to `to`.
    pub async fn add_partition(&self, from: NodeId, to: NodeId) {
        self.partitions.write().await.insert((from, to));
    }

    /// Remove a specific partition.
    pub async fn remove_partition(&self, from: NodeId, to: NodeId) {
        self.partitions.write().await.remove(&(from, to));
    }

    /// Remove all partitions.
    pub async fn heal_all(&self) {
        self.partitions.write().await.clear();
    }
}

#[async_trait::async_trait]
impl<T: KadTransport> KadTransport for ChaosTransport<T> {
    async fn send_request(
        &self,
        target: &NodeInfo,
        request: KadRequest,
    ) -> Result<KadResponse, KadError> {
        // 1. Check partition
        {
            let partitions = self.partitions.read().await;
            if partitions.contains(&(self.local_id, target.id)) {
                return Err(KadError::Internal("chaos: partitioned".into()));
            }
        }

        // 2. Read config snapshot
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

        // 3. Inject latency
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        // 4. Random failure
        if failure_rate > 0.0 && rand::thread_rng().gen_bool(failure_rate.min(1.0)) {
            return Err(KadError::Internal("chaos: random failure".into()));
        }

        // 5. Forward to inner transport
        self.inner.send_request(target, request).await
    }
}

// ────────────────────────── Tests ──────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{KadRequest, KadResponse};

    /// A passthrough transport that always succeeds.
    struct OkTransport;

    #[async_trait::async_trait]
    impl KadTransport for OkTransport {
        async fn send_request(
            &self,
            _target: &NodeInfo,
            _request: KadRequest,
        ) -> Result<KadResponse, KadError> {
            Ok(KadResponse::Pong {
                responder: NodeInfo::with_dummy_addr(NodeId::from_sha1(b"responder")),
            })
        }
    }

    fn local_id() -> NodeId {
        NodeId::from_sha1(b"local")
    }

    fn remote_info() -> NodeInfo {
        NodeInfo::with_dummy_addr(NodeId::from_sha1(b"remote"))
    }

    fn ping_request() -> KadRequest {
        KadRequest::Ping {
            sender: NodeInfo::with_dummy_addr(local_id()),
        }
    }

    #[tokio::test]
    async fn test_chaos_passthrough() {
        let chaos = ChaosTransport::new(OkTransport, local_id(), ChaosConfig::default());
        let result = chaos.send_request(&remote_info(), ping_request()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_chaos_failure_rate_100() {
        let config = ChaosConfig {
            failure_rate: 1.0,
            ..Default::default()
        };
        let chaos = ChaosTransport::new(OkTransport, local_id(), config);
        for _ in 0..10 {
            let result = chaos.send_request(&remote_info(), ping_request()).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_chaos_latency() {
        let config = ChaosConfig {
            latency: Duration::from_millis(100),
            ..Default::default()
        };
        let chaos = ChaosTransport::new(OkTransport, local_id(), config);

        let start = tokio::time::Instant::now();
        let result = chaos.send_request(&remote_info(), ping_request()).await;
        assert!(result.is_ok());
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_chaos_partition_blocks() {
        let chaos = ChaosTransport::new(OkTransport, local_id(), ChaosConfig::default());
        let remote = remote_info();

        // No partition — should succeed
        assert!(chaos.send_request(&remote, ping_request()).await.is_ok());

        // Add partition
        chaos.add_partition(local_id(), remote.id).await;
        assert!(chaos.send_request(&remote, ping_request()).await.is_err());

        // Other direction still works
        let other = NodeInfo::with_dummy_addr(NodeId::from_sha1(b"other"));
        assert!(chaos.send_request(&other, ping_request()).await.is_ok());
    }

    #[tokio::test]
    async fn test_chaos_partition_heal() {
        let chaos = ChaosTransport::new(OkTransport, local_id(), ChaosConfig::default());
        let remote = remote_info();

        chaos.add_partition(local_id(), remote.id).await;
        assert!(chaos.send_request(&remote, ping_request()).await.is_err());

        chaos.remove_partition(local_id(), remote.id).await;
        assert!(chaos.send_request(&remote, ping_request()).await.is_ok());
    }

    #[tokio::test]
    async fn test_chaos_dynamic_config() {
        let chaos = ChaosTransport::new(OkTransport, local_id(), ChaosConfig::default());
        let remote = remote_info();

        // Start with 0% failure
        assert!(chaos.send_request(&remote, ping_request()).await.is_ok());

        // Change to 100% failure
        chaos.set_failure_rate(1.0).await;
        assert!(chaos.send_request(&remote, ping_request()).await.is_err());

        // Back to 0%
        chaos.set_failure_rate(0.0).await;
        assert!(chaos.send_request(&remote, ping_request()).await.is_ok());
    }
}
