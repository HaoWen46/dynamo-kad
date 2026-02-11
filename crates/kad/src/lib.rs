//! dynamo-kad: Kademlia DHT implementation.
//!
//! This crate implements the Kademlia distributed hash table protocol
//! with iterative parallel lookups, k-bucket routing tables, and
//! a pluggable transport layer for testability.

pub mod chaos;
pub mod key;
pub mod lookup;
pub mod node_info;
pub mod refresh;
pub mod routing_table;
pub mod rpc;
pub mod store;

use lookup::LookupConfig;
use node_info::NodeInfo;
use routing_table::RoutingTable;
use rpc::{FindValueOutcome, KadRequest, KadResponse, KadTransport};
use store::{Record, RecordStore};

use dynamo_common::{KadError, NodeId};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for a Kademlia node.
#[derive(Debug, Clone)]
pub struct KadConfig {
    /// Number of entries per k-bucket and number of closest nodes to return.
    pub k: usize,
    /// Parallelism factor for iterative lookups.
    pub alpha: usize,
    /// Timeout for individual RPC calls.
    pub rpc_timeout: Duration,
    /// How often to refresh stale buckets.
    pub refresh_interval: Duration,
    /// Maximum number of records in the local store.
    pub max_records: usize,
}

impl Default for KadConfig {
    fn default() -> Self {
        Self {
            k: 20,
            alpha: 3,
            rpc_timeout: Duration::from_secs(5),
            refresh_interval: Duration::from_secs(3600),
            max_records: 1024,
        }
    }
}

impl KadConfig {
    fn to_lookup_config(&self) -> LookupConfig {
        LookupConfig {
            k: self.k,
            alpha: self.alpha,
            timeout: self.rpc_timeout,
        }
    }
}

// ---------------------------------------------------------------------------
// Kad coordinator
// ---------------------------------------------------------------------------

/// The main Kademlia node coordinator.
///
/// Wraps the routing table, record store, and transport layer.
/// Generic over `T: KadTransport` for testability.
pub struct Kad<T: KadTransport> {
    local_info: NodeInfo,
    routing_table: Arc<RwLock<RoutingTable>>,
    record_store: Arc<RwLock<RecordStore>>,
    transport: Arc<T>,
    config: KadConfig,
}

impl<T: KadTransport> std::fmt::Debug for Kad<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kad")
            .field("local_info", &self.local_info)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<T: KadTransport> Kad<T> {
    /// Create a new Kademlia node.
    pub fn new(local_info: NodeInfo, transport: T, config: KadConfig) -> Self {
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(local_info.id, config.k)));
        let record_store = Arc::new(RwLock::new(RecordStore::new(config.max_records)));

        Self {
            local_info,
            routing_table,
            record_store,
            transport: Arc::new(transport),
            config,
        }
    }

    /// The local node's info.
    pub fn local_info(&self) -> &NodeInfo {
        &self.local_info
    }

    /// The local node's ID.
    pub fn local_id(&self) -> NodeId {
        self.local_info.id
    }

    /// Access the routing table.
    pub fn routing_table(&self) -> &Arc<RwLock<RoutingTable>> {
        &self.routing_table
    }

    /// Access the record store.
    pub fn record_store(&self) -> &Arc<RwLock<RecordStore>> {
        &self.record_store
    }

    // -----------------------------------------------------------------------
    // Bootstrap
    // -----------------------------------------------------------------------

    /// Bootstrap this node into the network using the given seed nodes.
    ///
    /// 1. Add seed nodes to the routing table.
    /// 2. Perform a self-lookup to discover nearby nodes.
    /// 3. Refresh all k-buckets.
    pub async fn bootstrap(&self, seeds: Vec<NodeInfo>) -> Result<(), KadError> {
        if seeds.is_empty() {
            return Ok(()); // Nothing to bootstrap from
        }

        // Add seeds to the routing table
        {
            let mut table = self.routing_table.write().await;
            for seed in &seeds {
                table.update(seed.clone());
            }
        }

        // Perform a self-lookup to populate nearby buckets
        let lookup_config = self.config.to_lookup_config();
        let initial_nodes = {
            let table = self.routing_table.read().await;
            table.closest_nodes(&self.local_info.id, self.config.k)
        };

        let found = lookup::find_node(
            self.local_info.id,
            self.local_info.clone(),
            &lookup_config,
            self.transport.clone(),
            initial_nodes,
        )
        .await?;

        // Add discovered nodes
        {
            let mut table = self.routing_table.write().await;
            for node in found {
                table.update(node);
            }
        }

        // Refresh all buckets
        refresh::refresh_stale_buckets(
            &self.routing_table,
            &self.local_info,
            self.transport.clone(),
            &lookup_config,
            Duration::ZERO, // Force refresh of all buckets
        )
        .await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Find the k closest nodes to a target ID.
    pub async fn find_node(&self, target: &NodeId) -> Result<Vec<NodeInfo>, KadError> {
        let lookup_config = self.config.to_lookup_config();
        let initial_nodes = {
            let table = self.routing_table.read().await;
            table.closest_nodes(target, self.config.k)
        };

        if initial_nodes.is_empty() {
            return Ok(vec![]);
        }

        let found = lookup::find_node(
            *target,
            self.local_info.clone(),
            &lookup_config,
            self.transport.clone(),
            initial_nodes,
        )
        .await?;

        // Update routing table with discovered nodes
        {
            let mut table = self.routing_table.write().await;
            for node in &found {
                table.update(node.clone());
            }
        }

        Ok(found)
    }

    /// Store a value in the DHT.
    ///
    /// Finds the k closest nodes to the key and sends STORE RPCs to them.
    pub async fn store(&self, key: &NodeId, value: Vec<u8>) -> Result<(), KadError> {
        // Also store locally if we're among the closest
        {
            let mut store = self.record_store.write().await;
            store.put(Record {
                key: *key,
                value: value.clone(),
                publisher: self.local_info.id,
                expires: None,
                stored_at: Instant::now(),
            })?;
        }

        // Find the k closest nodes to the key
        let closest = self.find_node(key).await?;

        // Send STORE RPCs to all closest nodes
        let mut store_futures = Vec::new();
        for node in &closest {
            if node.id == self.local_info.id {
                continue; // Already stored locally
            }
            let transport = self.transport.clone();
            let request = KadRequest::Store {
                sender: self.local_info.clone(),
                key: *key,
                value: value.clone(),
            };
            let node = node.clone();
            let timeout = self.config.rpc_timeout;
            store_futures.push(async move {
                tokio::time::timeout(timeout, transport.send_request(&node, request)).await
            });
        }

        // Fire and forget (best effort) — don't fail if some nodes don't respond
        futures::future::join_all(store_futures).await;

        Ok(())
    }

    /// Retrieve a value from the DHT.
    ///
    /// First checks the local store, then performs a FIND_VALUE lookup.
    pub async fn find_value(&self, key: &NodeId) -> Result<Option<Vec<u8>>, KadError> {
        // Check local store first
        {
            let store = self.record_store.read().await;
            if let Some(record) = store.get(key) {
                return Ok(Some(record.value.clone()));
            }
        }

        // Perform a FIND_VALUE lookup
        let lookup_config = self.config.to_lookup_config();
        let initial_nodes = {
            let table = self.routing_table.read().await;
            table.closest_nodes(key, self.config.k)
        };

        if initial_nodes.is_empty() {
            return Ok(None);
        }

        let result = lookup::find_value(
            *key,
            self.local_info.clone(),
            &lookup_config,
            self.transport.clone(),
            initial_nodes,
        )
        .await?;

        match result {
            FindValueOutcome::Found(value) => Ok(Some(value)),
            FindValueOutcome::NotFound { .. } => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // Periodic refresh
    // -----------------------------------------------------------------------

    /// Spawn a background task that periodically refreshes stale k-buckets.
    ///
    /// Uses `config.refresh_interval` to determine how often to check.
    /// Returns a `JoinHandle` for the spawned task.
    pub fn spawn_refresh_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let kad = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(kad.config.refresh_interval);
            loop {
                tick.tick().await;
                let lookup_config = kad.config.to_lookup_config();
                match refresh::refresh_stale_buckets(
                    &kad.routing_table,
                    &kad.local_info,
                    kad.transport.clone(),
                    &lookup_config,
                    kad.config.refresh_interval,
                )
                .await
                {
                    Ok(n) => {
                        if n > 0 {
                            tracing::debug!("refreshed {} stale bucket(s)", n);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("bucket refresh failed: {}", e);
                    }
                }
            }
        })
    }

    // -----------------------------------------------------------------------
    // Request handling (incoming RPCs)
    // -----------------------------------------------------------------------

    /// Handle an incoming RPC request.
    ///
    /// This is called by the transport layer when a remote node sends us a request.
    pub async fn handle_request(&self, request: KadRequest) -> KadResponse {
        // Update routing table with the sender's info
        {
            let mut table = self.routing_table.write().await;
            table.update(request.sender().clone());
        }

        match request {
            KadRequest::Ping { .. } => KadResponse::Pong {
                responder: self.local_info.clone(),
            },

            KadRequest::FindNode { target, .. } => {
                let closest = {
                    let table = self.routing_table.read().await;
                    table.closest_nodes(&target, self.config.k)
                };
                KadResponse::FindNodeResult {
                    responder: self.local_info.clone(),
                    closest,
                }
            }

            KadRequest::FindValue { key, .. } => {
                // Check local store
                let store = self.record_store.read().await;
                if let Some(record) = store.get(&key) {
                    KadResponse::FindValueResult {
                        responder: self.local_info.clone(),
                        result: FindValueOutcome::Found(record.value.clone()),
                    }
                } else {
                    drop(store);
                    let closest = {
                        let table = self.routing_table.read().await;
                        table.closest_nodes(&key, self.config.k)
                    };
                    KadResponse::FindValueResult {
                        responder: self.local_info.clone(),
                        result: FindValueOutcome::NotFound { closest },
                    }
                }
            }

            KadRequest::Store { key, value, sender } => {
                let record = Record {
                    key,
                    value,
                    publisher: sender.id,
                    expires: None,
                    stored_at: Instant::now(),
                };
                let _ = self.record_store.write().await.put(record);
                KadResponse::StoreOk {
                    responder: self.local_info.clone(),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// End-to-end tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::RwLock as TokioRwLock;

    // -----------------------------------------------------------------------
    // Simulated transport using closure-based handler dispatch
    // -----------------------------------------------------------------------

    /// A simulated transport that forwards requests to Kad::handle_request
    /// on the target node.
    #[derive(Clone)]
    struct SimTransportInner {
        /// Shared registry of all nodes. We use a raw pointer-like approach
        /// via Arc to avoid circular deps.
        registry: Arc<TokioRwLock<HashMap<NodeId, HandlerFn>>>,
    }

    type HandlerFn = Arc<
        dyn Fn(
                KadRequest,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = KadResponse> + Send>>
            + Send
            + Sync,
    >;

    #[async_trait::async_trait]
    impl KadTransport for SimTransportInner {
        async fn send_request(
            &self,
            target: &NodeInfo,
            request: KadRequest,
        ) -> Result<KadResponse, KadError> {
            let registry = self.registry.read().await;
            let handler = registry
                .get(&target.id)
                .cloned()
                .ok_or(KadError::NodeNotFound(target.id))?;
            drop(registry);
            Ok(handler(request).await)
        }
    }

    /// Build a network of `n` Kad nodes with a simulated transport.
    /// Returns the list of Kad instances (each behind an Arc).
    async fn build_kad_network(
        n: usize,
    ) -> (
        Vec<Arc<Kad<SimTransportInner>>>,
        Arc<TokioRwLock<HashMap<NodeId, HandlerFn>>>,
    ) {
        let registry: Arc<TokioRwLock<HashMap<NodeId, HandlerFn>>> =
            Arc::new(TokioRwLock::new(HashMap::new()));

        let config = KadConfig {
            k: 20,
            alpha: 3,
            rpc_timeout: Duration::from_secs(5),
            refresh_interval: Duration::from_secs(3600),
            max_records: 1024,
        };

        let mut nodes = Vec::new();

        for _ in 0..n {
            let info = NodeInfo::with_dummy_addr(NodeId::random());
            let transport = SimTransportInner {
                registry: registry.clone(),
            };
            let kad = Arc::new(Kad::new(info, transport, config.clone()));
            nodes.push(kad);
        }

        // Register handlers
        for kad in &nodes {
            let kad_clone = kad.clone();
            let handler: HandlerFn = Arc::new(move |req| {
                let kad = kad_clone.clone();
                Box::pin(async move { kad.handle_request(req).await })
            });
            registry.write().await.insert(kad.local_id(), handler);
        }

        (nodes, registry)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_bootstrap_single_seed() {
        let (nodes, _registry) = build_kad_network(5).await;

        // Node 0 bootstraps with node 1 as seed
        let seed = nodes[1].local_info().clone();
        nodes[0].bootstrap(vec![seed]).await.unwrap();

        // Node 0 should now know about at least the seed
        let table = nodes[0].routing_table().read().await;
        assert!(!table.is_empty(), "should know about at least the seed");
    }

    #[tokio::test]
    async fn test_bootstrap_fills_table() {
        let (nodes, _registry) = build_kad_network(20).await;

        // Bootstrap all nodes incrementally: each uses the previous as a seed
        for pair in nodes.windows(2) {
            let seed = pair[0].local_info().clone();
            pair[1].bootstrap(vec![seed]).await.unwrap();
        }

        // Also bootstrap node 0 with node 1
        let seed = nodes[1].local_info().clone();
        nodes[0].bootstrap(vec![seed]).await.unwrap();

        // Each node should know about several others
        for (i, node) in nodes.iter().enumerate() {
            let table = node.routing_table().read().await;
            assert!(
                table.len() >= 2,
                "node {} should know about multiple peers, but knows {}",
                i,
                table.len()
            );
        }
    }

    #[tokio::test]
    async fn test_find_node_across_network() {
        let (nodes, _registry) = build_kad_network(20).await;

        // Bootstrap all nodes: each bootstraps from the first node
        let first_info = nodes[0].local_info().clone();
        for node in nodes.iter().skip(1) {
            node.bootstrap(vec![first_info.clone()]).await.unwrap();
        }
        // Re-bootstrap node 0 to learn about others
        let second_info = nodes[1].local_info().clone();
        nodes[0].bootstrap(vec![second_info]).await.unwrap();

        // Node 0 should be able to find node 15
        let target = nodes[15].local_id();
        let result = nodes[0].find_node(&target).await.unwrap();

        assert!(
            result.iter().any(|n| n.id == target),
            "should find target node in lookup results"
        );
    }

    #[tokio::test]
    async fn test_store_and_retrieve_across_network() {
        let (nodes, _registry) = build_kad_network(10).await;

        // Bootstrap all from node 0
        let first_info = nodes[0].local_info().clone();
        for node in nodes.iter().skip(1) {
            node.bootstrap(vec![first_info.clone()]).await.unwrap();
        }
        let second_info = nodes[1].local_info().clone();
        nodes[0].bootstrap(vec![second_info]).await.unwrap();

        // Store a value from node 0
        let key = NodeId::from_sha1(b"test-key");
        let value = b"test-value".to_vec();
        nodes[0].store(&key, value.clone()).await.unwrap();

        // Retrieve from node 5
        let result = nodes[5].find_value(&key).await.unwrap();
        assert_eq!(result, Some(value), "should retrieve the stored value");
    }

    #[tokio::test]
    async fn test_handle_ping() {
        let (nodes, _registry) = build_kad_network(2).await;
        let sender = nodes[1].local_info().clone();

        let response = nodes[0].handle_request(KadRequest::Ping { sender }).await;

        match response {
            KadResponse::Pong { responder } => {
                assert_eq!(responder.id, nodes[0].local_id());
            }
            other => panic!("expected Pong, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_store_and_find_value() {
        let (nodes, _registry) = build_kad_network(2).await;

        let sender = nodes[1].local_info().clone();
        let key = NodeId::from_sha1(b"my-key");
        let value = b"my-value".to_vec();

        // Store via RPC
        let resp = nodes[0]
            .handle_request(KadRequest::Store {
                sender: sender.clone(),
                key,
                value: value.clone(),
            })
            .await;
        assert!(matches!(resp, KadResponse::StoreOk { .. }));

        // Find value via RPC
        let resp = nodes[0]
            .handle_request(KadRequest::FindValue {
                sender: sender.clone(),
                key,
            })
            .await;
        match resp {
            KadResponse::FindValueResult {
                result: FindValueOutcome::Found(v),
                ..
            } => {
                assert_eq!(v, value);
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_node_departure_lookup_resilient() {
        let (nodes, registry) = build_kad_network(15).await;

        // Bootstrap all from node 0
        let first_info = nodes[0].local_info().clone();
        for node in nodes.iter().skip(1) {
            node.bootstrap(vec![first_info.clone()]).await.unwrap();
        }
        let second_info = nodes[1].local_info().clone();
        nodes[0].bootstrap(vec![second_info]).await.unwrap();

        // Remove node 5 from the network (simulating departure)
        let removed_id = nodes[5].local_id();
        registry.write().await.remove(&removed_id);

        // Node 0 should still be able to find other nodes
        let target = nodes[10].local_id();
        let result = nodes[0].find_node(&target).await.unwrap();
        assert!(
            !result.is_empty(),
            "lookup should succeed even after node departure"
        );
    }

    #[tokio::test]
    async fn test_self_lookup_in_bootstrap() {
        let (nodes, _registry) = build_kad_network(10).await;

        // Bootstrap node 0 with node 1 as seed
        let seed = nodes[1].local_info().clone();
        nodes[0].bootstrap(vec![seed]).await.unwrap();

        // After bootstrap, node 0's routing table should contain nearby nodes
        let table = nodes[0].routing_table().read().await;
        let nearby = table.closest_nodes(&nodes[0].local_id(), 5);

        // The self-lookup during bootstrap should have discovered some nodes
        assert!(
            !nearby.is_empty(),
            "self-lookup should have discovered nearby nodes"
        );
    }
}
