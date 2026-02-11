//! Iterative parallel lookup algorithm for Kademlia.
//!
//! Implements the core Kademlia lookup with configurable parallelism (α).
//! Used for both FIND_NODE and FIND_VALUE operations.

use crate::node_info::NodeInfo;
use crate::rpc::{FindValueOutcome, KadRequest, KadResponse, KadTransport};
use dynamo_common::{Distance, KadError, NodeId};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the iterative lookup.
#[derive(Debug, Clone)]
pub struct LookupConfig {
    /// Number of closest nodes to return (the `k` parameter).
    pub k: usize,
    /// Number of parallel queries in flight (the `α` parameter).
    pub alpha: usize,
    /// Timeout for individual RPC calls.
    pub timeout: Duration,
}

impl Default for LookupConfig {
    fn default() -> Self {
        Self {
            k: 20,
            alpha: 3,
            timeout: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// Candidate tracking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateState {
    NotQueried,
    InFlight,
    Responded,
    Failed,
}

#[derive(Debug, Clone)]
struct CandidateEntry {
    info: NodeInfo,
    #[allow(dead_code)] // kept for debugging/diagnostics
    distance: Distance,
    state: CandidateState,
}

// ---------------------------------------------------------------------------
// Iterative lookup
// ---------------------------------------------------------------------------

/// Run an iterative FIND_NODE lookup.
///
/// Seeds the candidate set with `initial_nodes`, then repeatedly queries
/// the closest un-queried candidates in parallel until convergence.
/// Returns the `k` closest responding nodes to `target`.
pub async fn find_node<T: KadTransport>(
    target: NodeId,
    local_info: NodeInfo,
    config: &LookupConfig,
    transport: Arc<T>,
    initial_nodes: Vec<NodeInfo>,
) -> Result<Vec<NodeInfo>, KadError> {
    let mut candidates = build_candidate_map(target, &initial_nodes);
    let queried_ids: HashSet<NodeId> = HashSet::new();

    run_lookup(
        target,
        local_info,
        config,
        transport,
        &mut candidates,
        queried_ids,
        false, // not a value lookup
    )
    .await
    .map(|outcome| match outcome {
        LookupOutcome::Nodes(nodes) => nodes,
        LookupOutcome::Value(_, _) => unreachable!(),
    })
}

/// Run an iterative FIND_VALUE lookup.
///
/// Like `find_node`, but uses FIND_VALUE RPCs. If any node returns the value,
/// the lookup short-circuits and returns it.
pub async fn find_value<T: KadTransport>(
    key: NodeId,
    local_info: NodeInfo,
    config: &LookupConfig,
    transport: Arc<T>,
    initial_nodes: Vec<NodeInfo>,
) -> Result<FindValueOutcome, KadError> {
    let mut candidates = build_candidate_map(key, &initial_nodes);
    let queried_ids: HashSet<NodeId> = HashSet::new();

    run_lookup(
        key,
        local_info,
        config,
        transport,
        &mut candidates,
        queried_ids,
        true, // value lookup
    )
    .await
    .map(|outcome| match outcome {
        LookupOutcome::Nodes(nodes) => FindValueOutcome::NotFound { closest: nodes },
        LookupOutcome::Value(value, _responder) => FindValueOutcome::Found(value),
    })
}

enum LookupOutcome {
    Nodes(Vec<NodeInfo>),
    Value(Vec<u8>, NodeInfo),
}

fn build_candidate_map(
    target: NodeId,
    initial_nodes: &[NodeInfo],
) -> BTreeMap<Distance, CandidateEntry> {
    let mut candidates = BTreeMap::new();
    for node in initial_nodes {
        let distance = target.distance(&node.id);
        candidates.entry(distance).or_insert(CandidateEntry {
            info: node.clone(),
            distance,
            state: CandidateState::NotQueried,
        });
    }
    candidates
}

async fn run_lookup<T: KadTransport>(
    target: NodeId,
    local_info: NodeInfo,
    config: &LookupConfig,
    transport: Arc<T>,
    candidates: &mut BTreeMap<Distance, CandidateEntry>,
    mut queried_ids: HashSet<NodeId>,
    is_value_lookup: bool,
) -> Result<LookupOutcome, KadError> {
    loop {
        // Count how many are currently in flight
        let in_flight_count = candidates
            .values()
            .filter(|c| c.state == CandidateState::InFlight)
            .count();

        // Select up to (alpha - in_flight_count) closest NotQueried candidates
        let to_query: Vec<(Distance, NodeInfo)> = candidates
            .iter()
            .filter(|(_, c)| c.state == CandidateState::NotQueried)
            .take(config.alpha.saturating_sub(in_flight_count))
            .map(|(d, c)| (*d, c.info.clone()))
            .collect();

        if to_query.is_empty() && in_flight_count == 0 {
            // No more candidates to query and nothing in flight: done.
            break;
        }

        // Check termination: are all NotQueried candidates farther than
        // the k-th closest responded node?
        if to_query.is_empty() && !has_closer_unqueried(candidates, config.k) {
            // Wait for remaining in-flight to finish, but don't start new queries
            // (we'll drain below)
        }

        // Mark selected candidates as InFlight
        for (dist, _) in &to_query {
            if let Some(c) = candidates.get_mut(dist) {
                c.state = CandidateState::InFlight;
                queried_ids.insert(c.info.id);
            }
        }

        if to_query.is_empty() && in_flight_count == 0 {
            break;
        }

        // Launch parallel requests
        let mut futures = FuturesUnordered::new();
        for (dist, node) in to_query {
            let transport = transport.clone();
            let request = if is_value_lookup {
                KadRequest::FindValue {
                    sender: local_info.clone(),
                    key: target,
                }
            } else {
                KadRequest::FindNode {
                    sender: local_info.clone(),
                    target,
                }
            };
            let timeout = config.timeout;

            futures.push(async move {
                let result = tokio::time::timeout(timeout, transport.send_request(&node, request))
                    .await
                    .map_err(|_| KadError::Timeout(node.id))
                    .and_then(|r| r);
                (dist, node, result)
            });
        }

        // Also add any existing in-flight futures? No — we can't track them
        // across iterations easily. Instead, we just process the batch we launched
        // and loop again. This is simpler and still correct; each iteration makes
        // progress.

        // Process all responses from this batch
        while let Some((dist, queried_node, result)) = futures.next().await {
            match result {
                Ok(response) => {
                    // Mark as Responded
                    if let Some(c) = candidates.get_mut(&dist) {
                        c.state = CandidateState::Responded;
                    }

                    match response {
                        KadResponse::FindNodeResult { closest, .. } => {
                            // Add newly discovered nodes
                            for node in closest {
                                if node.id == local_info.id || queried_ids.contains(&node.id) {
                                    continue;
                                }
                                let d = target.distance(&node.id);
                                candidates.entry(d).or_insert(CandidateEntry {
                                    info: node,
                                    distance: d,
                                    state: CandidateState::NotQueried,
                                });
                            }
                        }
                        KadResponse::FindValueResult { responder, result } => match result {
                            FindValueOutcome::Found(value) => {
                                return Ok(LookupOutcome::Value(value, responder));
                            }
                            FindValueOutcome::NotFound { closest } => {
                                for node in closest {
                                    if node.id == local_info.id || queried_ids.contains(&node.id) {
                                        continue;
                                    }
                                    let d = target.distance(&node.id);
                                    candidates.entry(d).or_insert(CandidateEntry {
                                        info: node,
                                        distance: d,
                                        state: CandidateState::NotQueried,
                                    });
                                }
                            }
                        },
                        KadResponse::Pong { .. } | KadResponse::StoreOk { .. } => {
                            // Unexpected response type for a lookup; just ignore.
                        }
                    }
                }
                Err(_) => {
                    // Mark as Failed
                    if let Some(c) = candidates.get_mut(&dist) {
                        c.state = CandidateState::Failed;
                    }
                    tracing::debug!("lookup: node {} failed", queried_node.id);
                }
            }
        }

        // Check termination condition:
        // If no NotQueried candidate is closer than the k-th closest responded node, done.
        if !has_closer_unqueried(candidates, config.k) {
            break;
        }
    }

    // Collect the k closest responded nodes
    let result: Vec<NodeInfo> = candidates
        .values()
        .filter(|c| c.state == CandidateState::Responded)
        .take(config.k)
        .map(|c| c.info.clone())
        .collect();

    Ok(LookupOutcome::Nodes(result))
}

/// Check if any NotQueried candidate is closer than the k-th closest responded node.
fn has_closer_unqueried(candidates: &BTreeMap<Distance, CandidateEntry>, k: usize) -> bool {
    // Find the distance of the k-th closest responded node
    let responded_distances: Vec<Distance> = candidates
        .iter()
        .filter(|(_, c)| c.state == CandidateState::Responded)
        .map(|(d, _)| *d)
        .take(k)
        .collect();

    let kth_distance = if responded_distances.len() >= k {
        responded_distances[k - 1]
    } else {
        // Haven't gotten k responses yet; keep going
        return true;
    };

    // Check if any NotQueried candidate is closer
    candidates
        .iter()
        .any(|(d, c)| c.state == CandidateState::NotQueried && *d < kth_distance)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_info::NodeInfo;
    use crate::rpc::{FindValueOutcome, KadRequest, KadResponse, KadTransport};
    use dynamo_common::NodeId;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    // -----------------------------------------------------------------------
    // Simulated network for testing
    // -----------------------------------------------------------------------

    /// A simulated Kademlia node for testing.
    struct SimNode {
        info: NodeInfo,
        /// Nodes this simulated node knows about (its "routing table").
        known_peers: Vec<NodeInfo>,
        /// Local key-value store.
        store: HashMap<NodeId, Vec<u8>>,
    }

    /// A simulated network connecting multiple SimNodes.
    struct SimulatedNetwork {
        nodes: RwLock<HashMap<NodeId, SimNode>>,
        /// Probability of a request failing (0.0 = no failures).
        failure_rate: f64,
    }

    impl SimulatedNetwork {
        fn new() -> Self {
            Self {
                nodes: RwLock::new(HashMap::new()),
                failure_rate: 0.0,
            }
        }

        fn with_failure_rate(failure_rate: f64) -> Self {
            Self {
                nodes: RwLock::new(HashMap::new()),
                failure_rate,
            }
        }

        async fn add_node(&self, info: NodeInfo, known_peers: Vec<NodeInfo>) {
            let node = SimNode {
                info,
                known_peers,
                store: HashMap::new(),
            };
            self.nodes.write().await.insert(node.info.id, node);
        }

        async fn store_value(&self, node_id: &NodeId, key: NodeId, value: Vec<u8>) {
            let mut nodes = self.nodes.write().await;
            if let Some(node) = nodes.get_mut(node_id) {
                node.store.insert(key, value);
            }
        }
    }

    struct SimTransport {
        network: Arc<SimulatedNetwork>,
    }

    #[async_trait::async_trait]
    impl KadTransport for SimTransport {
        async fn send_request(
            &self,
            target: &NodeInfo,
            request: KadRequest,
        ) -> Result<KadResponse, KadError> {
            // Simulate failure
            if self.network.failure_rate > 0.0 && rand::random::<f64>() < self.network.failure_rate
            {
                return Err(KadError::Timeout(target.id));
            }

            let nodes = self.network.nodes.read().await;
            let node = nodes
                .get(&target.id)
                .ok_or(KadError::NodeNotFound(target.id))?;

            match request {
                KadRequest::Ping { .. } => Ok(KadResponse::Pong {
                    responder: node.info.clone(),
                }),
                KadRequest::FindNode { target: tgt, .. } => {
                    // Return the k closest known peers to the target
                    let mut peers = node.known_peers.clone();
                    peers.sort_by_key(|p| tgt.distance(&p.id));
                    peers.truncate(20);
                    Ok(KadResponse::FindNodeResult {
                        responder: node.info.clone(),
                        closest: peers,
                    })
                }
                KadRequest::FindValue { key, .. } => {
                    if let Some(value) = node.store.get(&key) {
                        Ok(KadResponse::FindValueResult {
                            responder: node.info.clone(),
                            result: FindValueOutcome::Found(value.clone()),
                        })
                    } else {
                        let mut peers = node.known_peers.clone();
                        peers.sort_by_key(|p| key.distance(&p.id));
                        peers.truncate(20);
                        Ok(KadResponse::FindValueResult {
                            responder: node.info.clone(),
                            result: FindValueOutcome::NotFound { closest: peers },
                        })
                    }
                }
                KadRequest::Store { key, value, .. } => {
                    // We'd need mutable access; skip for now in lookup tests
                    drop(nodes);
                    self.network.store_value(&target.id, key, value).await;
                    Ok(KadResponse::StoreOk {
                        responder: target.clone(),
                    })
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helper to build a connected network of N nodes
    // -----------------------------------------------------------------------

    async fn build_network(n: usize) -> (Arc<SimulatedNetwork>, Vec<NodeInfo>) {
        let network = Arc::new(SimulatedNetwork::new());
        let mut all_infos: Vec<NodeInfo> = Vec::new();

        // Create all node infos first
        for _ in 0..n {
            all_infos.push(NodeInfo::with_dummy_addr(NodeId::random()));
        }

        // Each node knows about all other nodes (fully connected for simplicity)
        for i in 0..n {
            let peers: Vec<NodeInfo> = all_infos
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, info)| info.clone())
                .collect();
            network.add_node(all_infos[i].clone(), peers).await;
        }

        (network, all_infos)
    }

    async fn build_sparse_network(
        n: usize,
        peers_per_node: usize,
    ) -> (Arc<SimulatedNetwork>, Vec<NodeInfo>) {
        let network = Arc::new(SimulatedNetwork::new());
        let mut all_infos: Vec<NodeInfo> = Vec::new();

        for _ in 0..n {
            all_infos.push(NodeInfo::with_dummy_addr(NodeId::random()));
        }

        // Each node knows about only `peers_per_node` random others
        for i in 0..n {
            let mut peers: Vec<NodeInfo> = all_infos
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, info)| info.clone())
                .collect();

            // Sort by distance to self so each node knows its closest peers
            let self_id = all_infos[i].id;
            peers.sort_by_key(|p| self_id.distance(&p.id));
            peers.truncate(peers_per_node);

            network.add_node(all_infos[i].clone(), peers).await;
        }

        (network, all_infos)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_lookup_finds_exact_node() {
        let (network, all_infos) = build_network(10).await;
        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let target = all_infos[5].id;

        // Initial nodes: the querier's view (everyone except itself)
        let initial: Vec<NodeInfo> = all_infos[1..].to_vec();

        let config = LookupConfig {
            k: 5,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        let result = find_node(target, querier, &config, transport, initial)
            .await
            .unwrap();

        // The target node should be among the results
        assert!(
            result.iter().any(|n| n.id == target),
            "target node should be in results"
        );
    }

    #[tokio::test]
    async fn test_lookup_converges_to_closest() {
        let (network, all_infos) = build_network(50).await;
        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let target = NodeId::random(); // Random target, might not be any node's ID

        let initial: Vec<NodeInfo> = all_infos[1..4].to_vec(); // Start with just 3 seeds
        let config = LookupConfig {
            k: 10,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        let result = find_node(target, querier, &config, transport, initial)
            .await
            .unwrap();

        // Compute the actual k closest nodes by brute force
        let mut all_by_distance: Vec<(Distance, NodeId)> = all_infos[1..]
            .iter()
            .map(|n| (target.distance(&n.id), n.id))
            .collect();
        all_by_distance.sort_by_key(|(d, _)| *d);
        let expected_closest: Vec<NodeId> =
            all_by_distance.iter().take(10).map(|(_, id)| *id).collect();

        // All result nodes should be among the actual closest
        for node in &result {
            assert!(
                expected_closest.contains(&node.id),
                "result node {:?} is not among the {} truly closest nodes",
                node.id,
                config.k
            );
        }
    }

    #[tokio::test]
    async fn test_lookup_with_alpha_1() {
        let (network, all_infos) = build_network(20).await;
        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let target = all_infos[10].id;
        let initial: Vec<NodeInfo> = all_infos[1..5].to_vec();

        let config = LookupConfig {
            k: 5,
            alpha: 1, // Sequential
            timeout: Duration::from_secs(5),
        };

        let result = find_node(target, querier, &config, transport, initial)
            .await
            .unwrap();
        assert!(!result.is_empty(), "should find some nodes");
        assert!(result.iter().any(|n| n.id == target));
    }

    #[tokio::test]
    async fn test_lookup_handles_timeout() {
        let network = Arc::new(SimulatedNetwork::with_failure_rate(0.3));
        let mut all_infos: Vec<NodeInfo> = Vec::new();

        for _ in 0..30 {
            all_infos.push(NodeInfo::with_dummy_addr(NodeId::random()));
        }

        // Fully connected
        for i in 0..all_infos.len() {
            let peers: Vec<NodeInfo> = all_infos
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, info)| info.clone())
                .collect();
            network.add_node(all_infos[i].clone(), peers).await;
        }

        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let target = all_infos[15].id;
        let initial: Vec<NodeInfo> = all_infos[1..10].to_vec();

        let config = LookupConfig {
            k: 5,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        // Should still complete despite some failures
        let result = find_node(target, querier, &config, transport, initial).await;
        assert!(
            result.is_ok(),
            "lookup should succeed despite some failures"
        );
    }

    #[tokio::test]
    async fn test_lookup_terminates_with_stale_responses() {
        // All nodes return the same set of peers (no new closer nodes discovered).
        // The lookup should still terminate.
        let network = Arc::new(SimulatedNetwork::new());
        let mut all_infos: Vec<NodeInfo> = Vec::new();

        for _ in 0..5 {
            all_infos.push(NodeInfo::with_dummy_addr(NodeId::random()));
        }

        // Each node only knows about the same 3 nodes
        let shared_peers: Vec<NodeInfo> = all_infos[1..4].to_vec();
        for info in &all_infos {
            let peers: Vec<NodeInfo> = shared_peers
                .iter()
                .filter(|p| p.id != info.id)
                .cloned()
                .collect();
            network.add_node(info.clone(), peers).await;
        }

        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let target = NodeId::random();
        let initial = shared_peers.clone();

        let config = LookupConfig {
            k: 5,
            alpha: 3,
            timeout: Duration::from_secs(1),
        };

        let result = find_node(target, querier, &config, transport, initial).await;
        assert!(result.is_ok(), "lookup should terminate");
    }

    #[tokio::test]
    async fn test_find_value_found() {
        let (network, all_infos) = build_network(10).await;

        let key = NodeId::random();
        let value = b"found-me!".to_vec();

        // Store the value on the node closest to the key (mimicking real Kademlia STORE).
        // This ensures the iterative lookup, which converges toward close nodes, will find it.
        let mut closest_idx = 1;
        let mut closest_dist = key.distance(&all_infos[1].id);
        for (i, info) in all_infos.iter().enumerate().skip(2) {
            let dist = key.distance(&info.id);
            if dist < closest_dist {
                closest_dist = dist;
                closest_idx = i;
            }
        }
        network
            .store_value(&all_infos[closest_idx].id, key, value.clone())
            .await;

        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let initial: Vec<NodeInfo> = all_infos[1..].to_vec();

        let config = LookupConfig {
            k: 5,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        let result = find_value(key, querier, &config, transport, initial)
            .await
            .unwrap();
        match result {
            FindValueOutcome::Found(v) => assert_eq!(v, value),
            FindValueOutcome::NotFound { .. } => panic!("expected Found"),
        }
    }

    #[tokio::test]
    async fn test_find_value_not_found() {
        let (network, all_infos) = build_network(10).await;
        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let key = NodeId::random(); // No one has this key
        let initial: Vec<NodeInfo> = all_infos[1..].to_vec();

        let config = LookupConfig {
            k: 5,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        let result = find_value(key, querier, &config, transport, initial)
            .await
            .unwrap();
        match result {
            FindValueOutcome::NotFound { closest } => {
                assert!(!closest.is_empty(), "should return closest nodes");
            }
            FindValueOutcome::Found(_) => panic!("expected NotFound"),
        }
    }

    #[tokio::test]
    async fn test_lookup_sparse_network() {
        // Test with a sparse network where nodes only know a few peers
        let (network, all_infos) = build_sparse_network(50, 5).await;
        let transport = Arc::new(SimTransport {
            network: network.clone(),
        });

        let querier = all_infos[0].clone();
        let target = all_infos[25].id;

        // Start with just the querier's direct peers
        let nodes = network.nodes.read().await;
        let initial = nodes.get(&querier.id).unwrap().known_peers.clone();
        drop(nodes);

        let config = LookupConfig {
            k: 10,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        let result = find_node(target, querier, &config, transport, initial)
            .await
            .unwrap();
        // In a sparse network, we might not find the exact target, but we should get results
        assert!(
            !result.is_empty(),
            "should find some nodes even in sparse network"
        );
    }
}
