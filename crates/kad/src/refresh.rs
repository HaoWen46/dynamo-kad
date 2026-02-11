//! Bucket refresh logic for the Kademlia routing table.
//!
//! Periodically performs lookups for random IDs in stale buckets
//! to keep the routing table populated and up-to-date.

use crate::lookup::{self, LookupConfig};
use crate::node_info::NodeInfo;
use crate::routing_table::RoutingTable;
use crate::rpc::KadTransport;
use dynamo_common::{random_id_in_bucket, KadError};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

/// Refresh all stale buckets by performing lookups for random IDs
/// in each stale bucket's range.
///
/// This populates buckets that may have become sparse due to churn.
pub async fn refresh_stale_buckets<T: KadTransport>(
    routing_table: &Arc<RwLock<RoutingTable>>,
    local_info: &NodeInfo,
    transport: Arc<T>,
    config: &LookupConfig,
    refresh_interval: Duration,
) -> Result<usize, KadError> {
    let (stale_buckets, local_id) = {
        let table = routing_table.read().await;
        let stale = table.buckets_needing_refresh(refresh_interval);
        let local_id = table.local_id();
        (stale, local_id)
    };

    let mut refreshed = 0;

    for bucket_index in stale_buckets {
        let random_target = random_id_in_bucket(&local_id, bucket_index);

        // Get initial nodes for the lookup
        let initial_nodes = {
            let table = routing_table.read().await;
            table.closest_nodes(&random_target, config.k)
        };

        if initial_nodes.is_empty() {
            // Can't refresh if we don't know anyone
            continue;
        }

        // Perform the lookup (which will discover new nodes)
        let found_nodes = lookup::find_node(
            random_target,
            local_info.clone(),
            config,
            transport.clone(),
            initial_nodes,
        )
        .await?;

        // Add discovered nodes to the routing table
        {
            let mut table = routing_table.write().await;
            for node in found_nodes {
                table.update(node);
            }
            table.mark_refreshed(bucket_index);
        }

        refreshed += 1;
    }

    Ok(refreshed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{KadRequest, KadResponse};
    use dynamo_common::NodeId;
    use std::collections::HashMap;
    use tokio::sync::RwLock as TokioRwLock;

    // Simple mock transport that returns empty closest lists
    struct MockTransport {
        /// For each target node, what nodes they know about
        responses: TokioRwLock<HashMap<NodeId, Vec<NodeInfo>>>,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                responses: TokioRwLock::new(HashMap::new()),
            }
        }

        async fn add_response(&self, node_id: NodeId, peers: Vec<NodeInfo>) {
            self.responses.write().await.insert(node_id, peers);
        }
    }

    #[async_trait::async_trait]
    impl KadTransport for MockTransport {
        async fn send_request(
            &self,
            target: &NodeInfo,
            request: KadRequest,
        ) -> Result<KadResponse, KadError> {
            let responses = self.responses.read().await;
            let closest = responses.get(&target.id).cloned().unwrap_or_default();

            match request {
                KadRequest::FindNode { .. } => Ok(KadResponse::FindNodeResult {
                    responder: target.clone(),
                    closest,
                }),
                KadRequest::Ping { .. } => Ok(KadResponse::Pong {
                    responder: target.clone(),
                }),
                _ => Ok(KadResponse::FindNodeResult {
                    responder: target.clone(),
                    closest: vec![],
                }),
            }
        }
    }

    #[tokio::test]
    async fn test_refresh_stale_buckets() {
        tokio::time::pause();

        let local_id = NodeId::from_bytes([0u8; 20]);
        let local_info = NodeInfo::with_dummy_addr(local_id);
        let table = Arc::new(RwLock::new(RoutingTable::new(local_id, 20)));

        // Add a known peer so the table isn't empty
        let peer = NodeInfo::with_dummy_addr(random_id_in_bucket(&local_id, 100));
        table.write().await.update(peer.clone());

        let transport = Arc::new(MockTransport::new());
        // The peer responds with an empty list
        transport.add_response(peer.id, vec![]).await;

        let config = LookupConfig {
            k: 5,
            alpha: 3,
            timeout: Duration::from_secs(5),
        };

        // Initially no buckets are stale
        let refreshed = refresh_stale_buckets(
            &table,
            &local_info,
            transport.clone(),
            &config,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
        assert_eq!(refreshed, 0, "no buckets should be stale initially");

        // Advance time past the refresh interval
        tokio::time::advance(Duration::from_secs(3601)).await;

        // Now refresh should trigger
        let refreshed = refresh_stale_buckets(
            &table,
            &local_info,
            transport,
            &config,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        // Should have refreshed some buckets (at least the ones where we have peers)
        assert!(refreshed > 0, "should have refreshed some buckets");
    }
}
