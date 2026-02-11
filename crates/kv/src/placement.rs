//! Key placement: maps a key string to the N closest nodes.

use dynamo_common::NodeId;
use dynamo_kad::node_info::NodeInfo;
use dynamo_kad::routing_table::RoutingTable;

/// Map a key to its NodeId in the Kademlia space.
pub fn key_to_id(key: &str) -> NodeId {
    NodeId::from_sha1(key.as_bytes())
}

/// Find the N closest nodes to a key, which serve as its replica set.
///
/// The routing table excludes the local node, so `local_info` is injected
/// into the candidate set to ensure the local node participates if it's
/// among the N closest.
pub fn replica_nodes(
    key: &str,
    n: usize,
    routing_table: &RoutingTable,
    local_info: &NodeInfo,
) -> Vec<NodeInfo> {
    let key_id = key_to_id(key);

    // Get candidates from routing table (excludes self)
    let mut candidates = routing_table.closest_nodes(&key_id, n);

    // Include local node as a candidate
    candidates.push(local_info.clone());

    // Sort by distance to key and take N
    candidates.sort_by_key(|c| key_id.distance(&c.id));
    candidates.truncate(n);
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynamo_common::random_id_in_bucket;

    #[test]
    fn test_key_to_id_deterministic() {
        let id1 = key_to_id("hello");
        let id2 = key_to_id("hello");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_key_to_id_different_keys() {
        let id1 = key_to_id("key-a");
        let id2 = key_to_id("key-b");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_replica_nodes() {
        let local = NodeId::from_sha1(b"local");
        let local_info = NodeInfo::with_dummy_addr(local);
        let mut table = RoutingTable::new(local, 20);

        // Add some nodes
        for i in 0..30 {
            let id = random_id_in_bucket(&local, (i * 5) % 160);
            table.update(NodeInfo::with_dummy_addr(id));
        }

        let replicas = replica_nodes("test-key", 3, &table, &local_info);
        assert_eq!(replicas.len(), 3);

        // Verify they are sorted by distance to key
        let key_id = key_to_id("test-key");
        for i in 1..replicas.len() {
            assert!(
                key_id.distance(&replicas[i - 1].id) <= key_id.distance(&replicas[i].id),
                "replicas should be sorted by distance to key"
            );
        }
    }

    #[test]
    fn test_replica_nodes_includes_local() {
        let local = NodeId::from_sha1(b"local");
        let local_info = NodeInfo::with_dummy_addr(local);
        let table = RoutingTable::new(local, 20);

        // With an empty routing table, replicas should just be the local node
        let replicas = replica_nodes("any-key", 3, &table, &local_info);
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].id, local);
    }
}
