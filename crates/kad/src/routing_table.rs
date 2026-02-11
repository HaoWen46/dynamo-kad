//! Kademlia routing table with k-buckets.
//!
//! The routing table organises known peers into 160 buckets, one per
//! bit-distance from the local node. Each bucket holds at most `k` entries
//! in LRU order (head = least-recently seen, tail = most-recently seen).

use crate::key::{Distance, NodeId, ID_BITS};
use crate::node_info::NodeInfo;
use tokio::time::{Duration, Instant};

/// Default k-bucket capacity.
pub const DEFAULT_K: usize = 20;

// ---------------------------------------------------------------------------
// UpdateResult
// ---------------------------------------------------------------------------

/// Outcome of inserting/updating a node in the routing table.
#[derive(Debug)]
pub enum UpdateResult {
    /// Node was added to its bucket (had room).
    Added,
    /// Node was already present; its position was moved to tail (most-recently seen).
    Updated,
    /// The bucket is full. The caller should ping `to_ping` (the least-recently
    /// seen node). Then call `apply_ping_result` with the outcome.
    Pending {
        to_ping: NodeInfo,
        pending: NodeInfo,
    },
    /// The node was not added (e.g., tried to add self).
    Ignored,
}

// ---------------------------------------------------------------------------
// KBucket
// ---------------------------------------------------------------------------

/// A single k-bucket: a bounded list of peers ordered by recency.
#[derive(Debug)]
struct KBucket {
    /// Entries ordered: index 0 = least-recently seen, last = most-recently seen.
    entries: Vec<NodeInfo>,
    capacity: usize,
    last_refreshed: Instant,
}

impl KBucket {
    fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            capacity,
            last_refreshed: Instant::now(),
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    /// Find the position of a node by ID.
    fn position(&self, id: &NodeId) -> Option<usize> {
        self.entries.iter().position(|e| e.id == *id)
    }

    /// Move an existing entry to the tail (most-recently seen) and touch it.
    fn move_to_tail(&mut self, idx: usize) {
        let mut entry = self.entries.remove(idx);
        entry.touch();
        self.entries.push(entry);
    }

    /// Push a new entry to the tail.
    fn push(&mut self, info: NodeInfo) {
        debug_assert!(!self.is_full());
        self.entries.push(info);
    }

    /// Remove and return the head (least-recently seen).
    #[allow(dead_code)] // useful for future eviction logic
    fn remove_head(&mut self) -> NodeInfo {
        self.entries.remove(0)
    }

    /// Remove a specific node by ID.
    fn remove(&mut self, id: &NodeId) -> Option<NodeInfo> {
        if let Some(idx) = self.position(id) {
            Some(self.entries.remove(idx))
        } else {
            None
        }
    }

    /// Get the head (least-recently seen) entry.
    fn head(&self) -> Option<&NodeInfo> {
        self.entries.first()
    }

    /// Return all entries as a slice (LRU order).
    fn entries(&self) -> &[NodeInfo] {
        &self.entries
    }
}

// ---------------------------------------------------------------------------
// RoutingTable
// ---------------------------------------------------------------------------

/// The Kademlia routing table.
///
/// Contains 160 k-buckets indexed by XOR distance prefix length.
/// Bucket `i` stores nodes whose XOR distance from the local node
/// has its highest set bit at position `i` (i.e., `bucket_index` returns `i`).
#[derive(Debug)]
pub struct RoutingTable {
    local_id: NodeId,
    buckets: Vec<KBucket>,
    k: usize,
}

impl RoutingTable {
    /// Create a new routing table for the given local node ID.
    pub fn new(local_id: NodeId, k: usize) -> Self {
        let buckets = (0..ID_BITS).map(|_| KBucket::new(k)).collect();
        Self {
            local_id,
            buckets,
            k,
        }
    }

    /// The local node's ID.
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// The k parameter (bucket capacity).
    pub fn k(&self) -> usize {
        self.k
    }

    /// Determine which bucket a node belongs to.
    /// Returns `None` if `node_id == self.local_id`.
    pub fn bucket_index(&self, node_id: &NodeId) -> Option<usize> {
        self.local_id.bucket_index(node_id)
    }

    /// Insert or update a node in the routing table.
    ///
    /// - If the node is us, returns `Ignored`.
    /// - If the node is already known, moves it to the tail of its bucket → `Updated`.
    /// - If the bucket has room, adds the node → `Added`.
    /// - If the bucket is full, returns `Pending` with the LRS node to ping.
    pub fn update(&mut self, info: NodeInfo) -> UpdateResult {
        let idx = match self.bucket_index(&info.id) {
            Some(idx) => idx,
            None => return UpdateResult::Ignored, // self
        };

        let bucket = &mut self.buckets[idx];

        // Already in the bucket? Move to tail.
        if let Some(pos) = bucket.position(&info.id) {
            bucket.move_to_tail(pos);
            return UpdateResult::Updated;
        }

        // Room in the bucket? Add.
        if !bucket.is_full() {
            bucket.push(info);
            return UpdateResult::Added;
        }

        // Bucket full: return the LRS node for the caller to ping.
        let to_ping = bucket.head().unwrap().clone();
        UpdateResult::Pending {
            to_ping,
            pending: info,
        }
    }

    /// Resolve a pending insertion after the caller has pinged the LRS node.
    ///
    /// - If `alive` is true: the LRS node responded, so move it to tail and
    ///   discard the pending node.
    /// - If `alive` is false: evict the LRS node and add the pending node.
    pub fn apply_ping_result(
        &mut self,
        pinged_id: &NodeId,
        alive: bool,
        pending: NodeInfo,
    ) -> UpdateResult {
        let idx = match self.bucket_index(pinged_id) {
            Some(idx) => idx,
            None => return UpdateResult::Ignored,
        };

        let bucket = &mut self.buckets[idx];

        if alive {
            // LRS is alive: move it to tail, discard the pending node.
            if let Some(pos) = bucket.position(pinged_id) {
                bucket.move_to_tail(pos);
            }
            UpdateResult::Updated
        } else {
            // LRS is dead: evict it, add the pending node.
            bucket.remove(pinged_id);
            bucket.push(pending);
            UpdateResult::Added
        }
    }

    /// Return the `count` closest known nodes to `target`, sorted by XOR distance.
    pub fn closest_nodes(&self, target: &NodeId, count: usize) -> Vec<NodeInfo> {
        // Collect all nodes with their distances, sort, and take the closest `count`.
        // For typical routing tables (a few hundred nodes max), this is fast enough.
        let mut all: Vec<(Distance, NodeInfo)> = self
            .buckets
            .iter()
            .flat_map(|b| b.entries().iter())
            .map(|entry| (target.distance(&entry.id), entry.clone()))
            .collect();

        all.sort_by_key(|(d, _)| *d);
        all.truncate(count);
        all.into_iter().map(|(_, n)| n).collect()
    }

    /// Remove a node from the routing table.
    pub fn remove(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        let idx = self.bucket_index(node_id)?;
        self.buckets[idx].remove(node_id)
    }

    /// Look up a specific node by ID.
    pub fn get(&self, node_id: &NodeId) -> Option<&NodeInfo> {
        let idx = self.bucket_index(node_id)?;
        let bucket = &self.buckets[idx];
        bucket.entries().iter().find(|e| e.id == *node_id)
    }

    /// Returns indices of buckets that haven't been refreshed within `interval`.
    pub fn buckets_needing_refresh(&self, interval: Duration) -> Vec<usize> {
        let now = Instant::now();
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, b)| {
                // Only refresh non-empty buckets or buckets that exist
                // but haven't been refreshed recently.
                now.duration_since(b.last_refreshed) >= interval
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Mark a bucket as freshly refreshed.
    pub fn mark_refreshed(&mut self, bucket_index: usize) {
        if bucket_index < self.buckets.len() {
            self.buckets[bucket_index].last_refreshed = Instant::now();
        }
    }

    /// Iterate over all known nodes in the table.
    pub fn all_nodes(&self) -> Vec<NodeInfo> {
        self.buckets
            .iter()
            .flat_map(|b| b.entries().iter().cloned())
            .collect()
    }

    /// Total number of known nodes.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(KBucket::len).sum()
    }

    /// Whether the table has no known nodes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dynamo_common::random_id_in_bucket;

    fn make_table() -> RoutingTable {
        let local = NodeId::from_bytes([0u8; 20]);
        RoutingTable::new(local, DEFAULT_K)
    }

    fn node_in_bucket(local: &NodeId, bucket: usize) -> NodeInfo {
        let id = random_id_in_bucket(local, bucket);
        NodeInfo::with_dummy_addr(id)
    }

    #[test]
    fn test_insert_single_node() {
        let mut table = make_table();
        let node = node_in_bucket(&table.local_id(), 100);
        match table.update(node.clone()) {
            UpdateResult::Added => {}
            other => panic!("expected Added, got {:?}", other),
        }
        assert_eq!(table.len(), 1);
        assert!(table.get(&node.id).is_some());
    }

    #[test]
    fn test_insert_self_ignored() {
        let mut table = make_table();
        let self_node = NodeInfo::with_dummy_addr(table.local_id());
        match table.update(self_node) {
            UpdateResult::Ignored => {}
            other => panic!("expected Ignored, got {:?}", other),
        }
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_insert_updates_position() {
        let mut table = make_table();
        let local = table.local_id();

        // Add two nodes to the same bucket
        let n1 = node_in_bucket(&local, 100);
        let n2 = node_in_bucket(&local, 100);
        table.update(n1.clone());
        table.update(n2.clone());

        // Re-insert n1: should move to tail
        match table.update(n1.clone()) {
            UpdateResult::Updated => {}
            other => panic!("expected Updated, got {:?}", other),
        }

        // n1 should now be the tail (most-recently seen) in its bucket
        let bucket_idx = table.bucket_index(&n1.id).unwrap();
        let entries = &table.buckets[bucket_idx].entries;
        assert_eq!(entries.last().unwrap().id, n1.id);
    }

    #[test]
    fn test_bucket_fills_to_k() {
        let mut table = RoutingTable::new(NodeId::from_bytes([0u8; 20]), 3);
        let local = table.local_id();

        for _ in 0..3 {
            let node = node_in_bucket(&local, 50);
            match table.update(node) {
                UpdateResult::Added => {}
                other => panic!("expected Added, got {:?}", other),
            }
        }
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn test_bucket_overflow_returns_pending() {
        let mut table = RoutingTable::new(NodeId::from_bytes([0u8; 20]), 3);
        let local = table.local_id();

        // Fill bucket
        for _ in 0..3 {
            table.update(node_in_bucket(&local, 50));
        }

        // Overflow
        let extra = node_in_bucket(&local, 50);
        match table.update(extra) {
            UpdateResult::Pending {
                to_ping,
                pending: _,
            } => {
                // to_ping should be the first node we inserted (LRS)
                assert!(table.get(&to_ping.id).is_some());
            }
            other => panic!("expected Pending, got {:?}", other),
        }
    }

    #[test]
    fn test_apply_ping_evicts_dead_node() {
        let mut table = RoutingTable::new(NodeId::from_bytes([0u8; 20]), 3);
        let local = table.local_id();

        let mut first_id = NodeId::ZERO;
        for i in 0..3 {
            let node = node_in_bucket(&local, 50);
            if i == 0 {
                first_id = node.id;
            }
            table.update(node);
        }

        let extra = node_in_bucket(&local, 50);
        let extra_id = extra.id;
        match table.update(extra.clone()) {
            UpdateResult::Pending { to_ping, pending } => {
                assert_eq!(to_ping.id, first_id);
                // LRS didn't respond -> evict
                table.apply_ping_result(&to_ping.id, false, pending);
            }
            _ => panic!("expected Pending"),
        }

        // first_id should be gone, extra should be present
        assert!(table.get(&first_id).is_none());
        assert!(table.get(&extra_id).is_some());
    }

    #[test]
    fn test_apply_ping_keeps_alive_node() {
        let mut table = RoutingTable::new(NodeId::from_bytes([0u8; 20]), 3);
        let local = table.local_id();

        let mut first_id = NodeId::ZERO;
        for i in 0..3 {
            let node = node_in_bucket(&local, 50);
            if i == 0 {
                first_id = node.id;
            }
            table.update(node);
        }

        let extra = node_in_bucket(&local, 50);
        let extra_id = extra.id;
        match table.update(extra.clone()) {
            UpdateResult::Pending { to_ping, pending } => {
                assert_eq!(to_ping.id, first_id);
                // LRS responded -> keep it
                table.apply_ping_result(&to_ping.id, true, pending);
            }
            _ => panic!("expected Pending"),
        }

        // first_id should still be present, extra should NOT be present
        assert!(table.get(&first_id).is_some());
        assert!(table.get(&extra_id).is_none());
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn test_closest_nodes_basic() {
        let mut table = make_table();
        let local = table.local_id();

        // Insert nodes at various buckets
        let mut nodes = Vec::new();
        for bucket in [10, 50, 100, 150] {
            let node = node_in_bucket(&local, bucket);
            nodes.push(node.clone());
            table.update(node);
        }

        // Find closest to a target
        let target = random_id_in_bucket(&local, 50);
        let closest = table.closest_nodes(&target, 4);
        assert_eq!(closest.len(), 4);

        // Verify they are sorted by distance to target
        for i in 1..closest.len() {
            let d_prev = target.distance(&closest[i - 1].id);
            let d_curr = target.distance(&closest[i].id);
            assert!(d_prev <= d_curr, "closest_nodes not sorted by distance");
        }
    }

    #[test]
    fn test_closest_nodes_returns_at_most_count() {
        let mut table = make_table();
        let local = table.local_id();

        for _ in 0..50 {
            let bucket = rand::random::<usize>() % ID_BITS;
            let node = node_in_bucket(&local, bucket);
            table.update(node);
        }

        let target = NodeId::random();
        let closest = table.closest_nodes(&target, 10);
        assert!(closest.len() <= 10);
    }

    #[test]
    fn test_closest_nodes_spans_buckets() {
        let mut table = make_table();
        let local = table.local_id();

        // Insert 5 nodes in bucket 100 and 5 in bucket 101
        for _ in 0..5 {
            table.update(node_in_bucket(&local, 100));
            table.update(node_in_bucket(&local, 101));
        }

        let target = random_id_in_bucket(&local, 100);
        let closest = table.closest_nodes(&target, 10);
        assert_eq!(closest.len(), 10);
    }

    #[test]
    fn test_remove_node() {
        let mut table = make_table();
        let node = node_in_bucket(&table.local_id(), 80);
        let id = node.id;
        table.update(node);
        assert_eq!(table.len(), 1);

        table.remove(&id);
        assert_eq!(table.len(), 0);
        assert!(table.get(&id).is_none());
    }

    #[test]
    fn test_bucket_index_assignment() {
        let table = make_table();
        let local = table.local_id();

        for expected_bucket in [0, 1, 50, 100, 159] {
            let id = random_id_in_bucket(&local, expected_bucket);
            let actual = table.bucket_index(&id);
            assert_eq!(actual, Some(expected_bucket));
        }
    }

    #[tokio::test]
    async fn test_buckets_needing_refresh() {
        tokio::time::pause();

        let mut table = make_table();

        // All buckets start as "just refreshed" (Instant::now() when created).
        let stale = table.buckets_needing_refresh(Duration::from_secs(3600));
        assert!(stale.is_empty(), "no buckets should be stale yet");

        // Advance time past refresh interval
        tokio::time::advance(Duration::from_secs(3601)).await;

        let stale = table.buckets_needing_refresh(Duration::from_secs(3600));
        assert_eq!(stale.len(), ID_BITS, "all 160 buckets should be stale");

        // Mark one as refreshed
        table.mark_refreshed(42);
        let stale = table.buckets_needing_refresh(Duration::from_secs(3600));
        assert_eq!(stale.len(), ID_BITS - 1);
        assert!(!stale.contains(&42));
    }

    #[test]
    fn test_all_nodes() {
        let mut table = make_table();
        let local = table.local_id();

        for _ in 0..25 {
            let bucket = rand::random::<usize>() % ID_BITS;
            table.update(node_in_bucket(&local, bucket));
        }

        let all = table.all_nodes();
        assert_eq!(all.len(), table.len());
    }
}
