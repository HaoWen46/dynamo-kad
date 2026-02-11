//! In-memory table for fast key lookups.
//!
//! Stores all versions (siblings) per key, matching Dynamo semantics
//! where concurrent writes produce multiple versions.

use crate::record::StorageRecord;
use std::collections::HashMap;

/// In-memory key-value store. Each key maps to one or more versioned records
/// (siblings from concurrent writes).
#[derive(Debug)]
pub struct Memtable {
    /// key -> list of versioned records (siblings).
    data: HashMap<String, Vec<StorageRecord>>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Get all versions for a key. Returns empty vec if not found.
    pub fn get(&self, key: &str) -> Vec<StorageRecord> {
        self.data.get(key).cloned().unwrap_or_default()
    }

    /// Put a record. If it dominates existing versions (by vclock), replace them.
    /// If it's concurrent, add as a sibling. If it's dominated, discard it.
    pub fn put(&mut self, record: StorageRecord) {
        let versions = self.data.entry(record.key.clone()).or_default();

        if versions.is_empty() {
            versions.push(record);
            return;
        }

        // Check if new record dominates, is dominated by, or is concurrent
        // with existing versions.
        let mut dominated_indices = Vec::new();
        let mut is_dominated = false;

        for (i, existing) in versions.iter().enumerate() {
            match vclock_compare(&record.vclock, &existing.vclock) {
                VClockOrder::Dominates => {
                    dominated_indices.push(i);
                }
                VClockOrder::DominatedBy => {
                    is_dominated = true;
                    break;
                }
                VClockOrder::Equal => {
                    // Same vclock — replace (idempotent write)
                    dominated_indices.push(i);
                }
                VClockOrder::Concurrent => {
                    // Keep both as siblings
                }
            }
        }

        if is_dominated {
            return; // Discard the new record
        }

        // Remove dominated versions (in reverse order to preserve indices)
        for i in dominated_indices.into_iter().rev() {
            versions.remove(i);
        }

        versions.push(record);
    }

    /// Remove all versions for a key.
    pub fn remove(&mut self, key: &str) -> Vec<StorageRecord> {
        self.data.remove(key).unwrap_or_default()
    }

    /// Number of distinct keys.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Total number of versions across all keys.
    pub fn total_versions(&self) -> usize {
        self.data.values().map(Vec::len).sum()
    }

    /// Return all keys in the memtable.
    pub fn keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Return all keys matching a given prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Load a record directly (used during WAL replay). Does not do
    /// vclock-based dedup — assumes records arrive in WAL order and
    /// the latest write wins for the same key.
    pub fn load_from_wal(&mut self, record: StorageRecord) {
        // A tombstone with empty vclock is a "hard remove" (used by engine.remove()).
        // KV-layer tombstones always have a non-empty vclock.
        if record.tombstone && record.vclock.is_empty() {
            self.data.remove(&record.key);
            return;
        }
        // During replay, each WAL entry is the complete state at write time.
        // We simply overwrite: the last WAL entry for a key is the truth.
        self.data.insert(record.key.clone(), vec![record]);
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Vector clock comparison
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum VClockOrder {
    /// a dominates b (a >= b on all entries, a > b on at least one).
    Dominates,
    /// b dominates a.
    DominatedBy,
    /// Identical clocks.
    Equal,
    /// Neither dominates — concurrent writes.
    Concurrent,
}

/// Compare two vector clocks.
pub fn vclock_compare(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> VClockOrder {
    let mut a_gte_b = true; // a[k] >= b[k] for all k
    let mut b_gte_a = true; // b[k] >= a[k] for all k

    // Check all keys in a
    for (k, &av) in a {
        let bv = b.get(k).copied().unwrap_or(0);
        if av < bv {
            a_gte_b = false;
        }
        if bv < av {
            b_gte_a = false;
        }
    }

    // Check keys in b that are not in a
    for (k, &bv) in b {
        if !a.contains_key(k) {
            let av = 0u64;
            if av < bv {
                a_gte_b = false;
            }
            if bv < av {
                b_gte_a = false;
            }
        }
    }

    match (a_gte_b, b_gte_a) {
        (true, true) => VClockOrder::Equal,
        (true, false) => VClockOrder::Dominates,
        (false, true) => VClockOrder::DominatedBy,
        (false, false) => VClockOrder::Concurrent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(key: &str, value: &str, vclock: HashMap<String, u64>) -> StorageRecord {
        StorageRecord::new(key.to_string(), value.as_bytes().to_vec(), vclock)
    }

    fn vc(entries: &[(&str, u64)]) -> HashMap<String, u64> {
        entries.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn test_vclock_compare_equal() {
        let a = vc(&[("n1", 1), ("n2", 2)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(vclock_compare(&a, &b), VClockOrder::Equal);
    }

    #[test]
    fn test_vclock_compare_dominates() {
        let a = vc(&[("n1", 2), ("n2", 2)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(vclock_compare(&a, &b), VClockOrder::Dominates);
    }

    #[test]
    fn test_vclock_compare_dominated() {
        let a = vc(&[("n1", 1), ("n2", 2)]);
        let b = vc(&[("n1", 2), ("n2", 3)]);
        assert_eq!(vclock_compare(&a, &b), VClockOrder::DominatedBy);
    }

    #[test]
    fn test_vclock_compare_concurrent() {
        let a = vc(&[("n1", 2), ("n2", 1)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(vclock_compare(&a, &b), VClockOrder::Concurrent);
    }

    #[test]
    fn test_vclock_compare_missing_keys() {
        let a = vc(&[("n1", 1)]);
        let b = vc(&[("n2", 1)]);
        assert_eq!(vclock_compare(&a, &b), VClockOrder::Concurrent);
    }

    #[test]
    fn test_memtable_put_get() {
        let mut mt = Memtable::new();
        let record = make_record("k1", "v1", vc(&[("n1", 1)]));
        mt.put(record);

        let versions = mt.get("k1");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].value, b"v1");
    }

    #[test]
    fn test_memtable_dominating_write_replaces() {
        let mut mt = Memtable::new();
        mt.put(make_record("k1", "v1", vc(&[("n1", 1)])));
        mt.put(make_record("k1", "v2", vc(&[("n1", 2)])));

        let versions = mt.get("k1");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].value, b"v2");
    }

    #[test]
    fn test_memtable_dominated_write_discarded() {
        let mut mt = Memtable::new();
        mt.put(make_record("k1", "v2", vc(&[("n1", 2)])));
        mt.put(make_record("k1", "v1", vc(&[("n1", 1)]))); // older

        let versions = mt.get("k1");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].value, b"v2");
    }

    #[test]
    fn test_memtable_concurrent_writes_create_siblings() {
        let mut mt = Memtable::new();
        mt.put(make_record("k1", "v-from-a", vc(&[("a", 1)])));
        mt.put(make_record("k1", "v-from-b", vc(&[("b", 1)])));

        let versions = mt.get("k1");
        assert_eq!(
            versions.len(),
            2,
            "concurrent writes should produce 2 siblings"
        );
    }

    #[test]
    fn test_memtable_resolve_siblings() {
        let mut mt = Memtable::new();
        // Two concurrent writes
        mt.put(make_record("k1", "v-from-a", vc(&[("a", 1)])));
        mt.put(make_record("k1", "v-from-b", vc(&[("b", 1)])));
        assert_eq!(mt.get("k1").len(), 2);

        // Now a write that dominates both
        mt.put(make_record(
            "k1",
            "v-merged",
            vc(&[("a", 1), ("b", 1), ("c", 1)]),
        ));
        let versions = mt.get("k1");
        assert_eq!(
            versions.len(),
            1,
            "dominating write should collapse siblings"
        );
        assert_eq!(versions[0].value, b"v-merged");
    }

    #[test]
    fn test_memtable_not_found() {
        let mt = Memtable::new();
        assert!(mt.get("nonexistent").is_empty());
    }

    #[test]
    fn test_memtable_keys() {
        let mut mt = Memtable::new();
        mt.put(make_record("k1", "v1", vc(&[("n1", 1)])));
        mt.put(make_record("k2", "v2", vc(&[("n1", 1)])));
        mt.put(make_record("k3", "v3", vc(&[("n1", 1)])));

        let mut keys = mt.keys();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2", "k3"]);
    }

    #[test]
    fn test_memtable_keys_with_prefix() {
        let mut mt = Memtable::new();
        mt.put(make_record("user:1", "alice", vc(&[("n1", 1)])));
        mt.put(make_record("user:2", "bob", vc(&[("n1", 1)])));
        mt.put(make_record("order:1", "item", vc(&[("n1", 1)])));

        let mut user_keys = mt.keys_with_prefix("user:");
        user_keys.sort();
        assert_eq!(user_keys, vec!["user:1", "user:2"]);
        assert_eq!(mt.keys_with_prefix("order:").len(), 1);
        assert_eq!(mt.keys_with_prefix("nope:").len(), 0);
    }

    #[test]
    fn test_memtable_load_from_wal_hard_remove() {
        let mut mt = Memtable::new();
        mt.load_from_wal(make_record("k1", "v1", vc(&[("n1", 1)])));
        assert_eq!(mt.len(), 1);

        // Hard remove: tombstone with empty vclock
        mt.load_from_wal(StorageRecord::tombstone("k1".to_string(), HashMap::new()));
        assert_eq!(mt.len(), 0);
        assert!(mt.get("k1").is_empty());
    }
}
