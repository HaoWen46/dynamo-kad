//! Durable store for hinted-handoff hints.
//!
//! Each hint represents a write that could not be delivered to its intended
//! replica node. Hints are persisted via a separate `StorageEngine` instance
//! and delivered when the target node becomes reachable again.

use crate::coordinator::VersionedValue;
use crate::vclock::VClock;
use dynamo_common::NodeId;
use dynamo_storage::engine::StorageEngine;
use dynamo_storage::record::StorageRecord;
use dynamo_storage::wal::FsyncPolicy;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum HintStoreError {
    #[error("storage error: {0}")]
    Storage(#[from] dynamo_storage::engine::StorageError),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// A hint: a write that needs to be delivered to a specific target node.
#[derive(Debug, Clone)]
pub struct Hint {
    pub target_node_id: NodeId,
    pub key: String,
    pub versioned: VersionedValue,
    pub created_at_ms: u64,
}

/// Serializable payload stored inside the hint engine.
#[derive(serde::Serialize, serde::Deserialize)]
struct HintPayload {
    value: Vec<u8>,
    vclock: std::collections::HashMap<String, u64>,
    tombstone: bool,
    created_at_ms: u64,
}

/// Durable store for hinted-handoff hints.
///
/// Backed by a `StorageEngine` at `<data_dir>/hints/`. Each hint is keyed
/// as `"<target_node_id_hex>:<original_key>"`.
#[derive(Debug)]
pub struct HintStore {
    engine: StorageEngine,
}

impl HintStore {
    /// Open or create a hint store at the given directory.
    pub fn open(hint_dir: &Path) -> Result<Self, HintStoreError> {
        std::fs::create_dir_all(hint_dir)?;
        let engine = StorageEngine::open(hint_dir, FsyncPolicy::Batch)?;
        Ok(Self { engine })
    }

    /// Store a hint for a target node.
    pub fn store_hint(
        &mut self,
        target: &NodeId,
        key: &str,
        versioned: &VersionedValue,
    ) -> Result<(), HintStoreError> {
        let compound_key = hint_key(target, key);

        let payload = HintPayload {
            value: versioned.value.clone(),
            vclock: versioned.vclock.clone().into_map(),
            tombstone: versioned.tombstone,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        let payload_bytes = serde_json::to_vec(&payload)?;

        let record = StorageRecord::new(
            compound_key,
            payload_bytes,
            versioned.vclock.clone().into_map(),
        );

        self.engine.put(record)?;
        Ok(())
    }

    /// Get all hints destined for a specific target node.
    pub fn hints_for_node(&self, target: &NodeId) -> Vec<Hint> {
        let prefix = format!("{}:", hex::encode(target.as_bytes()));
        let keys = self.engine.keys_with_prefix(&prefix);

        let mut hints = Vec::new();
        for compound_key in keys {
            let records = self.engine.get(&compound_key);
            for record in records {
                if let Some(hint) = decode_hint(target, &compound_key, &prefix, &record) {
                    hints.push(hint);
                }
            }
        }
        hints
    }

    /// Delete a specific hint after successful delivery.
    pub fn delete_hint(&mut self, target: &NodeId, key: &str) -> Result<(), HintStoreError> {
        let compound_key = hint_key(target, key);
        self.engine.remove(&compound_key)?;
        Ok(())
    }

    /// Get all distinct target node IDs that have pending hints.
    pub fn all_target_nodes(&self) -> HashSet<NodeId> {
        let all_keys = self.engine.keys();
        let mut targets = HashSet::new();

        for compound_key in all_keys {
            if let Some(target_id) = parse_target_from_key(&compound_key) {
                targets.insert(target_id);
            }
        }
        targets
    }

    /// Total number of pending hints.
    pub fn hint_count(&self) -> usize {
        self.engine.key_count()
    }
}

// ---------------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------------

/// Encode a compound key: `"<target_hex>:<original_key>"`.
fn hint_key(target: &NodeId, key: &str) -> String {
    format!("{}:{}", hex::encode(target.as_bytes()), key)
}

/// Parse the target NodeId from a compound hint key.
fn parse_target_from_key(compound_key: &str) -> Option<NodeId> {
    // Node ID hex is always 40 chars (20 bytes * 2)
    if compound_key.len() < 42 {
        // At least 40 hex chars + ':' + 1 char key
        return None;
    }
    let hex_str = &compound_key[..40];
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 20 {
        return None;
    }
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&bytes);
    Some(NodeId::from_bytes(arr))
}

/// Decode a hint from a storage record.
fn decode_hint(
    target: &NodeId,
    _compound_key: &str,
    prefix: &str,
    record: &StorageRecord,
) -> Option<Hint> {
    let payload: HintPayload = serde_json::from_slice(&record.value).ok()?;
    // The original key is compound_key minus the prefix
    let original_key = record.key.strip_prefix(prefix)?;

    Some(Hint {
        target_node_id: *target,
        key: original_key.to_string(),
        versioned: VersionedValue {
            value: payload.value,
            vclock: VClock::from_map(payload.vclock),
            tombstone: payload.tombstone,
        },
        created_at_ms: payload.created_at_ms,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_versioned(value: &[u8], entries: &[(&str, u64)]) -> VersionedValue {
        VersionedValue {
            value: value.to_vec(),
            vclock: VClock::from_map(entries.iter().map(|(k, v)| (k.to_string(), *v)).collect()),
            tombstone: false,
        }
    }

    #[test]
    fn test_store_and_retrieve() {
        let dir = TempDir::new().unwrap();
        let mut hs = HintStore::open(dir.path()).unwrap();

        let target = NodeId::from_sha1(b"node-A");
        let vv = make_versioned(b"hello", &[("n1", 1)]);

        hs.store_hint(&target, "mykey", &vv).unwrap();

        let hints = hs.hints_for_node(&target);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].key, "mykey");
        assert_eq!(hints[0].versioned.value, b"hello");
        assert_eq!(hints[0].target_node_id, target);
    }

    #[test]
    fn test_hints_for_node() {
        let dir = TempDir::new().unwrap();
        let mut hs = HintStore::open(dir.path()).unwrap();

        let node_a = NodeId::from_sha1(b"node-A");
        let node_b = NodeId::from_sha1(b"node-B");

        hs.store_hint(&node_a, "k1", &make_versioned(b"v1", &[("n1", 1)]))
            .unwrap();
        hs.store_hint(&node_a, "k2", &make_versioned(b"v2", &[("n1", 2)]))
            .unwrap();
        hs.store_hint(&node_b, "k3", &make_versioned(b"v3", &[("n1", 3)]))
            .unwrap();

        assert_eq!(hs.hints_for_node(&node_a).len(), 2);
        assert_eq!(hs.hints_for_node(&node_b).len(), 1);
    }

    #[test]
    fn test_delete_hint() {
        let dir = TempDir::new().unwrap();
        let mut hs = HintStore::open(dir.path()).unwrap();

        let target = NodeId::from_sha1(b"node-A");
        hs.store_hint(&target, "k1", &make_versioned(b"v1", &[("n1", 1)]))
            .unwrap();
        hs.store_hint(&target, "k2", &make_versioned(b"v2", &[("n1", 2)]))
            .unwrap();
        assert_eq!(hs.hint_count(), 2);

        hs.delete_hint(&target, "k1").unwrap();
        assert_eq!(hs.hint_count(), 1);
        assert_eq!(hs.hints_for_node(&target).len(), 1);
        assert_eq!(hs.hints_for_node(&target)[0].key, "k2");
    }

    #[test]
    fn test_all_target_nodes() {
        let dir = TempDir::new().unwrap();
        let mut hs = HintStore::open(dir.path()).unwrap();

        let node_a = NodeId::from_sha1(b"node-A");
        let node_b = NodeId::from_sha1(b"node-B");
        let node_c = NodeId::from_sha1(b"node-C");

        hs.store_hint(&node_a, "k1", &make_versioned(b"v1", &[("n1", 1)]))
            .unwrap();
        hs.store_hint(&node_b, "k2", &make_versioned(b"v2", &[("n1", 1)]))
            .unwrap();
        hs.store_hint(&node_c, "k3", &make_versioned(b"v3", &[("n1", 1)]))
            .unwrap();

        let targets = hs.all_target_nodes();
        assert_eq!(targets.len(), 3);
        assert!(targets.contains(&node_a));
        assert!(targets.contains(&node_b));
        assert!(targets.contains(&node_c));
    }

    #[test]
    fn test_crash_recovery() {
        let dir = TempDir::new().unwrap();
        let target = NodeId::from_sha1(b"node-A");

        {
            let mut hs = HintStore::open(dir.path()).unwrap();
            hs.store_hint(&target, "k1", &make_versioned(b"v1", &[("n1", 1)]))
                .unwrap();
            hs.store_hint(&target, "k2", &make_versioned(b"v2", &[("n1", 2)]))
                .unwrap();
            // Drop without explicit close — simulates crash
        }

        // Re-open — hints should survive WAL replay
        {
            let hs = HintStore::open(dir.path()).unwrap();
            assert_eq!(hs.hint_count(), 2);
            let hints = hs.hints_for_node(&target);
            assert_eq!(hints.len(), 2);
        }
    }

    #[test]
    fn test_hint_count() {
        let dir = TempDir::new().unwrap();
        let mut hs = HintStore::open(dir.path()).unwrap();
        assert_eq!(hs.hint_count(), 0);

        let target = NodeId::from_sha1(b"node-A");
        hs.store_hint(&target, "k1", &make_versioned(b"v1", &[("n1", 1)]))
            .unwrap();
        assert_eq!(hs.hint_count(), 1);

        hs.store_hint(&target, "k2", &make_versioned(b"v2", &[("n1", 2)]))
            .unwrap();
        assert_eq!(hs.hint_count(), 2);

        hs.delete_hint(&target, "k1").unwrap();
        assert_eq!(hs.hint_count(), 1);
    }
}
