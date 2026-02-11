//! In-memory record store for the Kademlia DHT.
//!
//! Stores key-value records locally. This is the Kademlia-level store
//! (not the Dynamo KV layer).

use dynamo_common::{KadError, NodeId};
use std::collections::HashMap;
use tokio::time::Instant;

/// A record stored in the local DHT store.
#[derive(Debug, Clone)]
pub struct Record {
    /// The key (mapped to the 160-bit ID space).
    pub key: NodeId,
    /// The value bytes.
    pub value: Vec<u8>,
    /// The node that originally published this record.
    pub publisher: NodeId,
    /// Optional expiry time.
    pub expires: Option<Instant>,
    /// When the record was stored locally.
    pub stored_at: Instant,
}

impl Record {
    /// Whether this record has expired.
    pub fn is_expired(&self) -> bool {
        self.expires.is_some_and(|exp| Instant::now() >= exp)
    }
}

/// In-memory record store with a maximum capacity.
#[derive(Debug)]
pub struct RecordStore {
    records: HashMap<NodeId, Record>,
    max_records: usize,
}

impl RecordStore {
    /// Create a new store with the given capacity limit.
    pub fn new(max_records: usize) -> Self {
        Self {
            records: HashMap::new(),
            max_records,
        }
    }

    /// Get a record by key. Returns `None` if not found or expired.
    pub fn get(&self, key: &NodeId) -> Option<&Record> {
        self.records.get(key).filter(|r| !r.is_expired())
    }

    /// Store a record. Overwrites any existing record with the same key.
    /// Returns an error if the store is full and the key is new.
    pub fn put(&mut self, record: Record) -> Result<(), KadError> {
        if !self.records.contains_key(&record.key) && self.records.len() >= self.max_records {
            return Err(KadError::StoreFull(self.max_records));
        }
        self.records.insert(record.key, record);
        Ok(())
    }

    /// Remove a record by key.
    pub fn remove(&mut self, key: &NodeId) -> Option<Record> {
        self.records.remove(key)
    }

    /// Remove all expired records. Returns the number removed.
    pub fn remove_expired(&mut self) -> usize {
        let before = self.records.len();
        self.records.retain(|_, r| !r.is_expired());
        before - self.records.len()
    }

    /// Number of records currently stored.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_record(key: NodeId) -> Record {
        Record {
            key,
            value: b"hello".to_vec(),
            publisher: NodeId::random(),
            expires: None,
            stored_at: Instant::now(),
        }
    }

    #[test]
    fn test_store_put_get() {
        let mut store = RecordStore::new(100);
        let key = NodeId::random();
        let record = make_record(key);
        store.put(record).unwrap();

        let retrieved = store.get(&key).unwrap();
        assert_eq!(retrieved.key, key);
        assert_eq!(retrieved.value, b"hello");
    }

    #[test]
    fn test_store_overwrite() {
        let mut store = RecordStore::new(100);
        let key = NodeId::random();
        store.put(make_record(key)).unwrap();
        assert_eq!(store.len(), 1);

        // Overwrite with new value
        let mut record = make_record(key);
        record.value = b"world".to_vec();
        store.put(record).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(&key).unwrap().value, b"world");
    }

    #[test]
    fn test_store_remove() {
        let mut store = RecordStore::new(100);
        let key = NodeId::random();
        store.put(make_record(key)).unwrap();
        assert_eq!(store.len(), 1);

        store.remove(&key);
        assert_eq!(store.len(), 0);
        assert!(store.get(&key).is_none());
    }

    #[test]
    fn test_store_capacity() {
        let mut store = RecordStore::new(2);
        store.put(make_record(NodeId::random())).unwrap();
        store.put(make_record(NodeId::random())).unwrap();

        // Third insert should fail
        let result = store.put(make_record(NodeId::random()));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_expiry() {
        tokio::time::pause();

        let mut store = RecordStore::new(100);
        let key = NodeId::random();
        let record = Record {
            key,
            value: b"ephemeral".to_vec(),
            publisher: NodeId::random(),
            expires: Some(Instant::now() + Duration::from_secs(60)),
            stored_at: Instant::now(),
        };
        store.put(record).unwrap();

        // Not expired yet
        assert!(store.get(&key).is_some());

        // Advance past expiry
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(store.get(&key).is_none(), "record should be expired");

        // Cleanup
        let removed = store.remove_expired();
        assert_eq!(removed, 1);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_store_not_found() {
        let store = RecordStore::new(100);
        assert!(store.get(&NodeId::random()).is_none());
    }
}
