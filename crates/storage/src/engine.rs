//! Storage engine: combines WAL + Memtable.
//!
//! All writes go through the WAL first (for durability), then into
//! the in-memory memtable (for fast reads).

use crate::memtable::Memtable;
use crate::record::StorageRecord;
use crate::wal::{FsyncPolicy, Wal, WalError};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// The storage engine. All operations are synchronous (blocking I/O).
/// The async boundary is at the caller (KV layer).
#[derive(Debug)]
pub struct StorageEngine {
    memtable: Memtable,
    wal: Wal,
    #[allow(dead_code)] // will be used for WAL rotation/compaction
    wal_path: PathBuf,
}

impl StorageEngine {
    /// Open or create a storage engine at the given directory.
    pub fn open(data_dir: &Path, fsync: FsyncPolicy) -> Result<Self, StorageError> {
        std::fs::create_dir_all(data_dir)?;
        let wal_path = data_dir.join("wal.log");

        // Replay WAL to rebuild memtable
        let records = Wal::replay(&wal_path)?;
        let mut memtable = Memtable::new();
        for record in records {
            memtable.load_from_wal(record);
        }

        let wal = Wal::open(&wal_path, fsync)?;

        tracing::info!(
            "storage engine opened: {} keys recovered from WAL at {:?}",
            memtable.len(),
            wal_path
        );

        Ok(Self {
            memtable,
            wal,
            wal_path,
        })
    }

    /// Get all versions for a key.
    pub fn get(&self, key: &str) -> Vec<StorageRecord> {
        self.memtable.get(key)
    }

    /// Put a record: writes to WAL first, then memtable.
    pub fn put(&mut self, record: StorageRecord) -> Result<(), StorageError> {
        self.wal.append(&record)?;
        self.memtable.put(record);
        Ok(())
    }

    /// Number of distinct keys.
    pub fn key_count(&self) -> usize {
        self.memtable.len()
    }

    /// Total versions across all keys.
    pub fn version_count(&self) -> usize {
        self.memtable.total_versions()
    }

    /// Return all keys.
    pub fn keys(&self) -> Vec<String> {
        self.memtable.keys()
    }

    /// Return keys matching a prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.memtable.keys_with_prefix(prefix)
    }

    /// Remove a key entirely. Writes a removal tombstone to WAL
    /// (tombstone with empty vclock) and removes from memtable.
    pub fn remove(&mut self, key: &str) -> Result<(), StorageError> {
        let record = StorageRecord::tombstone(key.to_string(), std::collections::HashMap::new());
        self.wal.append(&record)?;
        self.memtable.remove(key);
        Ok(())
    }

    /// Sync the WAL to disk (for batch fsync mode).
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.wal.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn make_record(key: &str, value: &str) -> StorageRecord {
        StorageRecord::new(key.to_string(), value.as_bytes().to_vec(), HashMap::new())
    }

    #[test]
    fn test_engine_put_get() {
        let dir = TempDir::new().unwrap();
        let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();

        engine.put(make_record("k1", "v1")).unwrap();
        let versions = engine.get("k1");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].value, b"v1");
    }

    #[test]
    fn test_engine_crash_recovery() {
        let dir = TempDir::new().unwrap();

        // Write some data
        {
            let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::Always).unwrap();
            engine.put(make_record("k1", "v1")).unwrap();
            engine.put(make_record("k2", "v2")).unwrap();
            engine.put(make_record("k3", "v3")).unwrap();
            assert_eq!(engine.key_count(), 3);
        }
        // Engine dropped (simulating crash)

        // Re-open: should recover from WAL
        {
            let engine = StorageEngine::open(dir.path(), FsyncPolicy::Always).unwrap();
            assert_eq!(engine.key_count(), 3);
            assert_eq!(engine.get("k1")[0].value, b"v1");
            assert_eq!(engine.get("k2")[0].value, b"v2");
            assert_eq!(engine.get("k3")[0].value, b"v3");
        }
    }

    #[test]
    fn test_engine_overwrite_recovery() {
        let dir = TempDir::new().unwrap();

        // Write then overwrite
        {
            let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::Always).unwrap();
            engine.put(make_record("k1", "v1")).unwrap();
            engine.put(make_record("k1", "v2")).unwrap();
        }

        // Re-open: last write wins during WAL replay
        {
            let engine = StorageEngine::open(dir.path(), FsyncPolicy::Always).unwrap();
            assert_eq!(engine.key_count(), 1);
            assert_eq!(engine.get("k1")[0].value, b"v2");
        }
    }

    #[test]
    fn test_engine_remove() {
        let dir = TempDir::new().unwrap();
        let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();

        engine.put(make_record("k1", "v1")).unwrap();
        engine.put(make_record("k2", "v2")).unwrap();
        assert_eq!(engine.key_count(), 2);

        engine.remove("k1").unwrap();
        assert_eq!(engine.key_count(), 1);
        assert!(engine.get("k1").is_empty());
        assert_eq!(engine.get("k2")[0].value, b"v2");
    }

    #[test]
    fn test_engine_remove_recovery() {
        let dir = TempDir::new().unwrap();

        {
            let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::Always).unwrap();
            engine.put(make_record("k1", "v1")).unwrap();
            engine.put(make_record("k2", "v2")).unwrap();
            engine.remove("k1").unwrap();
        }

        // Re-open: k1 should still be removed after WAL replay
        {
            let engine = StorageEngine::open(dir.path(), FsyncPolicy::Always).unwrap();
            assert_eq!(engine.key_count(), 1);
            assert!(engine.get("k1").is_empty());
            assert_eq!(engine.get("k2")[0].value, b"v2");
        }
    }

    #[test]
    fn test_engine_keys() {
        let dir = TempDir::new().unwrap();
        let mut engine = StorageEngine::open(dir.path(), FsyncPolicy::None).unwrap();

        engine.put(make_record("a:1", "v1")).unwrap();
        engine.put(make_record("a:2", "v2")).unwrap();
        engine.put(make_record("b:1", "v3")).unwrap();

        let mut all_keys = engine.keys();
        all_keys.sort();
        assert_eq!(all_keys, vec!["a:1", "a:2", "b:1"]);

        let mut prefix_keys = engine.keys_with_prefix("a:");
        prefix_keys.sort();
        assert_eq!(prefix_keys, vec!["a:1", "a:2"]);
    }
}
