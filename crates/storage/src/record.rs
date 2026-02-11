//! Storage record format.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A versioned record stored on disk and in memory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageRecord {
    /// The key (opaque string).
    pub key: String,
    /// Vector clock: node-id-hex -> counter.
    pub vclock: HashMap<String, u64>,
    /// Value bytes (empty if tombstone).
    pub value: Vec<u8>,
    /// Whether this is a delete tombstone.
    pub tombstone: bool,
    /// Wall-clock timestamp (millis since epoch).
    pub timestamp_ms: u64,
}

impl StorageRecord {
    /// Create a new live record.
    pub fn new(key: String, value: Vec<u8>, vclock: HashMap<String, u64>) -> Self {
        Self {
            key,
            vclock,
            value,
            tombstone: false,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Create a tombstone record.
    pub fn tombstone(key: String, vclock: HashMap<String, u64>) -> Self {
        Self {
            key,
            vclock,
            value: Vec::new(),
            tombstone: true,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}
