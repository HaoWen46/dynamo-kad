//! Storage engine for dynamo-kad: WAL + in-memory memtable.
//!
//! Records are `{key, vclock, value, tombstone, timestamp}`.
//! The WAL provides durability; the memtable provides fast reads.

pub mod engine;
pub mod memtable;
pub mod record;
pub mod wal;

pub use engine::StorageEngine;
pub use record::StorageRecord;
