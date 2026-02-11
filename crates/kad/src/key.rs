//! Key helpers for the Kademlia DHT.
//!
//! Re-exports core types from `dynamo_common` and provides
//! Kademlia-specific key utilities.

pub use dynamo_common::{random_id_in_bucket, Distance, NodeId, ID_BITS, ID_BYTES};
