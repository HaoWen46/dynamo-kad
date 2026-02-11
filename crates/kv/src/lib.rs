//! Dynamo-style KV layer on top of Kademlia.
//!
//! Provides: key placement (N closest nodes), tunable quorums (R/W),
//! vector-clock versioning, and a coordinator that fans out replica RPCs.

pub mod chaos;
pub mod coordinator;
pub mod hint_delivery;
pub mod hint_store;
pub mod merkle;
pub mod placement;
pub mod replica_client;
pub mod vclock;
