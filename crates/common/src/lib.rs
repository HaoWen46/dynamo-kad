//! dynamo-common: shared types for the dynamo-kad project.
//!
//! Provides the 160-bit `NodeId` and `Distance` types that form
//! the foundation of the Kademlia XOR metric space.

use rand::Rng;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::fmt;

/// Number of bits in a Kademlia identifier.
pub const ID_BITS: usize = 160;

/// Number of bytes in a Kademlia identifier.
pub const ID_BYTES: usize = ID_BITS / 8; // 20

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

/// A 160-bit identifier used for both node IDs and keys in the Kademlia space.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId([u8; ID_BYTES]);

impl NodeId {
    /// The all-zeros identifier.
    pub const ZERO: Self = Self([0u8; ID_BYTES]);

    /// Create a `NodeId` from raw bytes.
    pub fn from_bytes(bytes: [u8; ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Return the raw bytes.
    pub fn as_bytes(&self) -> &[u8; ID_BYTES] {
        &self.0
    }

    /// Generate a random `NodeId`.
    pub fn random() -> Self {
        let mut bytes = [0u8; ID_BYTES];
        rand::thread_rng().fill(&mut bytes);
        Self(bytes)
    }

    /// Create a `NodeId` by SHA-1 hashing arbitrary data.
    pub fn from_sha1(data: &[u8]) -> Self {
        let hash = Sha1::digest(data);
        let mut bytes = [0u8; ID_BYTES];
        bytes.copy_from_slice(&hash);
        Self(bytes)
    }

    /// XOR distance to another `NodeId`.
    pub fn distance(&self, other: &Self) -> Distance {
        let mut d = [0u8; ID_BYTES];
        for (i, byte) in d.iter_mut().enumerate() {
            *byte = self.0[i] ^ other.0[i];
        }
        Distance(d)
    }

    /// Returns the index of the highest differing bit between `self` and `other`,
    /// which determines the k-bucket index. Returns `None` if `self == other`.
    ///
    /// Bucket 0 contains the most distant nodes (MSB differs),
    /// bucket 159 contains the closest (only LSB differs).
    ///
    /// Specifically, this returns `159 - leading_zeros_of_xor_distance`.
    /// A return value of 159 means the first bit differs (most distant half of the space).
    /// A return value of 0 means only the last bit differs (closest possible).
    pub fn bucket_index(&self, other: &Self) -> Option<usize> {
        let dist = self.distance(other);
        let lz = dist.leading_zeros();
        if lz == ID_BITS {
            None // self == other
        } else {
            Some(ID_BITS - 1 - lz)
        }
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Show first 4 bytes as hex for readability
        for byte in &self.0[..4] {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, "…")
    }
}

// ---------------------------------------------------------------------------
// Distance
// ---------------------------------------------------------------------------

/// XOR distance between two `NodeId`s.
///
/// Supports comparison via big-endian byte ordering so that
/// distances can be correctly ordered.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Distance([u8; ID_BYTES]);

impl Distance {
    /// The zero distance.
    pub const ZERO: Self = Self([0u8; ID_BYTES]);

    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Return the raw bytes.
    pub fn as_bytes(&self) -> &[u8; ID_BYTES] {
        &self.0
    }

    /// Returns the number of leading zero bits.
    pub fn leading_zeros(&self) -> usize {
        for (i, byte) in self.0.iter().enumerate() {
            if *byte != 0 {
                return i * 8 + byte.leading_zeros() as usize;
            }
        }
        ID_BITS
    }

    /// Returns `true` if the distance is zero (identical nodes).
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; ID_BYTES]
    }
}

impl Ord for Distance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Big-endian byte comparison: compare from most significant byte.
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Distance(lz={})", self.leading_zeros())
    }
}

impl fmt::Display for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0[..4] {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, "…")
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum KadError {
    #[error("RPC timeout contacting {0}")]
    Timeout(NodeId),

    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("bucket full for index {0}")]
    BucketFull(usize),

    #[error("lookup failed: {0}")]
    LookupFailed(String),

    #[error("store full (capacity {0})")]
    StoreFull(usize),

    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Utility: generate a random NodeId in a specific bucket range
// ---------------------------------------------------------------------------

/// Generate a random `NodeId` that would fall into bucket `bucket_index`
/// relative to `local_id`.
///
/// The generated ID differs from `local_id` at bit position
/// `(159 - bucket_index)` (counting from MSB = 0) and has random bits
/// in all less-significant positions.
pub fn random_id_in_bucket(local_id: &NodeId, bucket_index: usize) -> NodeId {
    assert!(bucket_index < ID_BITS, "bucket_index must be < {}", ID_BITS);

    let mut rng = rand::thread_rng();
    let mut result = *local_id;

    // The first differing bit is at position `159 - bucket_index` from the MSB.
    let bit_pos = ID_BITS - 1 - bucket_index; // position from MSB, 0-indexed
    let byte_idx = bit_pos / 8;
    let bit_idx = 7 - (bit_pos % 8); // bit within byte, 0 = LSB

    // Flip the target bit to ensure it differs
    result.0[byte_idx] ^= 1 << bit_idx;

    // All bits more significant than bit_pos must match local_id (already do).
    // All bits less significant than bit_pos should be random.
    // "less significant" means higher byte indices, and within the same byte,
    // lower bit positions.

    // Randomize remaining bits in the same byte (below bit_idx)
    if bit_idx > 0 {
        let mask = (1u8 << bit_idx) - 1; // bits below bit_idx
        let random_byte: u8 = rng.gen();
        result.0[byte_idx] = (result.0[byte_idx] & !mask) | (random_byte & mask);
    }

    // Randomize all bytes after byte_idx
    for b in (byte_idx + 1)..ID_BYTES {
        result.0[b] = rng.gen();
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_distance_identity() {
        let a = NodeId::random();
        assert!(a.distance(&a).is_zero());
        assert_eq!(a.distance(&a), Distance::ZERO);
    }

    #[test]
    fn test_xor_distance_symmetry() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_eq!(a.distance(&b), b.distance(&a));
    }

    #[test]
    fn test_distance_ordering() {
        // Construct known distances
        let mut d1_bytes = [0u8; ID_BYTES];
        d1_bytes[0] = 0x01; // small distance (leading zeros = 7)

        let mut d2_bytes = [0u8; ID_BYTES];
        d2_bytes[0] = 0x80; // large distance (leading zeros = 0)

        let d1 = Distance::from_bytes(d1_bytes);
        let d2 = Distance::from_bytes(d2_bytes);

        assert!(d1 < d2, "d1 (0x01..) should be less than d2 (0x80..)");
        assert!(Distance::ZERO < d1);
    }

    #[test]
    fn test_leading_zeros() {
        assert_eq!(Distance::ZERO.leading_zeros(), ID_BITS);

        let mut bytes = [0u8; ID_BYTES];
        bytes[0] = 0x80; // 1000_0000
        assert_eq!(Distance::from_bytes(bytes).leading_zeros(), 0);

        bytes[0] = 0x01; // 0000_0001
        assert_eq!(Distance::from_bytes(bytes).leading_zeros(), 7);

        let mut bytes2 = [0u8; ID_BYTES];
        bytes2[1] = 0x01; // first byte is 0, second byte is 0000_0001
        assert_eq!(Distance::from_bytes(bytes2).leading_zeros(), 15);

        // Last bit only
        let mut bytes3 = [0u8; ID_BYTES];
        bytes3[ID_BYTES - 1] = 0x01;
        assert_eq!(Distance::from_bytes(bytes3).leading_zeros(), ID_BITS - 1);
    }

    #[test]
    fn test_bucket_index_self() {
        let a = NodeId::random();
        assert_eq!(a.bucket_index(&a), None);
    }

    #[test]
    fn test_bucket_index_known_values() {
        let a = NodeId::from_bytes([0u8; ID_BYTES]);

        // b differs only in the last bit -> bucket 0
        let mut b_bytes = [0u8; ID_BYTES];
        b_bytes[ID_BYTES - 1] = 0x01;
        let b = NodeId::from_bytes(b_bytes);
        assert_eq!(a.bucket_index(&b), Some(0));

        // c differs in the MSB -> bucket 159
        let mut c_bytes = [0u8; ID_BYTES];
        c_bytes[0] = 0x80;
        let c = NodeId::from_bytes(c_bytes);
        assert_eq!(a.bucket_index(&c), Some(159));

        // d differs at bit 8 from MSB (second byte, MSB) -> bucket 151
        let mut d_bytes = [0u8; ID_BYTES];
        d_bytes[1] = 0x80;
        let d = NodeId::from_bytes(d_bytes);
        assert_eq!(a.bucket_index(&d), Some(151));
    }

    #[test]
    fn test_node_id_from_sha1() {
        let id = NodeId::from_sha1(b"hello");
        assert_ne!(id, NodeId::ZERO);
        // SHA-1 of "hello" is deterministic
        let id2 = NodeId::from_sha1(b"hello");
        assert_eq!(id, id2);
        // Different input -> different ID
        let id3 = NodeId::from_sha1(b"world");
        assert_ne!(id, id3);
    }

    #[test]
    fn test_node_id_random_uniqueness() {
        let ids: Vec<NodeId> = (0..1000).map(|_| NodeId::random()).collect();
        let unique: std::collections::HashSet<NodeId> = ids.iter().copied().collect();
        assert_eq!(unique.len(), 1000, "expected 1000 unique random IDs");
    }

    #[test]
    fn test_random_id_in_bucket() {
        let local = NodeId::random();
        for bucket in [0, 1, 10, 50, 100, 150, 159] {
            let generated = random_id_in_bucket(&local, bucket);
            let actual_bucket = local.bucket_index(&generated);
            assert_eq!(
                actual_bucket,
                Some(bucket),
                "expected bucket {}, got {:?} for local={:?}, generated={:?}",
                bucket,
                actual_bucket,
                local,
                generated
            );
        }
    }

    #[test]
    fn test_random_id_in_bucket_randomness() {
        // Generate multiple IDs for the same bucket, they should differ
        let local = NodeId::random();
        let ids: Vec<NodeId> = (0..100).map(|_| random_id_in_bucket(&local, 80)).collect();
        let unique: std::collections::HashSet<NodeId> = ids.iter().copied().collect();
        // Not all should be identical (statistical, but 100 random 160-bit IDs won't collide)
        assert!(unique.len() > 1, "random IDs in same bucket should vary");
    }

    #[test]
    fn test_display_and_debug() {
        let mut bytes = [0u8; ID_BYTES];
        bytes[0] = 0xAB;
        bytes[1] = 0xCD;
        bytes[2] = 0xEF;
        bytes[3] = 0x01;
        let id = NodeId::from_bytes(bytes);
        let s = format!("{}", id);
        assert!(s.contains("…"), "Display should truncate with …");

        let d = format!("{:?}", id);
        assert!(d.starts_with("NodeId("), "Debug should start with NodeId(");
    }

    #[test]
    fn test_distance_xor_identity() {
        // Key XOR property: d(a,c) = d(a,b) XOR d(b,c)
        // Because (a^c) = (a^b) ^ (b^c).
        for _ in 0..100 {
            let a = NodeId::random();
            let b = NodeId::random();
            let c = NodeId::random();

            let d_ab = a.distance(&b);
            let d_bc = b.distance(&c);
            let d_ac = a.distance(&c);

            // Compute d_ab XOR d_bc
            let mut xor_result = [0u8; ID_BYTES];
            for (i, byte) in xor_result.iter_mut().enumerate() {
                *byte = d_ab.as_bytes()[i] ^ d_bc.as_bytes()[i];
            }
            assert_eq!(
                d_ac.as_bytes(),
                &xor_result,
                "d(a,c) should equal d(a,b) XOR d(b,c)",
            );
        }
    }

    #[test]
    fn test_serde_roundtrip() {
        let id = NodeId::random();
        let json = serde_json::to_string(&id).unwrap();
        let id2: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);

        let dist = id.distance(&NodeId::random());
        let json = serde_json::to_string(&dist).unwrap();
        let dist2: Distance = serde_json::from_str(&json).unwrap();
        assert_eq!(dist, dist2);
    }
}
