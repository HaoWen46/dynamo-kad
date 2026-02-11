//! Minimal Merkle tree for anti-entropy key-range comparison.
//!
//! This is a **data-structure-only** module — no RPCs, no background tasks.
//! Two nodes each build a `MerkleTree` over their local key-range, exchange
//! root hashes, and if they differ, call [`MerkleTree::diff`] to find the
//! keys whose values diverge.

use sha1::{Digest, Sha1};

/// SHA-1 hash output (20 bytes).
type Hash = [u8; 20];

/// A binary Merkle tree built from a sorted set of `(key, value_hash)` entries.
///
/// Leaf nodes hold a single key and the SHA-1 of `key || value_bytes`.
/// Internal nodes hold the SHA-1 of `left_hash || right_hash`.
#[derive(Debug, Clone)]
pub struct MerkleTree {
    nodes: Vec<MerkleNode>,
}

#[derive(Debug, Clone)]
struct MerkleNode {
    hash: Hash,
    /// `Some(key)` for leaf nodes, `None` for internal nodes.
    key: Option<String>,
    left: Option<usize>,
    right: Option<usize>,
}

impl MerkleTree {
    /// Build a Merkle tree from an **already-sorted** list of `(key, value_bytes)`.
    ///
    /// If `entries` is empty the tree contains a single zero-hash root.
    pub fn build(entries: &[(String, Vec<u8>)]) -> Self {
        let mut nodes: Vec<MerkleNode> = Vec::new();

        if entries.is_empty() {
            nodes.push(MerkleNode {
                hash: [0u8; 20],
                key: None,
                left: None,
                right: None,
            });
            return Self { nodes };
        }

        // Create leaf nodes.
        let leaf_indices: Vec<usize> = entries
            .iter()
            .map(|(k, v)| {
                let hash = leaf_hash(k, v);
                let idx = nodes.len();
                nodes.push(MerkleNode {
                    hash,
                    key: Some(k.clone()),
                    left: None,
                    right: None,
                });
                idx
            })
            .collect();

        // Build tree bottom-up by pairing nodes at each level.
        let mut current_level = leaf_indices;
        while current_level.len() > 1 {
            let mut next_level = Vec::new();
            let mut i = 0;
            while i < current_level.len() {
                if i + 1 < current_level.len() {
                    let left = current_level[i];
                    let right = current_level[i + 1];
                    let hash = internal_hash(&nodes[left].hash, &nodes[right].hash);
                    let idx = nodes.len();
                    nodes.push(MerkleNode {
                        hash,
                        key: None,
                        left: Some(left),
                        right: Some(right),
                    });
                    next_level.push(idx);
                    i += 2;
                } else {
                    // Odd node out — promote directly.
                    next_level.push(current_level[i]);
                    i += 1;
                }
            }
            current_level = next_level;
        }

        // Root is the last remaining node in current_level.
        // Make sure it's the last element in `nodes` for easy access.
        let root_idx = current_level[0];
        if root_idx != nodes.len() - 1 {
            // Swap root to the end so `root()` is O(1).
            let last = nodes.len() - 1;
            nodes.swap(root_idx, last);
            // Fix references that pointed to `last` or `root_idx`.
            fix_references(&mut nodes, root_idx, last);
        }

        Self { nodes }
    }

    /// Return the root hash of the tree.
    pub fn root_hash(&self) -> Hash {
        self.nodes.last().map(|n| n.hash).unwrap_or([0u8; 20])
    }

    /// Compare two trees and return the **keys** whose leaf hashes differ,
    /// plus keys present in one tree but not the other.
    ///
    /// Both trees must have been built from sorted entries for the diff to
    /// be meaningful.
    pub fn diff(&self, other: &MerkleTree) -> Vec<String> {
        let self_leaves = self.leaves();
        let other_leaves = other.leaves();

        let mut result = Vec::new();
        let mut i = 0;
        let mut j = 0;

        while i < self_leaves.len() && j < other_leaves.len() {
            let (sk, sh) = &self_leaves[i];
            let (ok, oh) = &other_leaves[j];

            match sk.cmp(ok) {
                std::cmp::Ordering::Equal => {
                    if sh != oh {
                        result.push(sk.clone());
                    }
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    // Key in self but not in other.
                    result.push(sk.clone());
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    // Key in other but not in self.
                    result.push(ok.clone());
                    j += 1;
                }
            }
        }

        // Remaining keys on either side.
        for (k, _) in &self_leaves[i..] {
            result.push(k.clone());
        }
        for (k, _) in &other_leaves[j..] {
            result.push(k.clone());
        }

        result
    }

    /// Collect all leaf `(key, hash)` pairs in tree order (which preserves
    /// the original sorted insertion order).
    fn leaves(&self) -> Vec<(String, Hash)> {
        let mut out = Vec::new();
        if !self.nodes.is_empty() {
            self.collect_leaves(self.nodes.len() - 1, &mut out);
        }
        out
    }

    fn collect_leaves(&self, idx: usize, out: &mut Vec<(String, Hash)>) {
        let node = &self.nodes[idx];
        if let Some(ref key) = node.key {
            out.push((key.clone(), node.hash));
            return;
        }
        if let Some(left) = node.left {
            self.collect_leaves(left, out);
        }
        if let Some(right) = node.right {
            self.collect_leaves(right, out);
        }
    }
}

/// SHA-1( key_bytes || value_bytes )
fn leaf_hash(key: &str, value: &[u8]) -> Hash {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(value);
    hasher.finalize().into()
}

/// SHA-1( left_hash || right_hash )
fn internal_hash(left: &Hash, right: &Hash) -> Hash {
    let mut hasher = Sha1::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// After swapping two nodes in the flat vec, fix any left/right references
/// that pointed to either swapped index.
fn fix_references(nodes: &mut [MerkleNode], old_a: usize, old_b: usize) {
    for node in nodes.iter_mut() {
        if let Some(ref mut l) = node.left {
            if *l == old_a {
                *l = old_b;
            } else if *l == old_b {
                *l = old_a;
            }
        }
        if let Some(ref mut r) = node.right {
            if *r == old_a {
                *r = old_b;
            } else if *r == old_b {
                *r = old_a;
            }
        }
    }
}

// ────────────────────────── Tests ──────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(pairs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn test_identical_trees() {
        let data = entries(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let t1 = MerkleTree::build(&data);
        let t2 = MerkleTree::build(&data);

        assert_eq!(t1.root_hash(), t2.root_hash());
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn test_different_trees() {
        let d1 = entries(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let d2 = entries(&[("a", "1"), ("b", "CHANGED"), ("c", "3")]);

        let t1 = MerkleTree::build(&d1);
        let t2 = MerkleTree::build(&d2);

        assert_ne!(t1.root_hash(), t2.root_hash());
        let diff = t1.diff(&t2);
        assert_eq!(diff, vec!["b".to_string()]);
    }

    #[test]
    fn test_diff_finds_mismatches() {
        let d1 = entries(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        let d2 = entries(&[("a", "X"), ("b", "2"), ("c", "Y"), ("d", "4")]);

        let t1 = MerkleTree::build(&d1);
        let t2 = MerkleTree::build(&d2);

        let diff = t1.diff(&t2);
        assert_eq!(diff, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_empty_tree() {
        let t1 = MerkleTree::build(&[]);
        let t2 = MerkleTree::build(&[]);
        assert_eq!(t1.root_hash(), [0u8; 20]);
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn test_single_entry() {
        let d1 = entries(&[("key", "val")]);
        let t1 = MerkleTree::build(&d1);
        assert_ne!(t1.root_hash(), [0u8; 20]);

        let t2 = MerkleTree::build(&d1);
        assert_eq!(t1.root_hash(), t2.root_hash());
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn test_diff_extra_keys() {
        // t1 has extra key "d", t2 has extra key "e"
        let d1 = entries(&[("a", "1"), ("b", "2"), ("d", "4")]);
        let d2 = entries(&[("a", "1"), ("b", "2"), ("e", "5")]);

        let t1 = MerkleTree::build(&d1);
        let t2 = MerkleTree::build(&d2);

        let diff = t1.diff(&t2);
        assert_eq!(diff, vec!["d".to_string(), "e".to_string()]);
    }

    #[test]
    fn test_diff_subset() {
        // t2 is a superset of t1
        let d1 = entries(&[("a", "1"), ("c", "3")]);
        let d2 = entries(&[("a", "1"), ("b", "2"), ("c", "3")]);

        let t1 = MerkleTree::build(&d1);
        let t2 = MerkleTree::build(&d2);

        let diff = t1.diff(&t2);
        assert_eq!(diff, vec!["b".to_string()]);
    }

    #[test]
    fn test_large_tree() {
        let data: Vec<(String, Vec<u8>)> = (0..100)
            .map(|i| (format!("key_{:04}", i), format!("val_{}", i).into_bytes()))
            .collect();

        let t1 = MerkleTree::build(&data);
        let t2 = MerkleTree::build(&data);

        assert_eq!(t1.root_hash(), t2.root_hash());
        assert!(t1.diff(&t2).is_empty());

        // Change one key in the middle.
        let mut data2 = data.clone();
        data2[50].1 = b"CHANGED".to_vec();
        let t3 = MerkleTree::build(&data2);
        assert_ne!(t1.root_hash(), t3.root_hash());
        let diff = t1.diff(&t3);
        assert_eq!(diff, vec!["key_0050".to_string()]);
    }
}
