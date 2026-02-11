//! Vector clock implementation for causal versioning.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A vector clock: maps node identifiers to monotonic counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VClock {
    entries: HashMap<String, u64>,
}

impl VClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a raw map.
    pub fn from_map(entries: HashMap<String, u64>) -> Self {
        Self { entries }
    }

    /// Get the counter for a node (0 if not present).
    pub fn get(&self, node: &str) -> u64 {
        self.entries.get(node).copied().unwrap_or(0)
    }

    /// Increment the counter for a node and return the new clock.
    pub fn increment(&self, node: &str) -> Self {
        let mut new = self.clone();
        let counter = new.entries.entry(node.to_string()).or_insert(0);
        *counter += 1;
        new
    }

    /// Merge two vector clocks (element-wise max).
    pub fn merge(&self, other: &Self) -> Self {
        let mut merged = self.entries.clone();
        for (k, &v) in &other.entries {
            let entry = merged.entry(k.clone()).or_insert(0);
            *entry = (*entry).max(v);
        }
        Self { entries: merged }
    }

    /// Compare two vector clocks.
    pub fn compare(&self, other: &Self) -> VClockOrder {
        let mut self_gte = true;
        let mut other_gte = true;

        for (k, &sv) in &self.entries {
            let ov = other.get(k);
            if sv < ov {
                self_gte = false;
            }
            if ov < sv {
                other_gte = false;
            }
        }
        for (k, &ov) in &other.entries {
            if !self.entries.contains_key(k) && ov > 0 {
                self_gte = false;
            }
        }

        match (self_gte, other_gte) {
            (true, true) => VClockOrder::Equal,
            (true, false) => VClockOrder::Dominates,
            (false, true) => VClockOrder::DominatedBy,
            (false, false) => VClockOrder::Concurrent,
        }
    }

    /// Dominates: self >= other on all entries, > on at least one.
    pub fn dominates(&self, other: &Self) -> bool {
        self.compare(other) == VClockOrder::Dominates
    }

    /// Are these clocks concurrent (neither dominates)?
    pub fn is_concurrent(&self, other: &Self) -> bool {
        self.compare(other) == VClockOrder::Concurrent
    }

    /// Return the raw entries.
    pub fn entries(&self) -> &HashMap<String, u64> {
        &self.entries
    }

    /// Convert to raw map (for storage layer).
    pub fn into_map(self) -> HashMap<String, u64> {
        self.entries
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum VClockOrder {
    Equal,
    Dominates,
    DominatedBy,
    Concurrent,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vc(entries: &[(&str, u64)]) -> VClock {
        VClock::from_map(entries.iter().map(|(k, v)| (k.to_string(), *v)).collect())
    }

    #[test]
    fn test_increment() {
        let clock = VClock::new();
        let c1 = clock.increment("a");
        assert_eq!(c1.get("a"), 1);
        let c2 = c1.increment("a");
        assert_eq!(c2.get("a"), 2);
        let c3 = c2.increment("b");
        assert_eq!(c3.get("a"), 2);
        assert_eq!(c3.get("b"), 1);
    }

    #[test]
    fn test_merge() {
        let a = vc(&[("n1", 3), ("n2", 1)]);
        let b = vc(&[("n1", 1), ("n2", 5), ("n3", 2)]);
        let merged = a.merge(&b);
        assert_eq!(merged.get("n1"), 3);
        assert_eq!(merged.get("n2"), 5);
        assert_eq!(merged.get("n3"), 2);
    }

    #[test]
    fn test_compare_equal() {
        let a = vc(&[("n1", 1), ("n2", 2)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(a.compare(&b), VClockOrder::Equal);
    }

    #[test]
    fn test_compare_dominates() {
        let a = vc(&[("n1", 2), ("n2", 2)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(a.compare(&b), VClockOrder::Dominates);
    }

    #[test]
    fn test_compare_dominated_by() {
        let a = vc(&[("n1", 1)]);
        let b = vc(&[("n1", 2)]);
        assert_eq!(a.compare(&b), VClockOrder::DominatedBy);
    }

    #[test]
    fn test_compare_concurrent() {
        let a = vc(&[("n1", 2), ("n2", 1)]);
        let b = vc(&[("n1", 1), ("n2", 2)]);
        assert_eq!(a.compare(&b), VClockOrder::Concurrent);
    }

    #[test]
    fn test_compare_missing_keys_concurrent() {
        let a = vc(&[("n1", 1)]);
        let b = vc(&[("n2", 1)]);
        assert!(a.is_concurrent(&b));
    }

    #[test]
    fn test_dominates_with_superset_keys() {
        let a = vc(&[("n1", 1), ("n2", 1)]);
        let b = vc(&[("n1", 1)]);
        assert!(a.dominates(&b));
    }
}
