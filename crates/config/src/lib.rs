//! Configuration schema and loader for dynamo-kad nodes.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Top-level node configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// This node's listen address.
    pub listen: SocketAddr,

    /// Kademlia settings.
    #[serde(default)]
    pub kademlia: KademliaConfig,

    /// KV / Dynamo settings.
    #[serde(default)]
    pub kv: KvConfig,

    /// Storage settings.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Bootstrap seed addresses.
    #[serde(default)]
    pub seeds: Vec<String>,

    /// Optional Prometheus metrics HTTP port.
    #[serde(default)]
    pub metrics_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KademliaConfig {
    /// Bucket size (k).
    #[serde(default = "default_k")]
    pub k: usize,

    /// Lookup parallelism (alpha).
    #[serde(default = "default_alpha")]
    pub alpha: usize,

    /// RPC timeout in milliseconds.
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,

    /// Bucket refresh interval in seconds.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,

    /// Maximum number of records stored in the DHT record store.
    #[serde(default = "default_max_records")]
    pub max_records: usize,
}

impl Default for KademliaConfig {
    fn default() -> Self {
        Self {
            k: default_k(),
            alpha: default_alpha(),
            rpc_timeout_ms: default_rpc_timeout_ms(),
            refresh_interval_secs: default_refresh_interval_secs(),
            max_records: default_max_records(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvConfig {
    /// Replication factor (N).
    #[serde(default = "default_n")]
    pub n: usize,

    /// Default read quorum (R).
    #[serde(default = "default_r")]
    pub r: usize,

    /// Default write quorum (W).
    #[serde(default = "default_w")]
    pub w: usize,

    /// Read timeout in milliseconds.
    #[serde(default = "default_rpc_timeout_ms")]
    pub read_timeout_ms: u64,

    /// Write timeout in milliseconds.
    #[serde(default = "default_rpc_timeout_ms")]
    pub write_timeout_ms: u64,

    /// Whether read-repair is enabled.
    #[serde(default = "default_true")]
    pub read_repair: bool,

    /// Whether hinted handoff is enabled.
    #[serde(default = "default_true")]
    pub hinted_handoff: bool,

    /// Hint delivery check interval in seconds.
    #[serde(default = "default_hint_delivery_interval_secs")]
    pub hint_delivery_interval_secs: u64,

    /// Maximum hints to attempt delivery per cycle.
    #[serde(default = "default_max_hints_per_cycle")]
    pub max_hints_per_cycle: usize,
}

impl Default for KvConfig {
    fn default() -> Self {
        Self {
            n: default_n(),
            r: default_r(),
            w: default_w(),
            read_timeout_ms: default_rpc_timeout_ms(),
            write_timeout_ms: default_rpc_timeout_ms(),
            read_repair: true,
            hinted_handoff: true,
            hint_delivery_interval_secs: default_hint_delivery_interval_secs(),
            max_hints_per_cycle: default_max_hints_per_cycle(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory for WAL and data files.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Fsync policy: "always", "batch", "none".
    #[serde(default = "default_fsync")]
    pub fsync: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            fsync: default_fsync(),
        }
    }
}

// --- Defaults ---

fn default_k() -> usize {
    20
}
fn default_alpha() -> usize {
    3
}
fn default_rpc_timeout_ms() -> u64 {
    5000
}
fn default_refresh_interval_secs() -> u64 {
    3600
}
fn default_n() -> usize {
    3
}
fn default_r() -> usize {
    2
}
fn default_w() -> usize {
    2
}
fn default_true() -> bool {
    true
}
fn default_hint_delivery_interval_secs() -> u64 {
    30
}
fn default_max_records() -> usize {
    10_000
}
fn default_max_hints_per_cycle() -> usize {
    100
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}
fn default_fsync() -> String {
    "batch".to_string()
}

// --- Loading ---

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl NodeConfig {
    /// Validate that configuration values are consistent.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.kademlia.k == 0 {
            return Err(ConfigError::Invalid("kademlia.k must be > 0".into()));
        }
        if self.kademlia.alpha == 0 {
            return Err(ConfigError::Invalid("kademlia.alpha must be > 0".into()));
        }
        if self.kv.n == 0 {
            return Err(ConfigError::Invalid("kv.n must be > 0".into()));
        }
        if self.kv.r > self.kv.n {
            return Err(ConfigError::Invalid(format!(
                "kv.r ({}) must be <= kv.n ({})",
                self.kv.r, self.kv.n
            )));
        }
        if self.kv.w > self.kv.n {
            return Err(ConfigError::Invalid(format!(
                "kv.w ({}) must be <= kv.n ({})",
                self.kv.w, self.kv.n
            )));
        }
        Ok(())
    }
}

/// Load a `NodeConfig` from a YAML file path.
pub fn load_from_file(path: &std::path::Path) -> Result<NodeConfig, ConfigError> {
    let contents = std::fs::read_to_string(path)?;
    let config: NodeConfig = serde_yaml::from_str(&contents)?;
    config.validate()?;
    Ok(config)
}

/// Load a `NodeConfig` from a YAML string.
pub fn load_from_str(yaml: &str) -> Result<NodeConfig, ConfigError> {
    let config: NodeConfig = serde_yaml::from_str(yaml)?;
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let yaml = r#"
listen: "127.0.0.1:7000"
seeds:
  - "127.0.0.1:7001"
  - "127.0.0.1:7002"
"#;
        let config = load_from_str(yaml).unwrap();
        assert_eq!(config.listen.port(), 7000);
        assert_eq!(config.seeds.len(), 2);
        assert_eq!(config.kademlia.k, 20);
        assert_eq!(config.kv.n, 3);
        assert_eq!(config.kv.r, 2);
        assert_eq!(config.kv.w, 2);
    }

    #[test]
    fn test_parse_full_config() {
        let yaml = r#"
listen: "0.0.0.0:8000"
seeds: []
kademlia:
  k: 10
  alpha: 5
  rpc_timeout_ms: 3000
  refresh_interval_secs: 1800
kv:
  n: 5
  r: 3
  w: 3
  read_repair: false
  hinted_handoff: true
storage:
  data_dir: /tmp/dynamo-test
  fsync: always
"#;
        let config = load_from_str(yaml).unwrap();
        assert_eq!(config.kademlia.k, 10);
        assert_eq!(config.kademlia.alpha, 5);
        assert_eq!(config.kv.n, 5);
        assert!(!config.kv.read_repair);
        assert_eq!(config.storage.fsync, "always");
    }

    #[test]
    fn test_roundtrip_yaml() {
        let yaml = r#"
listen: "127.0.0.1:9000"
seeds: []
"#;
        let config = load_from_str(yaml).unwrap();
        let serialized = serde_yaml::to_string(&config).unwrap();
        let config2 = load_from_str(&serialized).unwrap();
        assert_eq!(config.listen, config2.listen);
        assert_eq!(config.kademlia.k, config2.kademlia.k);
    }

    #[test]
    fn test_rejects_r_greater_than_n() {
        let yaml = r#"
listen: "127.0.0.1:7000"
seeds: []
kv:
  n: 3
  r: 5
  w: 2
"#;
        let result = load_from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("kv.r"), "error should mention kv.r: {}", err);
    }

    #[test]
    fn test_rejects_w_greater_than_n() {
        let yaml = r#"
listen: "127.0.0.1:7000"
seeds: []
kv:
  n: 3
  r: 2
  w: 5
"#;
        let result = load_from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("kv.w"), "error should mention kv.w: {}", err);
    }

    #[test]
    fn test_rejects_zero_n() {
        let yaml = r#"
listen: "127.0.0.1:7000"
seeds: []
kv:
  n: 0
  r: 0
  w: 0
"#;
        let result = load_from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("kv.n"), "error should mention kv.n: {}", err);
    }

    #[test]
    fn test_rejects_zero_k() {
        let yaml = r#"
listen: "127.0.0.1:7000"
seeds: []
kademlia:
  k: 0
"#;
        let result = load_from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("kademlia.k"),
            "error should mention kademlia.k: {}",
            err
        );
    }
}
