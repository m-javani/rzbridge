use std::fs;

use serde::Deserialize;

use crate::error::RZError;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    // Set from CLI, not from YAML
    #[serde(skip)]
    pub bridge_id: String,
    #[serde(skip)]
    pub zone_id: String,
    #[serde(skip)]
    pub shard_id: String,

    // --- Cluster node endpoints ---
    #[serde(default = "default_roomzin_api_port")]
    pub roomzin_api_port: u16,
    #[serde(default = "default_roomzin_tcp_port")]
    pub roomzin_tcp_port: u16,

    // --- External services ---
    /// RzID registry (bootstrap + heartbeat)
    pub rzid_addr: String,
    /// RzPoint hostname resolver
    pub rzpoint_addr: String,

    #[serde(default = "default_rzid_heartbeat")]
    pub rzid_heartbeat_interval_secs: u64,

    // --- Router-facing listen ---
    #[serde(default = "default_listen_host")]
    pub listen_host: String,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,

    // --- Connection / pool ---
    #[serde(default = "default_conn_per_node")]
    pub conn_per_roomzin_node: usize,
    #[serde(default = "default_max_active_conns")]
    pub max_active_conns: usize,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    // --- Timeouts / intervals ---
    #[serde(default = "default_timeout_sec")]
    pub timeout_sec: u64,
    #[serde(default = "default_http_timeout_sec")]
    pub http_timeout_sec: u64,
    #[serde(default = "default_keep_alive_sec")]
    pub keep_alive_sec: u64,
    #[serde(default = "default_node_probe_interval")]
    pub node_probe_interval_sec: u64,

    // --- Runtime ---
    #[serde(default)]
    pub worker_threads: usize,
    #[serde(default)]
    pub tokens_path: String,
}

// ---- defaults (used by serde + Default) ----

fn default_roomzin_api_port() -> u16 {
    8080
}
fn default_roomzin_tcp_port() -> u16 {
    7777
}
fn default_rzid_heartbeat() -> u64 {
    10
}
fn default_listen_host() -> String {
    "0.0.0.0".into()
}
fn default_listen_port() -> u16 {
    9000
}
fn default_conn_per_node() -> usize {
    1
}
fn default_max_active_conns() -> usize {
    10_000
}
fn default_max_connections() -> usize {
    10_000
}
fn default_timeout_sec() -> u64 {
    2
}
fn default_http_timeout_sec() -> u64 {
    2
}
fn default_keep_alive_sec() -> u64 {
    30
}
fn default_node_probe_interval() -> u64 {
    2
}

impl Config {
    pub fn load(config_path: &str) -> Result<Self, RZError> {
        let content = fs::read_to_string(config_path).map_err(|e| {
            RZError::Config(format!("Failed to read config file {}: {}", config_path, e))
        })?;

        let mut config: Config = serde_yml::from_str(&content)
            .map_err(|e| RZError::Config(format!("Failed to parse config YAML: {}", e)))?;

        // Fill remaining zeros that serde defaults might not cover
        if config.worker_threads == 0 {
            config.worker_threads = num_cpus::get_physical().max(1) * 3;
        }
        if config.tokens_path.is_empty() {
            config.tokens_path = "./auth.yml".into();
        }
        if config.rzid_heartbeat_interval_secs == 0 {
            config.rzid_heartbeat_interval_secs = default_rzid_heartbeat();
        }

        // Basic validation
        if config.rzid_addr.is_empty() {
            return Err(RZError::Config("rzid_addr is required".into()));
        }
        if config.rzpoint_addr.is_empty() {
            return Err(RZError::Config("rzpoint_addr is required".into()));
        }

        Ok(config)
    }

    /// Address the TCP server binds to.
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.listen_port)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bridge_id: String::new(),
            zone_id: String::new(),
            shard_id: String::new(),
            roomzin_api_port: default_roomzin_api_port(),
            roomzin_tcp_port: default_roomzin_tcp_port(),
            rzid_addr: "localhost:8080".into(),
            rzpoint_addr: "localhost:9090".into(),
            rzid_heartbeat_interval_secs: default_rzid_heartbeat(),
            listen_host: default_listen_host(),
            listen_port: default_listen_port(),
            conn_per_roomzin_node: default_conn_per_node(),
            max_active_conns: default_max_active_conns(),
            max_connections: default_max_connections(),
            timeout_sec: default_timeout_sec(),
            http_timeout_sec: default_http_timeout_sec(),
            keep_alive_sec: default_keep_alive_sec(),
            node_probe_interval_sec: default_node_probe_interval(),
            worker_threads: 0,
            tokens_path: "./auth.yml".into(),
        }
    }
}
