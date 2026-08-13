use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about = "RzBridge – topology-aware roomzin gateway", long_about = None)]
pub struct Config {
    /// Zone ID (REQUIRED)
    #[clap(short = 'z', long, required = true)]
    pub zone_id: String,

    /// Shard ID (REQUIRED)
    #[clap(short = 's', long, required = true)]
    pub shard_id: String,

    /// Bridge ID (REQUIRED)
    #[clap(short = 'b', long, required = true)]
    pub bridge_id: String,

    /// RzID service address (REQUIRED)
    #[clap(long, required = true)]
    pub rzid_addr: String,

    /// RzPoint resolver address (REQUIRED)
    #[clap(long, required = true)]
    pub rzpoint_addr: String,

    /// Roomzin API port
    #[clap(long, default_value = "8080")]
    pub roomzin_api_port: u16,

    /// Roomzin TCP port
    #[clap(long, default_value = "7777")]
    pub roomzin_tcp_port: u16,

    /// RzID heartbeat interval in seconds
    #[clap(long, default_value = "10")]
    pub rzid_heartbeat_interval_secs: u64,

    /// Listen host address (TCP server)
    #[clap(long, default_value = "0.0.0.0")]
    pub listen_host: String,

    /// Listen port (TCP server)
    #[clap(long, default_value = "9000")]
    pub listen_port: u16,

    /// HTTP API listen address (metrics + health), e.g. "0.0.0.0:9100"
    #[clap(long, default_value = "0.0.0.0:9100")]
    pub api_listening_addr: String,

    /// Connections per Roomzin node
    #[clap(long, default_value = "1")]
    pub conn_per_roomzin_node: usize,

    /// Maximum active connections
    #[clap(long, default_value = "10000")]
    pub max_active_conns: usize,

    /// Maximum connections
    #[clap(long, default_value = "10000")]
    pub max_connections: usize,

    /// Request timeout in seconds
    #[clap(long, default_value = "2")]
    pub timeout_sec: u64,

    /// HTTP timeout in seconds
    #[clap(long, default_value = "2")]
    pub http_timeout_sec: u64,

    /// Keep-alive interval in seconds (used by other layers)
    #[clap(long, default_value = "15")]
    pub keep_alive_sec: u64,

    /// Node probe interval in seconds
    #[clap(long, default_value = "2")]
    pub node_probe_interval_sec: u64,

    /// Tokio worker threads (0 = auto = cores * 3)
    #[clap(long, default_value = "0")]
    pub worker_threads: usize,

    // ------------------------------------------------------------------
    // Inbound TCP robustness (same semantics as the router)
    // ------------------------------------------------------------------
    /// Maximum time allowed to complete a single frame (seconds).
    /// Protects against slowloris / partial-frame attacks.
    #[clap(long, default_value = "20")]
    pub frame_timeout_secs: u64,

    /// Absolute idle timeout (seconds). Connection is closed if no activity
    /// is seen for this long (the side that opened the connection is responsible
    /// for sending keepalives).
    #[clap(long, default_value = "90")]
    pub idle_timeout_secs: u64,

    /// Maximum size of the receive buffer per connection (bytes).
    #[clap(long, default_value = "262144")] // 256 KiB
    pub max_buffer_size: usize,

    /// Maximum allowed size of a single frame (bytes).
    #[clap(long, default_value = "16384")] // 16 KiB
    pub max_frame_size: usize,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }

    /// Address the TCP server binds to
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.listen_port)
    }

    /// Effective worker threads (auto-detect if 0)
    pub fn effective_workers(&self) -> usize {
        if self.worker_threads == 0 {
            num_cpus::get_physical().max(1) * 3
        } else {
            self.worker_threads
        }
    }

    // ------------------------------------------------------------------
    // Duration helpers used by the TCP server
    // ------------------------------------------------------------------

    #[inline]
    pub fn frame_timeout(&self) -> Duration {
        Duration::from_secs(self.frame_timeout_secs)
    }

    #[inline]
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }
}
