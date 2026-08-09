use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::fmt::time::UtcTime;

use rzbridge::{
    auth,
    cluster_handler::ClusterHandler,
    config::Config,
    error::RZError,
    resolver::RzPointResolver,
    rzid::{RzidClient, spawn_heartbeat_task},
    tcp_server::TcpServer, // adjust module path if different
    topology::{NodeClient, TopologyDiscovery}, // or node_client::NodeClient if separate
};

#[derive(Parser, Debug)]
#[clap(author, version, about = "RzBridge – topology-aware edge proxy", long_about = None)]
struct Args {
    /// Path to the YAML configuration file
    #[clap(short, long)]
    config: Option<String>,

    /// Zone ID
    #[clap(short, long)]
    zone_id: String,

    /// Shard ID
    #[clap(short, long)]
    shard_id: String,

    /// Bridge ID
    #[clap(short, long)]
    bridge_id: String,

    /// Path to the auth tokens file
    #[clap(short = 't', long)]
    tokens_path: Option<String>,
}

fn main() -> Result<(), RZError> {
    // Logging
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_timer(UtcTime::rfc_3339())
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_names(false)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global tracing subscriber");

    let args = Args::parse();

    // Config
    let config_path = find_config_path(args.config.as_ref())
        .ok_or_else(|| RZError::Config("No configuration file found".into()))?;

    let mut config = Config::load(&config_path)?;
    config.bridge_id = args.bridge_id;
    config.zone_id = args.zone_id;
    config.shard_id = args.shard_id;

    // Auth tokens path
    let tokens_path = resolve_tokens_path(&args.tokens_path, &config)?;

    // Load tokens on a small runtime before the main multi-thread runtime
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RZError::System(format!("Failed to build runtime: {e}")))?;
        rt.block_on(async {
            auth::init_token(&tokens_path)
                .await
                .map_err(|e| RZError::Config(format!("Auth error: {e}")))
        })?;
    }

    let desired_workers = if config.worker_threads == 0 {
        num_cpus::get_physical().max(1) * 3
    } else {
        config.worker_threads
    };
    tracing::info!(workers = desired_workers, "starting tokio runtime");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(desired_workers)
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .map_err(|e| RZError::System(format!("Failed to build runtime: {e}")))?;

    rt.block_on(async_main(config, tokens_path))
}

async fn async_main(config: Config, tokens_path: String) -> Result<(), RZError> {
    let shutdown = CancellationToken::new();

    // Ctrl+C → cancel
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Ctrl+C received, shutting down");
            shutdown.cancel();
        });
    }

    // Auth token watcher
    tokio::spawn(auth::start_watcher(tokens_path, shutdown.clone()));

    // --- Topology stack -------------------------------------------------------
    let rzid = RzidClient::new(
        &config.rzid_addr,
        &config.bridge_id,
        &config.shard_id,
        &config.zone_id,
    )?;

    let resolver = RzPointResolver::new(&config.rzpoint_addr, &config.shard_id)?;

    let node_client = NodeClient::new(config.roomzin_api_port, config.http_timeout_sec)?;

    // Empty seeds → first discover() falls back to RzID
    let topology_discovery =
        TopologyDiscovery::new(rzid.clone(), resolver, node_client, Vec::new());

    // RZID heartbeat (independent of discovery)
    spawn_heartbeat_task(
        config.rzid_addr.clone(),
        config.bridge_id.clone(),
        config.shard_id.clone(),
        config.zone_id.clone(),
        shutdown.clone(),
        config.rzid_heartbeat_interval_secs,
    );

    // --- Cluster handler (owns TCP connections to roomzin nodes) --------------
    let cluster = ClusterHandler::new(config.clone(), shutdown.clone(), topology_discovery);

    // --- TCP server (router-facing) -------------------------------------------
    let listener = TcpListener::bind(config.listen_addr()).await?;
    tracing::info!("{} tcp server bound", config.listen_addr());

    let server = TcpServer::new(Arc::new(config), shutdown.clone(), cluster);

    // Run until cancelled
    tokio::select! {
        _ = shutdown.cancelled() => {
            tracing::info!("shutdown complete");
            Ok(())
        }
        res = server.run(listener) => {
            res.map_err(|e| RZError::System(format!("tcp server error: {e}")))
        }
    }
}

fn find_config_path(cli_config: Option<&String>) -> Option<String> {
    if let Some(path) = cli_config {
        if Path::new(path).exists() {
            return Some(path.clone());
        }
    }
    if Path::new("rzbridge.yml").exists() {
        return Some("rzbridge.yml".to_string());
    }
    if Path::new("/etc/rzbridge/rzbridge.yml").exists() {
        return Some("/etc/rzbridge/rzbridge.yml".to_string());
    }
    None
}

fn resolve_tokens_path(cli: &Option<String>, config: &Config) -> Result<String, RZError> {
    if let Some(path) = cli {
        return Ok(path.clone());
    }
    if !config.tokens_path.is_empty() {
        return Ok(config.tokens_path.clone());
    }
    if Path::new("auth.yml").exists() {
        return Ok("auth.yml".to_string());
    }
    if Path::new("/etc/rzbridge/auth.yml").exists() {
        return Ok("/etc/rzbridge/auth.yml".to_string());
    }
    Err(RZError::Config("No auth file found".into()))
}
