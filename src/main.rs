// main.rs
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::fmt::time::UtcTime;

use rzbridge::{
    cluster_handler::ClusterHandler,
    config::Config,
    error::RZError,
    resolver::RzPointResolver,
    rzid::{RzidClient, spawn_heartbeat_task},
    tcp_server::TcpServer,
    topology::{NodeClient, TopologyDiscovery},
};

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

    let config = Config::parse();

    let desired_workers = config.effective_workers();
    tracing::info!(
        bridge_id = %config.bridge_id,
        shard_id = %config.shard_id,
        zone_id = %config.zone_id,
        listen = %config.listen_addr(),
        workers = desired_workers,
        "starting RzBridge"
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(desired_workers)
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .map_err(|e| RZError::System(format!("Failed to build runtime: {e}")))?;

    rt.block_on(async_main(config))
}

async fn async_main(config: Config) -> Result<(), RZError> {
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
