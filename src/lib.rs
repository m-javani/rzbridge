// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

pub mod api;
pub mod cluster_handler;
pub mod config;
pub mod connection;
pub mod demux;
pub mod error;
pub mod metrics;
pub mod protocol;
pub mod resolver;
pub mod rzid;
pub mod tcp_server;
pub mod topology;

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{
    api::run_api_server,
    cluster_handler::ClusterHandler,
    config::Config,
    error::RZError,
    metrics::Metrics,
    resolver::RzPointResolver,
    rzid::{RzidClient, spawn_heartbeat_task},
    tcp_server::TcpServer,
    topology::{NodeClient, TopologyDiscovery},
};

pub async fn async_main(config: Config, shutdown: CancellationToken) -> Result<(), RZError> {
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

    let metrics = Arc::new(Metrics::new());

    let api_cancel = shutdown.clone();
    let api_metrics = metrics.clone();
    let api_addr = config.api_listening_addr.clone();

    tokio::spawn(async move {
        if let Err(e) = run_api_server(api_addr, api_metrics, api_cancel).await {
            tracing::error!("API server failed: {}", e);
        }
    });

    // when creating TcpServer
    let server = TcpServer::new(Arc::new(config), shutdown.clone(), cluster, metrics.clone());

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
