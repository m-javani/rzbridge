use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::error::RZError;

/// Registration request payload for RZID.
#[derive(Debug, Clone, Serialize)]
struct RegisterRequest {
    kind: String,
    id: String,
    zone: String,
    shard: Option<String>,
}

/// Response from RZID for shard nodes query.
#[derive(Debug, Clone, Deserialize)]
pub struct NodesResponse {
    pub nodes: Vec<String>,
}

/// RZID client — only knows how to talk to RzID.
///
/// Topology discovery is *not* responsible for heartbeats.
#[derive(Clone)]
pub struct RzidClient {
    client: Client,
    base_url: String,
    bridge_id: String,
    shard_id: String,
    zone_id: String,
}

impl RzidClient {
    /// Create a new RZID client.
    pub fn new(
        rzid_addr: &str,
        bridge_id: &str,
        shard_id: &str,
        zone_id: &str,
    ) -> Result<Self, RZError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| RZError::System(format!("Failed to build RZID client: {}", e)))?;

        let base_url = if rzid_addr.starts_with("http://") {
            rzid_addr.trim_end_matches('/').to_string()
        } else {
            // Even if someone passes "https://", this will convert to "http://"
            // Remove any protocol first, then add http://
            let clean = rzid_addr
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .trim_end_matches('/');
            format!("http://{}", clean)
        };

        Ok(Self {
            client,
            base_url,
            bridge_id: bridge_id.to_string(),
            shard_id: shard_id.to_string(),
            zone_id: zone_id.to_string(),
        })
    }

    /// Send heartbeat / registration to RZID.
    pub async fn send_heartbeat(&self) -> Result<(), RZError> {
        let url = format!("{}/register", self.base_url);
        let request = RegisterRequest {
            kind: "bridge".to_string(),
            id: self.bridge_id.clone(),
            zone: self.zone_id.clone(),
            shard: Some(self.shard_id.clone()),
        };

        debug!(
            bridge_id = %self.bridge_id,
            shard_id = %self.shard_id,
            zone_id = %self.zone_id,
            "Sending heartbeat to RZID"
        );

        match self.client.post(&url).json(&request).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    debug!("Heartbeat sent successfully to RZID");
                    Ok(())
                } else {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    Err(RZError::Http(format!(
                        "RZID heartbeat returned {}: {}",
                        status, text
                    )))
                }
            }
            Err(e) => Err(RZError::Http(format!(
                "Failed to send heartbeat to RZID: {}",
                e
            ))),
        }
    }

    /// Fetch node IDs that belong to this shard from RZID.
    ///
    /// This is a bootstrap / fallback source only — not the authoritative
    /// live topology.
    pub async fn fetch_shard_nodes(&self) -> Result<Vec<String>, RZError> {
        let url = format!("{}/shards/{}/nodes", self.base_url, self.shard_id);

        debug!(
            shard_id = %self.shard_id,
            "Fetching shard nodes from RZID"
        );

        match self.client.get(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let nodes_response: NodesResponse = response.json().await.map_err(|e| {
                        RZError::Http(format!("Failed to parse nodes response: {}", e))
                    })?;
                    debug!(
                        shard_id = %self.shard_id,
                        node_count = %nodes_response.nodes.len(),
                        "Fetched shard nodes from RZID"
                    );
                    Ok(nodes_response.nodes)
                } else {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    Err(RZError::Http(format!(
                        "RZID nodes query returned {}: {}",
                        status, text
                    )))
                }
            }
            Err(e) => Err(RZError::Http(format!(
                "Failed to fetch shard nodes from RZID: {}",
                e
            ))),
        }
    }
}

/// Run the RZID heartbeat task for RzBridge.
pub async fn run_heartbeat_task(
    rzid_client: RzidClient,
    cancel_token: CancellationToken,
    heartbeat_interval_secs: u64,
) -> Result<(), RZError> {
    let mut interval = time::interval(Duration::from_secs(heartbeat_interval_secs));

    info!(
        bridge_id = %rzid_client.bridge_id,
        shard_id = %rzid_client.shard_id,
        zone_id = %rzid_client.zone_id,
        heartbeat_interval_secs = %heartbeat_interval_secs,
        "RZID heartbeat task started"
    );

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("RZID heartbeat task cancelled");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = rzid_client.send_heartbeat().await {
                    error!("Failed to send heartbeat to RZID: {}", e);
                }
            }
        }
    }
    Ok(())
}

/// Spawn the RZID heartbeat task.
pub fn spawn_heartbeat_task(
    rzid_addr: String,
    bridge_id: String,
    shard_id: String,
    zone_id: String,
    cancel_token: CancellationToken,
    heartbeat_interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let rzid_client = match RzidClient::new(&rzid_addr, &bridge_id, &shard_id, &zone_id) {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to create RZID client: {}", e);
                return;
            }
        };

        if let Err(e) = run_heartbeat_task(rzid_client, cancel_token, heartbeat_interval_secs).await
        {
            error!("RZID heartbeat task failed: {}", e);
        }
    })
}
