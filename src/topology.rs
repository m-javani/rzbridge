use std::collections::{HashMap, HashSet};

use futures::future::join_all;
use tracing::{debug, info};

use crate::error::RZError;
use crate::resolver::RzPointResolver;
use crate::rzid::RzidClient;

use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

/// Information returned by a cluster node's `/node-info` endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeInfo {
    #[serde(rename = "node_id")]
    pub node_id: String,
    #[serde(rename = "zone_id")]
    pub zone_id: String,
    #[serde(rename = "shard_id")]
    pub shard_id: String,
    #[serde(rename = "leader_id")]
    pub leader_id: String,
}

/// HTTP client for cluster-node APIs.
///
/// Owns a reusable `reqwest::Client` and knows the API port.
/// All methods take a *hostname* (no port); the client adds the API port.
#[derive(Clone)]
pub struct NodeClient {
    client: Client,
    api_port: u16,
}

impl NodeClient {
    /// Create a new node client.
    ///
    /// `http_timeout_sec` comes from the existing Config.
    pub fn new(api_port: u16, http_timeout_sec: u64) -> Result<Self, RZError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(http_timeout_sec))
            .build()
            .map_err(|e| RZError::System(format!("Failed to build NodeClient: {}", e)))?;

        Ok(Self { client, api_port })
    }

    fn addr(&self, hostname: &str) -> String {
        format!("{}:{}", hostname, self.api_port)
    }

    async fn http_get<T: for<'de> Deserialize<'de>>(
        &self,
        hostname: &str,
        path: &str,
    ) -> Result<T, RZError> {
        let url = format!("http://{}{}", self.addr(hostname), path);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(RZError::Http(resp.status().to_string()));
        }
        Ok(resp.json().await?)
    }

    /// GET /healthz → body string (trimmed).
    pub async fn health(&self, hostname: &str) -> Result<String, RZError> {
        let url = format!("http://{}/healthz", self.addr(hostname));
        let resp = self.client.get(&url).send().await?;
        if resp.status().as_u16() != 200 {
            return Err(RZError::Http(resp.status().to_string()));
        }
        let body = resp.text().await?;
        Ok(body.trim().to_string())
    }

    /// GET /node-info.
    pub async fn node_info(&self, hostname: &str) -> Result<NodeInfo, RZError> {
        self.http_get(hostname, "/node-info").await
    }

    /// GET /peers → list of peer node IDs.
    pub async fn peers(&self, hostname: &str) -> Result<Vec<String>, RZError> {
        self.http_get(hostname, "/peers").await
    }
}

/// Live cluster topology discovered from the nodes themselves.
///
/// Contains *hostnames only* — no TCP or API ports.
/// The caller (ClusterHandler) is responsible for appending the TCP port.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    pub leader: String,
    pub followers: Vec<String>,
}

/// Internal view of a single probed node.
#[derive(Debug, Clone)]
struct NodeData {
    host: String,
    health: String,
    leader_id: String,
}

/// Orchestrates topology discovery.
///
/// Combines RzID (bootstrap/fallback node IDs), RzPoint (ID → hostname)
/// and NodeClient (live cluster HTTP APIs) to produce a `ClusterTopology`.
///
/// `discover()` performs **one** discovery attempt. It never loops forever
/// and never spawns background tasks. The caller decides retry cadence.
#[derive(Clone)]
pub struct TopologyDiscovery {
    rzid: RzidClient,
    resolver: RzPointResolver,
    node_client: NodeClient,

    /// Current seed node IDs used for discovery.
    /// Updated after a successful discovery so subsequent calls prefer
    /// the live cluster over a potentially stale RzID.
    ///
    /// Protected by a simple Mutex so `discover` can be called through
    /// an `Arc<TopologyDiscovery>` without requiring `&mut self`.
    known_node_ids: std::sync::Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl TopologyDiscovery {
    /// Construct with explicit dependencies.
    ///
    /// `initial_seed_ids` may be empty; in that case the first `discover()`
    /// will fall back to RzID immediately.
    pub fn new(
        rzid: RzidClient,
        resolver: RzPointResolver,
        node_client: NodeClient,
        initial_seed_ids: Vec<String>,
    ) -> Self {
        Self {
            rzid,
            resolver,
            node_client,
            known_node_ids: std::sync::Arc::new(tokio::sync::Mutex::new(initial_seed_ids)),
        }
    }

    /// Perform one discovery attempt.
    ///
    /// Preferred path:
    ///   known node IDs → RzPoint → cluster APIs → leader/followers
    ///
    /// Fallback (when no known nodes are reachable):
    ///   RzID → fresh node IDs → RzPoint → cluster APIs → …
    pub async fn discover(&self) -> Result<ClusterTopology, RZError> {
        // Snapshot current seeds.
        let seeds = {
            let guard = self.known_node_ids.lock().await;
            guard.clone()
        };

        // 1. Try with currently known seeds.
        if !seeds.is_empty() {
            match self.discover_from_seeds(&seeds).await {
                Ok((topo, observed_ids)) => {
                    // Remember the node IDs we actually observed so the next
                    // refresh can start from the live cluster.
                    if !observed_ids.is_empty() {
                        let mut guard = self.known_node_ids.lock().await;
                        *guard = observed_ids;
                    }
                    return Ok(topo);
                }
                Err(e) => {
                    debug!(
                        error = %e,
                        "Discovery from known seeds failed; falling back to RzID"
                    );
                }
            }
        }

        // 2. Fallback: obtain fresh node IDs from RzID.
        let seed_ids = self.rzid.fetch_shard_nodes().await?;
        if seed_ids.is_empty() {
            return Err(RZError::Validation(
                "roomzin seed node list is empty".into(),
            ));
        }

        let (topo, observed_ids) = self.discover_from_seeds(&seed_ids).await?;
        if !observed_ids.is_empty() {
            let mut guard = self.known_node_ids.lock().await;
            *guard = observed_ids;
        }
        Ok(topo)
    }

    /// Core discovery algorithm starting from a list of node IDs.
    ///
    /// Returns the topology *and* the set of node IDs that were successfully
    /// probed (so the caller can refresh `known_node_ids`).
    async fn discover_from_seeds(
        &self,
        seed_ids: &[String],
    ) -> Result<(ClusterTopology, Vec<String>), RZError> {
        if seed_ids.is_empty() {
            return Err(RZError::Validation(
                "roomzin seed node list is empty".into(),
            ));
        }

        let existing: HashSet<String> = seed_ids.iter().cloned().collect();

        // Phase 1 – probe seed nodes and collect newly discovered peers.
        let seed_results = self.probe_nodes(seed_ids).await;

        let mut nodes: HashMap<String, NodeData> = HashMap::new();
        let mut discovered_peers: HashSet<String> = HashSet::new();
        let mut observed_ids: Vec<String> = Vec::new();

        for (node_id, host, data, peers) in seed_results {
            observed_ids.push(node_id);
            nodes.insert(host, data);
            for peer in peers {
                if !existing.contains(&peer) {
                    discovered_peers.insert(peer);
                }
            }
        }

        // Phase 2 – probe newly discovered peer node IDs.
        if !discovered_peers.is_empty() {
            let peer_ids: Vec<String> = discovered_peers.into_iter().collect();
            let peer_results = self.probe_nodes(&peer_ids).await;
            for (node_id, host, data, _) in peer_results {
                observed_ids.push(node_id);
                nodes.insert(host, data);
            }
        }

        if nodes.is_empty() {
            return Err(RZError::NoLeaderAvailable);
        }

        // Voting: majority of reported leader_id wins.
        let mut votes: HashMap<String, usize> = HashMap::new();
        for node in nodes.values() {
            if !node.leader_id.is_empty() {
                *votes.entry(node.leader_id.clone()).or_insert(0) += 1;
            }
        }

        let leader_id = votes
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(id, _)| id)
            .ok_or(RZError::NoLeaderAvailable)?;

        let mut leader_host = None;
        let mut followers = Vec::new();

        for node in nodes.values() {
            if node.leader_id == leader_id {
                match node.health.as_str() {
                    "active_leader" => leader_host = Some(node.host.clone()),
                    "active_follower" => followers.push(node.host.clone()),
                    _ => {}
                }
            }
        }

        let leader = leader_host.ok_or(RZError::NoLeaderAvailable)?;

        info!(leader = %leader, followers = ?followers, "cluster topology discovered");

        Ok((ClusterTopology { leader, followers }, observed_ids))
    }

    /// Resolve + health-check + node-info (+ peers) for a batch of node IDs.
    ///
    /// Each task returns its own result; we collect with `join_all`.
    /// Unreachable / unhealthy nodes simply do not appear in the output.
    ///
    /// Return type: (node_id, hostname, NodeData, peers)
    async fn probe_nodes(
        &self,
        node_ids: &[String],
    ) -> Vec<(String, String, NodeData, Vec<String>)> {
        let tasks = node_ids.iter().map(|node_id| {
            let node_id = node_id.clone();
            let resolver = self.resolver.clone();
            let node_client = self.node_client.clone();

            async move {
                let hostname = match resolver.resolve(&node_id).await {
                    Ok(h) => h,
                    Err(e) => {
                        debug!(node_id = %node_id, error = %e, "failed to resolve hostname");
                        return None;
                    }
                };

                let health = match node_client.health(&hostname).await {
                    Ok(h) if h != "unavailable" => h,
                    Ok(_) => {
                        debug!(node_id = %node_id, host = %hostname, "node reports unavailable");
                        return None;
                    }
                    Err(e) => {
                        debug!(
                            node_id = %node_id,
                            host = %hostname,
                            error = %e,
                            "health check failed"
                        );
                        return None;
                    }
                };

                let info = match node_client.node_info(&hostname).await {
                    Ok(i) => i,
                    Err(e) => {
                        debug!(
                            node_id = %node_id,
                            host = %hostname,
                            error = %e,
                            "node-info failed"
                        );
                        return None;
                    }
                };

                // Peers are best-effort; failure does not discard the node.
                let peers = node_client.peers(&hostname).await.unwrap_or_default();

                Some((
                    node_id,
                    hostname.clone(),
                    NodeData {
                        host: hostname,
                        health,
                        leader_id: info.leader_id,
                    },
                    peers,
                ))
            }
        });

        join_all(tasks).await.into_iter().flatten().collect()
    }
}
