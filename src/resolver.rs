use std::time::Duration;

use reqwest::Client;

use crate::error::RZError;

/// Resolves node_id + shard_id → hostname via RzPoint.
///
/// Owns a reusable HTTP client. Does not append any ports.
#[derive(Clone)]
pub struct RzPointResolver {
    client: Client,
    base_url: String,
    shard_id: String,
}

impl RzPointResolver {
    /// Create a new resolver.
    ///
    /// `rzpoint_address` is the host:port (or host) of RzPoint.
    pub fn new(rzpoint_address: &str, shard_id: &str) -> Result<Self, RZError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| RZError::System(format!("Failed to build RzPoint client: {}", e)))?;

        let base_url =
            if rzpoint_address.starts_with("http://") || rzpoint_address.starts_with("https://") {
                rzpoint_address.trim_end_matches('/').to_string()
            } else {
                format!("http://{}", rzpoint_address.trim_end_matches('/'))
            };

        Ok(Self {
            client,
            base_url,
            shard_id: shard_id.to_string(),
        })
    }

    /// Resolve a node ID to its hostname.
    ///
    /// Retries up to 3 times with a short backoff (same behaviour as before).
    pub async fn resolve(&self, node_id: &str) -> Result<String, RZError> {
        let url = format!(
            "{}/shards/{}/nodes/{}",
            self.base_url, self.shard_id, node_id
        );

        let mut last_error = None;
        for attempt in 0..3 {
            match self.client.get(&url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let hostname = resp.text().await.map_err(|e| {
                            RZError::Resolver(format!("Failed to read response: {}", e))
                        })?;
                        return Ok(hostname.trim().to_string());
                    } else if resp.status() == 404 {
                        return Err(RZError::Resolver(format!("Node {} not found", node_id)));
                    } else {
                        last_error =
                            Some(RZError::Resolver(format!("HTTP status: {}", resp.status())));
                    }
                }
                Err(e) => {
                    last_error = Some(RZError::Resolver(format!("Request failed: {}", e)));
                }
            }

            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
            }
        }

        Err(last_error.unwrap_or_else(|| RZError::Resolver("All retries failed".into())))
    }
}
