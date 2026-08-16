use rzbridge::error::RZError;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::fmt::time::UtcTime;

use rzbridge::async_main;
use rzbridge::config::Config;

use crate::common::client::TestClient;
use crate::common::command::{CommandResponse, get_serialized_command, process_response};
pub struct TestHelper {
    bridge_addr: String,
    shutdown: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestHelper {
    pub async fn new() -> Self {
        // Setup test logging
        let _ = tracing_subscriber::fmt()
            .with_timer(UtcTime::rfc_3339())
            .with_max_level(Level::DEBUG)
            .with_target(false)
            .with_thread_names(false)
            .with_ansi(true)
            .try_init();

        let config = Self::test_config();
        let bridge_addr = config.listen_addr();

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        // Spawn the bridge
        let handle = tokio::spawn(async move {
            let _ = async_main(config, shutdown_clone).await;
        });

        // Wait for bridge to be ready
        Self::wait_for_bridge(&bridge_addr).await;

        Self {
            bridge_addr,
            shutdown,
            handle: Some(handle),
        }
    }

    #[allow(unused)]
    pub async fn shutdown(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }

    fn test_config() -> Config {
        use clap::Parser;

        // Build config with test values
        let args = vec![
            "rzbridge",
            "--zone-id",
            "zone1",
            "--shard-id",
            "shard1",
            "--bridge-id",
            "bridge-test",
            "--rzid-addr",
            "172.20.0.41:8080",
            "--rzpoint-addr",
            "172.20.0.40:9090",
            "--listen-host",
            "127.0.0.1",
            "--listen-port",
            "9000",
            "--api-listening-addr",
            "127.0.0.1:9101",
        ];

        Config::parse_from(args)
    }

    async fn wait_for_bridge(addr: &str) {
        let max_attempts = 30;
        for attempt in 0..max_attempts {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(_) => {
                    tracing::info!("Bridge is ready on {}", addr);
                    return;
                }
                Err(_) => {
                    if attempt == max_attempts - 1 {
                        panic!("Bridge failed to start after {} attempts", max_attempts);
                    }
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    pub async fn create_client(&self) -> TestClient {
        TestClient::connect(&self.bridge_addr)
            .await
            .expect("Failed to connect to bridge")
    }

    pub async fn send_command(&self, cmd: &str) -> Result<CommandResponse, RZError> {
        let mut client = self.create_client().await;
        let data = get_serialized_command(cmd);

        let response_payload = client.send_and_receive(&data).await?;
        process_response(cmd, &response_payload)
    }

    #[allow(unused)]
    pub fn bridge_addr(&self) -> &str {
        &self.bridge_addr
    }
}

impl Drop for TestHelper {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
