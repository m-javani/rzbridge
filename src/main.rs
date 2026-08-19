// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use tokio_util::sync::CancellationToken;
// main.rs
use tracing::Level;
use tracing_subscriber::fmt::time::UtcTime;

use rzbridge::{async_main, config::Config, error::RZError};

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

    rt.block_on(async {
        let shutdown = CancellationToken::new();

        // Ctrl+C handler INSIDE the runtime
        {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("Ctrl+C received, shutting down");
                shutdown.cancel();
            });
        }

        async_main(config, shutdown).await
    })
}
