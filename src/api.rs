use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use axum_server::Handle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::error::RZError;
use crate::metrics::SharedMetrics;

pub async fn run_api_server(
    listen_addr: String,
    metrics: SharedMetrics,
    cancel: CancellationToken,
) -> Result<(), RZError> {
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route(
            "/metrics",
            get({
                let metrics = metrics.clone();
                move || {
                    let metrics = metrics.clone();
                    async move {
                        let body = metrics.prometheus_handle.render();
                        (StatusCode::OK, body)
                    }
                }
            }),
        );

    let addr: SocketAddr = listen_addr
        .parse()
        .map_err(|e| RZError::Config(format!("invalid api listen addr: {}", e)))?;

    let handle = Handle::new();

    // Graceful shutdown
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        cancel.cancelled().await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(2)));
    });

    info!("API server listening on http://{}", listen_addr);

    axum_server::bind(addr)
        .handle(handle)
        .serve(app.into_make_service())
        .await
        .map_err(|e| RZError::System(format!("api server error: {}", e)))?;

    Ok(())
}
