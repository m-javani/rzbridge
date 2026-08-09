use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::cluster_handler::ClusterHandler;
use crate::config::Config;
use crate::error::RZError;
use crate::protocol::try_decode_router; // or wherever you put it

pub struct TcpServer {
    pub config: Arc<Config>,
    pub cancel_token: CancellationToken,
    pub active_connections: Arc<AtomicUsize>,
    pub cluster: Arc<ClusterHandler>,
}

impl TcpServer {
    pub fn new(
        config: Arc<Config>,
        cancel_token: CancellationToken,
        cluster: Arc<ClusterHandler>,
    ) -> Self {
        Self {
            config,
            cancel_token,
            active_connections: Arc::new(AtomicUsize::new(0)),
            cluster,
        }
    }

    pub async fn run(&self, listener: TcpListener) -> Result<(), std::io::Error> {
        info!("tcp server listening");
        let (close_tx, mut close_rx) = mpsc::unbounded_channel::<()>();

        // connection counter cleanup
        let cancel = self.cancel_token.clone();
        let active = self.active_connections.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    msg = close_rx.recv() => {
                        if msg.is_none() { break; }
                        active.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        });

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => return Ok(()),
                accept = listener.accept() => {
                    let (stream, _) = accept?;
                    if self.active_connections.load(Ordering::Relaxed)
                        >= self.config.max_connections
                    {
                        drop(stream);
                        continue;
                    }
                    self.active_connections.fetch_add(1, Ordering::Relaxed);

                    let cluster = self.cluster.clone();
                    let cancel = self.cancel_token.clone();
                    let close_tx = close_tx.clone();

                    tokio::spawn(async move {
                        let _ = handle_connection(stream, cluster, cancel, close_tx).await;
                    });
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    cluster: Arc<ClusterHandler>,
    cancel: CancellationToken,
    close_tx: mpsc::UnboundedSender<()>,
) -> Result<(), RZError> {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = BytesMut::with_capacity(16 * 1024);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            read = reader.read_buf(&mut buf) => {
                match read {
                    Ok(0) => break,          // EOF
                    Ok(_) => {}
                    Err(_) => break,
                }

                // Drain every complete router frame currently in the buffer.
                while let Some((consumed, is_write, shard_frame)) = try_decode_router(&buf) {
                    let _ = buf.split_to(consumed);

                    match cluster.execute(is_write, shard_frame).await {
                        Ok(resp) => {
                            if writer.write_all(&resp).await.is_err() {
                                let _ = close_tx.send(());
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            tracing::debug!("execute failed: {:?}", e);
                            // Optional: write a short error frame, or just close.
                            let _ = close_tx.send(());
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    let _ = close_tx.send(());
    Ok(())
}
