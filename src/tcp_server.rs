// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cluster_handler::ClusterHandler;
use crate::config::Config;
use crate::error::RZError;
use crate::protocol::{
    DecodeResult, KEEPALIVE_SEGMENT, build_error_response, find_next_router_magic,
    try_decode_router,
};

use crate::metrics::SharedMetrics;

pub struct TcpServer {
    pub config: Arc<Config>,
    pub cancel_token: CancellationToken,
    pub active_connections: Arc<AtomicUsize>,
    pub cluster: Arc<ClusterHandler>,
    pub metrics: SharedMetrics,
}

impl TcpServer {
    pub fn new(
        config: Arc<Config>,
        cancel_token: CancellationToken,
        cluster: Arc<ClusterHandler>,
        metrics: SharedMetrics,
    ) -> Self {
        Self {
            config,
            cancel_token,
            active_connections: Arc::new(AtomicUsize::new(0)),
            cluster,
            metrics,
        }
    }

    pub async fn run(&self, listener: TcpListener) -> Result<(), std::io::Error> {
        info!("rzbridge tcp server listening");

        let (close_tx, mut close_rx) = mpsc::unbounded_channel::<()>();
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
                _ = self.cancel_token.cancelled() => {
                    info!("tcp server shutting down");
                    return Ok(());
                }
                accept = listener.accept() => {
                    let (stream, addr) = match accept {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::error!("accept failed: {}", e);
                            continue;
                        }
                    };

                    if self.active_connections.load(Ordering::Relaxed) >= self.config.max_connections {
                        warn!("max connections reached, dropping {}", addr);
                        drop(stream);
                        continue;
                    }

                    self.active_connections.fetch_add(1, Ordering::Relaxed);

                    let cluster = self.cluster.clone();
                    let cancel = self.cancel_token.clone();
                    let close_tx = close_tx.clone();
                    let config = self.config.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let _ = handle_connection(
                            stream,
                            addr,
                            cluster,
                            cancel,
                            config,
                            close_tx,
                           metrics,
                        ).await;
                    });
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    addr: std::net::SocketAddr,
    cluster: Arc<ClusterHandler>,
    cancel: CancellationToken,
    config: Arc<Config>,
    close_tx: mpsc::UnboundedSender<()>,
    metrics: SharedMetrics,
) -> Result<(), RZError> {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();

    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut last_frame_time = Instant::now();

    let frame_timeout = config.frame_timeout();
    let idle_timeout = config.idle_timeout();
    let max_buffer = config.max_buffer_size;
    let max_frame = config.max_frame_size;

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => break,

            read = timeout(idle_timeout, reader.read_buf(&mut buf)) => {
                match read {
                    Ok(Ok(0)) => {
                        debug!(%addr, "peer closed connection");
                        break;
                    }
                    Ok(Ok(n)) => {
                       metrics.bridge.bytes_received.increment(n as u64);
                    }
                    Ok(Err(e)) => {
                        debug!(%addr, "read error: {}", e);
                        break;
                    }
                    Err(_) => {
                        warn!(%addr, "idle timeout, closing");
                       metrics.bridge.timeouts.increment(1);
                        break;
                    }
                }

                // Drain complete frames
                loop {
                    if buf.len() > max_buffer {
                        warn!(%addr, size = buf.len(), "buffer too large, dropping");
                       metrics.bridge.client_errors.increment(1);
                        let _ = close_tx.send(());
                        return Ok(());
                    }

                    match try_decode_router(&buf) {
                        DecodeResult::NeedMore => {
                            if last_frame_time.elapsed() > frame_timeout {
                                warn!(%addr, "frame timeout (slow peer)");
                               metrics.bridge.timeouts.increment(1);
                                let _ = close_tx.send(());
                                return Ok(());
                            }
                            break;
                        }

                        DecodeResult::Frame {
                            frame_len,
                            segment,
                            is_write,
                            shard_frame,
                            original_clrid,
                            ..
                        } => {
                            if frame_len > max_frame {
                                warn!(%addr, size = frame_len, "frame too large, dropping");
                               metrics.bridge.client_errors.increment(1);
                                let _ = close_tx.send(());
                                return Ok(());
                            }

                            last_frame_time = Instant::now();
                           metrics.bridge.frames_received.increment(1);

                            let _ = buf.split_to(frame_len);

                            // Keepalive from the side that opened the connection
                            if segment == KEEPALIVE_SEGMENT {
                               metrics.bridge.keepalives_received.increment(1);
                                continue;
                            }

                            // Real work
                            match cluster.execute(is_write, shard_frame).await {
                                Ok(resp) => {
                                   metrics.bridge.frames_executed.increment(1);
                                   metrics.bridge.bytes_sent.increment(resp.len() as u64);

                                    if writer.write_all(&resp).await.is_err() {
                                        let _ = close_tx.send(());
                                        return Ok(());
                                    }
                                    let _ = writer.flush().await;
                                }
                                Err(e) => {
                                    debug!(%addr, "execute failed: {:?}", e);
                                   metrics.bridge.execute_errors.increment(1);

                                    // Send a proper error response so the upstream doesn't hang
                                    let error_frame = build_error_response(original_clrid, 4); // 4 = internal
                                    let _ = writer.write_all(&error_frame).await;
                                    let _ = writer.flush().await;

                                    // You can choose to keep the connection open or close it.
                                    // Closing is safer for now:
                                    let _ = close_tx.send(());
                                    return Ok(());
                                }
                            }
                        }

                        DecodeResult::Error(e) => {
                           metrics.bridge.client_errors.increment(1);
                           metrics.bridge.resyncs.increment(1);
                            debug!(%addr, error = %e, "parse error, attempting resync");

                            if buf.is_empty() {
                                break;
                            }

                            buf.advance(1);

                            match find_next_router_magic(&buf, 0) {
                                Some(pos) => {
                                    if pos > 0 {
                                        debug!(%addr, discarded = pos, "resync: skipping garbage");
                                        buf.advance(pos);
                                    }
                                    continue;
                                }
                                None => {
                                    if buf.len() > 64 {
                                        let keep = 8.min(buf.len());
                                        buf.advance(buf.len() - keep);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let _ = close_tx.send(());
    Ok(())
}
