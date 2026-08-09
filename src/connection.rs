use crate::config::Config;
use crate::{auth, error::RZError};

use crate::demux::DemuxMap;
use crate::protocol::drain_frame_async;
use crate::protocol::prepend_header;

use bytes::{Bytes, BytesMut};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Connection {
    pub inner: Arc<ConnectionInner>,
}

pub struct ConnectionInner {
    #[allow(unused)]
    pub addr: String,
    pub demux: Arc<DemuxMap>,
    send_tx: mpsc::Sender<Vec<u8>>,
    corr_id: AtomicU32,
    closed: AtomicBool,
}

impl Connection {
    pub async fn connect(
        host: String,
        cfg: &Config,
        demux: Arc<DemuxMap>,
    ) -> Result<Self, RZError> {
        let stream = TcpStream::connect((&*host, cfg.roomzin_tcp_port)).await?;
        let (mut reader, mut writer) = stream.into_split();

        let login_frame = super::protocol::prepend_header(
            0,
            &super::protocol::build_login_payload(&auth::get_roomzin_token())?,
        );
        writer.write_all(&login_frame).await?;
        writer.flush().await?;

        let mut login_resp = [0u8; 8];
        reader.read_exact(&mut login_resp).await?;
        if &login_resp != b"LOGIN OK" {
            return Err(RZError::Auth("login failed".into()));
        }

        let (send_tx, mut send_rx) = mpsc::channel(cfg.max_active_conns.max(2048));

        let inner = Arc::new(ConnectionInner {
            addr: host.clone(),
            demux,
            send_tx: send_tx.clone(),
            corr_id: AtomicU32::new(1),
            closed: AtomicBool::new(false),
        });

        let conn = Connection {
            inner: inner.clone(),
        };

        // Write loop
        let write_inner = inner.clone();
        tokio::spawn(async move {
            while let Some(frame) = send_rx.recv().await {
                if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
                    break;
                }
            }
            write_inner.closed.store(true, Ordering::Release);
        });

        // Read loop
        let read_inner = inner.clone();
        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(8192);
            loop {
                let (hdr, payload) = match drain_frame_async(&mut reader, &mut buf).await {
                    Ok(v) => v,
                    Err(_) => break,
                };

                let (tx, _sent_at) = match read_inner.demux.load_remove(hdr.clr_id).await {
                    Some(v) => v,
                    None => break,
                };

                let field_data_start = 1 + hdr.status_len as usize + 2;

                // Special error handling
                let mut should_close = false;
                if payload.len() >= field_data_start
                    && &payload[1..1 + hdr.status_len as usize] == b"ERROR"
                {
                    if let Some(code) =
                        std::str::from_utf8(&payload[field_data_start..field_data_start + 3]).ok()
                    {
                        match code {
                            "308" | "405" | "503" => should_close = true,
                            "429" => {}
                            _ => {}
                        }
                    }
                }

                let _ = tx.send(Bytes::copy_from_slice(&payload));

                if should_close {
                    read_inner.closed.store(true, Ordering::Release);
                    break;
                }
            }
            read_inner.closed.store(true, Ordering::Release);
        });

        Ok(conn)
    }

    pub fn next_corr_id(&self) -> u32 {
        self.inner.corr_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn send(&self, corr_id: u32, payload: Vec<u8>) -> Result<(), ()> {
        let frame = prepend_header(corr_id, &payload);
        self.inner.send_tx.send(frame).await.map_err(|_| ())
    }

    /// Send a fully-formed frame (must already start with 0xFF and contain the corr_id).
    /// Used by the bridge hop so we do not double-prepend a header.
    pub async fn send_raw(&self, frame: Vec<u8>) -> Result<(), ()> {
        self.inner.send_tx.send(frame).await.map_err(|_| ())
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
}
