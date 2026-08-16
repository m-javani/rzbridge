use rzbridge::error::RZError;
use std::io;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use bytes::BytesMut;

use crate::common::command::drain_frame_async;

pub struct TestClient {
    stream: TcpStream,
    read_buf: BytesMut,
    timeout: Duration,
}

impl TestClient {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(4096),
            timeout: Duration::from_secs(5),
        })
    }

    pub async fn send(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn receive(&mut self) -> Result<Vec<u8>, RZError> {
        // Use drain_frame_async to parse the frame
        let (_clr_id, payload) = timeout(self.timeout, async {
            drain_frame_async(&mut self.stream, &mut self.read_buf).await
        })
        .await
        .map_err(|_| RZError::Timeout)??;

        Ok(payload.to_vec())
    }

    pub async fn send_and_receive(&mut self, data: &[u8]) -> Result<Vec<u8>, RZError> {
        self.send(data)
            .await
            .map_err(|e| RZError::System(format!("Send error: {}", e)))?;
        self.receive().await
    }

    #[allow(unused)]
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}
