use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, oneshot};

pub struct DemuxMap {
    entries: RwLock<HashMap<u32, (oneshot::Sender<Bytes>, Instant)>>,
}

impl DemuxMap {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
        })
    }

    pub async fn store(&self, corr_id: u32, tx: oneshot::Sender<Bytes>, sent_at: Instant) {
        let mut map = self.entries.write().await;
        map.insert(corr_id, (tx, sent_at));
    }

    pub async fn load_remove(&self, corr_id: u32) -> Option<(oneshot::Sender<Bytes>, Instant)> {
        let mut map = self.entries.write().await;
        map.remove(&corr_id)
    }

    pub async fn cleanup(&self, max_age: Duration) {
        let threshold = Instant::now() - max_age;
        let mut map = self.entries.write().await;

        let mut timed_out = Vec::new();
        for (&id, (_, sent_at)) in map.iter() {
            if *sent_at < threshold {
                timed_out.push(id);
            }
        }

        for id in timed_out {
            if let Some((tx, _)) = map.remove(&id) {
                // Send a dummy timeout response (or proper error payload)
                let _ = tx.send(Bytes::new()); // or Bytes::from_static(b"timeout")
            }
        }
    }
}
