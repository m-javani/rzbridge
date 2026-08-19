// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::connection::Connection;
use crate::demux::DemuxMap;
use crate::error::RZError;
use crate::topology::TopologyDiscovery;

#[derive(Clone)]
pub struct ClusterHandler {
    inner: Arc<HandlerInner>,
}

#[derive(Default)]
struct LoadBalancer {
    follower_history: HashMap<(String, u32), u32>, // node id, conn id
    last_node_id: u32,
    last_leader_id: u32,
}

struct HandlerInner {
    cfg: Config,
    topology_discovery: TopologyDiscovery,
    pub leader_conns: Arc<RwLock<Vec<Option<Connection>>>>,
    pub leader_host: ArcSwap<Option<String>>,
    followers: Arc<RwLock<HashMap<(String, u32, u32), Connection>>>, // node_name, node idx, conn idx
    load_balancer: Arc<Mutex<LoadBalancer>>,                         // node_id, conn_id
    cancel_token: CancellationToken,
    syncing: AtomicBool,
}

impl ClusterHandler {
    pub fn new(
        cfg: Config,
        cancel_token: CancellationToken,
        topology_discovery: TopologyDiscovery,
    ) -> Arc<Self> {
        let conn_per_roomzin_node = cfg.conn_per_roomzin_node.clone();
        let leader_vec: Vec<Option<Connection>> = vec![None; conn_per_roomzin_node];

        let handler = Arc::new(Self {
            inner: Arc::new(HandlerInner {
                cfg: cfg.clone(),
                leader_conns: Arc::new(RwLock::new(leader_vec)),
                leader_host: ArcSwap::new(Arc::new(None)),
                followers: Arc::new(RwLock::new(HashMap::new())),
                load_balancer: Arc::new(Mutex::new(LoadBalancer::default())),
                cancel_token: cancel_token.clone(),
                topology_discovery,
                syncing: AtomicBool::new(false),
            }),
        });

        let h = handler.clone();
        tokio::spawn(async move { h.sync_task().await });

        handler
    }

    async fn sync_task(self: Arc<Self>) {
        let mut fast = interval(Duration::from_millis(300));
        let mut slow = interval(Duration::from_secs(self.inner.cfg.node_probe_interval_sec));

        loop {
            let self_clone = self.clone();
            let cancel = self_clone.inner.cancel_token.clone();
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("Handler sync cancelled");
                    break;
                }

                _ = fast.tick() => {
                    let any_leader_cons = self_clone.inner.leader_conns.read().await.iter().any(|c| match c {
                        Some(con) => !con.is_closed(),
                        None => false,
                    });

                    let any_follower_cons = self.inner.followers.read().await.values().any(|x| !x.is_closed());

                    if !any_leader_cons || !any_follower_cons{
                        self_clone.sync_with_cluster().await;
                    }
                }

                _ = slow.tick() => {
                    self_clone.sync_with_cluster().await;
                }
            }
        }
    }

    async fn sync_with_cluster(self: Arc<Self>) {
        // already running → skip
        if self.inner.syncing.swap(true, Ordering::SeqCst) {
            return;
        }

        let result = self.inner.topology_discovery.discover().await;
        match result {
            Ok(topo) => {
                self.clone().sync_leader(topo.leader).await;
                self.sync_followers(topo.followers).await;
            }
            Err(e) => {
                tracing::debug!("failed to discover topology: {:?}", e);
            }
        }

        self.inner.syncing.store(false, Ordering::SeqCst);
    }

    async fn sync_leader(self: Arc<Self>, leader_host: String) {
        // 1. Detect leader change
        let current = self.inner.leader_host.load();
        let leader_changed = match current.as_ref() {
            Some(old) => old.as_str() != leader_host.as_str(),
            None => true,
        };

        if leader_changed {
            tracing::info!(
                old = ?current.as_ref(),
                new = %leader_host,
                "leader host changed — resetting leader connections"
            );

            // Drop all existing leader connections (they point at the old host).
            // In-flight requests on them will fail and be retried by `execute`.
            {
                let mut conns = self.inner.leader_conns.write().await;
                for slot in conns.iter_mut() {
                    *slot = None; // drops the Connection
                }
            }
            self.inner
                .leader_host
                .store(Arc::new(Some(leader_host.clone())));
        }

        // 2. Fill any empty / closed slots (same as before)
        let required_vec: Vec<usize> = self
            .inner
            .leader_conns
            .read()
            .await
            .iter()
            .enumerate()
            .filter(|(_, x)| match x {
                Some(c) => c.is_closed(),
                None => true,
            })
            .map(|(i, _)| i)
            .collect();

        for i in required_vec {
            let demux = DemuxMap::new();
            match Connection::connect(leader_host.clone(), &self.inner.cfg, demux).await {
                Ok(conn) => {
                    self.inner.leader_conns.write().await[i] = Some(conn);
                    // keep the stored host in sync even if it was already correct
                    self.inner
                        .leader_host
                        .store(Arc::new(Some(leader_host.clone())));
                }
                Err(e) => {
                    tracing::debug!("failed to connect to leader {}: {:?}", leader_host, e);
                }
            }
        }
    }

    async fn sync_followers(&self, followers: Vec<String>) {
        let mut required_conns: HashMap<String, u32> = HashMap::new(); // node, required connections
        let cur_node_ids: Vec<(String, u32)> = self
            .inner
            .followers
            .read()
            .await
            .keys()
            .map(|(name, idx, _)| (name.clone(), idx.clone()))
            .collect();

        let conn_per_roomzin_node = self.inner.cfg.conn_per_roomzin_node;
        for name in &followers {
            if !cur_node_ids.iter().any(|(x, _)| x == name) {
                required_conns.insert(name.clone(), conn_per_roomzin_node as u32);
            }
        }
        for (i, _) in cur_node_ids.iter() {
            let required =
                conn_per_roomzin_node - cur_node_ids.iter().filter(|(x, _)| x == i).count();
            if required > 0 {
                required_conns.insert(i.clone(), required as u32);
            }
        }
        let mut new_conns: HashMap<String, Vec<Option<Connection>>> = HashMap::new();
        for (host, &count) in required_conns.iter() {
            for _i in 0..count {
                match Connection::connect(host.clone(), &self.inner.cfg, DemuxMap::new()).await {
                    Ok(conn) => {
                        new_conns
                            .entry(host.clone())
                            .or_insert_with(|| vec![])
                            .push(Some(conn));
                    }
                    Err(e) => {
                        tracing::debug!("connection error: {}", e);
                    }
                }
            }
        }

        let mut follower_nodes = self.inner.followers.write().await;
        let cur_node_ids: Vec<u32> = follower_nodes
            .iter()
            .map(|((_, idx, _), _)| idx)
            .cloned()
            .collect();
        let mut free_node_ids: Vec<u32> = vec![];
        for i in 0..followers.len() as u32 {
            if !cur_node_ids.contains(&i) && !free_node_ids.contains(&i) {
                free_node_ids.push(i);
            }
        }
        let mut new_assigned_node_ids: HashMap<String, u32> = HashMap::new();
        for key in self
            .inner
            .load_balancer
            .lock()
            .await
            .follower_history
            .keys()
        {
            new_assigned_node_ids.insert(key.0.clone(), key.1);
        }

        for (name, mut conns) in new_conns.into_iter() {
            let cur_conn_ids: Vec<u32> = follower_nodes
                .iter()
                .filter(|((n, _, _), c)| name == *n && !c.is_closed())
                .map(|((_, _, idx), _)| idx)
                .cloned()
                .collect();
            let mut free_conn_ids: Vec<u32> = vec![];
            for i in 0..self.inner.cfg.conn_per_roomzin_node as u32 {
                if !cur_conn_ids.contains(&i) {
                    free_conn_ids.push(i);
                }
            }

            let mut nidx = follower_nodes
                .iter()
                .find(|((n, _, _), _)| name == *n)
                .map(|((_, nidx, _), _)| nidx)
                .cloned()
                .unwrap_or(0); // take from indexes not in cur_node_ids
            if nidx == 0 {
                match new_assigned_node_ids.get(&name) {
                    Some(idx) => {
                        nidx = *idx;
                    }
                    None => {
                        if free_node_ids.len() > 0 {
                            let idx = free_node_ids.pop().unwrap_or_default();
                            new_assigned_node_ids.insert(name.clone(), idx);
                            nidx = idx;
                        } else {
                            continue;
                        }
                    }
                }
            }

            for i in 0..conns.len() {
                let c = match conns[i].take() {
                    Some(cn) => cn,
                    None => {
                        continue;
                    }
                };

                follower_nodes.insert((name.clone(), nidx, free_conn_ids[i]), c);
                self.inner
                    .load_balancer
                    .lock()
                    .await
                    .follower_history
                    .entry((name.clone(), nidx))
                    .or_insert(free_conn_ids[i]);
            }
        }
    }

    async fn next_follower_conn(&self) -> Option<Connection> {
        let len = self.inner.load_balancer.lock().await.follower_history.len();

        for _i in 1..=len {
            let mut lb = self.inner.load_balancer.lock().await;
            let mut next_node_indx = (lb.last_node_id + 1) as usize;
            if next_node_indx >= lb.follower_history.len() {
                next_node_indx = 0;
                lb.last_node_id = 0;
            }
            lb.last_node_id = next_node_indx as u32;

            let target = lb
                .follower_history
                .iter()
                .find(|&((_, nidx), _)| *nidx == next_node_indx as u32);
            if target.is_none() {
                continue;
            }

            let target = target.unwrap();
            let mut next_conn_id = *target.1;
            if next_conn_id >= self.inner.cfg.conn_per_roomzin_node as u32 {
                next_conn_id = 0;
            }
            let mut tried = 0;
            let mut followers = self.inner.followers.write().await;

            while tried < self.inner.cfg.conn_per_roomzin_node {
                let key: (String, u32, u32) = (target.0.0.clone(), target.0.1, next_conn_id);
                tried += 1;
                let mut closed = false;
                if let Some(c) = followers.get(&key) {
                    if c.is_closed() {
                        c.inner
                            .demux
                            .cleanup(Duration::from_secs(self.inner.cfg.timeout_sec * 2))
                            .await;
                        closed = true;
                    } else {
                        lb.last_node_id = target.0.1;
                        lb.follower_history
                            .entry((key.0, key.1))
                            .and_modify(|counter| {
                                *counter =
                                    (next_conn_id + 1) % self.inner.cfg.conn_per_roomzin_node as u32
                            })
                            .or_insert(1);
                        return Some(c.clone());
                    }
                }
                if closed == true {
                    followers.remove(&key);
                }
                next_conn_id += 1;
                if next_conn_id >= self.inner.cfg.conn_per_roomzin_node as u32 {
                    next_conn_id = 0;
                }
            }
        }
        None
    }

    async fn next_leader_conn(&self) -> Option<Connection> {
        let mut lb = self.inner.load_balancer.lock().await;
        let mut next = (lb.last_leader_id + 1) as usize;
        if next >= self.inner.cfg.conn_per_roomzin_node {
            next = 0;
        }
        let ld_vec = self.inner.leader_conns.write().await;
        if ld_vec.is_empty() || !ld_vec.iter().any(|x| x.is_some()) {
            return None;
        }
        for _i in 0..self.inner.cfg.conn_per_roomzin_node.min(ld_vec.len()) {
            let c = ld_vec.get(next).and_then(|x| x.as_ref());
            if c.is_none() {
                next += 1;
                if next >= self.inner.cfg.conn_per_roomzin_node {
                    next = 0;
                }
                continue;
            }
            let c = c.unwrap();
            if c.is_closed() {
                c.inner
                    .demux
                    .cleanup(Duration::from_secs(self.inner.cfg.timeout_sec * 2))
                    .await;
                let _ = ld_vec.get(next).take();
                next += 1;
                if next >= self.inner.cfg.conn_per_roomzin_node {
                    next = 0;
                }
                continue;
            }
            lb.last_leader_id = next as u32;
            return Some(c.clone());
        }

        None
    }

    /// Execute a request.
    /// `shard_frame` must be a complete Roomzin frame starting with 0xFF.
    /// We rewrite its clrid for demux, then restore the original clrid on the response.
    pub async fn execute(
        &self,
        is_write: bool,
        mut shard_frame: Vec<u8>,
    ) -> Result<Bytes, RZError> {
        if shard_frame.len() < 9 || shard_frame[0] != 0xFF {
            return Err(RZError::Validation("invalid shard frame".into()));
        }

        let original_clrid = u32::from_le_bytes(shard_frame[1..5].try_into().unwrap());

        let mut attempts = 0u32;
        loop {
            let conn = if is_write {
                match self.next_leader_conn().await {
                    Some(c) if !c.is_closed() => c,
                    _ => {
                        attempts += 1;
                        if attempts >= 3 {
                            return Err(RZError::NoLeaderAvailable);
                        }
                        sleep(Duration::from_millis(50 * (attempts as u64 + 1))).await;
                        continue;
                    }
                }
            } else {
                match self.next_follower_conn().await {
                    Some(c) if !c.is_closed() => c,
                    _ => {
                        attempts += 1;
                        if attempts >= 3 {
                            return Err(RZError::NoFollowerNodeAvailable);
                        }
                        sleep(Duration::from_millis(50 * (attempts as u64 + 1))).await;
                        continue;
                    }
                }
            };

            // Assign a bridge-local corr_id and patch the frame in-place.
            let new_clrid = conn.next_corr_id();
            shard_frame[1..5].copy_from_slice(&new_clrid.to_le_bytes());

            let (resp_tx, resp_rx) = oneshot::channel();
            let now = Instant::now();
            conn.inner.demux.store(new_clrid, resp_tx, now).await;

            // Send the ready frame (no extra header).
            if conn.send_raw(shard_frame.clone()).await.is_err() {
                attempts += 1;
                if attempts >= 3 {
                    return Err(RZError::Timeout);
                }
                sleep(Duration::from_millis(50 * (attempts as u64 + 1))).await;
                continue;
            }

            match resp_rx.await {
                Ok(response) => {
                    if response.len() >= 9 {
                        let mut bytes = BytesMut::from(response.as_ref());
                        bytes[1..5].copy_from_slice(&original_clrid.to_le_bytes());
                        return Ok(bytes.freeze()); // Zero-copy: converts to Bytes
                    }
                    return Ok(response);
                }
                Err(_) => {
                    // oneshot dropped → timeout / connection closed
                    attempts += 1;
                    if attempts >= 3 {
                        return Err(RZError::Timeout);
                    }
                    sleep(Duration::from_millis(50 * (attempts as u64 + 1))).await;
                }
            }
        }
    }
}
