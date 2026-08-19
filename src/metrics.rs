// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use metrics::{Counter, counter};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::Arc;

#[derive(Debug)]
pub struct BridgeMetrics {
    // Connections
    pub connections_opened: Counter,
    pub connections_closed: Counter,

    // Traffic
    pub frames_received: Counter,
    pub frames_executed: Counter,
    pub bytes_received: Counter,
    pub bytes_sent: Counter,

    // Keepalives (inbound only – we do not send them)
    pub keepalives_received: Counter,

    // Errors & robustness
    pub client_errors: Counter,  // parse / protocol / oversized frames
    pub execute_errors: Counter, // cluster.execute failed
    pub timeouts: Counter,       // idle or frame timeout
    pub resyncs: Counter,        // recovered from bad magic / garbage
}

impl BridgeMetrics {
    pub fn new() -> Self {
        Self {
            connections_opened: counter!("rzbridge_connections_opened_total"),
            connections_closed: counter!("rzbridge_connections_closed_total"),

            frames_received: counter!("rzbridge_frames_received_total"),
            frames_executed: counter!("rzbridge_frames_executed_total"),
            bytes_received: counter!("rzbridge_bytes_received_total"),
            bytes_sent: counter!("rzbridge_bytes_sent_total"),

            keepalives_received: counter!("rzbridge_keepalives_received_total"),

            client_errors: counter!("rzbridge_client_errors_total"),
            execute_errors: counter!("rzbridge_execute_errors_total"),
            timeouts: counter!("rzbridge_timeouts_total"),
            resyncs: counter!("rzbridge_resyncs_total"),
        }
    }
}

pub struct Metrics {
    pub bridge: BridgeMetrics,
    pub prometheus_handle: PrometheusHandle,
}

impl Metrics {
    pub fn new() -> Self {
        let prometheus_handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("Failed to install Prometheus recorder");

        Self {
            bridge: BridgeMetrics::new(),
            prometheus_handle,
        }
    }
}

/// Shared handle used throughout the process
pub type SharedMetrics = Arc<Metrics>;
