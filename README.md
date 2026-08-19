# RzBridge

A purpose-built bridge connecting the [Roomzin](https://m-javani.github.io/roomzin-doc/) routing layer to individual Roomzin shards. It handles the final hop from the routing tier to the database tier, routing requests to the correct node within a shard based on whether the operation is a read or write.

## What It Does

RzBridge is shard-aware. It knows which Roomzin nodes belong to the shard and which node is currently the leader.

When a request arrives, the bridge uses the request's routing information to send:

- **Writes** to the shard leader
- **Reads** to an appropriate follower

Bridges can be deployed with multiple instances for high availability — each bridge instance connects to the same shard. The shard remains single, while bridge instances provide redundancy and load distribution.

## How It Fits in Roomzin

```
Client SDK ──┐
             │
HTTP Proxy ──┼──► Edge Router ──► Zone Router ──► Bridge ──► Roomzin Shard
             │        │               │              │            │
Other SDKs ──┘        │               │              │            │
                      ▼               ▼              ▼            ▼
                    RzID ◄────────────────────────────┘            │
                    (Service Registry)                            │
                                                                   │
                                              ┌────────────────────┼────────────────────┐
                                              │                    │                    │
                                              ▼                    ▼                    ▼
                                          Leader             Follower            Follower
                                         (writes)            (reads)              (reads)
```

### Request Flow

1. **Request arrives** from Zone Router
2. **Bridge** reads the shard ID and determines if it's a read or write operation
3. **For writes**: Bridge forwards to the shard leader
4. **For reads**: Bridge forwards to an appropriate follower
5. **Response** flows back through the bridge to the client

Zone routers only need to know which bridge handles a shard. The bridge handles the internal shard topology.

## Why It Exists

Roomzin shards have internal Raft-based cluster topology with leaders and followers. The routing layer above the bridge should not need to know:

- Which node is the current leader
- Which nodes are followers
- How many followers exist
- Replication configuration

RzBridge abstracts all of this. The bridge maintains connection pools to all nodes in the shard and routes accordingly.

This also means topology changes (leader elections, node failures, scaling followers) are isolated within the bridge. Routers do not need to be reconfigured.

## Infrastructure Independence

The bridge architecture decouples identities from infrastructure:

- **RzID** maintains the logical topology (zones, shards, nodes)
- **RzPoint** resolves logical node IDs to actual hostnames - this is a service implemented by the company based on their own infrastructure
- **Bridges** only need to know logical node IDs

Infrastructure changes (new instances, IP changes, scaling) do not require bridge reconfiguration. The resolver service translates IDs to the current infrastructure state.

## Components

### RzID (Service Registry)
Central source of truth for shard topology. Stores which nodes belong to which shard, which node is the leader, and which bridges are active.

### RzPoint (Resolver)
Translates component IDs (node IDs, bridge IDs) to actual hostnames. Decouples routing logic from infrastructure details.

### RzBridge
- Maintains connection pools to all nodes in the shard
- Routes writes to the leader
- Routes reads to followers
- Handles shard topology changes automatically
- Exposes health and metrics endpoints

## Running a Bridge

### Prerequisites

- **RzID** service must be running and reachable
- **RzPoint** resolver must be running and reachable

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--zone-id` | Zone identifier for this bridge instance |
| `--shard-id` | Shard identifier — each bridge connects to exactly one shard |
| `--bridge-id` | Unique identifier for this bridge instance |
| `--rzid-addr` | RzID service address (e.g., `http://rzid.internal:8080`) |
| `--rzpoint-addr` | RzPoint resolver address (e.g., `http://rzpoint.internal:8080`) |

### Example

```bash
./rzbridge \
  --zone-id zone1 \
  --shard-id shard1 \
  --bridge-id bridge-1 \
  --rzid-addr http://rzid.internal:8080 \
  --rzpoint-addr http://rzpoint.internal:8080
```

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--listen-host` | `0.0.0.0` | TCP listen address |
| `--listen-port` | `9000` | TCP listen port |
| `--api-listen-addr` | `0.0.0.0:9100` | HTTP API (metrics + health) |
| `--roomzin-tcp-port` | `7777` | Roomzin node TCP port |
| `--roomzin-api-port` | `8080` | Roomzin node API port |
| `--conn-per-roomzin-node` | `1` | Connections per Roomzin node |
| `--max-connections` | `10000` | Maximum concurrent connections |
| `--timeout-sec` | `2` | Request timeout |
| `--rzid-heartbeat-interval-secs` | `10` | Heartbeat interval to RzID |

For a complete list of all available options:

```bash
./rzbridge --help
```

## Deployment

### Single Bridge Instance
- One bridge instance connects to a single shard
- Simple deployment for development or low-traffic environments

### High Availability
- Multiple bridge instances can connect to the same shard
- Zone routers distribute requests across available bridge instances
- Provides redundancy and load distribution

```
         ┌─────────────┐
         │ Zone Router │
         └──────┬──────┘
                │
    ┌───────────┼───────────┐
    │           │           │
    ▼           ▼           ▼
┌──────┐   ┌──────┐   ┌──────┐
│Bridge│   │Bridge│   │Bridge│
│  -1  │   │  -2  │   │  -3  │
└──┬───┘   └──┬───┘   └──┬───┘
   │          │          │
   └──────────┼──────────┘
              ▼
        ┌──────────┐
        │  Shard   │
        │ (shard1) │
        └──────────┘
```

### Registration
- Bridges register with RzID with their `bridge_id`, `shard_id`, and `zone_id`
- RzID tracks which bridges are active and can serve which shards

### Topology Discovery
Bridges discover shard topology from RzID, then fetch detailed node information directly from the shard itself:
1. Bridge starts and registers with RzID
2. Bridge queries RzID for the shard's node list
3. Bridge requests detailed node information from the shard API
4. Bridge establishes connection pools to all nodes
5. Bridge monitors for topology changes via RzID and re-fetches from the shard as needed

## Monitoring

Bridges expose:

- **Logs**: Structured JSON logs via `RUST_LOG` environment variable
- **Metrics**: Prometheus metrics available at `/metrics` endpoint on the API port
- **Health**: `/health` endpoint for readiness/liveness probes

---

## Contributing

Contributions are welcome!

Please open an issue before proposing large changes. All contributions are subject to the BUSL-1.1 License terms.

---

## License

This project is licensed under the [BUSL-1.1 License](LICENSE).

**Note:** RzBridge is designed to communicate with Roomzin Server, which requires a valid Roomzin license.

---

## Support

- **Community Q&A**: [GitHub Discussions](https://github.com/m-javani/roomzin-doc/discussions)
- **Issues**: [GitHub Issues](https://github.com/m-javani/rzbridge/issues)

---

## Related Repositories

- [Roomzin](https://m-javani.github.io/roomzin-doc/) - Roomzin Documents
- [RzRouter](https://github.com/m-javani/rzrouter) - Routing fabric
- [RzID](https://github.com/m-javani/rzid) - Roomzin Service Registry
- [RzProxy](https://github.com/m-javani/rzproxy) - HTTP/JSON proxy
- [Roomzin Quickstart](https://github.com/m-javani/roomzin-quickstart) — Local Docker cluster
- [Roomzin Bench](https://github.com/m-javani/roomzin-bench) — Benchmarking tool