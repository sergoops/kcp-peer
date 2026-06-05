# kcp-peer

Symmetric P2P transport layer around [KCP](https://github.com/skywind3000/kcp) using a single UDP socket.

## Features

- **Symmetric design** — no client/server distinction; every node is identical.
- **Single UDP socket** — all traffic (all peers) shares one bound socket.
- **Auto-initiate** — first `send()` to an unknown peer triggers an automatic handshake.
- **Crash recovery** — when a peer restarts on the same address, the other side detects it via incarnation mismatch and fires `PeerRestarted`.
- **Event-driven API** — `Connected`, `Disconnected`, `Data`, `PeerRestarted` via broadcast channel.
- **Tokio integration** — `KcpConnection` implements `AsyncRead` + `AsyncWrite` for use with tokio framing utilities.
- **Fully configurable** — all KCP parameters exposed via builder.

## Quick Start

```rust,no_run
# use std::error::Error;
# use kcp_peer::{KcpConfig, KcpPeer, Event};
# async fn example() -> Result<(), Box<dyn Error>> {
let config = KcpConfig::builder()
    .tick_interval(std::time::Duration::from_millis(10))
    .build();

let a = KcpPeer::bind_with("127.0.0.1:0", config.clone()).await?;
let b = KcpPeer::bind_with("127.0.0.1:0", config).await?;

let mut events = b.events();

a.send(b.local_addr(), b"hello").await?;

// B receives Connected then Data
match events.recv().await? {
    Event::Connected(addr) => println!("connected {addr}"),
    _ => {}
}
match events.recv().await? {
    Event::Data(addr, data) => println!("got {:?} from {addr}", &data),
    _ => {}
}
# Ok(())
# }
```

## API Overview

### `KcpPeer`

Main handle. Binds a UDP socket and spawns background receive + update tasks. Clone-friendly (Arc internals).

| Method | Description |
|--------|-------------|
| `bind(addr)` | Bind with default config |
| `bind_with(addr, config)` | Bind with custom config |
| `send(peer, data)` | Send to peer; auto-initiates handshake if needed |
| `connect(peer)` | Initiate (or return existing) `KcpConnection` |
| `events()` | Subscribe to the event broadcast channel |
| `peers()` | List connected peer addresses |
| `stats(peer)` | KCP stats for a specific peer |
| `disconnect(peer)` | Force-close a session |
| `shutdown()` | Graceful shutdown — sends RESET to all peers and awaits bg tasks |
| `local_addr()` | Bound socket address |

Dropping `KcpPeer` cancels background tasks (shutdown token). The UDP socket closes when all references are released.

### `KcpConnection`

An `AsyncRead` + `AsyncWrite` handle wrapping a peer session. Obtained via `connect()`. Composes with tokio framing (`Framed`, `LengthDelimitedCodec`, etc.).

### `Event`

Events delivered via a `tokio::sync::broadcast` channel.

| Variant | Meaning |
|---------|---------|
| `Connected(SocketAddr)` | Handshake completed or inbound connection accepted |
| `Disconnected(SocketAddr)` | Session closed (timeout, RESET received, or `disconnect()`) |
| `Data(SocketAddr, Bytes)` | Application data received from peer |
| `PeerRestarted(SocketAddr)` | Peer restarted — old session was replaced by a new incarnation |

`Data` events are only emitted when `receiver_count() > 0`. If no event subscribers exist, `KcpConnection::poll_read` reads directly from KCP. When subscribers exist, KCP messages are drained into events and `poll_read` returns `Pending`.

### `KcpConfig`

Builder API:

```rust,no_run
# use std::time::Duration;
# use kcp_peer::KcpConfig;
let config = KcpConfig::builder()
    .kcp_nodelay(2, 10, 2, true)
    .tick_interval(Duration::from_millis(10))
    .session_timeout(Duration::from_secs(30))
    .maximum_resend_times(10)
    .build();
```

## Architecture

### Single socket

All peers share one UDP socket. Sessions are identified by remote `SocketAddr` (canonicalized — IPv4-mapped IPv6 addresses are normalized to plain IPv4).

### Background tasks

- **Receive task** — reads UDP packets in a loop, routes them to the correct session or control handler.
- **Update task** — drives KCP timer updates for all sessions, prunes sessions that exceed `session_timeout` or are dead (retransmission exhaustion).

### Handshake protocol

```text
Initiator                    Receiver
   |                            |
   |──── SYN (conv_id) ────────→|
   |                            |  create session (Established)
   |←─── SYN_ACK (conv_id) ─────|
   |                            |
   |  transition to Established |
   |  fire Connected            |
   |                            |
   |──── KCP_DATA ────────────→|
   |                            |  fire Connected
   |                            |  fire Data
```

Auto-initiate: when `send()` is called for an unknown address, a new session is created in `SynSent` state and a SYN is sent. The handshake completes asynchronously; queued data is flushed once the session transitions to `Established`.

### Crash recovery

When a peer restarts on the same address, the new instance generates a random incarnation number. The old instance's session on the remote side still exists. When the new instance sends a SYN:

1. Remote finds the old session → marks it closed → fires `PeerRestarted`
2. Remote creates a new session → fires `Connected`
3. Data flows over the new session

The incarnation number prevents stale SYN packets from interfering with active sessions.

## Configuration Reference

| Field | Default | Description |
|-------|---------|-------------|
| `kcp_interval_ms` | 20 | KCP internal update interval (ms) |
| `kcp_nodelay` | `(1, 20, 2, true)` | `(nodelay, interval, resend, nc)` passed to `kcp_set_nodelay` |
| `snd_wnd` | 128 | Send window size (segments) |
| `rcv_wnd` | 128 | Receive window size (segments) |
| `rx_minrto` | 10 | Minimum RTO (ms) |
| `fast_resend` | 1 | Fast retransmission threshold |
| `maximum_resend_times` | 20 | Max retransmits before dead link |
| `tick_interval` | 10ms | How often the bg update task runs |
| `session_timeout` | 60s | Close idle sessions after this duration |
| `event_channel_capacity` | 1024 | Broadcast channel capacity |

## Examples

See `examples/`:

- **echo** — server echoes data back; client crashes and restarts on the same address, demonstrating `PeerRestarted`.
- **duplex** — bidirectional exchange between two peers.
- **p2p_gossip** — three-node gossip where one node sends to two peers simultaneously.
