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

### Basic send / receive

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

### Error handling with reconnect

```rust,no_run
# use kcp_peer::{KcpConfig, KcpPeer, Error};
# async fn example() {
let peer = KcpPeer::bind_with("127.0.0.1:0", KcpConfig::default()).await.unwrap();
let remote = "127.0.0.1:9000".parse().unwrap();

loop {
    match peer.send(remote, b"ping").await {
        Ok(()) => break,
        Err(Error::DeadLink | Error::SessionClosed) => {
            // Session is gone — next send() will auto-reconnect.
            // Optionally wait before retry to avoid busy-loop.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Err(e) => {
            eprintln!("unexpected error: {e}");
            break;
        }
    }
}
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

- **Receive task** — reads UDP packets in a loop, routes them to the correct session or control handler. Sends `RESET` (3 copies) for unknown sessions.
- **Update task** — drives KCP timer updates for all sessions; retransmits `SYN` with exponential backoff for sessions stuck in handshake; prunes sessions that exceed `session_timeout`, are dead (retransmission exhaustion), or have exhausted SYN retries. Sends `RESET` (3 copies) when pruning, to notify the peer.

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

**Non-blocking semantics:** both `connect()` and `send()` return immediately
after dispatching the SYN — they never wait for the handshake to complete.
Data sent during `SynSent` is buffered in KCP and transmitted automatically
once the session reaches `Established`.

**Buffered data on failure:** if the handshake fails (SYN retries exhausted),
any data that was queued during `SynSent` is dropped silently. The caller is
notified via `Event::Disconnected`. On receiving this event, re-send important
data after the next `Event::Connected`.

**SYN retransmission:** if no `SYN_ACK` arrives, the background update task retransmits the SYN with exponential backoff (base [`syn_retry_interval`](#configuration-reference), doubled each attempt). After [`syn_max_retries`](#configuration-reference) the session is abandoned and `Disconnected` fires.

**SYN_ACK re-send:** if the receiver gets a duplicate SYN for an already-established session (the original SYN_ACK was lost), it re-sends the SYN_ACK.

**Simultaneous handshake:** when both sides initiate at the same time, the tie is broken by comparing socket addresses — the "higher" address wins and the other side adopts its conversation ID.

### Crash recovery

When a peer restarts on the same address, the new instance generates a random incarnation number. The old instance's session on the remote side still exists. When the new instance sends a SYN:

1. Remote finds the old session → marks it closed → fires `PeerRestarted`
2. Remote creates a new session → fires `Connected`
3. Data flows over the new session

The incarnation number prevents stale SYN packets from interfering with active sessions.

## Session Lifecycle

### States

Every peer session follows this state machine:

```text
                         ┌──────────────────────────┐
                         │      No session           │
                         └───────────┬──────────────┘
                                     │ send() / connect()
                                     │ or incoming SYN
                                     v
                         ┌──────────────────────────┐
                  ┌─────│        SynSent            │
                  │     │  (SYN sent, waiting for   │
                  │     │   SYN_ACK or peer's SYN)  │
                  │     └───────────┬──────────────┘
                  │                 │ SYN_ACK / tie-break
                  │                 v
         ┌────────┴────────┐
         │  Established    │
         │ (data can flow) │
         └────────┬────────┘
                  │
        ┌─────────┼──────────┐
        v         v          v
   timeout   DeadLink    RESET / disconnect()
        │         │          │
        └─────────┴──────────┘
                  v
         ┌──────────────────┐
         │  Session removed │  → Event::Disconnected
         │  from session map│
         └──────────────────┘
```

- **SynSent** — handshake in progress. Data is buffered in KCP but not yet sent over the wire.
- **Established** — handshake complete. Data flows freely.
- **Removed** — session is gone. A new `send()` will auto-reconnect.

### When does a session close?

| Cause | Trigger | Event |
|-------|---------|-------|
| Idle timeout | No incoming packets for `session_timeout` (default 60s) | `Disconnected` |
| Dead link | KCP retransmission exhausted (default `maximum_resend_times` = 20) | `Disconnected` |
| Remote RESET | Peer called `shutdown()` or dropped its `KcpPeer` | `Disconnected` |
| Explicit disconnect | Local call to `disconnect()` | `Disconnected` |
| Crash recovery | Peer restarts on the same address (new incarnation) | `PeerRestarted` then `Connected` |

### How to reconnect

In all cases except crash recovery, simply call `send()` or `connect()` again —
the transport will automatically initiate a fresh handshake.

## Error Handling

Most operations that can fail return a [`Result`](https://doc.rust-lang.org/std/result/enum.Result.html)
with [`kcp_peer::Error`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html).
Non-fatal errors are safe to handle by re-sending the data.

| Error | When it happens | Recovery |
|-------|----------------|----------|
| [`DeadLink`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html#variant.DeadLink) | KCP retransmission exhausted — peer unreachable | Call `send()` again; auto-reconnects |
| [`SessionClosed`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html#variant.SessionClosed) | Session was already removed (timeout, RESET, disconnect) | Same — re-send triggers new handshake |
| [`SessionNotFound`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html#variant.SessionNotFound) | [`stats()`](https://docs.rs/kcp-peer/latest/kcp_peer/struct.KcpPeer.html#method.stats) called for an unknown peer | The session may have timed out; check the address |
| [`ShuttingDown`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html#variant.ShuttingDown) | [`send()`](https://docs.rs/kcp-peer/latest/kcp_peer/struct.KcpPeer.html#method.send) after [`shutdown()`](https://docs.rs/kcp-peer/latest/kcp_peer/struct.KcpPeer.html#method.shutdown) | Create a new `KcpPeer` |
| [`EventChannelLagged`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html#variant.EventChannelLagged) | Event consumer too slow | Increase `event_channel_capacity` or poll faster |
| [`Io`](https://docs.rs/kcp-peer/latest/kcp_peer/enum.Error.html#variant.Io) | UDP socket bind / send / recv failure | Usually fatal; check address/port |

### Observing disconnections via events

Subscribe to the event channel to be notified when sessions are established
or closed:

```rust,no_run
# use kcp_peer::{KcpPeer, KcpConfig, Event};
# async fn example() {
# let peer = KcpPeer::bind_with("127.0.0.1:0", KcpConfig::default()).await.unwrap();
let mut rx = peer.events();
loop {
    match rx.recv().await {
        Ok(Event::Connected(addr)) => println!("connected {addr}"),
        Ok(Event::Disconnected(addr)) => println!("disconnected {addr}"),
        Ok(Event::PeerRestarted(addr)) => println!("peer {addr} restarted, reconnecting"),
        Ok(Event::Data(addr, data)) => println!("{data:?} from {addr}"),
        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
            eprintln!("dropped {n} events — consumer too slow");
        }
        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
    }
}
# }
```

## Cancel Safety

All public async APIs in `kcp-peer` are **cancel-safe** — dropping the future
mid-execution never leaves internal state inconsistent or leaks resources:

| API | Why it's safe |
|-----|---------------|
| `bind_with()` | Only one await (`UdpSocket::bind`); cancellation before completion creates no state. |
| `send()` / `connect()` | Session is created and inserted into the map in synchronous code after the only await point. If cancelled during `initiate_session`, no session record is created. |
| `shutdown()` | RESET packets are sent synchronously before the first await. The cancellation token is already fired when the await runs, so background tasks still exit promptly. |
| `events()` → `recv()` | `broadcast::Receiver::recv` is cancel-safe (tokio guarantee). Dropping the future does not consume the event. |
| `KcpConnection::poll_*` | All methods are synchronous (`Poll::Ready`). No waker registration that would leave dangling state on drop. |

No special combinator usage is required on your side — `tokio::select!`,
`join!`, `spawn`, or direct `.await` all work correctly.

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
| `syn_retry_interval` | 150ms | Base interval for SYN retry exponential backoff |
| `syn_max_retries` | 5 | Max SYN retransmits before DeadLink (~4.7s total) |

## Examples

See `examples/`:

- **echo** — server echoes data back; client crashes and restarts on the same address, demonstrating `PeerRestarted`.
- **duplex** — bidirectional exchange between two peers.
- **p2p_gossip** — three-node gossip where one node sends to two peers simultaneously.
