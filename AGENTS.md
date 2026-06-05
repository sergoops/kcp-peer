# AGENTS.md

## Commands

Run from `kcp-peer/`:

| Action | Command |
|--------|---------|
| Test all | `cargo test` |
| Single test | `cargo test <name>` (e.g. `cargo test crash_initiator`) |
| Example | `cargo run --example <echo\|duplex\|p2p_gossip>` |

No CI, no rustfmt/clippy config, no rust-toolchain. Tests are `#[tokio::test]`; they bind ephemeral loopback ports — no services needed.

## Dependency

`kcp-peer` depends on `../kcp` via path. The parent directory has no Cargo workspace — each crate is built independently. If you change `kcp`, build/test from `kcp/` first, then from `kcp-peer/`.

## Key source files

| File | Role |
|------|------|
| `src/transport.rs` | `KcpPeer`, `Event`, receive/update background tasks, session map |
| `src/session.rs` | Session state machine (SynSent → Established → removed) |
| `src/config.rs` | `KcpConfig` builder |
| `src/packet.rs` | Wire packet types (SYN, SYN_ACK, KCP_DATA, RESET) |
| `src/error.rs` | `Error` enum |
| `src/lib.rs` | Public re-exports only |
| `tests/integration.rs` | All integration tests |

## Gotchas

- **`Data` events only fire when `receiver_count() > 0`.** Subscribe to `events()` *before* sending data to avoid missing events.
- **All public async APIs are cancel-safe.** `tokio::select!`, `spawn`, `.await` all work safely.
- **`send()` to unknown peer auto-initiates handshake.** Data is buffered in KCP until Established. If handshake fails, buffered data is dropped silently — caller sees `Event::Disconnected`.
- **Crash recovery** uses random incarnation numbers. A peer restarting on the same `SocketAddr` triggers `PeerRestarted` on the remote side, then `Connected`.
- **IPv4-mapped IPv6** addresses are normalized to plain IPv4 for session lookup.

## Parent context

See `/compare/AGENTS.md` for notes on the `kcp` crate API quirks (nodelay params, `set_wndsize` order, `fastack-conserve` feature).
