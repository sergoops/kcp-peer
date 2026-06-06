# kcp-peer — agent guide

## Build & test

All commands are standard `cargo`:

```sh
cargo build
cargo test                  # all integration tests (26 tests, all #[tokio::test])
cargo test basic_send_recv  # single test by function name
cargo clippy                # no custom config — uses cargo defaults
cargo fmt                   # no custom config — uses cargo defaults
```

## Local path dependency

`Cargo.toml` references `kcp = { path = "../kcp" }`. The crate **will not build** without its sibling `../kcp` directory present and buildable.

## Tests

- Single integration test file: `tests/integration.rs`
- All async tests use `#[tokio::test]`
- No flaky or expensive test suites; the full suite runs in a few seconds
- Tests bind ephemeral ports on `127.0.0.1` — no external services required

## Project structure

| Path | Purpose |
|------|---------|
| `src/lib.rs` | Re-exports; crate doc from `README.md` via `#![doc = include_str!("../README.md")]` |
| `src/config.rs` | `KcpConfig` / `KcpConfigBuilder` |
| `src/error.rs` | `Error` enum (thiserror) |
| `src/packet.rs` | Wire protocol (Syn, SynAck, KcpData, Reset) |
| `src/session.rs` | Per-peer session state machine |
| `src/transport.rs` | Main `KcpPeer` handle, background tasks, event loop |
| `examples/echo.rs` | Crash recovery demo |
| `examples/duplex.rs` | Simultaneous handshake (tie-breaking) demo |
| `examples/p2p_gossip.rs` | 3-node gossip |

## Key conventions

- **Edition 2021**, no `rust-toolchain.toml` — uses whatever `rustc` is on `$PATH`
- No `rustfmt.toml` / `clippy.toml` — all defaults
- No CI workflows
- `KcpPeer` is `Send + Sync`; all public async methods are documented as cancel-safe
- `CanonicalAddr` normalizes IPv4-mapped IPv6 → plain IPv4 for session keys
