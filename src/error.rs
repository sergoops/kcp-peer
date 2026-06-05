use std::io;
use std::net::SocketAddr;

/// Errors returned by kcp-peer operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// UDP socket I/O error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// KCP protocol error.
    #[error("KCP error: {0}")]
    Kcp(#[from] kcp::Error),

    /// No session found for the requested peer address.
    #[error("session with {0} not found")]
    SessionNotFound(SocketAddr),

    /// KCP retransmission limit reached — peer is unreachable.
    #[error("KCP connection is dead — no ACK from peer")]
    DeadLink,

    /// The transport is shutting down and cannot accept new work.
    #[error("transport is shutting down")]
    ShuttingDown,

    /// Event channel consumer is too slow; events were dropped.
    #[error("event channel lagged, dropped {0} events")]
    EventChannelLagged(u64),

    /// The session has been closed and cannot send data.
    #[error("session is closed")]
    SessionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
