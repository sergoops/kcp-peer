use std::io;
use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("KCP error: {0}")]
    Kcp(#[from] kcp::Error),

    #[error("session with {0} not found")]
    SessionNotFound(SocketAddr),

    #[error("KCP connection is dead — no ACK from peer")]
    DeadLink,

    #[error("transport is shutting down")]
    ShuttingDown,

    #[error("event channel lagged, dropped {0} events")]
    EventChannelLagged(u64),

    #[error("session is closed")]
    SessionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
