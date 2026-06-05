use std::io;
use std::net::SocketAddr;

/// Errors returned by kcp-peer operations.
///
/// # Recovery guide
///
/// | Error | When it happens | How to recover |
/// |-------|----------------|----------------|
/// | [`Io`](Error::Io) | UDP socket bind / send / recv fails | Check address/port availability. Usually fatal for the transport. |
/// | [`Kcp`](Error::Kcp) | KCP protocol error (rare) | Log and investigate; typically indicates a bug or corrupted data. |
/// | [`SessionNotFound`](Error::SessionNotFound) | [`stats()`](crate::KcpPeer::stats) for an unknown peer | The session may have timed out or never existed. |
/// | [`DeadLink`](Error::DeadLink) | [`send()`](crate::KcpPeer::send) while KCP retransmissions are exhausted | The peer is unreachable or crashed without RESET. Call [`send()`](crate::KcpPeer::send) again to re-initiate a handshake. |
/// | [`ShuttingDown`](Error::ShuttingDown) | [`send()`](crate::KcpPeer::send) after [`shutdown()`](crate::KcpPeer::shutdown) | Create a new [`KcpPeer`](crate::KcpPeer) if the transport should continue. |
/// | [`EventChannelLagged`](Error::EventChannelLagged) | Slow consumer on [`events()`](crate::KcpPeer::events) broadcast | Increase [`event_channel_capacity`](crate::KcpConfig::event_channel_capacity) or poll faster. |
/// | [`SessionClosed`](Error::SessionClosed) | [`send()`](crate::KcpPeer::send) on a closed session | The session was already removed (timeout, RESET, or [`disconnect()`](crate::KcpPeer::disconnect)). Call [`send()`](crate::KcpPeer::send) again to auto-reconnect. |
///
/// Most non-fatal errors can be handled by simply re-sending the data —
/// [`send()`](crate::KcpPeer::send) will auto-initiate a new handshake if the session is gone.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// UDP socket I/O error.
    ///
    /// Occurs when binding the socket, or when raw `send_to` / `recv_from` fails.
    /// Usually fatal for the entire [`KcpPeer`](crate::KcpPeer) instance.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// KCP protocol error.
    ///
    /// Propagated from the underlying KCP stack. Rare in practice —
    /// typically indicates corrupted data or a bug.
    #[error("KCP error: {0}")]
    Kcp(#[from] kcp::Error),

    /// No session found for the requested peer address.
    ///
    /// Returned by [`stats()`](crate::KcpPeer::stats) when the peer has no
    /// active session (timed out, never connected, or explicitly disconnected).
    #[error("session with {0} not found")]
    SessionNotFound(SocketAddr),

    /// KCP retransmission limit reached — peer is unreachable.
    ///
    /// The session is dead: [`send()`](crate::KcpPeer::send) will return this
    /// error, and an [`Event::Disconnected`](crate::Event::Disconnected) is
    /// about to fire. To recover, call [`send()`](crate::KcpPeer::send) again —
    /// it will automatically initiate a fresh handshake.
    #[error("KCP connection is dead — no ACK from peer")]
    DeadLink,

    /// The transport is shutting down and cannot accept new work.
    ///
    /// [`shutdown()`](crate::KcpPeer::shutdown) has been called (or [`KcpPeer`](crate::KcpPeer)
    /// is being dropped). Create a new instance to continue.
    #[error("transport is shutting down")]
    ShuttingDown,

    /// Event channel consumer is too slow; events were dropped.
    ///
    /// Arises from [`EventReceiver::recv()`](tokio::sync::broadcast::Receiver::recv) when
    /// the consumer cannot keep up with the event rate. The lost count is included.
    /// Increase [`event_channel_capacity`](crate::KcpConfig::event_channel_capacity)
    /// or poll more frequently.
    #[error("event channel lagged, dropped {0} events")]
    EventChannelLagged(u64),

    /// The session has been closed and cannot send data.
    ///
    /// The session was already removed (timeout, RESET received, or explicit
    /// [`disconnect()`](crate::KcpPeer::disconnect)). Call [`send()`](crate::KcpPeer::send)
    /// again — it will auto-initiate a fresh handshake.
    #[error("session is closed")]
    SessionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
