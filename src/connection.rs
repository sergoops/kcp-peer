use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Error;
use crate::session::Session;

/// A handle to a single peer session implementing [`AsyncRead`](tokio::io::AsyncRead) +
/// [`AsyncWrite`](tokio::io::AsyncWrite).
///
/// Obtained via [`KcpPeer::connect`](crate::KcpPeer::connect). Composes with tokio framing
/// utilities such as `Framed`, `LengthDelimitedCodec`, etc.
///
/// # Interaction with events
///
/// When event subscribers exist on the [`KcpPeer`](crate::KcpPeer) (via
/// [`events()`](crate::KcpPeer::events)), incoming data is drained into
/// [`Event::Data`](crate::Event::Data) and `poll_read` returns [`Pending`](std::task::Poll::Pending).
/// To read via `KcpConnection`, either:
/// * Avoid subscribing to events, or
/// * Read data from [`Event::Data`](crate::Event::Data) instead of using `poll_read`.
///
/// # Lifecycle
///
/// The [`KcpConnection`](KcpConnection) remains valid until the session is
/// removed (timeout, DeadLink, RESET, or [`disconnect()`](crate::KcpPeer::disconnect)).
/// After removal, [`poll_read`](tokio::io::AsyncRead::poll_read) returns
/// `Err(ConnectionReset)` and [`poll_write`](tokio::io::AsyncWrite::poll_write)
/// returns `Err(ConnectionReset)`. The handle should be discarded and a new one
/// obtained via [`connect()`](crate::KcpPeer::connect).
///
/// # Cancel safety
///
/// All `poll_*` methods return immediately (synchronously) — they never
/// register a waker that would leave state inconsistent on drop. The
/// `AtomicWaker` registered in `poll_read` is cancel-safe: dropping the
/// waker-future is harmless (a subsequent wake finds no task and is a no-op).
#[derive(Debug, Clone)]
pub struct KcpConnection {
    pub(crate) session: Arc<Session>,
}

impl KcpConnection {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    /// Consume the connection and return the underlying `Arc<Session>`.
    pub fn into_inner(self) -> Arc<Session> {
        self.session
    }

    /// Returns `true` if the underlying session has been closed.
    ///
    /// A closed connection returns [`ConnectionReset`](std::io::ErrorKind::ConnectionReset)
    /// from [`poll_read`](tokio::io::AsyncRead::poll_read) and
    /// [`poll_write`](tokio::io::AsyncWrite::poll_write).
    pub fn is_closed(&self) -> bool {
        self.session
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the remote peer's socket address.
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        self.session.peer_addr
    }
}

impl AsyncRead for KcpConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this
            .session
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "session closed",
            )));
        }

        let result = {
            let mut inner = this.session.inner.lock().unwrap();

            if !inner.recv_buf.is_empty() {
                let to_copy = std::cmp::min(buf.remaining(), inner.recv_buf.len());
                buf.put_slice(&inner.recv_buf[..to_copy]);
                if to_copy < inner.recv_buf.len() {
                    let remaining = inner.recv_buf.split_off(to_copy);
                    inner.recv_buf = remaining;
                } else {
                    inner.recv_buf.clear();
                }
                return Poll::Ready(Ok(()));
            }

            match inner.kcp.peeksize() {
                Ok(size) => {
                    let avail = buf.remaining();
                    if size > avail {
                        let mut tmp = vec![0u8; size];
                        let n = inner.kcp.recv(&mut tmp)?;
                        buf.put_slice(&tmp[..avail]);
                        inner.recv_buf.extend_from_slice(&tmp[avail..n]);
                    } else {
                        let room = buf.initialize_unfilled_to(size);
                        let n = inner.kcp.recv(room)?;
                        buf.advance(n);
                    }
                    Ok(())
                }
                Err(kcp::Error::RecvQueueEmpty) => {
                    this.session.waker.register(cx.waker());
                    return Poll::Pending;
                }
                Err(e) => {
                    return Poll::Ready(Err(io::Error::other(e)));
                }
            }
        };

        Poll::Ready(result)
    }
}

impl AsyncWrite for KcpConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        if this
            .session
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "session closed",
            )));
        }

        match this.session.send_data(buf) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => {
                let io_err = match &e {
                    Error::SessionClosed | Error::DeadLink => {
                        io::Error::new(io::ErrorKind::ConnectionReset, e.to_string())
                    }
                    _ => {
                        this.session.waker.register(cx.waker());
                        return Poll::Pending;
                    }
                };
                Poll::Ready(Err(io_err))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.session.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "session closed",
            )));
        }
        match this.session.flush() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(io::Error::other(e))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.session.mark_closed();
        Poll::Ready(Ok(()))
    }
}
