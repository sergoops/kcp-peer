use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Error;
use crate::session::Session;

/// Handle wrapping a peer session that implements AsyncRead + AsyncWrite.
/// Composes with tokio framing utilities (Framed, LengthDelimitedCodec, etc.).
#[derive(Debug, Clone)]
pub struct KcpConnection {
    pub(crate) session: Arc<Session>,
}

impl KcpConnection {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    /// Unwrap and get back the Arc<Session>.
    pub fn into_inner(self) -> Arc<Session> {
        self.session
    }
}

impl AsyncRead for KcpConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this.session.closed.load(std::sync::atomic::Ordering::Acquire) {
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

        if this.session.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "session closed",
            )));
        }

        match this.session.send_data(buf) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => {
                let io_err = match &e {
                    Error::SessionClosed => {
                        io::Error::new(io::ErrorKind::ConnectionReset, "session closed")
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
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.session.mark_closed();
        Poll::Ready(Ok(()))
    }
}
