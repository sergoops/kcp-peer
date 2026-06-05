use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, UNIX_EPOCH};

use atomic_waker::AtomicWaker;
use bytes::BytesMut;
use kcp::Kcp;
use tokio::net::UdpSocket;

use crate::config::KcpConfig;
use crate::error::{Error, Result};
use crate::packet::{self, PacketType};

/// Output that writes KCP frames directly to the UDP socket.
/// On `WouldBlock` the bytes are silently dropped — KCP retransmission handles it.
pub struct DirectOutput {
    pub socket: Arc<UdpSocket>,
    pub peer: SocketAddr,
}

impl Write for DirectOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let packet = packet::encode_kcp_data(buf);
        match self.socket.try_send_to(&packet, self.peer) {
            Ok(n) => Ok(n.saturating_sub(1)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                Ok(buf.len())
            }
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Internal mutable state behind a per-session Mutex.
pub struct SessionInner {
    pub kcp: Kcp<DirectOutput>,
    pub conv_id: u32,
    pub state: SessionState,
    pub incarnation: u64,
    pub recv_buf: BytesMut,
}

/// Session state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    SynSent { retries: u32, deadline: Instant },
    Established,
    Closing,
}

/// A peer session. Clone-friendly (Arc internals).
pub struct Session {
    pub inner: std::sync::Mutex<SessionInner>,
    pub waker: AtomicWaker,
    pub closed: AtomicBool,
    pub last_rx: AtomicU64,
    pub peer_addr: SocketAddr,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("peer_addr", &self.peer_addr)
            .field("closed", &self.closed.load(Ordering::Acquire))
            .field("last_rx", &self.last_rx.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Create a new outbound (initiator) session. Sends SYN.
    pub async fn new_outbound(
        conv_id: u32,
        peer_addr: SocketAddr,
        socket: Arc<UdpSocket>,
        _incarnation: u64,
        config: &KcpConfig,
        now: Instant,
    ) -> Result<Arc<Self>> {
        let output = DirectOutput {
            socket: socket.clone(),
            peer: peer_addr,
        };
        let mut kcp = Kcp::new(conv_id, output);
        config.apply_to(&mut kcp);

        let syn = packet::encode_control(PacketType::Syn, conv_id);
        match socket.send_to(&syn, peer_addr).await {
            Ok(n) => tracing::trace!("SYN sent {n} bytes to {peer_addr}"),
            Err(e) => tracing::warn!("SYN send failed to {peer_addr}: {e}"),
        }

        let inner = SessionInner {
            kcp,
            conv_id,
            state: SessionState::SynSent {
                retries: 0,
                deadline: now + config.handshake_timeout,
            },
            incarnation: _incarnation,
            recv_buf: BytesMut::new(),
        };

        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(inner),
            waker: AtomicWaker::new(),
            closed: AtomicBool::new(false),
            last_rx: AtomicU64::new(epoch_ms()),
            peer_addr,
        }))
    }

    /// Create a new inbound (receiver) session after receiving SYN.
    pub async fn new_inbound(
        conv_id: u32,
        peer_addr: SocketAddr,
        socket: Arc<UdpSocket>,
        _incarnation: u64,
        config: &KcpConfig,
    ) -> Result<Arc<Self>> {
        let output = DirectOutput {
            socket: socket.clone(),
            peer: peer_addr,
        };
        let mut kcp = Kcp::new(conv_id, output);
        config.apply_to(&mut kcp);

        let syn_ack = packet::encode_control(PacketType::SynAck, conv_id);
        if let Err(e) = socket.send_to(&syn_ack, peer_addr).await {
            tracing::warn!("SYN_ACK send failed: {e}");
        }

        let inner = SessionInner {
            kcp,
            conv_id,
            state: SessionState::Established,
            incarnation: _incarnation,
            recv_buf: BytesMut::new(),
        };

        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(inner),
            waker: AtomicWaker::new(),
            closed: AtomicBool::new(false),
            last_rx: AtomicU64::new(epoch_ms()),
            peer_addr,
        }))
    }

    /// Mark session as closed and wake any blocked reader.
    pub fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Send data through this session.
    /// Queues data to KCP's send buffer. The background update task flushes it.
    pub fn send_data(&self, data: &[u8]) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::SessionClosed);
        }
        let mut inner = self.inner.lock().unwrap();
        inner.kcp.send(data)?;
        Ok(())
    }

    /// Drive KCP update timer. Returns number of segments still in flight.
    pub fn update(&self, current_ms: u32) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        inner.kcp.update(current_ms)?;
        Ok(inner.kcp.wait_snd())
    }

    /// Feed incoming KCP data. Wakes reader afterwards.
    pub fn input(&self, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.kcp.input(data)?;
        self.waker.wake();
        Ok(())
    }

    /// Try to extract a complete message. Returns `Some(data)` if available.
    /// Call after `input()` to drain newly arrived messages.
    pub fn try_recv(&self) -> Result<Option<BytesMut>> {
        let mut inner = self.inner.lock().unwrap();
        match inner.kcp.peeksize() {
            Ok(size) => {
                let mut buf = BytesMut::zeroed(size);
                inner.kcp.recv(&mut buf)?;
                Ok(Some(buf))
            }
            Err(kcp::Error::RecvQueueEmpty) => Ok(None),
            Err(e) => Err(Error::Kcp(e)),
        }
    }

    /// Peek at the next message size.
    pub fn peeksize(&self) -> Result<usize> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.kcp.peeksize()?)
    }

    /// Receive into user buffer. Returns bytes written.
    /// If user buffer is too small, spills remainder to recv_buf.
    pub fn recv_into(&self, buf: &mut [u8]) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();

        if !inner.recv_buf.is_empty() {
            let to_copy = std::cmp::min(buf.len(), inner.recv_buf.len());
            buf[..to_copy].copy_from_slice(&inner.recv_buf[..to_copy]);
            if to_copy < inner.recv_buf.len() {
                let remaining = inner.recv_buf.split_off(to_copy);
                inner.recv_buf = remaining;
            } else {
                inner.recv_buf.clear();
            }
            return Ok(to_copy);
        }

        match inner.kcp.peeksize() {
            Ok(size) if size > buf.len() => {
                let mut tmp = vec![0u8; size];
                let n = inner.kcp.recv(&mut tmp)?;
                let to_copy = std::cmp::min(buf.len(), n);
                buf[..to_copy].copy_from_slice(&tmp[..to_copy]);
                if to_copy < n {
                    inner.recv_buf.extend_from_slice(&tmp[to_copy..n]);
                }
                Ok(to_copy)
            }
            _ => {
                let n = inner.kcp.recv(buf)?;
                Ok(n)
            }
        }
    }
}

pub fn epoch_ms() -> u64 {
    UNIX_EPOCH.elapsed().unwrap_or_default().as_millis() as u64
}

pub fn current_ms() -> u32 {
    epoch_ms() as u32
}
