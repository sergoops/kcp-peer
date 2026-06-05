use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use atomic_waker::AtomicWaker;
use bytes::BytesMut;
use kcp::Kcp;
use tokio::net::UdpSocket;

use crate::config::KcpConfig;
use crate::error::{Error, Result};
use crate::packet::{self, PacketType};

/// KCP output adapter that writes frames directly to a UDP socket.
///
/// On `WouldBlock` the bytes are silently dropped — KCP retransmission guarantees delivery.
pub struct DirectOutput {
    pub socket: Arc<UdpSocket>,
    pub peer: SocketAddr,
    buf: BytesMut,
}

impl Write for DirectOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.clear();
        self.buf.reserve(1 + buf.len());
        self.buf.extend_from_slice(&[PacketType::KcpData as u8]);
        self.buf.extend_from_slice(buf);
        match self.socket.try_send_to(&self.buf, self.peer) {
            Ok(n) => Ok(n.saturating_sub(1)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(buf.len()),
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
///
/// ```text
///                         ┌──────────────────────────┐
///                         │      No session           │
///                         └───────────┬──────────────┘
///                                     │ send() / connect()
///                                     │ (or incoming SYN)
///                                     v
///                         ┌──────────────────────────┐
///                  ┌─────│        SynSent            │
///                  │     │  (SYN sent, waiting for   │
///                  │     │   SYN_ACK or SYN from peer)│
///                  │     └───────────┬──────────────┘
///                  │                 │ SYN_ACK received
///                  │            ┌────┴────┐
///                  │            │         │
///                  │      tie-break   match conv
///                  │     (simultaneous  (normal)
///                  │      handshake)
///                  │            └────┬────┘
///                  │                 v
///         ┌────────┴────────┐
///         │  Established    │
///         │ (data can flow) │
///         └────────┬────────┘
///                  │
///        ┌─────────┼──────────┐
///        v         v          v
///   timeout   DeadLink    RESET / disconnect()
///        │         │          │
///        └─────────┴──────────┘
///                  v
///         ┌──────────────────┐
///         │  Session removed │  ← Event::Disconnected fired
///         │  from session map│
///         └──────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Outbound SYN sent, waiting for SYN_ACK (or inbound SYN for simultaneous handshake).
    ///
    /// Data queued via [`send_data()`](Session::send_data) is buffered in KCP
    /// but not flushed until the session transitions to [`Established`](SessionState::Established).
    /// The background update task retransmits SYN with exponential backoff;
    /// after [`syn_max_retries`](crate::KcpConfig::syn_max_retries) the session
    /// is pruned and [`Event::Disconnected`](crate::Event::Disconnected) fires.
    SynSent,
    /// Handshake complete — KCP data can be sent and received.
    ///
    /// The session remains in this state until one of:
    /// * Idle timeout (no packets received for [`session_timeout`](crate::KcpConfig::session_timeout))
    /// * KCP retransmission exhaustion ([`DeadLink`](crate::Error::DeadLink))
    /// * Explicit [`disconnect()`](crate::KcpPeer::disconnect) or incoming RESET
    /// * Crash recovery: SYN from same address with a different incarnation
    ///   replaces this session with a new one (fires [`PeerRestarted`](crate::Event::PeerRestarted))
    Established,
}

/// A peer session. Clone-friendly (Arc internals).
pub struct Session {
    pub inner: std::sync::Mutex<SessionInner>,
    pub waker: AtomicWaker,
    pub closed: AtomicBool,
    pub last_rx: AtomicU64,
    pub peer_addr: SocketAddr,
    pub socket: Arc<UdpSocket>,
    pub syn_sent_at: AtomicU64,
    pub syn_retries: AtomicU32,
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
    ) -> Arc<Self> {
        let output = DirectOutput {
            socket: socket.clone(),
            peer: peer_addr,
            buf: BytesMut::with_capacity(1500),
        };
        let mut kcp = Kcp::new(conv_id, output);
        config.apply_to(&mut kcp);

        let syn = packet::encode_control(PacketType::Syn, conv_id);
        match socket.send_to(&syn, peer_addr).await {
            Ok(n) => tracing::trace!("SYN sent {n} bytes to {peer_addr}"),
            Err(e) => tracing::warn!("SYN send failed to {peer_addr}: {e}"),
        }

        let now = epoch_ms();

        let inner = SessionInner {
            kcp,
            conv_id,
            state: SessionState::SynSent,
            incarnation: _incarnation,
            recv_buf: BytesMut::with_capacity(2048),
        };

        Arc::new(Self {
            inner: std::sync::Mutex::new(inner),
            waker: AtomicWaker::new(),
            closed: AtomicBool::new(false),
            last_rx: AtomicU64::new(now),
            peer_addr,
            socket,
            syn_sent_at: AtomicU64::new(now),
            syn_retries: AtomicU32::new(0),
        })
    }

    /// Create a new inbound (receiver) session after receiving SYN.
    pub async fn new_inbound(
        conv_id: u32,
        peer_addr: SocketAddr,
        socket: Arc<UdpSocket>,
        _incarnation: u64,
        config: &KcpConfig,
    ) -> Arc<Self> {
        let output = DirectOutput {
            socket: socket.clone(),
            peer: peer_addr,
            buf: BytesMut::with_capacity(1500),
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
            recv_buf: BytesMut::with_capacity(2048),
        };

        Arc::new(Self {
            inner: std::sync::Mutex::new(inner),
            waker: AtomicWaker::new(),
            closed: AtomicBool::new(false),
            last_rx: AtomicU64::new(epoch_ms()),
            peer_addr,
            socket,
            syn_sent_at: AtomicU64::new(0),
            syn_retries: AtomicU32::new(0),
        })
    }

    /// Mark session as closed and wake any blocked reader.
    pub fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Send data through this session.
    /// Queues data to KCP. Flushes immediately if the session is established.
    pub fn send_data(&self, data: &[u8]) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::SessionClosed);
        }
        let mut inner = self.inner.lock().unwrap();
        inner.kcp.send(data)?;
        if matches!(inner.state, SessionState::Established) {
            let now = current_ms();
            inner.kcp.update(now)?;
            inner.kcp.flush()?;
        }
        if inner.kcp.is_dead_link() {
            return Err(Error::DeadLink);
        }
        Ok(())
    }

    /// Explicitly flush queued data. No-op for non-Established sessions.
    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if matches!(inner.state, SessionState::Established) {
            let now = current_ms();
            inner.kcp.update(now)?;
            inner.kcp.flush()?;
        }
        Ok(())
    }

    /// Drive KCP update timer. Returns number of segments still in flight.
    /// Returns `DeadLink` if KCP has exhausted retransmissions.
    /// No-op for non-Established sessions (returns 0).
    pub fn update(&self, current_ms: u32) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        if !matches!(inner.state, SessionState::Established) {
            return Ok(inner.kcp.wait_snd());
        }
        if inner.kcp.is_dead_link() {
            return Err(Error::DeadLink);
        }
        inner.kcp.update(current_ms)?;
        if inner.kcp.is_dead_link() {
            return Err(Error::DeadLink);
        }
        Ok(inner.kcp.wait_snd())
    }

    /// Feed incoming KCP data. Wakes reader only if data becomes readable.
    pub fn input(&self, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.kcp.input(data)?;
        if inner.kcp.peeksize().is_ok() {
            self.waker.wake();
        }
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

    /// Drain all available messages into `out`, holding the lock for the entire drain.
    pub fn try_recv_all(&self, out: &mut Vec<BytesMut>) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        while let Ok(size) = inner.kcp.peeksize() {
            let mut buf = BytesMut::zeroed(size);
            inner.kcp.recv(&mut buf)?;
            out.push(buf);
        }
        Ok(())
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

    /// Retransmit SYN if session is still in SynSent and retry interval has elapsed.
    /// Returns `Ok(true)` if a retry was sent, `Ok(false)` if not yet time.
    /// Returns `Err(Error::DeadLink)` when max retries exhausted.
    pub fn maybe_retry_syn(&self, config: &crate::config::KcpConfig, now_ms: u32) -> Result<bool> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::SessionClosed);
        }

        let conv_id = {
            let inner = self.inner.lock().unwrap();
            if !matches!(inner.state, SessionState::SynSent) {
                return Ok(false);
            }
            inner.conv_id
        };

        let retries = self.syn_retries.load(Ordering::Acquire);
        if retries >= config.syn_max_retries {
            tracing::debug!("SYN retry exhausted for {}", self.peer_addr);
            return Err(Error::DeadLink);
        }

        let last_sent = self.syn_sent_at.load(Ordering::Acquire);
        let elapsed = (now_ms as u64).saturating_sub(last_sent);
        if elapsed < (config.syn_retry_interval.as_millis() as u64) << retries {
            return Ok(false);
        }

        let syn = packet::encode_control(PacketType::Syn, conv_id);
        let _ = self.socket.try_send_to(&syn, self.peer_addr);

        let now = epoch_ms();
        self.syn_sent_at.store(now, Ordering::Release);
        self.syn_retries.fetch_add(1, Ordering::Release);

        tracing::trace!("SYN retry {} sent to {}", retries + 1, self.peer_addr);

        Ok(true)
    }
}

pub fn epoch_ms() -> u64 {
    UNIX_EPOCH.elapsed().unwrap_or_default().as_millis() as u64
}

pub fn current_ms() -> u32 {
    epoch_ms() as u32
}
