use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::Rng;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::config::KcpConfig;
use crate::connection::KcpConnection;
use crate::error::Result;
use crate::packet::{self, PacketType};
use crate::session::{self, epoch_ms, Session, SessionState};

/// Events produced by KcpPeer.
#[derive(Debug, Clone)]
pub enum Event {
    /// Application data received from a peer.
    Data(SocketAddr, Bytes),
    /// Handshake completed; session is ready.
    Connected(SocketAddr),
    /// Session closed (timeout, disconnect, or peer restarted).
    Disconnected(SocketAddr),
    /// Peer restarted — old session was force-replaced.
    PeerRestarted(SocketAddr),
}

/// Receiver for [`Event`]s. Clone the broadcast receiver.
pub type EventReceiver = broadcast::Receiver<Event>;

/// A symmetric P2P transport over a single UDP socket.
///
/// Each peer is identified by its `SocketAddr`. At most one session per peer.
/// Sessions are auto-created on first `send()` or incoming SYN.
pub struct KcpPeer {
    pub(crate) socket: Arc<UdpSocket>,
    pub(crate) sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    pub(crate) event_tx: broadcast::Sender<Event>,
    pub(crate) incarnation: u64,
    pub(crate) shutdown: CancellationToken,
    pub(crate) config: Arc<KcpConfig>,
    pub(crate) _recv_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) _update_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) local_addr: SocketAddr,
}

impl Drop for KcpPeer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl std::fmt::Debug for KcpPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KcpPeer")
            .field("local_addr", &self.local_addr)
            .field("sessions", &self.sessions.read().unwrap().len())
            .finish_non_exhaustive()
    }
}

impl KcpPeer {
    /// Bind to a local address with default config.
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> Result<Self> {
        Self::bind_with(addr, KcpConfig::default()).await
    }

    /// Bind to a local address with the given config.
    pub async fn bind_with(
        addr: impl tokio::net::ToSocketAddrs,
        config: KcpConfig,
    ) -> Result<Self> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let local_addr = socket.local_addr()?;
        let incarnation = rand::rng().random::<u64>();
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity);
        let shutdown = CancellationToken::new();
        let sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>> =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let config = Arc::new(config);

        let recv_handle = spawn_receive_task(
            socket.clone(),
            sessions.clone(),
            event_tx.clone(),
            config.clone(),
            incarnation,
            shutdown.clone(),
        );

        let update_handle = spawn_update_task(
            sessions.clone(),
            event_tx.clone(),
            config.clone(),
            shutdown.clone(),
        );

        Ok(Self {
            socket,
            sessions,
            event_tx,
            incarnation,
            shutdown,
            config,
            _recv_handle: Some(recv_handle),
            _update_handle: Some(update_handle),
            local_addr,
        })
    }

    /// Get the bound local address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Send data to a peer. Auto-initiates a handshake if no session exists.
    pub async fn send(&self, peer: SocketAddr, data: &[u8]) -> Result<()> {
        let can = canonicalize(peer);

        let session = {
            let map = self.sessions.read().unwrap();
            map.get(&can).cloned()
        };

        match session {
            Some(s) => s.send_data(data),
            None => {
                let session = self.initiate_session(peer).await?;
                session.send_data(data)
            }
        }
    }

    /// Initiate (or return existing) connection to a peer. Idempotent.
    pub async fn connect(&self, peer: SocketAddr) -> Result<KcpConnection> {
        let can = canonicalize(peer);

        // fast path: existing session
        if let Some(s) = self.sessions.read().unwrap().get(&can) {
            return Ok(KcpConnection::new(s.clone()));
        }

        let session = self.initiate_session(peer).await?;
        Ok(KcpConnection::new(session))
    }

    /// Initiate a session to a peer (handshake).
    async fn initiate_session(&self, peer: SocketAddr) -> Result<Arc<Session>> {
        let conv_id = rand::rng().random::<u32>();

        let session = Session::new_outbound(
            conv_id,
            peer,
            self.socket.clone(),
            self.incarnation,
            self.config.as_ref(),
        )
        .await?;

        let can = canonicalize(peer);
        {
            let mut map = self.sessions.write().unwrap();
            map.insert(can, session.clone());
        }

        Ok(session)
    }

    /// Subscribe to transport events.
    pub fn events(&self) -> EventReceiver {
        self.event_tx.subscribe()
    }

    /// List all connected peers.
    pub fn peers(&self) -> Vec<SocketAddr> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| !s.closed.load(Ordering::Acquire))
            .map(|s| s.peer_addr)
            .collect()
    }

    /// Stats for a specific peer.
    pub fn stats(&self, peer: SocketAddr) -> Option<PeerStats> {
        let map = self.sessions.read().unwrap();
        let s = map.get(&canonicalize(peer))?;
        if s.closed.load(Ordering::Acquire) {
            return None;
        }
        let inner = s.inner.lock().unwrap();
        Some(PeerStats {
            conv_id: inner.conv_id,
            send_wnd: inner.kcp.snd_wnd(),
            recv_wnd: inner.kcp.rcv_wnd(),
            rmt_wnd: inner.kcp.rmt_wnd(),
            wait_snd: inner.kcp.wait_snd(),
            dead_link: inner.kcp.is_dead_link(),
            elapsed: Duration::from_millis(s.last_rx.load(Ordering::Acquire)),
        })
    }

    /// Force close a session with a peer.
    pub fn disconnect(&self, peer: SocketAddr) {
        let can = canonicalize(peer);
        if let Some(s) = self.sessions.write().unwrap().remove(&can) {
            s.mark_closed();
            let _ = self.event_tx.send(Event::Disconnected(peer));
        }
    }

    /// Gracefully shut down, sending RESET to all peers.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        // send RESET to all active sessions
        let peers: Vec<Arc<Session>> = self.sessions.read().unwrap().values().cloned().collect();
        for s in &peers {
            s.mark_closed();
            let conv_id = s.inner.lock().unwrap().conv_id;
            let reset = packet::encode_control(PacketType::Reset, conv_id);
            let _ = self.socket.try_send_to(&reset, s.peer_addr);
        }
        if let Some(h) = self._recv_handle.take() {
            h.await.ok();
        }
        if let Some(h) = self._update_handle.take() {
            h.await.ok();
        }
    }
}

/// Per-peer connection statistics.
#[derive(Debug, Clone)]
pub struct PeerStats {
    pub conv_id: u32,
    pub send_wnd: u32,
    pub recv_wnd: u32,
    pub rmt_wnd: u32,
    pub wait_snd: usize,
    pub dead_link: bool,
    pub elapsed: Duration,
}

/// Spawn the receive task: reads UDP packets and dispatches to sessions.
fn spawn_receive_task(
    socket: Arc<UdpSocket>,
    sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    event_tx: broadcast::Sender<Event>,
    config: Arc<KcpConfig>,
    incarnation: u64,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut recv_buf = vec![0u8; 65535];

        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => break,

                recv = socket.recv_from(&mut recv_buf) => {
                    let (n, from) = match recv {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("recv_from error: {e}");
                            continue;
                        }
                    };
                    let data = &recv_buf[..n];
                    tracing::trace!("recv {} bytes from {}: {:02x?}", n, from, data);
                    if let Err(e) = handle_incoming(
                        data, from, &socket, &sessions, &event_tx,
                        incarnation, config.as_ref(),
                    ).await {
                        tracing::debug!("handle_incoming from {from}: {e}");
                    }
                }
            }
        }

        tracing::trace!("receive task stopped");
    })
}

/// Spawn the update task: drives KCP updates and prunes stale sessions.
fn spawn_update_task(
    sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    event_tx: broadcast::Sender<Event>,
    config: Arc<KcpConfig>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(config.tick_interval);

        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => break,

                _ = tick.tick() => {
                    let now_ms = session::current_ms();
                    let mut to_remove: Vec<CanonicalAddr> = Vec::new();

                    {
                        let map = sessions.read().unwrap();
                        for (can, sess) in map.iter() {
                            if sess.closed.load(Ordering::Acquire) {
                                continue;
                            }

                            let is_established = {
                                let inner = sess.inner.lock().unwrap();
                                matches!(inner.state, SessionState::Established)
                            };

                            if is_established {
                                match sess.update(now_ms) {
                                    Err(crate::error::Error::DeadLink) => {
                                        to_remove.push(*can);
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::debug!("update error for {}: {e}", sess.peer_addr);
                                    }
                                    Ok(_) => {}
                                }
                            }

                            // session timeout check
                            let last_rx_ms = sess.last_rx.load(Ordering::Acquire);
                            if last_rx_ms > 0 {
                                let age = Duration::from_millis(epoch_ms().saturating_sub(last_rx_ms));
                                if age > config.session_timeout {
                                    to_remove.push(*can);
                                }
                            }
                        }
                    }

                    for can in to_remove {
                        if let Some(sess) = sessions.write().unwrap().remove(&can) {
                            sess.mark_closed();
                            let _ = event_tx.send(Event::Disconnected(sess.peer_addr));
                        }
                    }
                }
            }
        }

        tracing::trace!("update task stopped");
    })
}

/// Route an incoming UDP packet to the correct session or control handler.
async fn handle_incoming(
    data: &[u8],
    from: SocketAddr,
    socket: &Arc<UdpSocket>,
    sessions: &Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    event_tx: &broadcast::Sender<Event>,
    incarnation: u64,
    config: &KcpConfig,
) -> Result<()> {
    let (typ, payload) = packet::split_packet(data)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty packet"))?;

    let can = canonicalize(from);

    match typ {
        PacketType::KcpData => {
            let session = {
                let map = sessions.read().unwrap();
                map.get(&can).cloned()
            };
            match session {
                Some(s) => {
                    s.last_rx.store(epoch_ms(), Ordering::Release);
                    s.input(payload)?;
                    // Only drain messages for event subscribers; otherwise
                    // KcpConnection::poll_read reads directly from KCP.
                    if event_tx.receiver_count() > 0 {
                        while let Some(msg) = s.try_recv()? {
                            let _ = event_tx.send(Event::Data(s.peer_addr, msg.freeze()));
                        }
                    }
                }
                None => {
                    // Unknown session: send RESET
                    if payload.len() >= 4 {
                        let mut conv_bytes = [0u8; 4];
                        conv_bytes.copy_from_slice(&payload[..4]);
                        let conv = u32::from_le_bytes(conv_bytes);
                        let reset = packet::encode_control(PacketType::Reset, conv);
                        let _ = socket.try_send_to(&reset, from);
                    }
                }
            }
        }

        PacketType::Syn => {
            if payload.len() < 4 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated SYN").into());
            }
            let mut conv_bytes = [0u8; 4];
            conv_bytes.copy_from_slice(&payload[..4]);
            let conv = u32::from_le_bytes(conv_bytes);

            let my_addr = socket.local_addr()?;

            // Check for existing session. All locks are dropped before any await.
            struct SynResult {
                action: SynAction,
                send_ack_to: Option<SocketAddr>,
                ack_conv: u32,
            }
            enum SynAction {
                CreateNew,
                KeepExisting,
                Ignore,
            }

            let result = {
                let map = sessions.read().unwrap();
                let mut send_ack_to = None;
                let mut ack_conv = 0;

                let action = match map.get(&can) {
                    Some(session) => {
                        let mut inner = session.inner.lock().unwrap();
                        let old_conv = inner.conv_id;
                        if old_conv == conv {
                            SynAction::Ignore
                        } else if matches!(inner.state, SessionState::SynSent) {
                            // Simultaneous handshake — tie-break by address.
                            if from > my_addr {
                                // We lose — adopt peer's conv_id
                                inner.kcp.set_conv(conv);
                                inner.conv_id = conv;
                                inner.state = SessionState::Established;
                                session.last_rx.store(epoch_ms(), Ordering::Release);
                                send_ack_to = Some(from);
                                ack_conv = conv;
                            } else {
                                // We win — keep conv_id, SYN acts as SYN_ACK
                                inner.state = SessionState::Established;
                                session.last_rx.store(epoch_ms(), Ordering::Release);
                                let _ = inner.kcp.update(session::current_ms());
                                send_ack_to = Some(from);
                                ack_conv = old_conv;
                            }
                            // inner & session locks dropped here
                            let _ = event_tx.send(Event::Connected(from));
                            SynAction::KeepExisting
                        } else {
                            session.mark_closed();
                            let _ = event_tx.send(Event::PeerRestarted(from));
                            SynAction::CreateNew
                        }
                    }
                    None => SynAction::CreateNew,
                };
                // map read lock dropped here

                SynResult {
                    action,
                    send_ack_to,
                    ack_conv,
                }
            };

            // Send SYN_ACK if needed (no locks held)
            if let Some(peer) = result.send_ack_to {
                let syn_ack = packet::encode_control(PacketType::SynAck, result.ack_conv);
                let _ = socket.send_to(&syn_ack, peer).await;
            }

            match result.action {
                SynAction::CreateNew => {
                    let session =
                        Session::new_inbound(conv, from, socket.clone(), incarnation, config)
                            .await?;
                    let mut map = sessions.write().unwrap();
                    map.insert(can, session.clone());
                    let _ = event_tx.send(Event::Connected(from));
                }
                SynAction::KeepExisting | SynAction::Ignore => {}
            }
        }

        PacketType::SynAck => {
            if payload.len() < 4 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated SYN_ACK").into());
            }
            let mut conv_bytes = [0u8; 4];
            conv_bytes.copy_from_slice(&payload[..4]);
            let conv = u32::from_le_bytes(conv_bytes);

            let map = sessions.read().unwrap();
            if let Some(s) = map.get(&can) {
                let mut inner = s.inner.lock().unwrap();
                if let SessionState::SynSent = inner.state {
                    if inner.conv_id == conv {
                        inner.state = SessionState::Established;
                        s.last_rx.store(epoch_ms(), Ordering::Release);
                        // flush any data that was queued during handshake
                        let _ = inner.kcp.update(session::current_ms());
                        let _ = event_tx.send(Event::Connected(from));
                    }
                }
            }
        }

        PacketType::Reset => {
            let can = canonicalize(from);
            if let Some(s) = sessions.write().unwrap().remove(&can) {
                s.mark_closed();
                let _ = event_tx.send(Event::Disconnected(from));
            }
        }
    }

    Ok(())
}

/// Normalize IPv4-mapped IPv6 addresses to plain IPv4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanonicalAddr(SocketAddr);

fn canonicalize(addr: SocketAddr) -> CanonicalAddr {
    let addr = match addr {
        SocketAddr::V6(v6) => {
            if let Some(v4) = v6.ip().to_ipv4_mapped() {
                SocketAddr::V4(std::net::SocketAddrV4::new(v4, v6.port()))
            } else {
                SocketAddr::V6(v6)
            }
        }
        other => other,
    };
    CanonicalAddr(addr)
}

impl std::ops::Deref for CanonicalAddr {
    type Target = SocketAddr;
    fn deref(&self) -> &SocketAddr {
        &self.0
    }
}
