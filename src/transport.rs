use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::Rng;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::config::KcpConfig;
use crate::error::Result;
use crate::packet::{self, PacketType};
use crate::session::{self, epoch_ms, Session, SessionState};

/// Events produced by [`KcpPeer`].
///
/// Delivered via a `tokio::sync::broadcast` channel. Subscribe with [`KcpPeer::events`].
///
/// Data is read separately via [`KcpPeer::recv()`], not through events.
///
/// # Event ordering
///
/// For a normal outbound connection the sequence is:
/// 1. [`Connected`](Event::Connected) — handshake complete
/// 2. Data via [`recv()`](KcpPeer::recv) — application messages
/// 3. [`Disconnected`](Event::Disconnected) — session ended
///
/// For crash recovery the sequence is:
/// 1. [`PeerRestarted`](Event::PeerRestarted) — old session detected stale
/// 2. [`Connected`](Event::Connected) — new session established
#[derive(Debug, Clone)]
pub enum Event {
    /// Handshake completed or inbound connection accepted.
    ///
    /// For the **outbound** (initiator) side: fires when `SYN_ACK` arrives
    /// and the session transitions to [`Established`](crate::session::SessionState::Established).
    ///
    /// For the **inbound** (receiver) side: fires immediately when the `SYN` is
    /// processed, before the session is inserted into the peer map.
    Connected(SocketAddr),
    /// Session closed.
    ///
    /// Fires when one of:
    /// * Idle timeout — no packets received for [`session_timeout`](crate::KcpConfig::session_timeout)
    /// * Dead link — KCP retransmission limit exhausted (peer unreachable)
    /// * Incoming [`Reset`](crate::packet::PacketType::Reset) packet
    /// * Explicit [`disconnect()`](crate::KcpPeer::disconnect) call
    ///
    /// The session is removed from the internal map before this event fires.
    /// Subsequent [`send()`](crate::KcpPeer::send) calls will auto-reconnect.
    Disconnected(SocketAddr),
    /// Peer restarted — old session was force-replaced by a new incarnation.
    ///
    /// Fires when a `SYN` arrives from an address that already has an
    /// [`Established`](crate::session::SessionState::Established) session but
    /// carries a different incarnation (random ID generated on every
    /// [`bind`](crate::KcpPeer::bind)).
    ///
    /// Always followed by [`Connected`](Event::Connected) for the new session.
    /// The old session handle is now stale — call [`send()`](crate::KcpPeer::send)
    /// to use the new session.
    PeerRestarted(SocketAddr),
}

/// Application data received from a peer.
///
/// Returned by [`KcpPeer::recv()`]. Data is pulled from KCP on demand —
/// no broadcast channel involved.
#[derive(Debug, Clone)]
pub struct DataMessage {
    /// Remote peer address.
    pub peer: SocketAddr,
    /// Application payload.
    pub data: Bytes,
}

/// Receiver for [`Event`]s. Clone the broadcast receiver.
pub type EventReceiver = broadcast::Receiver<Event>;

/// A symmetric P2P transport over a single UDP socket.
///
/// All peers share one UDP socket. Sessions are identified by remote
/// `SocketAddr` (canonicalized — IPv4-mapped IPv6 → plain IPv4).
/// At most one session per peer.
///
/// # Lifecycle
///
/// 1. **Bind** — [`bind()`](KcpPeer::bind) or [`bind_with()`](KcpPeer::bind_with)
///    binds a UDP socket and spawns background receive + update tasks.
/// 2. **Connect** — first [`send()`](KcpPeer::send)
///    to an unknown address auto-creates a session and sends a `SYN` handshake.
///    Data queued during the handshake is buffered and flushed once the session
///    is established.
/// 3. **Established** — data flows bidirectionally over KCP. The background
///    update task drives retransmission and detects DeadLink.
/// 4. **Teardown** — sessions close on timeout, DeadLink, incoming `RESET`,
///    or explicit [`disconnect()`](KcpPeer::disconnect).
///    [`shutdown()`](KcpPeer::shutdown) sends `RESET` (3 copies) to all peers.
/// 5. **Drop** — dropping the [`KcpPeer`](KcpPeer) cancels background tasks.
///    The UDP socket closes when all `Arc` references are released.
///
/// For a state-machine diagram see [`SessionState`](crate::session::SessionState).
///
/// # Thread safety
///
/// Internally `Arc`-based. [`KcpPeer`] is `Send` + `Sync` and can be
/// shared across tasks via `Arc<KcpPeer>`.
///
/// # Cancel safety
///
/// All async methods on `KcpPeer` are **cancel-safe**:
///
/// - [`send`](KcpPeer::send) either completes fully
///   (session created, data queued) or leaves no state behind. No intermediate
///   session leaks if the future is dropped mid-flight.
/// - [`shutdown`](KcpPeer::shutdown) sends RESETs synchronously then awaits
///   background tasks; cancellation during the await leaves tasks that still
///   exit promptly (shutdown token is already cancelled).
/// - [`bind_with`](KcpPeer::bind_with) is cancel-safe — dropped before the socket
///   binds, no state is created.
pub struct KcpPeer {
    pub(crate) socket: Arc<UdpSocket>,
    pub(crate) sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    pub(crate) event_tx: broadcast::Sender<Event>,
    pub(crate) pending_data: Arc<std::sync::Mutex<VecDeque<DataMessage>>>,
    pub(crate) data_notify: Arc<tokio::sync::Notify>,
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
    /// Bind to a local address with the default [`KcpConfig`].
    ///
    /// Spawns the background receive and update tasks.
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> Result<Self> {
        Self::bind_with(addr, KcpConfig::default()).await
    }

    /// Bind to a local address with the given [`KcpConfig`].
    ///
    /// Each bind generates a random incarnation number used for crash detection.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe. If the future is dropped before the UDP socket is bound,
    /// no state is created and no background tasks are spawned.
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
        let pending_data = Arc::new(std::sync::Mutex::new(VecDeque::new()));
        let data_notify = Arc::new(tokio::sync::Notify::new());

        let recv_handle = spawn_receive_task(
            socket.clone(),
            sessions.clone(),
            event_tx.clone(),
            pending_data.clone(),
            data_notify.clone(),
            config.clone(),
            incarnation,
            shutdown.clone(),
        );

        let update_handle = spawn_update_task(
            sessions.clone(),
            event_tx.clone(),
            config.clone(),
            socket.clone(),
            shutdown.clone(),
        );

        Ok(Self {
            socket,
            sessions,
            event_tx,
            pending_data,
            data_notify,
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

    /// Send data to a peer.
    ///
    /// Auto-initiates a handshake (SYN) if no session exists for this peer.
    /// Data is queued into KCP immediately and flushed once the session
    /// transitions to [`Established`](crate::session::SessionState::Established).
    ///
    /// If the session is in [`SynSent`](crate::session::SessionState::SynSent)
    /// (handshake in progress), the data is buffered — the update task retries
    /// the handshake with exponential backoff (see
    /// [`syn_retry_interval`](crate::KcpConfig::syn_retry_interval)).
    ///
    /// # Buffered data on handshake failure
    ///
    /// Data queued during [`SynSent`](crate::session::SessionState::SynSent) is
    /// **dropped silently** if the handshake eventually fails (SYN retries
    /// exhausted). [`send()`](KcpPeer::send) returned `Ok(())`, but the data
    /// never hit the wire. To detect this, monitor [`events()`](KcpPeer::events)
    /// for [`Event::Disconnected`](Event::Disconnected) and re-send important
    /// data after the next [`Event::Connected`](Event::Connected).
    ///
    /// # Errors
    ///
    /// Returns [`DeadLink`](crate::Error::DeadLink) if the session's KCP
    /// retransmission limit is exhausted (peer unreachable).
    /// Returns [`SessionClosed`](crate::Error::SessionClosed) if the session
    /// was already removed (timeout, RESET, or [`disconnect()`](KcpPeer::disconnect)).
    ///
    /// In both cases, calling [`send()`](KcpPeer::send) again will
    /// auto-reconnect — the error is not fatal.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe. If cancelled during handshake initiation, a [`SynSent`]
    /// session entry may remain in the map, but it will be cleaned up by
    /// the background update task on timeout. A subsequent [`send()`] will
    /// find the existing session and continue normally.
    /// Safe to use inside `tokio::select!`.
    pub async fn send(&self, peer: SocketAddr, data: &[u8]) -> Result<()> {
        let can = canonicalize(peer);

        let session = {
            let map = self.sessions.read().unwrap();
            map.get(&can).cloned()
        };

        match session {
            Some(s) => s.send_data(data),
            None => {
                let session = self.initiate_session(peer).await;
                session.send_data(data)
            }
        }
    }

    /// Initiate a session to a peer (handshake).
    async fn initiate_session(&self, peer: SocketAddr) -> Arc<Session> {
        let can = canonicalize(peer);

        // Fast check under read lock — avoids creating a session if another
        // task already inserted one for this peer while we were awaiting.
        if let Some(existing) = self.sessions.read().unwrap().get(&can).cloned() {
            return existing;
        }

        let conv_id = rand::rng().random_range(1..=u32::MAX);

        let session = Session::new_outbound(
            conv_id,
            peer,
            self.socket.clone(),
            self.incarnation,
            self.config.as_ref(),
        );

        {
            let mut map = self.sessions.write().unwrap();
            // Double-check: another task may have inserted since we last checked.
            // If so, discard our session (no SYN was sent yet).
            if let Some(existing) = map.get(&can) {
                return existing.clone();
            }
            map.insert(can, session.clone());
        }

        // SYN is sent only after map insertion — guarantees at most one SYN
        // per peer, so the receiver never sees a spurious second SYN that
        // would trigger crash recovery and destroy the session.
        session.send_syn().await;

        session
    }

    /// Subscribe to transport events.
    ///
    /// Returns a `tokio::sync::broadcast::Receiver`. Each subscriber gets all events
    /// from the point of subscription onward. Events include [`Connected`](Event::Connected),
    /// [`Disconnected`](Event::Disconnected), and [`PeerRestarted`](Event::PeerRestarted).
    /// Data is read via [`recv()`](KcpPeer::recv) instead.
    ///
    /// # Cancel safety
    ///
    /// [`broadcast::Receiver::recv`](tokio::sync::broadcast::Receiver::recv) is
    /// cancel-safe — dropping the future does not consume the event. Other
    /// subscribers are unaffected.
    pub fn events(&self) -> EventReceiver {
        self.event_tx.subscribe()
    }

    /// Receive the next data message from any peer.
    ///
    /// Drains KCP receive buffers on demand. Data is buffered internally until
    /// consumed. Returns `Err(ShuttingDown)` after
    /// [`shutdown()`](KcpPeer::shutdown).
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe. Dropping the future mid-wait does not lose data;
    /// the next `recv()` call will return it.
    pub async fn recv(&self) -> Result<DataMessage> {
        loop {
            if self.shutdown.is_cancelled() {
                return Err(crate::error::Error::ShuttingDown);
            }
            {
                let mut pending = self.pending_data.lock().unwrap();
                if let Some(msg) = pending.pop_front() {
                    return Ok(msg);
                }
            }
            tokio::select! {
                _ = self.data_notify.notified() => {}
                _ = self.shutdown.cancelled() => {
                    return Err(crate::error::Error::ShuttingDown);
                }
            }
        }
    }

    /// List all connected peers (filters out closed sessions).
    pub fn peers(&self) -> Vec<SocketAddr> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| !s.closed.load(Ordering::Acquire))
            .map(|s| s.peer_addr)
            .collect()
    }

    /// KCP statistics for a specific peer session.
    ///
    /// Returns `None` if the peer has no active session.
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
            elapsed: Duration::from_millis(
                epoch_ms().saturating_sub(s.last_rx.load(Ordering::Acquire)),
            ),
        })
    }

    /// Force close a session with a peer.
    ///
    /// Removes the local session, sends a `RESET` packet (3 copies for reliability)
    /// to the remote peer, and fires [`Event::Disconnected`].
    /// After calling this, the remote peer's session is closed immediately.
    ///
    /// Unlike [`shutdown()`](KcpPeer::shutdown), this operates on a single peer
    /// and does not affect background tasks.
    pub fn disconnect(&self, peer: SocketAddr) {
        let can = canonicalize(peer);
        if let Some(s) = self.sessions.write().unwrap().remove(&can) {
            s.mark_closed();
            let conv_id = s.inner.lock().unwrap().conv_id;
            let reset = packet::encode_control(PacketType::Reset, conv_id);
            for _ in 0..3 {
                let _ = self.socket.try_send_to(&reset, peer);
            }
            let _ = self.event_tx.send(Event::Disconnected(peer));
        }
    }

    /// Gracefully shut down, sending RESET to all peers.
    ///
    /// 1. Cancels background tasks (shutdown token).
    /// 2. Sends a `RESET` packet (3 copies for reliability) to every active peer.
    /// 3. Waits for the receive and update tasks to exit.
    ///
    /// On drop (without calling [`shutdown()`](KcpPeer::shutdown)), background
    /// tasks are cancelled and no `RESET` is sent — remote peers
    /// discover the disconnection only when their session timers expire.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe. RESET packets are sent synchronously before any await.
    /// If cancelled while awaiting background task handles, those tasks are
    /// still guaranteed to exit promptly (the shutdown token was already
    /// cancelled). Takes `self` so it can only be called once.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        // send RESET to all active sessions (3 copies for reliability)
        let peers: Vec<Arc<Session>> = self.sessions.read().unwrap().values().cloned().collect();
        for s in &peers {
            s.mark_closed();
            let conv_id = s.inner.lock().unwrap().conv_id;
            let reset = packet::encode_control(PacketType::Reset, conv_id);
            for _ in 0..3 {
                let _ = self.socket.try_send_to(&reset, s.peer_addr);
            }
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
    /// KCP conversation ID for this session.
    pub conv_id: u32,
    /// Local send window size (segments).
    pub send_wnd: u32,
    /// Local receive window size (segments).
    pub recv_wnd: u32,
    /// Remote peer's advertised window size.
    pub rmt_wnd: u32,
    /// Number of segments waiting to be sent (in-flight + queued).
    pub wait_snd: usize,
    /// Whether KCP has declared the connection dead (retransmission exhausted).
    pub dead_link: bool,
    /// Time elapsed since the last packet was received from this peer.
    pub elapsed: Duration,
}

/// Spawn the receive task: reads UDP packets and dispatches to sessions.
#[allow(clippy::too_many_arguments)]
fn spawn_receive_task(
    socket: Arc<UdpSocket>,
    sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    event_tx: broadcast::Sender<Event>,
    pending_data: Arc<std::sync::Mutex<VecDeque<DataMessage>>>,
    data_notify: Arc<tokio::sync::Notify>,
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
                        &pending_data, &data_notify,
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

/// Spawn the update task: drives KCP updates, retries SYN, and prunes stale sessions.
fn spawn_update_task(
    sessions: Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    event_tx: broadcast::Sender<Event>,
    config: Arc<KcpConfig>,
    socket: Arc<UdpSocket>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(config.tick_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => break,

                _ = tick.tick() => {}
            }

            // Fast path: no sessions → skip per-session work
            if sessions.read().unwrap().is_empty() {
                continue;
            }

            let now_ms = session::current_ms();
            let now_epoch = epoch_ms();
            let mut to_remove: Vec<CanonicalAddr> = Vec::new();

            {
                let map = sessions.read().unwrap();
                for (can, sess) in map.iter() {
                    if sess.closed.load(Ordering::Acquire) {
                        continue;
                    }

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

                    match sess.maybe_retry_syn(&config, now_epoch) {
                        Err(crate::error::Error::DeadLink) => {
                            to_remove.push(*can);
                            continue;
                        }
                        Err(_) => {}
                        Ok(_) => {}
                    }

                    let last_rx_ms = sess.last_rx.load(Ordering::Acquire);
                    if last_rx_ms > 0 {
                        let age =
                            Duration::from_millis(now_epoch.saturating_sub(last_rx_ms));
                        if age > config.session_timeout {
                            to_remove.push(*can);
                        }
                    }
                }
            }

            for can in to_remove {
                if let Some(sess) = sessions.write().unwrap().remove(&can) {
                    // Send RESET to notify peer the session is gone
                    let conv_id = sess.inner.lock().unwrap().conv_id;
                    let reset = packet::encode_control(PacketType::Reset, conv_id);
                    for _ in 0..3 {
                        let _ = socket.try_send_to(&reset, sess.peer_addr);
                    }
                    sess.mark_closed();
                    let _ = event_tx.send(Event::Disconnected(sess.peer_addr));
                }
            }
        }

        tracing::trace!("update task stopped");
    })
}

/// Route an incoming UDP packet to the correct session or control handler.
#[allow(clippy::too_many_arguments)]
async fn handle_incoming(
    data: &[u8],
    from: SocketAddr,
    socket: &Arc<UdpSocket>,
    sessions: &Arc<std::sync::RwLock<HashMap<CanonicalAddr, Arc<Session>>>>,
    event_tx: &broadcast::Sender<Event>,
    pending_data: &std::sync::Mutex<VecDeque<DataMessage>>,
    data_notify: &tokio::sync::Notify,
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
                    // Drain KCP messages into pending buffer.
                    let mut msgs = Vec::new();
                    s.try_recv_all(&mut msgs)?;
                    if !msgs.is_empty() {
                        let mut pending = pending_data.lock().unwrap();
                        for msg in msgs {
                            pending.push_back(DataMessage {
                                peer: s.peer_addr,
                                data: msg.freeze(),
                            });
                        }
                        drop(pending);
                        data_notify.notify_one();
                    }
                }
                None => {
                    if payload.len() >= 4 {
                        let mut conv_bytes = [0u8; 4];
                        conv_bytes.copy_from_slice(&payload[..4]);
                        let conv = u32::from_le_bytes(conv_bytes);
                        let reset = packet::encode_control(PacketType::Reset, conv);
                        for _ in 0..3 {
                            let _ = socket.try_send_to(&reset, from);
                        }
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
                            if matches!(inner.state, SessionState::Established) {
                                // SYN_ACK may have been lost — re-acknowledge
                                session.last_rx.store(epoch_ms(), Ordering::Release);
                                send_ack_to = Some(from);
                                ack_conv = conv;
                            }
                            SynAction::Ignore
                        } else if matches!(inner.state, SessionState::SynSent) {
                            // Simultaneous handshake — tie-break by address.
                            if from > my_addr {
                                // We lose — adopt peer's conv_id
                                inner.kcp.set_conv(conv);
                                inner.conv_id = conv;
                                inner.state = SessionState::Established;
                                session.last_rx.store(epoch_ms(), Ordering::Release);
                                // flush data queued during SynSent handshake
                                let _ = inner.kcp.update(session::current_ms());
                                let _ = inner.kcp.flush();
                                send_ack_to = Some(from);
                                ack_conv = conv;
                            } else {
                                // We win — keep conv_id, SYN acts as SYN_ACK
                                inner.state = SessionState::Established;
                                session.last_rx.store(epoch_ms(), Ordering::Release);
                                let _ = inner.kcp.update(session::current_ms());
                                let _ = inner.kcp.flush();
                                send_ack_to = Some(from);
                                ack_conv = old_conv;
                            }
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
                        Session::new_inbound(conv, from, socket.clone(), incarnation, config).await;
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
                        let _ = inner.kcp.flush();
                        let _ = event_tx.send(Event::Connected(from));
                    }
                }
            }
        }

        PacketType::Reset => {
            if payload.len() < 4 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated RESET").into());
            }
            let mut conv_bytes = [0u8; 4];
            conv_bytes.copy_from_slice(&payload[..4]);
            let conv = u32::from_le_bytes(conv_bytes);

            let can = canonicalize(from);
            let session = {
                let map = sessions.read().unwrap();
                map.get(&can).cloned()
            };
            if let Some(s) = session {
                let conv_match = s.inner.lock().unwrap().conv_id == conv;
                if conv_match {
                    sessions.write().unwrap().remove(&can);
                    s.mark_closed();
                    let _ = event_tx.send(Event::Disconnected(from));
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_ipv4_mapped_ipv6() {
        let v4: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:127.0.0.1]:8080".parse().unwrap();
        let v6: SocketAddr = "[::1]:8080".parse().unwrap();

        assert_eq!(canonicalize(mapped).0, v4, "IPv4-mapped IPv6 → IPv4");
        assert_eq!(canonicalize(v4).0, v4, "plain IPv4 unchanged");
        assert_eq!(canonicalize(v6).0, v6, "true IPv6 unchanged");
    }
}
