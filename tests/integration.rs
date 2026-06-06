use std::net::SocketAddr;
use std::time::Duration;

use kcp_peer::{DataMessage, Event, KcpConfig, KcpPeer};
use tokio::time::sleep;

/// Send a raw UDP packet to `target` and read one response.
/// Returns the response bytes or None on timeout/error.
fn raw_packet(target: SocketAddr, packet: &[u8]) -> Option<Vec<u8>> {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").ok()?;
    sock.send_to(packet, target).ok()?;
    sock.set_read_timeout(Some(Duration::from_millis(300))).ok()?;
    let mut buf = vec![0u8; 1500];
    match sock.recv_from(&mut buf) {
        Ok((n, _)) => {
            buf.truncate(n);
            Some(buf)
        }
        Err(_) => None,
    }
}

/// Helper: bind two KcpPeers on loopback, ephemeral ports.
async fn bind_pair() -> (KcpPeer, KcpPeer, SocketAddr, SocketAddr) {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind B");

    let addr_a = a.local_addr();
    let addr_b = b.local_addr();

    (a, b, addr_a, addr_b)
}

/// Wait for up to `timeout` for a lifecycle event matching a predicate.
async fn wait_for_event<F>(
    rx: &mut tokio::sync::broadcast::Receiver<Event>,
    f: F,
    timeout: Duration,
) -> Event
where
    F: Fn(&Event) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline - tokio::time::Instant::now();
        if remaining.is_zero() {
            panic!("timeout waiting for event");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(ev)) if f(&ev) => return ev,
            Ok(Ok(_)) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                panic!("event channel lagged by {n}");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("event channel closed");
            }
            Err(_) => panic!("timeout waiting for event"),
        }
    }
}

/// Wait for up to `timeout` for a data message matching a predicate.
async fn wait_for_data<F>(peer: &KcpPeer, f: F, timeout: Duration) -> DataMessage
where
    F: Fn(&DataMessage) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline - tokio::time::Instant::now();
        if remaining.is_zero() {
            panic!("timeout waiting for data");
        }
        match tokio::time::timeout(remaining, peer.recv()).await {
            Ok(Ok(msg)) if f(&msg) => return msg,
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("recv error: {e}"),
            Err(_) => panic!("timeout waiting for data"),
        }
    }
}

#[tokio::test]
async fn basic_send_recv() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // A initiates to B with data
    a.send(addr_b, b"hello").await.expect("A→B send");

    // B gets Connected, then Data
    let connected = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(_)));

    let data = wait_for_data(&b, |m| m.peer == addr_a && m.data[..] == *b"hello", Duration::from_secs(5)).await;
    assert_eq!(data.peer, addr_a);
    assert_eq!(&data.data[..], b"hello");

    // B responds to A
    b.send(addr_a, b"world").await.expect("B→A send");

    // A should get Data from B
    let data2 = wait_for_data(&a, |m| m.peer == addr_b && m.data[..] == *b"world", Duration::from_secs(5)).await;
    assert_eq!(data2.peer, addr_b);
    assert_eq!(&data2.data[..], b"world");
}

#[tokio::test]
async fn crash_initiator() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    // A binds to a fixed port so we can rebind the same address after crash
    let a = KcpPeer::bind_with("127.0.0.1:9870", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind B");
    let addr_a = a.local_addr();
    let addr_b = b.local_addr();
    let mut events_b = b.events();

    // 1. A sends data, B establishes session with A
    a.send(addr_b, b"before_crash").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // 2. A crashes (Drop closes socket + cancels bg tasks)
    drop(a);
    sleep(Duration::from_millis(100)).await;

    // 3. A2 rebinds the same address (same port as original A)
    let config2 = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();
    let a2 = KcpPeer::bind_with(addr_a, config2)
        .await
        .expect("rebind A on same address");

    // 4. A2 sends to B — B sees same source address but new incarnation + conv_id
    a2.send(addr_b, b"after_crash")
        .await
        .expect("send after crash");

    // 5. B still has A's old session (only 100ms elapsed, timeout is 60s).
    //    A2's SYN from the same address with a different incarnation triggers
    //    crash recovery: PeerRestarted for the old session, then Connected for the new one.
    let peer_restarted = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::PeerRestarted(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(peer_restarted, Event::PeerRestarted(addr) if addr == addr_a));

    let connected = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(addr) if addr == addr_a));

    // 6. Data arrives at B
    let data = wait_for_data(&b, |m| m.peer == addr_a && m.data[..] == *b"after_crash", Duration::from_secs(5)).await;
    assert_eq!(data.peer, addr_a);
    assert_eq!(&data.data[..], b"after_crash");

    drop(a2);
    drop(b);
}

#[tokio::test]
async fn crash_receiver() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish a session
    a.send(addr_b, b"before_crash").await.expect("initial send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;
    drop(events_b); // no longer needed

    // B "crashes"
    drop(b);
    tokio::task::yield_now().await;
    sleep(Duration::from_millis(300)).await;

    // B2 starts on a different port (same-port rebind is unreliable without SO_REUSEADDR)
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();
    let b2 = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("rebind B");
    let addr_b2 = b2.local_addr();
    let mut events_a = a.events();

    // A initiates a fresh connection to B2's address (application-level reconnect)
    a.send(addr_b2, b"after_crash")
        .await
        .expect("send after crash");

    // A should get Connected for the new session
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    // Data arrives at B2
    let data = wait_for_data(&b2, |m| m.data[..] == *b"after_crash", Duration::from_secs(5)).await;
    assert_eq!(&data.data[..], b"after_crash", "B2 received fresh data");

    drop(a);
    drop(b2);
}

// Helper: build default test config
fn test_config() -> KcpConfig {
    KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build()
}

#[tokio::test]
async fn initiator_gets_connected_event() {
    let a = KcpPeer::bind_with("127.0.0.1:0", test_config())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:9901", test_config())
        .await
        .expect("bind B");
    let addr_b = b.local_addr();
    let mut events_a = a.events();

    a.send(addr_b, b"hello").await.expect("A send");
    let connected = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(_)));

    drop(a);
    drop(b);
}

// A receives PeerRestarted when B crashes, restarts on the same address, and
// B2 initiates a new connection. A detects the restart via SYN from a
// previously-Established address with a different incarnation.
#[tokio::test]
async fn peer_restarted_detection() {
    let a = KcpPeer::bind_with("127.0.0.1:0", test_config())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:9900", test_config())
        .await
        .expect("bind B");
    let addr_b = b.local_addr();
    let addr_a = a.local_addr();
    let mut events_a = a.events();
    let mut events_b = b.events();

    // 1. A → B establishes session, data flows
    a.send(addr_b, b"first").await.expect("A send");

    // A receives Connected when SYN_ACK arrives (handshake complete)
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    // B receives Connected + Data
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;
    drop(events_b);

    // 2. B crashes
    drop(b);
    sleep(Duration::from_millis(100)).await;

    // 3. B2 rebinds the same address as B
    let b2 = KcpPeer::bind_with(addr_b, test_config())
        .await
        .expect("rebind B on same address");

    // 4. B2 initiates to A — SYN from an address A already has an Established session for
    b2.send(addr_a, b"after_restart").await.expect("B2 send");

    // 5. A detects crash: old Established session for B's address → PeerRestarted
    let peer_restarted = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::PeerRestarted(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(peer_restarted, Event::PeerRestarted(addr) if addr == addr_b));

    // 6. New session established → Connected
    let connected = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(addr) if addr == addr_b));

    // 7. Data from B2 arrives at A
    let data = wait_for_data(&a, |m| m.peer == addr_b && m.data[..] == *b"after_restart", Duration::from_secs(5)).await;
    assert_eq!(data.peer, addr_b);
    assert_eq!(&data.data[..], b"after_restart");

    drop(a);
    drop(b2);
}

#[tokio::test]
async fn simultaneous_handshake() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // Both sides initiate at roughly the same time
    a.send(addr_b, b"from_a").await.expect("A send");
    b.send(addr_a, b"from_b").await.expect("B send");

    // Both should get Connected and Data
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let _ = wait_for_data(&a, |_| true, Duration::from_secs(5)).await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn session_timeout() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_millis(300))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind B");
    let addr_b = b.local_addr();
    let mut events_b = b.events();

    a.send(addr_b, b"hi").await.expect("send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // Stop sending; after timeout, B should prune the session
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    drop(a);
    drop(b);
}

#[tokio::test]
async fn multiple_peers() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind B");
    let c = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind C");

    let addr_b = b.local_addr();
    let addr_c = c.local_addr();

    let mut events_b = b.events();
    let mut events_c = c.events();

    a.send(addr_b, b"a_to_b").await.expect("A→B");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    a.send(addr_c, b"a_to_c").await.expect("A→C");
    let _ = wait_for_event(
        &mut events_c,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&c, |_| true, Duration::from_secs(5)).await;

    drop(a);
    drop(b);
    drop(c);
}

#[tokio::test]
async fn large_message() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    let payload = vec![0xABu8; 15_000];
    a.send(addr_b, &payload).await.expect("send large");

    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let data = wait_for_data(&b, |m| m.data.len() == 15_000, Duration::from_secs(5)).await;
    assert_eq!(data.data.len(), 15_000, "large message size");
    assert_eq!(&data.data[..], &payload[..], "large message content");

    drop(a);
    drop(b);
}

#[tokio::test]
async fn dead_link() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_interval_ms(10)
        .kcp_nodelay(1, 10, 2, true)
        .maximum_resend_times(4)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind B");
    let addr_b = b.local_addr();
    let mut events_a = a.events();

    // Establish session — wait for data delivery to confirm Established + data flowed
    a.send(addr_b, b"ping").await.expect("first send");
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(15)).await;

    // Drop B (Drop impl cancels bg tasks, socket closes). Brief pause for cleanup.
    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send data after B is gone — never ACKed → retransmissions exhaust → dead link
    let _ = a.send(addr_b, b"trigger").await;

    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(15),
    )
    .await;

    drop(a);
}

#[tokio::test]
async fn many_small_messages() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    let count = 100;
    for i in 0..count {
        let msg = vec![i as u8; 1];
        a.send(addr_b, &msg).await.expect("send small");
    }

    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let mut received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received < count {
        let remaining = deadline - tokio::time::Instant::now();
        if remaining.is_zero() {
            panic!("timed out after {received}/{count} messages");
        }
        match tokio::time::timeout(remaining, b.recv()).await {
            Ok(Ok(msg)) => {
                assert_eq!(msg.data.len(), 1, "message {} size", received);
                assert_eq!(msg.data[0], received as u8, "message {} content", received);
                received += 1;
            }
            Ok(Err(e)) => panic!("recv error: {e}"),
            Err(_) => panic!("timed out after {received}/{count} messages"),
        }
    }

    assert_eq!(received, count, "all small messages received");
    drop(a);
    drop(b);
}

#[tokio::test]
async fn concurrent_send_to_unknown_peer() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Both futures run concurrently to trigger the race: two `send()` calls
    // for the same unknown peer at the same time.
    let s1 = a.send(addr_b, b"msg1");
    let s2 = a.send(addr_b, b"msg2");
    let (r1, r2) = tokio::join!(s1, s2);
    r1.expect("send1");
    r2.expect("send2");

    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let mut received = Vec::new();
    for _ in 0..2 {
        let data = tokio::time::timeout(Duration::from_secs(5), b.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        received.push(data.data.to_vec());
    }
    received.sort();
    assert_eq!(received, vec![b"msg1".to_vec(), b"msg2".to_vec()]);

    drop(a);
    drop(b);
}

#[tokio::test]
async fn shutdown_lifecycle() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish session: A → B
    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // Shutdown A — sends RESET to all peers
    a.shutdown().await;

    // B should receive Disconnected (RESET was sent)
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;
}

#[tokio::test]
async fn stats_active_peer() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish session
    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // Check stats on B's side for A
    let stats = b.stats(addr_a).expect("stats should return Some");
    assert!(stats.conv_id != 0, "conv_id should be non-zero");
    assert_eq!(stats.send_wnd, 128, "send_wnd matches config");
    assert_eq!(stats.recv_wnd, 128, "recv_wnd matches config");
    assert!(!stats.dead_link, "dead_link should be false");

    // Check stats on A's side for B
    let stats_a = a.stats(addr_b).expect("stats should return Some");
    assert!(stats_a.conv_id != 0, "conv_id should be non-zero");
    assert!(!stats_a.dead_link, "dead_link should be false");

    drop(a);
    drop(b);
}

#[tokio::test]
async fn stats_after_disconnect() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish session
    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // Stats works before disconnect
    assert!(a.stats(addr_b).is_some(), "stats before disconnect");

    // Disconnect B from A
    a.disconnect(addr_b);

    // Stats returns None after disconnect
    assert!(a.stats(addr_b).is_none(), "stats after disconnect returns None");

    drop(a);
    drop(b);
}

#[tokio::test]
async fn peers_list() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind B");
    let c = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind C");

    let addr_b = b.local_addr();
    let addr_c = c.local_addr();

    let mut events_b = b.events();
    let mut events_c = c.events();

    // Initially empty
    assert!(a.peers().is_empty(), "no peers before connecting");

    // A → B
    a.send(addr_b, b"hi_b").await.expect("A→B");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    let peers = a.peers();
    assert_eq!(peers.len(), 1, "one peer after A→B");
    assert!(peers.contains(&addr_b), "peers contains B");

    // A → C
    a.send(addr_c, b"hi_c").await.expect("A→C");
    let _ = wait_for_event(
        &mut events_c,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&c, |_| true, Duration::from_secs(5)).await;

    let peers = a.peers();
    assert_eq!(peers.len(), 2, "two peers after A→C");
    assert!(peers.contains(&addr_b), "peers contains B");
    assert!(peers.contains(&addr_c), "peers contains C");

    // Disconnect B
    a.disconnect(addr_b);

    let peers = a.peers();
    assert_eq!(peers.len(), 1, "one peer after disconnect B");
    assert!(peers.contains(&addr_c), "peers contains only C");

    drop(a);
    drop(b);
    drop(c);
}

#[tokio::test]
async fn syn_retry_exhaustion() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .syn_retry_interval(Duration::from_millis(50))
        .syn_max_retries(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind A");
    let mut events_a = a.events();

    // Send to an address that will never respond — SYN retry then exhaustion
    let unreachable: SocketAddr = "127.0.0.1:59999".parse().unwrap();
    let _ = a.send(unreachable, b"hello").await;

    // Should get Connected (handshake initiated) then Disconnected (retries exhausted)
    // The update task detects dead_link after syn_max_retries and prunes the session
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    drop(a);
}

#[tokio::test]
async fn send_after_disconnect() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // Establish session and exchange data
    a.send(addr_b, b"first").await.expect("A→B first");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // A disconnects from B
    a.disconnect(addr_b);

    // B sees Disconnected
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    sleep(Duration::from_millis(100)).await;

    // A reconnects — send() auto-initiates new handshake
    a.send(addr_b, b"second").await.expect("reconnect send");

    // A gets Connected for the new session
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    // B gets Connected + Data from the reconnection
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let data = wait_for_data(&b, |m| m.data[..] == *b"second", Duration::from_secs(5)).await;
    assert_eq!(&data.data[..], b"second", "reconnected data received");

    drop(a);
    drop(b);
}

#[test]
fn unknown_session_reset() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();

        // Send a KcpData packet from a raw socket — B has no session for us
        let conv_id: u32 = 12345;
        let mut packet = vec![0x00u8]; // KcpData type
        packet.extend_from_slice(&conv_id.to_le_bytes());
        packet.extend_from_slice(&[0xAB; 8]); // fake KCP payload

        let resp = raw_packet(addr_b, &packet);
        let resp = resp.expect("should receive RESET response");

        // Expect RESET: [0x03][conv_id LE]
        assert_eq!(resp.len(), 5, "RESET is 5 bytes");
        assert_eq!(resp[0], 0x03, "type byte is Reset");
        assert_eq!(&resp[1..5], &conv_id.to_le_bytes(), "conv_id matches");

        drop(b);
    });
}

#[test]
fn truncated_syn() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        // Send a SYN with only 1 byte of payload (needs ≥ 4)
        let mut packet = vec![0x01u8]; // SYN type
        packet.push(0x00); // 1 byte payload — truncated

        let _ = raw_packet(addr_b, &packet);

        // No session should be created, no events should fire
        sleep(Duration::from_millis(200)).await;
        assert!(b.peers().is_empty(), "no sessions created from truncated SYN");
        assert!(events_b.try_recv().is_err(), "no events from truncated SYN");

        drop(b);
    });
}

#[test]
fn truncated_syn_ack() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        // Send a SYN_ACK with only 1 byte of payload (needs ≥ 4)
        let mut packet = vec![0x02u8]; // SYN_ACK type
        packet.push(0x00); // 1 byte payload — truncated

        let _ = raw_packet(addr_b, &packet);

        // No session should be created, no events should fire
        sleep(Duration::from_millis(200)).await;
        assert!(b.peers().is_empty(), "no sessions created from truncated SYN_ACK");
        assert!(events_b.try_recv().is_err(), "no events from truncated SYN_ACK");

        drop(b);
    });
}

#[test]
fn syn_ack_unknown_session() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        // Send a valid SYN_ACK for a session that doesn't exist on B
        let conv_id: u32 = 99999;
        let mut packet = vec![0x02u8]; // SYN_ACK type
        packet.extend_from_slice(&conv_id.to_le_bytes());

        let _ = raw_packet(addr_b, &packet);

        // Should be silently ignored — no session, no events
        sleep(Duration::from_millis(200)).await;
        assert!(b.peers().is_empty(), "no sessions created from SYN_ACK to unknown session");
        assert!(events_b.try_recv().is_err(), "no events from SYN_ACK to unknown session");

        drop(b);
    });
}

#[test]
fn reset_unknown_session() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        // Send a valid RESET for a session that doesn't exist on B
        let conv_id: u32 = 77777;
        let mut packet = vec![0x03u8]; // RESET type
        packet.extend_from_slice(&conv_id.to_le_bytes());

        let _ = raw_packet(addr_b, &packet);

        // Should be silently ignored — no session, no events
        sleep(Duration::from_millis(200)).await;
        assert!(b.peers().is_empty(), "no sessions created from RESET to unknown session");
        assert!(events_b.try_recv().is_err(), "no events from RESET to unknown session");

        drop(b);
    });
}

#[tokio::test]
async fn recv_cancel_safety() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish session
    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    // Call recv() with a very short timeout — future is cancelled while waiting
    let _ = tokio::time::timeout(Duration::from_millis(10), b.recv()).await;

    // Now send data — it should be receivable (not lost by the cancelled recv)
    a.send(addr_b, b"after_cancel").await.expect("A send after cancel");

    let data = tokio::time::timeout(Duration::from_secs(5), b.recv())
        .await
        .expect("timeout")
        .expect("recv error");
    assert_eq!(&data.data[..], b"after_cancel", "data not lost after cancel");

    drop(a);
    drop(b);
}

#[tokio::test]
async fn send_cancel_during_handshake() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // Cancel send() during handshake initiation using tokio::select!
    // The send will start initiate_session (SYN sent), then be cancelled
    tokio::select! {
        _ = a.send(addr_b, b"cancelled") => {}
        _ = sleep(Duration::from_millis(5)) => {} // cancel after a few ms
    }

    // B may or may not have received the SYN — either way, the session
    // on A's side should not permanently block further sends.
    sleep(Duration::from_millis(100)).await;

    // A new send() should succeed — it creates a fresh session
    a.send(addr_b, b"after_cancel").await.expect("send after cancel");

    // A gets Connected (new handshake)
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    // B gets Connected + Data
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let data = wait_for_data(&b, |m| m.data[..] == *b"after_cancel", Duration::from_secs(5)).await;
    assert_eq!(&data.data[..], b"after_cancel", "data after cancel handshake");

    drop(a);
    drop(b);
}

#[test]
fn config_mtu_clamp() {
    // MTU below 50 should be clamped to 50
    let config = KcpConfig::builder().mtu(30).build();
    assert_eq!(config.mtu, 50, "mtu=30 clamped to 50");

    let config = KcpConfig::builder().mtu(0).build();
    assert_eq!(config.mtu, 50, "mtu=0 clamped to 50");

    // MTU >= 50 stays as-is
    let config = KcpConfig::builder().mtu(1400).build();
    assert_eq!(config.mtu, 1400, "mtu=1400 unchanged");

    let config = KcpConfig::builder().mtu(50).build();
    assert_eq!(config.mtu, 50, "mtu=50 unchanged (boundary)");
}

#[tokio::test]
async fn custom_syn_retry_config() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .syn_retry_interval(Duration::from_millis(30))
        .syn_max_retries(2)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind A");
    let mut events_a = a.events();

    // Send to an unreachable address — SYN retries with exponential backoff
    let unreachable: SocketAddr = "127.0.0.1:59998".parse().unwrap();
    let start = tokio::time::Instant::now();
    let _ = a.send(unreachable, b"hello").await;

    // Wait for Disconnected (retries exhausted)
    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;
    let elapsed = start.elapsed();

    // With syn_retry_interval=30ms, syn_max_retries=2:
    // retry 0 at ~30ms, retry 1 at ~60ms, then DeadLink
    // Total should be roughly 90-200ms (including tick_interval overhead)
    assert!(
        elapsed.as_millis() < 2000,
        "SYN retries exhausted quickly ({:?} < 2s)",
        elapsed
    );
    assert!(
        elapsed.as_millis() >= 50,
        "SYN retries take some time ({:?} >= 50ms)",
        elapsed
    );

    drop(a);
}
