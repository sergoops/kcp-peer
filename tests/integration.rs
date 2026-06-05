use std::net::SocketAddr;
use std::time::Duration;

use kcp_peer::{Event, KcpConfig, KcpPeer};
use tokio::time::sleep;

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

/// Wait for up to `timeout` for an event matching a predicate.
async fn wait_for<F>(
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

#[tokio::test]
async fn basic_send_recv() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // A initiates to B with data
    a.send(addr_b, b"hello").await.expect("A→B send");

    // B gets Connected, then Data
    let connected = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(_)));

    let data = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_a, "B: data from A's address");
            assert_eq!(&msg[..], b"hello");
        }
        _ => unreachable!(),
    }

    // B responds to A
    b.send(addr_a, b"world").await.expect("B→A send");

    // A should get Data from B
    let data2 = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data2 {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_b, "A: data from B's address");
            assert_eq!(&msg[..], b"world");
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn bidirectional() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // A initiates
    a.send(addr_b, b"from_a").await.expect("A send");
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;

    // B sends back immediately
    b.send(addr_a, b"from_b").await.expect("B send");
    let data = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_b);
            assert_eq!(&msg[..], b"from_b");
        }
        _ => unreachable!(),
    }
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
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;

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
    let peer_restarted = wait_for(
        &mut events_b,
        |e| matches!(e, Event::PeerRestarted(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(peer_restarted, Event::PeerRestarted(addr) if addr == addr_a));

    let connected = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(addr) if addr == addr_a));

    // 6. Data arrives at B
    let data_ev = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data_ev {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_a, "B: data from A's original address");
            assert_eq!(&msg[..], b"after_crash");
        }
        _ => unreachable!(),
    }

    drop(a2);
    drop(b);
}

#[tokio::test]
async fn crash_receiver() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish a session
    a.send(addr_b, b"before_crash").await.expect("initial send");
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
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
    let mut events_b2 = b2.events();
    let mut events_a = a.events();

    // A initiates a fresh connection to B2's address (application-level reconnect)
    a.send(addr_b2, b"after_crash")
        .await
        .expect("send after crash");

    // A should get Connected for the new session
    let _ = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    // Data arrives at B2
    let data_ev = wait_for(
        &mut events_b2,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data_ev {
        Event::Data(_addr, msg) => {
            assert_eq!(&msg[..], b"after_crash", "B2 received fresh data");
        }
        _ => unreachable!(),
    }

    drop(a);
    drop(b2);
}

// A receives PeerRestarted when B crashes, restarts on the same address, and
// B2 initiates a new connection. A detects the restart via SYN from a
// previously-Established address with a different incarnation.

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

// Simplified: verify that A (the initiator) receives Connected after send()
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
    let connected = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(_)));

    drop(a);
    drop(b);
}

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
    let _ = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    // B receives Connected + Data
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
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
    let peer_restarted = wait_for(
        &mut events_a,
        |e| matches!(e, Event::PeerRestarted(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(peer_restarted, Event::PeerRestarted(addr) if addr == addr_b));

    // 6. New session established → Connected
    let connected = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(addr) if addr == addr_b));

    // 7. Data from B2 arrives at A
    let data = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_b);
            assert_eq!(&msg[..], b"after_restart");
        }
        _ => unreachable!(),
    }

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
    let _ = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let _ = wait_for(
        &mut events_a,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
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
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;

    // Stop sending; after timeout, B should prune the session
    let _ = wait_for(
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
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;

    a.send(addr_c, b"a_to_c").await.expect("A→C");
    let _ = wait_for(
        &mut events_c,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for(
        &mut events_c,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;

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

    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let data = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data {
        Event::Data(_addr, msg) => {
            assert_eq!(msg.len(), 15_000, "large message size");
            assert_eq!(&msg[..], &payload[..], "large message content");
        }
        _ => unreachable!(),
    }

    drop(a);
    drop(b);
}

#[tokio::test]
async fn reconnect() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    a.send(addr_b, b"first").await.expect("first send");
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;

    // Disconnect on A — sends RESET to B so B immediately closes the session.
    a.disconnect(addr_b);
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    sleep(Duration::from_millis(100)).await;

    // Reconnect: send() auto-initiates a fresh handshake.
    // B's session was already removed by the RESET, so this is a normal
    // handshake (not crash recovery).
    a.send(addr_b, b"second").await.expect("reconnect send");
    let data = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data {
        Event::Data(_addr, msg) => {
            assert_eq!(&msg[..], b"second", "reconnected message");
        }
        _ => unreachable!(),
    }

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
    let mut events_b = b.events();
    let mut events_a = a.events();

    // Establish session — wait for data delivery to confirm Established + data flowed
    a.send(addr_b, b"ping").await.expect("first send");
    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(15),
    )
    .await;

    // Drop B (Drop impl cancels bg tasks, socket closes). Brief pause for cleanup.
    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send data after B is gone — never ACKed → retransmissions exhaust → dead link
    let _ = a.send(addr_b, b"trigger").await;

    let _ = wait_for(
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

    let _ = wait_for(
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
        match tokio::time::timeout(remaining, events_b.recv()).await {
            Ok(Ok(Event::Data(_addr, msg))) => {
                assert_eq!(msg.len(), 1, "message {} size", received);
                assert_eq!(msg[0], received as u8, "message {} content", received);
                received += 1;
            }
            Ok(Ok(_)) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                panic!("event channel lagged by {n}");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("event channel closed");
            }
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

    let _ = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let mut received = Vec::new();
    for _ in 0..2 {
        let data = wait_for(
            &mut events_b,
            |e| matches!(e, Event::Data(..)),
            Duration::from_secs(5),
        )
        .await;
        match data {
            Event::Data(_, msg) => received.push(msg.to_vec()),
            _ => unreachable!(),
        }
    }
    received.sort();
    assert_eq!(received, vec![b"msg1".to_vec(), b"msg2".to_vec()]);

    drop(a);
    drop(b);
}
