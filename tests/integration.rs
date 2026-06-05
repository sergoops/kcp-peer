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
async fn wait_for<F>(rx: &mut tokio::sync::broadcast::Receiver<Event>, f: F, timeout: Duration) -> Event
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

// ─── Basic send/recv ─────────────────────────────────────────────────

#[tokio::test]
async fn basic_send_recv() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // A initiates to B with data
    a.send(addr_b, b"hello").await.expect("A→B send");

    // B gets Connected, then Data
    let connected = wait_for(&mut events_b, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    assert!(matches!(connected, Event::Connected(_)));

    let data = wait_for(&mut events_b, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;
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
    let data2 = wait_for(&mut events_a, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;
    match data2 {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_b, "A: data from B's address");
            assert_eq!(&msg[..], b"world");
        }
        _ => unreachable!(),
    }
}

// ─── Bidirectional exchange ──────────────────────────────────────────

#[tokio::test]
async fn bidirectional() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // A initiates
    a.send(addr_b, b"from_a").await.expect("A send");
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;

    // B sends back immediately
    b.send(addr_a, b"from_b").await.expect("B send");
    let data = wait_for(&mut events_a, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;
    match data {
        Event::Data(addr, msg) => {
            assert_eq!(addr, addr_b);
            assert_eq!(&msg[..], b"from_b");
        }
        _ => unreachable!(),
    }
}

// ─── Crash: initiator restarts ───────────────────────────────────────

#[tokio::test]
async fn crash_initiator() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    drop(a); // A "crashes"
    sleep(Duration::from_millis(100)).await;

    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    // New A (same or new address — but B identifies by addr, so use a new port)
    let a2 = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("rebind A");
    let mut events_b = b.events();

    // A2 sends to B — should create new session with new conv_id
    a2.send(addr_b, b"after_crash").await.expect("send after crash");

    // B should get Connected (new session), possibly preceded by PeerReset
    let ev = wait_for(
        &mut events_b,
        |e| matches!(e, Event::PeerReset(_) | Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    match ev {
        Event::PeerReset(_) => {
            let _ = wait_for(
                &mut events_b,
                |e| matches!(e, Event::Connected(_)),
                Duration::from_secs(5),
            )
            .await;
        }
        Event::Connected(_) => {}
        _ => unreachable!(),
    }

    // Data arrives at B
    let data_ev = wait_for(
        &mut events_b,
        |e| matches!(e, Event::Data(..)),
        Duration::from_secs(5),
    )
    .await;
    match data_ev {
        Event::Data(_addr, msg) => {
            assert_eq!(&msg[..], b"after_crash", "B received message from new A");
        }
        _ => unreachable!(),
    }

    drop(a2);
    drop(b);
}

// ─── Crash: receiver restarts ────────────────────────────────────────

#[tokio::test]
async fn crash_receiver() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    // Establish a session
    a.send(addr_b, b"before_crash").await.expect("initial send");
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;
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
    a.send(addr_b2, b"after_crash").await.expect("send after crash");

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

// ─── Simultaneous handshake ──────────────────────────────────────────

#[tokio::test]
async fn simultaneous_handshake() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    // Both sides initiate at roughly the same time
    a.send(addr_b, b"from_a").await.expect("A send");
    b.send(addr_a, b"from_b").await.expect("B send");

    // Both should get Connected and Data
    let _ = wait_for(&mut events_a, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;

    let _ = wait_for(&mut events_a, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;
}

// ─── Session timeout ─────────────────────────────────────────────────

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
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;

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

// ─── Multiple peers ──────────────────────────────────────────────────

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
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_b, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;

    a.send(addr_c, b"a_to_c").await.expect("A→C");
    let _ = wait_for(&mut events_c, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _ = wait_for(&mut events_c, |e| matches!(e, Event::Data(..)), Duration::from_secs(5)).await;

    drop(a);
    drop(b);
    drop(c);
}
