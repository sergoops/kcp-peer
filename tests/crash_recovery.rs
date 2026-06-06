mod common;
use common::*;
use std::time::Duration;

#[tokio::test]
async fn crash_initiator() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let a = KcpPeer::bind_with("127.0.0.1:9870", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind B");
    let addr_a = a.local_addr();
    let addr_b = b.local_addr();
    let mut events_b = b.events();

    a.send(addr_b, b"before_crash").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    drop(a);
    tokio::time::sleep(Duration::from_millis(100)).await;

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

    a2.send(addr_b, b"after_crash")
        .await
        .expect("send after crash");

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

    let data = wait_for_data(
        &b,
        |m| m.peer == addr_a && m.data[..] == *b"after_crash",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(data.peer, addr_a);
    assert_eq!(&data.data[..], b"after_crash");

    drop(a2);
    drop(b);
}

#[tokio::test]
async fn crash_receiver() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    a.send(addr_b, b"before_crash").await.expect("initial send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;
    drop(events_b);

    drop(b);
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

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

    a.send(addr_b2, b"after_crash")
        .await
        .expect("send after crash");

    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let data = wait_for_data(
        &b2,
        |m| m.data[..] == *b"after_crash",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(&data.data[..], b"after_crash", "B2 received fresh data");

    drop(a);
    drop(b2);
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

    a.send(addr_b, b"first").await.expect("A send");

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
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;
    drop(events_b);

    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let b2 = KcpPeer::bind_with(addr_b, test_config())
        .await
        .expect("rebind B on same address");

    b2.send(addr_a, b"after_restart").await.expect("B2 send");

    let peer_restarted = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::PeerRestarted(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(peer_restarted, Event::PeerRestarted(addr) if addr == addr_b));

    let connected = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(addr) if addr == addr_b));

    let data = wait_for_data(
        &a,
        |m| m.peer == addr_b && m.data[..] == *b"after_restart",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(data.peer, addr_b);
    assert_eq!(&data.data[..], b"after_restart");

    drop(a);
    drop(b2);
}
