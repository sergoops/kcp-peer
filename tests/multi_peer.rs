mod common;
use common::*;
use std::time::Duration;

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

    assert!(a.peers().is_empty(), "no peers before connecting");

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

    a.disconnect(addr_b);

    let peers = a.peers();
    assert_eq!(peers.len(), 1, "one peer after disconnect B");
    assert!(peers.contains(&addr_c), "peers contains only C");

    drop(a);
    drop(b);
    drop(c);
}
