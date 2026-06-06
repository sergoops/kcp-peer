mod common;
use common::*;
use std::time::Duration;

#[tokio::test]
async fn recv_cancel_safety() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    let _ = tokio::time::timeout(Duration::from_millis(10), b.recv()).await;

    a.send(addr_b, b"after_cancel")
        .await
        .expect("A send after cancel");

    let data = tokio::time::timeout(Duration::from_secs(5), b.recv())
        .await
        .expect("timeout")
        .expect("recv error");
    assert_eq!(
        &data.data[..],
        b"after_cancel",
        "data not lost after cancel"
    );

    drop(a);
    drop(b);
}

#[tokio::test]
async fn multiple_event_subscribers() {
    let config = test_config();
    let a = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", KcpConfig::builder().tick_interval(Duration::from_millis(10)).session_timeout(Duration::from_secs(60)).build())
        .await
        .expect("bind B");
    let addr_b = b.local_addr();

    let mut ev1 = b.events();
    let mut ev2 = b.events();

    a.send(addr_b, b"hello").await.expect("A send");

    let _c1 = wait_for_event(&mut ev1, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;
    let _c2 = wait_for_event(&mut ev2, |e| matches!(e, Event::Connected(_)), Duration::from_secs(5)).await;

    drop(a);
    drop(b);
}

#[test]
fn config_mtu_clamp() {
    let config = KcpConfig::builder().mtu(30).build();
    assert_eq!(config.mtu, 50, "mtu=30 clamped to 50");

    let config = KcpConfig::builder().mtu(0).build();
    assert_eq!(config.mtu, 50, "mtu=0 clamped to 50");

    let config = KcpConfig::builder().mtu(1400).build();
    assert_eq!(config.mtu, 1400, "mtu=1400 unchanged");

    let config = KcpConfig::builder().mtu(50).build();
    assert_eq!(config.mtu, 50, "mtu=50 unchanged (boundary)");
}
