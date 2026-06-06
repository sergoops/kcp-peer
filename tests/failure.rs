mod common;
use common::*;
use std::time::Duration;

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

    let unreachable: SocketAddr = "127.0.0.1:59999".parse().unwrap();
    let _ = a.send(unreachable, b"hello").await;

    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    drop(a);
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

    let unreachable: SocketAddr = "127.0.0.1:59998".parse().unwrap();
    let start = tokio::time::Instant::now();
    let _ = a.send(unreachable, b"hello").await;

    let _ = wait_for_event(
        &mut events_a,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;
    let elapsed = start.elapsed();

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

    a.send(addr_b, b"ping").await.expect("first send");
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(15)).await;

    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;

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
async fn concurrent_send_to_unknown_peer() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

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
