mod common;
use common::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

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
async fn drop_without_shutdown() {
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

    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    drop(a);

    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    drop(b);
}

#[tokio::test]
async fn shutdown_lifecycle() {
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

    a.shutdown().await;

    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;
}

#[tokio::test]
async fn send_after_disconnect() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    a.send(addr_b, b"first").await.expect("A→B first");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    a.disconnect(addr_b);

    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Disconnected(_)),
        Duration::from_secs(5),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    a.send(addr_b, b"second").await.expect("reconnect send");

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
    let data = wait_for_data(&b, |m| m.data[..] == *b"second", Duration::from_secs(5)).await;
    assert_eq!(&data.data[..], b"second", "reconnected data received");

    drop(a);
    drop(b);
}

#[tokio::test]
async fn send_cancel_during_handshake() {
    let (a, b, _addr_a, addr_b) = bind_pair().await;
    let mut events_a = a.events();
    let mut events_b = b.events();

    tokio::select! {
        _ = a.send(addr_b, b"cancelled") => {}
        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    a.send(addr_b, b"after_cancel")
        .await
        .expect("send after cancel");

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
    let data = wait_for_data(
        &b,
        |m| m.data[..] == *b"after_cancel",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        &data.data[..],
        b"after_cancel",
        "data after cancel handshake"
    );

    drop(a);
    drop(b);
}

async fn simultaneous_handshake_n(n: usize) {
    let config = test_config();

    let mut peers: Vec<Arc<KcpPeer>> = Vec::new();
    let mut addrs = Vec::new();
    let mut event_rxs = Vec::new();

    for _ in 0..n {
        let p = Arc::new(
            KcpPeer::bind_with("127.0.0.1:0", config.clone())
                .await
                .expect("bind"),
        );
        addrs.push(p.local_addr());
        event_rxs.push(p.events());
        peers.push(p);
    }

    let timeout = match n {
        2 => Duration::from_secs(5),
        3 => Duration::from_secs(10),
        _ => Duration::from_secs(15),
    };

    let mut handles = Vec::new();
    for (i, mut rx) in event_rxs.into_iter().enumerate() {
        let peer = peers[i].clone();
        let addrs = addrs.clone();

        handles.push(tokio::spawn(async move {
            let msg = vec![i as u8; 32];
            for (j, &addr) in addrs.iter().enumerate() {
                if j == i {
                    continue;
                }
                peer.send(addr, &msg).await.expect("send");
            }

            let mut connected = HashSet::new();
            while connected.len() < n - 1 {
                match tokio::time::timeout(timeout, rx.recv()).await {
                    Ok(Ok(Event::Connected(addr))) => {
                        connected.insert(addr);
                    }
                    Ok(Ok(_)) => continue,
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(k))) => {
                        panic!("event channel lagged by {k}");
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                        panic!("event channel closed");
                    }
                    Err(_) => panic!("timeout waiting for Connected events"),
                }
            }

            let mut received = HashSet::new();
            while received.len() < n - 1 {
                match tokio::time::timeout(timeout, peer.recv()).await {
                    Ok(Ok(msg)) => {
                        received.insert(msg.data[0]);
                    }
                    Ok(Err(e)) => panic!("recv error: {e}"),
                    Err(_) => panic!("timeout waiting for data"),
                }
            }

            for j in 0..n {
                if i != j {
                    assert!(
                        received.contains(&(j as u8)),
                        "peer {i} did not receive data from peer {j}",
                    );
                }
            }
        }));
    }

    for h in handles {
        h.await.expect("task panicked");
    }
}

#[tokio::test]
async fn simultaneous_handshake_2() {
    simultaneous_handshake_n(2).await;
}

#[tokio::test]
async fn simultaneous_handshake_3() {
    simultaneous_handshake_n(3).await;
}

#[tokio::test]
async fn simultaneous_handshake_4() {
    simultaneous_handshake_n(4).await;
}
