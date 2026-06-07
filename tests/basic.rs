mod common;
use common::*;
use std::time::Duration;

#[tokio::test]
async fn basic_send_recv() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    a.send(addr_b, b"hello").await.expect("A→B send");

    let connected = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(connected, Event::Connected(_)));

    let data = wait_for_data(
        &b,
        |m| m.peer == addr_a && m.data[..] == *b"hello",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(data.peer, addr_a);
    assert_eq!(&data.data[..], b"hello");

    b.send(addr_a, b"world").await.expect("B→A send");

    let data2 = wait_for_data(
        &a,
        |m| m.peer == addr_b && m.data[..] == *b"world",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(data2.peer, addr_b);
    assert_eq!(&data2.data[..], b"world");
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
async fn send_burst_established() {
    let (a, b, _, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    a.send(addr_b, b"connect").await.expect("connect send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;

    let dm = b.recv().await.expect("b recv connect");
    assert_eq!(&dm.data[..], b"connect");

    let n = 200;
    for i in 0..n {
        let msg = format!("msg-{i}");
        a.send(addr_b, msg.as_bytes()).await.expect("burst send");
    }

    for i in 0..n {
        let expected = format!("msg-{i}");
        let dm = b.recv().await.expect("b recv burst");
        assert_eq!(dm.data, expected.as_bytes(), "burst message {i}");
    }

    drop(a);
    drop(b);
}
