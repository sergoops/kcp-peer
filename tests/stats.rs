mod common;
use common::*;
use std::time::Duration;

#[tokio::test]
async fn stats_active_peer() {
    let (a, b, addr_a, addr_b) = bind_pair().await;
    let mut events_b = b.events();

    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    let stats = b.stats(addr_a).expect("stats should return Some");
    assert!(stats.conv_id != 0, "conv_id should be non-zero");
    assert_eq!(stats.send_wnd, 128, "send_wnd matches config");
    assert_eq!(stats.recv_wnd, 128, "recv_wnd matches config");
    assert!(!stats.dead_link, "dead_link should be false");

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

    a.send(addr_b, b"ping").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    assert!(a.stats(addr_b).is_some(), "stats before disconnect");

    a.disconnect(addr_b);

    assert!(
        a.stats(addr_b).is_none(),
        "stats after disconnect returns None"
    );

    drop(a);
    drop(b);
}
