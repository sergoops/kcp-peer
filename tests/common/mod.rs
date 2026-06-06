pub use std::net::SocketAddr;
pub use std::time::Duration;

pub use kcp_peer::{DataMessage, Event, KcpConfig, KcpPeer};

/// Send a raw UDP packet to `target` and read one response.
/// Returns the response bytes or None on timeout/error.
pub fn raw_packet(target: SocketAddr, packet: &[u8]) -> Option<Vec<u8>> {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").ok()?;
    sock.send_to(packet, target).ok()?;
    sock.set_read_timeout(Some(Duration::from_millis(300)))
        .ok()?;
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
pub async fn bind_pair() -> (KcpPeer, KcpPeer, SocketAddr, SocketAddr) {
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
pub async fn wait_for_event<F>(
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
pub async fn wait_for_data<F>(peer: &KcpPeer, f: F, timeout: Duration) -> DataMessage
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

/// Build default test config.
pub fn test_config() -> KcpConfig {
    KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .session_timeout(Duration::from_secs(60))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build()
}
