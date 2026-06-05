use std::time::Duration;

use kcp_peer::{Event, KcpConfig, KcpPeer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let server = KcpPeer::bind_with("127.0.0.1:9876", config.clone())
        .await
        .expect("bind server");

    let mut events = server.events();

    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(Event::Data(addr, data)) => {
                    println!("server: got {} bytes from {addr}", data.len());
                    let _ = server.send(addr, &data).await;
                }
                Ok(Event::Connected(addr)) => {
                    println!("server: connected {addr}");
                }
                Ok(Event::Disconnected(addr)) => {
                    println!("server: disconnected {addr}");
                }
                Ok(Event::PeerRestarted(addr)) => {
                    println!("server: peer restarted {addr}");
                }
                Err(_) => break,
            }
        }
    });

    let client = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind client");

    let mut conn = client
        .connect("127.0.0.1:9876".parse().unwrap())
        .await
        .expect("connect");

    let payload = b"hello kcp_peer";
    conn.write_all(payload).await.expect("write");
    conn.flush().await.expect("flush");

    let mut response = vec![0u8; payload.len()];
    conn.read_exact(&mut response).await.expect("read");
    assert_eq!(&response, payload);
    println!("echo OK");
}
