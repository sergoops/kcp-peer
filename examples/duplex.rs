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

    let server = KcpPeer::bind_with("127.0.0.1:9877", config.clone())
        .await
        .expect("bind server");

    let mut events = server.events();

    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(Event::Data(addr, data)) => {
                    let msg = String::from_utf8_lossy(&data);
                    println!("server: received \"{msg}\"");
                    let _ = server.send(addr, &data).await;
                }
                Ok(Event::Disconnected(_)) | Err(_) => break,
                _ => {}
            }
        }
        println!("server: done");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind client");

    let mut conn = client
        .connect("127.0.0.1:9877".parse().unwrap())
        .await;

    for msg in &["hello", "world", "from", "client"] {
        conn.write_all(msg.as_bytes()).await.unwrap();
        conn.flush().await.unwrap();
        println!("client: sent \"{msg}\"");
    }

    for _ in 0..4 {
        let mut buf = [0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        println!("client: got \"{}\"", String::from_utf8_lossy(&buf[..n]));
    }

    println!("duplex done");
}
