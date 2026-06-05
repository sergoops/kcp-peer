use std::time::Duration;

use kcp_peer::{KcpConfig, KcpPeer};

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

    tokio::spawn(async move {
        loop {
            match server.recv().await {
                Ok(msg) => {
                    let msg_str = String::from_utf8_lossy(&msg.data);
                    println!("server: received \"{msg_str}\"");
                    let _ = server.send(msg.peer, &msg.data).await;
                }
                Err(_) => break,
            }
        }
        println!("server: done");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind client");

    let server_addr = "127.0.0.1:9877".parse().unwrap();

    for msg in &["hello", "world", "from", "client"] {
        client
            .send(server_addr, msg.as_bytes())
            .await
            .expect("send");
        println!("client: sent \"{msg}\"");
    }

    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
            Ok(Ok(msg)) => {
                println!("client: got \"{}\"", String::from_utf8_lossy(&msg.data));
            }
            _ => panic!("timeout or channel closed"),
        }
    }

    println!("duplex done");
}
