use std::time::Duration;

use kcp_peer::{Event, KcpConfig, KcpPeer};

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

    let mut client_events = client.events();
    let server_addr = "127.0.0.1:9877".parse().unwrap();

    for msg in &["hello", "world", "from", "client"] {
        client
            .send(server_addr, msg.as_bytes())
            .await
            .expect("send");
        println!("client: sent \"{msg}\"");
    }

    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(5), client_events.recv()).await {
            Ok(Ok(Event::Data(_, data))) => {
                println!("client: got \"{}\"", String::from_utf8_lossy(&data));
            }
            Ok(Ok(_)) => continue,
            Ok(Err(_)) | Err(_) => panic!("timeout or channel closed"),
        }
    }

    println!("duplex done");
}
