use std::time::Duration;

use kcp_peer::{Event, KcpConfig, KcpPeer};
use tokio::time::sleep;

const CLIENT_ADDR: &str = "127.0.0.1:9900";
const SERVER_ADDR: &str = "127.0.0.1:9876";

#[tokio::main]
async fn main() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    // ── Server ──
    let server = KcpPeer::bind_with(SERVER_ADDR, config.clone())
        .await
        .expect("bind server");
    let mut server_events = server.events();
    tokio::spawn(async move {
        loop {
            match server.recv().await {
                Ok(msg) => {
                    println!("server: echo {} bytes to {}", msg.data.len(), msg.peer);
                    let _ = server.send(msg.peer, &msg.data).await;
                }
                Err(_) => break,
            }
        }
        // Drain lifecycle events to keep the task alive until shutdown
        loop {
            match server_events.recv().await {
                Ok(Event::Disconnected(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    // ── Helper: run one client incarnation ──
    async fn run_client(label: &str, addr: &str, payload: &[u8], config: KcpConfig) {
        let client = KcpPeer::bind_with(addr, config).await.expect("bind client");

        let server_addr = SERVER_ADDR.parse().unwrap();
        client.send(server_addr, payload).await.expect("send");

        match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
            Ok(Ok(msg)) => {
                println!(
                    "{label}: Data({}, {:?})",
                    msg.peer,
                    std::str::from_utf8(&msg.data)
                );
                assert_eq!(&msg.data[..], payload);
                println!("{label}: echo OK");
            }
            _ => panic!("{label}: no echo received"),
        }

        drop(client);
    }

    // ── Client v1 ──
    run_client("client[v1]", CLIENT_ADDR, b"hello v1", config.clone()).await;

    // ── Crash + wait ──
    sleep(Duration::from_millis(100)).await;

    // ── Client v2 (restart on same port) ──
    run_client(
        "client[v2]",
        CLIENT_ADDR,
        b"hello v2 (after restart)",
        config,
    )
    .await;
}
