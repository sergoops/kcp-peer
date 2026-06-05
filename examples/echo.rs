use std::time::Duration;

use kcp_peer::{Event, KcpConfig, KcpPeer};
use tokio::io::AsyncWriteExt;
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
            match server_events.recv().await {
                Ok(Event::Data(addr, data)) => {
                    println!("server: echo {} bytes to {addr}", data.len());
                    let _ = server.send(addr, &data).await;
                }
                Ok(ev) => println!("server: {ev:?}"),
                Err(_) => break,
            }
        }
    });

    // ── Helper: run one client incarnation ──
    async fn run_client(label: &str, addr: &str, payload: &[u8], config: KcpConfig) {
        let client = KcpPeer::bind_with(addr, config)
            .await
            .expect("bind client");
        let mut events = client.events();

        let mut conn = client
            .connect(SERVER_ADDR.parse().unwrap())
            .await
            .expect("connect");

        conn.write_all(payload).await.expect("write");
        conn.flush().await.expect("flush");

        loop {
            match events.recv().await {
                Ok(Event::Data(peer, data)) => {
                    println!("{label}: Data({peer}, {:?})", std::str::from_utf8(&data));
                    assert_eq!(&data[..], payload);
                    println!("{label}: echo OK");
                    break;
                }
                Ok(ev) => println!("{label}: {ev:?}"),
                Err(_) => break,
            }
        }

        drop(conn);
        drop(client);
    }

    // ── Client v1 ──
    run_client("client[v1]", CLIENT_ADDR, b"hello v1", config.clone()).await;

    // ── Crash + wait ──
    sleep(Duration::from_millis(100)).await;

    // ── Client v2 (restart on same port) ──
    run_client("client[v2]", CLIENT_ADDR, b"hello v2 (after restart)", config).await;
}
