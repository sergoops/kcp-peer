use std::time::Duration;

use kcp_peer::{Event, KcpConfig, KcpPeer};
use tokio::io::AsyncWriteExt;

const PORTS: &[u16] = &[9879, 9880, 9881];

#[tokio::main]
async fn main() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let node0 = new_node(PORTS[0], config.clone()).await;
    let node1 = new_node(PORTS[1], config.clone()).await;
    let node2 = new_node(PORTS[2], config).await;

    tokio::spawn(accept_loop(node1, 1));
    tokio::spawn(accept_loop(node2, 2));

    tokio::time::sleep(Duration::from_millis(50)).await;

    for &port in &PORTS[1..] {
        let mut conn = node0
            .connect(format!("127.0.0.1:{port}").parse().unwrap())
            .await
            .expect("connect");
        let msg = format!("hello from node 0 to node {port}");
        conn.write_all(msg.as_bytes()).await.unwrap();
        conn.flush().await.unwrap();
        println!("node0: sent to {port}");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("p2p gossip done");
}

async fn new_node(port: u16, config: KcpConfig) -> KcpPeer {
    KcpPeer::bind_with(format!("127.0.0.1:{port}"), config)
        .await
        .expect("bind node")
}

async fn accept_loop(node: KcpPeer, id: usize) {
    let mut events = node.events();
    loop {
        match events.recv().await {
            Ok(Event::Data(_peer, data)) => {
                let msg = String::from_utf8_lossy(&data);
                println!("node{id}: received \"{msg}\"");
            }
            Ok(Event::Disconnected(_)) | Err(_) => break,
            _ => {}
        }
    }
    println!("node{id}: done");
}
