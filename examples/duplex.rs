use std::time::Duration;

use kcp_peer::{KcpConfig, KcpPeer};

const ALICE_ADDR: &str = "127.0.0.1:9877";
const BOB_ADDR: &str = "127.0.0.1:9878";

#[tokio::main]
async fn main() {
    let config = KcpConfig::builder()
        .tick_interval(Duration::from_millis(10))
        .kcp_nodelay(2, 10, 2, true)
        .rx_minrto(10)
        .fast_resend(1)
        .build();

    let alice = KcpPeer::bind_with(ALICE_ADDR, config.clone())
        .await
        .expect("bind Alice");
    let bob = KcpPeer::bind_with(BOB_ADDR, config)
        .await
        .expect("bind Bob");

    let bob_addr = bob.local_addr();
    let alice_addr = alice.local_addr();

    // Both peers initiate to each other at the same time.
    // The protocol's tie-breaking handshake resolves the collision.
    let handle_a = tokio::spawn(async move {
        alice.send(bob_addr, b"ping from Alice").await.unwrap();
        match tokio::time::timeout(Duration::from_secs(5), alice.recv()).await {
            Ok(Ok(msg)) => {
                println!(
                    "Alice: got {:?} from {}",
                    std::str::from_utf8(&msg.data).unwrap(),
                    msg.peer
                );
            }
            _ => eprintln!("Alice: recv timeout or error"),
        }
    });

    let handle_b = tokio::spawn(async move {
        bob.send(alice_addr, b"ping from Bob").await.unwrap();
        match tokio::time::timeout(Duration::from_secs(5), bob.recv()).await {
            Ok(Ok(msg)) => {
                println!(
                    "Bob: got {:?} from {}",
                    std::str::from_utf8(&msg.data).unwrap(),
                    msg.peer
                );
            }
            _ => eprintln!("Bob: recv timeout or error"),
        }
    });

    handle_a.await.unwrap();
    handle_b.await.unwrap();
    println!("duplex done");
}
