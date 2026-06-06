mod common;
use common::*;
use std::time::Duration;

#[test]
fn unknown_session_reset() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();

        let conv_id: u32 = 12345;
        let mut packet = vec![0x00u8];
        packet.extend_from_slice(&conv_id.to_le_bytes());
        packet.extend_from_slice(&[0xAB; 8]);

        let resp = raw_packet(addr_b, &packet);
        let resp = resp.expect("should receive RESET response");

        assert_eq!(resp.len(), 5, "RESET is 5 bytes");
        assert_eq!(resp[0], 0x03, "type byte is Reset");
        assert_eq!(&resp[1..5], &conv_id.to_le_bytes(), "conv_id matches");

        drop(b);
    });
}

#[test]
fn truncated_syn() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        let mut packet = vec![0x01u8];
        packet.push(0x00);

        let _ = raw_packet(addr_b, &packet);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            b.peers().is_empty(),
            "no sessions created from truncated SYN"
        );
        assert!(events_b.try_recv().is_err(), "no events from truncated SYN");

        drop(b);
    });
}

#[test]
fn truncated_syn_ack() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        let mut packet = vec![0x02u8];
        packet.push(0x00);

        let _ = raw_packet(addr_b, &packet);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            b.peers().is_empty(),
            "no sessions created from truncated SYN_ACK"
        );
        assert!(
            events_b.try_recv().is_err(),
            "no events from truncated SYN_ACK"
        );

        drop(b);
    });
}

#[test]
fn syn_ack_unknown_session() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        let conv_id: u32 = 99999;
        let mut packet = vec![0x02u8];
        packet.extend_from_slice(&conv_id.to_le_bytes());

        let _ = raw_packet(addr_b, &packet);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            b.peers().is_empty(),
            "no sessions created from SYN_ACK to unknown session"
        );
        assert!(
            events_b.try_recv().is_err(),
            "no events from SYN_ACK to unknown session"
        );

        drop(b);
    });
}

#[test]
fn reset_unknown_session() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let b = KcpPeer::bind_with("127.0.0.1:0", test_config())
            .await
            .expect("bind B");
        let addr_b = b.local_addr();
        let mut events_b = b.events();

        let conv_id: u32 = 77777;
        let mut packet = vec![0x03u8];
        packet.extend_from_slice(&conv_id.to_le_bytes());

        let _ = raw_packet(addr_b, &packet);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            b.peers().is_empty(),
            "no sessions created from RESET to unknown session"
        );
        assert!(
            events_b.try_recv().is_err(),
            "no events from RESET to unknown session"
        );

        drop(b);
    });
}

#[tokio::test]
async fn reset_wrong_conv_id() {
    let config = test_config();

    let a = KcpPeer::bind_with("127.0.0.1:0", config.clone())
        .await
        .expect("bind A");
    let b = KcpPeer::bind_with("127.0.0.1:0", config)
        .await
        .expect("bind B");
    let addr_a = a.local_addr();
    let addr_b = b.local_addr();
    let mut events_b = b.events();

    a.send(addr_b, b"hello").await.expect("A send");
    let _ = wait_for_event(
        &mut events_b,
        |e| matches!(e, Event::Connected(_)),
        Duration::from_secs(5),
    )
    .await;
    let _ = wait_for_data(&b, |_| true, Duration::from_secs(5)).await;

    let conv = b.stats(addr_a).expect("stats").conv_id;

    drop(a);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let wrong_conv = conv.wrapping_add(1);
    {
        let raw = std::net::UdpSocket::bind(addr_a).expect("bind raw on A addr");
        let reset = kcp_peer::packet::encode_control(kcp_peer::PacketType::Reset, wrong_conv);
        raw.send_to(&reset, addr_b).ok();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        b.stats(addr_a).is_some(),
        "session not removed by RESET with wrong conv_id"
    );

    {
        let raw = std::net::UdpSocket::bind(addr_a).expect("bind raw on A addr");
        let reset = kcp_peer::packet::encode_control(kcp_peer::PacketType::Reset, conv);
        raw.send_to(&reset, addr_b).ok();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        b.stats(addr_a).is_none(),
        "session removed by RESET with correct conv_id"
    );

    drop(b);
}
