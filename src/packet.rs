use bytes::BytesMut;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    KcpData = 0,
    Syn = 1,
    SynAck = 2,
    Reset = 3,
}

impl PacketType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(PacketType::KcpData),
            1 => Some(PacketType::Syn),
            2 => Some(PacketType::SynAck),
            3 => Some(PacketType::Reset),
            _ => None,
        }
    }
}

/// Encode a control packet (SYN / SYN_ACK / RESET).
/// Layout: [type: 1B][conv_id: 4B LE]
pub fn encode_control(typ: PacketType, conv_id: u32) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5);
    buf.extend_from_slice(&[typ as u8]);
    buf.extend_from_slice(&conv_id.to_le_bytes());
    buf
}

/// Encode a KCP data packet.
/// Layout: [type: 1B][kcp_packet: …]
pub fn encode_kcp_data(kcp_packet: &[u8]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + kcp_packet.len());
    buf.extend_from_slice(&[PacketType::KcpData as u8]);
    buf.extend_from_slice(kcp_packet);
    buf
}

/// Split an incoming buffer into (type byte, rest).
/// Returns None if the buffer is empty.
pub fn split_packet(buf: &[u8]) -> Option<(PacketType, &[u8])> {
    let (&first, rest) = buf.split_first()?;
    let typ = PacketType::from_byte(first)?;
    Some((typ, rest))
}
