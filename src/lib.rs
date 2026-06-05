#![doc = include_str!("../README.md")]

pub mod config;
pub mod error;
pub mod packet;
pub mod session;
pub mod transport;

pub use config::{KcpConfig, KcpConfigBuilder};
pub use error::{Error, Result};
pub use packet::PacketType;
pub use session::Session;
pub use transport::{DataMessage, Event, EventReceiver, KcpPeer, PeerStats};
