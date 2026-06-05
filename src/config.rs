use std::time::Duration;

use kcp::Kcp;

/// P2P-oriented KCP configuration.
///
/// Create via [`KcpConfigBuilder`] (obtained from [`KcpConfig::builder`]).
#[derive(Debug, Clone)]
pub struct KcpConfig {
    /// KCP internal update interval in milliseconds (passed to `kcp_set_interval`).
    ///
    /// Controls how frequently KCP checks for retransmission and window updates.
    pub kcp_interval_ms: u32,
    /// `(nodelay, interval, resend, nc)` passed directly to `kcp_set_nodelay`.
    ///
    /// `nodelay=2` enables aggressive RTO calculation.
    pub kcp_nodelay: (i32, i32, i32, bool),
    /// KCP send window size (segments).
    pub snd_wnd: u16,
    /// KCP receive window size (segments).
    pub rcv_wnd: u16,
    /// Minimum RTO in milliseconds.
    pub rx_minrto: u32,
    /// Fast retransmission threshold (number of duplicate ACKs).
    pub fast_resend: u32,
    /// Maximum retransmissions before [`DeadLink`](crate::Error::DeadLink) is reported.
    pub maximum_resend_times: u32,
    /// How often the background update task drives KCP updates.
    pub tick_interval: Duration,
    /// Close sessions that have been idle (no incoming packets) for this duration.
    pub session_timeout: Duration,
    /// Capacity of the `tokio::sync::broadcast` event channel.
    ///
    /// If subscribers are slower than the event rate, old events are dropped.
    /// Check [`Event::EventChannelLagged`](crate::Error::EventChannelLagged).
    pub event_channel_capacity: usize,
}

impl Default for KcpConfig {
    fn default() -> Self {
        Self {
            kcp_interval_ms: 20,
            kcp_nodelay: (1, 20, 2, true),
            snd_wnd: 128,
            rcv_wnd: 128,
            rx_minrto: 10,
            fast_resend: 1,
            maximum_resend_times: 20,
            tick_interval: Duration::from_millis(10),
            session_timeout: Duration::from_secs(60),
            event_channel_capacity: 1024,
        }
    }
}

impl KcpConfig {
    /// Apply every field to a freshly-created Kcp instance.
    pub fn apply_to(&self, kcp: &mut Kcp<impl std::io::Write>) {
        let (nodelay, interval, resend, nc) = self.kcp_nodelay;
        kcp.set_nodelay(nodelay, interval, resend, nc);
        kcp.set_wndsize(self.snd_wnd, self.rcv_wnd);
        kcp.set_rx_minrto(self.rx_minrto);
        kcp.set_fast_resend(self.fast_resend);
        kcp.set_maximum_resend_times(self.maximum_resend_times);
        kcp.set_interval(self.kcp_interval_ms);
    }

    /// Create a builder.
    pub fn builder() -> KcpConfigBuilder {
        KcpConfigBuilder(KcpConfig::default())
    }
}

/// Builder for KcpConfig; setters chain.
#[derive(Debug)]
pub struct KcpConfigBuilder(pub(crate) KcpConfig);

impl KcpConfigBuilder {
    pub fn kcp_interval_ms(mut self, v: u32) -> Self {
        self.0.kcp_interval_ms = v;
        self
    }

    pub fn kcp_nodelay(mut self, nodelay: i32, interval: i32, resend: i32, nc: bool) -> Self {
        self.0.kcp_nodelay = (nodelay, interval, resend, nc);
        self
    }

    pub fn snd_wnd(mut self, v: u16) -> Self {
        self.0.snd_wnd = v;
        self
    }

    pub fn rcv_wnd(mut self, v: u16) -> Self {
        self.0.rcv_wnd = v;
        self
    }

    pub fn rx_minrto(mut self, v: u32) -> Self {
        self.0.rx_minrto = v;
        self
    }

    pub fn fast_resend(mut self, v: u32) -> Self {
        self.0.fast_resend = v;
        self
    }

    pub fn maximum_resend_times(mut self, v: u32) -> Self {
        self.0.maximum_resend_times = v;
        self
    }

    pub fn tick_interval(mut self, v: Duration) -> Self {
        self.0.tick_interval = v;
        self
    }

    pub fn session_timeout(mut self, v: Duration) -> Self {
        self.0.session_timeout = v;
        self
    }

    pub fn event_channel_capacity(mut self, v: usize) -> Self {
        self.0.event_channel_capacity = v;
        self
    }

    pub fn build(self) -> KcpConfig {
        self.0
    }
}
