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
    /// Base interval for SYN retransmission with exponential backoff.
    ///
    /// The wait before retry `i` (0-indexed) is `base * 2^i`. Default 150ms
    /// with [`syn_max_retries`](KcpConfig::syn_max_retries) = 5 gives the
    /// timeline:
    /// - T+0.15s — retry 1
    /// - T+0.45s — retry 2
    /// - T+1.05s — retry 3
    /// - T+2.25s — retry 4
    /// - T+4.65s — retry 5, then [`DeadLink`](crate::Error::DeadLink)
    ///
    /// The initial SYN is sent in [`initiate_session`](crate::KcpPeer::send);
    /// only subsequent retries follow this schedule.
    pub syn_retry_interval: Duration,
    /// Maximum number of SYN retransmissions before the handshake is abandoned
    /// and [`DeadLink`](crate::Error::DeadLink) is reported.
    ///
    /// Default is 5, giving ~4.7s total time to exhaustion with the default
    /// [`syn_retry_interval`](KcpConfig::syn_retry_interval).
    pub syn_max_retries: u32,
    /// Maximum Transmission Unit for KCP segments.
    ///
    /// Controls the largest unfragmented segment size KCP will produce.
    /// Must be at least 50 (KCP minimum). Default is 1400.
    pub mtu: u16,
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
            tick_interval: Duration::from_millis(20),
            session_timeout: Duration::from_secs(60),
            event_channel_capacity: 1024,
            syn_retry_interval: Duration::from_millis(150),
            syn_max_retries: 5,
            mtu: 1400,
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
        let mtu = std::cmp::max(self.mtu, 50) as usize;
        kcp.set_mtu(mtu).ok();
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

    pub fn syn_retry_interval(mut self, v: Duration) -> Self {
        self.0.syn_retry_interval = v;
        self
    }

    pub fn syn_max_retries(mut self, v: u32) -> Self {
        self.0.syn_max_retries = v;
        self
    }

    pub fn mtu(mut self, v: u16) -> Self {
        self.0.mtu = v;
        self
    }

    pub fn build(self) -> KcpConfig {
        KcpConfig {
            mtu: std::cmp::max(self.0.mtu, 50),
            ..self.0
        }
    }
}
