// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Shared client options — capture, reconnect, timeout.
//!
//! Used by all transport clients for a consistent API.

use std::sync::Arc;
use std::time::Duration;

use crate::bus_timing::BusTiming;
use crate::reconnect::ReconnectConfig;
use crate::wire_tap::WireTap;

/// Common options for all Modbus clients — timeout, WireTap, reconnect, timing.
///
/// # Examples
///
/// ```no_run
/// use oms_modbus::*;
/// use std::time::Duration;
///
/// let opts = ClientOptions::default()
///     .with_timeout(Duration::from_secs(3))
///     .with_tap(std::sync::Arc::new(BusCapture::unbounded()))
///     .with_reconnect(3, Duration::from_millis(100));
///
/// # let port = tokio::io::duplex(64).0;
/// let client = rtu::with_options(port, opts);
/// ```
#[derive(Clone)]
pub struct ClientOptions {
    pub(crate) timeout: Duration,
    pub(crate) tap: Option<Arc<dyn WireTap>>,
    pub(crate) reconnect: Option<ReconnectConfig>,
    pub(crate) bus_timing: Option<Arc<BusTiming>>,
    pub(crate) data_channel_capacity: Option<usize>,
    pub(crate) tap_channel_capacity: Option<usize>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
            tap: None,
            reconnect: None,
            bus_timing: Some(Arc::new(BusTiming::rtu_35t(9600))),
            data_channel_capacity: None,
            tap_channel_capacity: None,
        }
    }
}

impl ClientOptions {
    /// Create a new `ClientOptions` with default values (1s timeout, 3.5T @ 9600 baud).
    pub fn new() -> Self {
        Self::default()
    }
    /// Current request timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    /// Configured reconnect parameters, if any.
    pub fn reconnect(&self) -> Option<&ReconnectConfig> {
        self.reconnect.as_ref()
    }
    /// Configured bus timing, if any.
    pub fn bus_timing(&self) -> Option<&Arc<BusTiming>> {
        self.bus_timing.as_ref()
    }
    /// Attached WireTap, if any.
    pub fn tap(&self) -> Option<&Arc<dyn WireTap>> {
        self.tap.as_ref()
    }

    /// Set the per-request timeout. Applied to both send and receive.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attach any [`WireTap`] — `BusCapture`, `FileRecorder`, or custom.
    pub fn with_tap(mut self, tap: Arc<dyn WireTap>) -> Self {
        self.tap = Some(tap);
        self
    }

    /// Enable auto-reconnect on transport errors with a fixed retry interval.
    /// `max_retries = 0` means infinite.
    pub fn with_reconnect(mut self, max_retries: u32, interval: Duration) -> Self {
        self.reconnect = Some(ReconnectConfig::new(max_retries, interval));
        self
    }

    /// Enforce minimum spacing between Modbus frames.
    ///
    /// Prevents sending too fast on RS-485 buses where low-performance
    /// slave devices may crash. Use [`BusTiming::rtu_35t`] for standard
    /// RTU/ASCII timing, or [`BusTiming::custom`] for a fixed delay.
    pub fn with_bus_timing(mut self, timing: BusTiming) -> Self {
        self.bus_timing = Some(Arc::new(timing));
        self
    }

    /// Set the SniffIo data channel capacity in bytes (default: 1 MB).
    ///
    /// Only meaningful when a tap is attached. The data channel buffers
    /// bytes between the background reader and `poll_read`. When full,
    /// oldest chunks are evicted.
    pub fn with_data_channel_capacity(mut self, bytes: usize) -> Self {
        self.data_channel_capacity = Some(bytes);
        self
    }

    /// Set the SniffIo tap channel capacity in bytes (default: 256 KB).
    ///
    /// Only meaningful when a tap is attached. The tap channel buffers
    /// bytes for the tap forwarder task. When full, oldest chunks are evicted.
    pub fn with_tap_channel_capacity(mut self, bytes: usize) -> Self {
        self.tap_channel_capacity = Some(bytes);
        self
    }
}
