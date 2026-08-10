// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Passive bus observer — attached to the physical transport via
//! `SniffIo`.
//!
//! Unlike higher-level abstractions, `WireTap` knows nothing about Modbus,
//! slave addresses, or protocol framing. It sees only bytes moving across
//! the wire and the microseconds at which they moved — exactly like a
//! hardware logic analyzer clipped onto an RS-485 bus.
//!
//! ## Design
//!
//! Hooks fire at the I/O boundary (immediately after `poll_read` / `poll_write`
//! return) so timestamps reflect real wire activity. The tap is driven
//! exclusively by `SniffIo` — neither the client nor the server
//! participates in capture.
//!
//! ## Consumers
//!
//! Implement this trait to build recording backends:
//! - [`BusCapture`](crate::BusCapture) — in-memory buffer + statistics
//! - [`RingBufferCapture`](crate::monitor::RingBufferCapture) — bounded / unbounded ring
//! - [`ChannelRecorder`](crate::monitor::ChannelRecorder) + [`RecordSink`](crate::monitor::RecordSink) — async dispatch
//! - [`FileRecorder`](crate::monitor::FileRecorder) — disk logging
//! - [`TrafficStats`](crate::TrafficStats) — lightweight counters

/// Passive observer attached to the physical transport.
///
/// Every method has a default no-op implementation — implement only the
/// hooks you need. Attach via
/// [`ClientOptions::with_tap`](crate::ClientOptions::with_tap) or
/// [`ClientOptions::with_tap`](crate::ClientOptions::with_tap).
///
/// # Custom implementation
///
/// ```no_run
/// use oms_modbus::*;
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// struct ByteCounter {
///     tx: AtomicU64,
///     rx: AtomicU64,
/// }
///
/// impl WireTap for ByteCounter {
///     fn on_write(&self, bytes: &[u8], _ts: u64) {
///         self.tx.fetch_add(bytes.len() as u64, Ordering::Relaxed);
///     }
///     fn on_read(&self, bytes: &[u8], _ts: u64) {
///         self.rx.fetch_add(bytes.len() as u64, Ordering::Relaxed);
///     }
///     // on_error has a default no-op — not implemented here
/// }
///
/// let tap = std::sync::Arc::new(ByteCounter { tx: 0.into(), rx: 0.into() });
/// let opts = ClientOptions::default().with_tap(tap);
/// ```
///
/// ## Timestamps
///
/// `timestamp_us` is microseconds since Unix epoch — hybrid `Instant` +
/// `SystemTime` clock. Real wall-clock time, formatted as ISO 8601 by
/// [`format_timestamp`](crate::format_timestamp).
pub trait WireTap: Send + Sync {
    /// Bytes successfully written to the transport.
    /// Called once per `poll_write` that returns `Ok(n)` with `n > 0`.
    fn on_write(&self, _bytes: &[u8], _timestamp_us: u64) {}

    /// Bytes successfully read from the transport.
    /// Called once per `poll_read` that returns `Ready(Ok(()))` with new data.
    fn on_read(&self, _bytes: &[u8], _timestamp_us: u64) {}

    /// Read returned an error, but partial data may have been received.
    /// Called when `poll_read` returns `Ready(Err(e))` and the read buffer
    /// contains bytes that arrived before the error.
    fn on_error(&self, _bytes: &[u8], _error: &str, _timestamp_us: u64) {}
}

/// Microsecond timestamp since Unix epoch — real wall-clock time.
///
/// # Performance
///
/// On first call, captures both `Instant::now()` and `SystemTime::now()`
/// to compute a fixed epoch-offset. All subsequent calls use only
/// `Instant::elapsed()` + offset — one monotonic clock read, no syscall.
/// At Modbus rates (≤1000 exchanges/sec), the overhead is invisible.
///
/// # Precision
///
/// The absolute value is meaningful: `PacketRecord::Display` and
/// `FileRecorder` format it as human-readable date/time. Monotonic
/// within a process lifetime — immune to system clock adjustments.
#[inline]
pub fn now_micros() -> u64 {
    use std::sync::OnceLock;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    /// (base_instant, epoch_offset_micros)
    static TIMEBASE: OnceLock<(Instant, u64)> = OnceLock::new();

    let &(base, offset) = TIMEBASE.get_or_init(|| {
        let now_inst = Instant::now();
        let now_sys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        (now_inst, now_sys)
    });

    offset + base.elapsed().as_micros() as u64
}
