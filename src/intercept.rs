// SPDX-License-Identifier: MIT OR Apache-2.0
//! Packet data types for Modbus traffic capture.
//!
//! These types are produced by [`crate::WireTap`] implementations and consumed
//! by recording backends (ring buffers, file loggers, network forwarders).
//! They carry raw bytes with microsecond timestamps — no PDU decoding.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::wire_tap::WireTap;

// ── Traffic statistics ────────────────────────────────────────────────────

/// Atomic request/response/error counter — cheap enough for production use.
///
/// Implements [`WireTap`] to count frames at the transport boundary:
/// `on_write` → request, `on_read` → response, `on_error` → error.
#[derive(Debug, Default, Clone)]
pub struct TrafficStats {
    requests: Arc<AtomicU64>,
    responses: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

impl TrafficStats {
    /// Create a new zeroed traffic counter.
    pub fn new() -> Self {
        Self::default()
    }
    /// Number of write (request) events captured.
    pub fn count_requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }
    /// Number of read (response) events captured.
    pub fn count_responses(&self) -> u64 {
        self.responses.load(Ordering::Relaxed)
    }
    /// Number of read-error events captured.
    pub fn count_errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
    /// Reset all counters to zero.
    ///
    /// Note: the three counters are reset individually — a concurrent reader
    /// may observe a mix of pre- and post-reset values. Call this from a
    /// quiescent point (no concurrent I/O) for a consistent snapshot.
    pub fn reset(&self) {
        self.requests.store(0, Ordering::Relaxed);
        self.responses.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }
}

impl WireTap for TrafficStats {
    fn on_write(&self, _bytes: &[u8], _ts: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
    fn on_read(&self, _bytes: &[u8], _ts: u64) {
        self.responses.fetch_add(1, Ordering::Relaxed);
    }
    fn on_error(&self, _bytes: &[u8], _error: &str, _ts: u64) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Packet data ────────────────────────────────────────────────────────────

/// Raw captured traffic — bytes that moved across the wire at a specific instant.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PacketData {
    /// Bytes successfully written to the transport.
    RawTx(Vec<u8>),
    /// Bytes successfully read from the transport.
    RawRx(Vec<u8>),
    /// Bytes received before a read error occurred, with the error message.
    RawError(Vec<u8>, String),
}

/// One captured I/O event with timestamp.
#[derive(Debug, Clone)]
pub struct PacketRecord {
    pub timestamp_us: u64,
    pub data: PacketData,
}

impl fmt::Display for PacketRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ts = format_timestamp(self.timestamp_us);
        match &self.data {
            PacketData::RawTx(bytes) => {
                write!(f, "[TX] {ts}  {:>3}B  ", bytes.len())?;
                write_hex(f, bytes)
            }
            PacketData::RawRx(bytes) => {
                write!(f, "[RX] {ts}  {:>3}B  ", bytes.len())?;
                write_hex(f, bytes)
            }
            PacketData::RawError(bytes, err) => {
                write!(f, "[ERR] {ts}  {:>3}B  ", bytes.len())?;
                write_hex(f, bytes)?;
                write!(f, "  — {err}")
            }
        }
    }
}

/// Format a microsecond Unix-epoch timestamp as ISO 8601.
///
/// Returns `YYYY-MM-DDTHH:MM:SS.uuuuuu` — 26 characters.
/// Allocates a new `String` on each call.
/// Used by `PacketRecord::Display` and `FileRecorder`.
///
/// # For RecordSink implementors
///
/// Import this function to format timestamps consistently with the built-in
/// display and file backends:
///
/// ```ignore
/// use oms_modbus::format_timestamp;
/// impl RecordSink for MySink {
///     fn on_packet(&mut self, record: PacketRecord) {
///         let ts = format_timestamp(record.timestamp_us);
///         println!("{ts}  {:?}", record.data);
///     }
/// }
/// ```
pub fn format_timestamp(timestamp_us: u64) -> String {
    let secs = (timestamp_us / 1_000_000) as i64;
    let us = timestamp_us % 1_000_000;

    let days = secs / 86_400;
    let time_of_day = secs % 86_400;

    let hour = time_of_day / 3600;
    let min = (time_of_day % 3600) / 60;
    let sec = time_of_day % 60;

    let (y, m, d) = days_to_ymd(days);

    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{us:06}")
}

/// Format bytes as hex: `[01 03 00 00 00 02]`
fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    write!(f, "[")?;
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            write!(f, " ")?;
        }
        write!(f, "{b:02X}")?;
    }
    write!(f, "]")
}

/// Convert days since 1970-01-01 to (year, month, day).
///
/// Used internally by [`format_timestamp`]. Exposed as `pub(crate)` for
/// [`FileRecorder`](crate::monitor::FileRecorder) which uses its own
/// record formatting but shares this calendar conversion.
///
/// Based on Howard Hinnant's civil_from_days algorithm.
pub(crate) fn days_to_ymd(mut z: i64) -> (i64, u32, u32) {
    z += 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ── Unbounded in-memory capture ────────────────────────────────────────────

/// Records all traffic in memory via [`WireTap`].
///
/// Use [`drain`](PacketCapture::drain) to retrieve and clear the buffer.
/// For bounded capture with automatic eviction, see
/// [`RingBufferCapture`](crate::monitor::RingBufferCapture).
///
/// Prefer [`BusCapture`](crate::BusCapture) for new code — it includes built-in
/// statistics counters.
pub struct PacketCapture {
    records: Mutex<Vec<PacketRecord>>,
}

impl Default for PacketCapture {
    fn default() -> Self {
        Self {
            records: Mutex::new(Vec::with_capacity(256)),
        }
    }
}

impl PacketCapture {
    /// Create an empty packet capture buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieve and clear all captured records.
    pub fn drain(&self) -> Vec<PacketRecord> {
        std::mem::take(&mut *self.records.lock().unwrap_or_else(|e| e.into_inner()))
    }
    /// Number of recorded packets.
    pub fn len(&self) -> usize {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    /// Returns `true` if no packets have been recorded.
    pub fn is_empty(&self) -> bool {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    fn record(&self, ts: u64, data: PacketData) {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(PacketRecord {
                timestamp_us: ts,
                data,
            });
    }
}

impl WireTap for PacketCapture {
    fn on_write(&self, bytes: &[u8], ts: u64) {
        self.record(ts, PacketData::RawTx(bytes.to_vec()));
    }
    fn on_read(&self, bytes: &[u8], ts: u64) {
        self.record(ts, PacketData::RawRx(bytes.to_vec()));
    }
    fn on_error(&self, bytes: &[u8], error: &str, ts: u64) {
        self.record(ts, PacketData::RawError(bytes.to_vec(), error.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── days_to_ymd ────────────────────────────────────────────────────

    #[test]
    fn epoch_zero() {
        let (y, m, d) = days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn day_one() {
        let (y, m, d) = days_to_ymd(1);
        assert_eq!((y, m, d), (1970, 1, 2));
    }

    #[test]
    fn day_364_before_year_roll() {
        let (y, m, d) = days_to_ymd(364);
        assert_eq!((y, m, d), (1970, 12, 31));
    }

    #[test]
    fn day_365_year_roll() {
        let (y, m, d) = days_to_ymd(365);
        assert_eq!((y, m, d), (1971, 1, 1));
    }

    #[test]
    fn year_2000_jan_1() {
        // 1970-01-01 → 2000-01-01: 30 years (1970–1999)
        // Leap years: 1972,76,80,84,88,92,96 = 7 of 30 years
        // Days: 23×365 + 7×366 = 8395 + 2562 = 10957
        let (y, m, d) = days_to_ymd(10957);
        assert_eq!((y, m, d), (2000, 1, 1));
    }

    #[test]
    fn feb_29_leap_year() {
        // 1972-02-29: days from epoch =
        //   1970(365) + 1971(365) + Jan-1972(31) + 28 days of Feb = 789
        let (y, m, d) = days_to_ymd(789);
        assert_eq!((y, m, d), (1972, 2, 29));
    }

    #[test]
    fn year_2026_august() {
        // 1970-2025: 56 years. 14 leap years (1972..2024).
        // Full years: 42×365 + 14×366 = 15330 + 5124 = 20454
        // 2026 before Aug 10: Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30)+Jul(31)+9 = 221
        // Total: 20454 + 221 = 20675
        let (y, m, d) = days_to_ymd(20675);
        assert_eq!((y, m, d), (2026, 8, 10));
    }

    #[test]
    fn far_future_year_2100() {
        // 2100 is NOT a leap year (divisible by 100 but not 400)
        // 1970-2099: 130 years, 32 leap years
        // Days: 98×365 + 32×366 = 35770 + 11712 = 47482
        let (y, m, d) = days_to_ymd(47482);
        assert_eq!((y, m, d), (2100, 1, 1));
    }

    #[test]
    fn negative_days_before_epoch() {
        let (y, m, d) = days_to_ymd(-1);
        assert_eq!((y, m, d), (1969, 12, 31));
    }
}
