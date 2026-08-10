// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! In-memory ring buffer for Modbus traffic capture.
//!
//! Default mode: bounded ring buffer (10,000 records).
//! Optional: unlimited mode (never drops).
//!
//! Records are always returned in chronological order by [`RingBufferCapture::drain`] and [`RingBufferCapture::snapshot`].

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::intercept::{PacketData, PacketRecord};
use crate::wire_tap::WireTap;

/// In-memory packet capture with optional ring-buffer eviction.
///
/// Prefer [`crate::BusCapture`] for most use cases — it includes
/// statistics counters and supports stats-only, bounded, and unbounded
/// modes. Use `RingBufferCapture` only when you need a standalone
/// ring buffer without statistics tracking.
///
/// # Example
///
/// ```no_run
/// use oms_modbus::monitor::RingBufferCapture;
/// let capture = RingBufferCapture::new(10_000); // bounded, 10k records
/// ```
pub struct RingBufferCapture {
    records: Mutex<VecDeque<PacketRecord>>,
    capacity: usize,
    unbounded: AtomicBool,
    dropped: AtomicU64,
}

impl RingBufferCapture {
    /// Create a bounded ring buffer with the given capacity.
    /// When full, oldest records are evicted.
    ///
    /// `capacity` is clamped to a minimum of 1. Passing 0 is equivalent to
    /// passing 1 — a single-record buffer.
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            records: Mutex::new(VecDeque::with_capacity(cap)),
            capacity: cap,
            unbounded: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
        }
    }

    /// Create an unbounded capture that never drops records.
    /// Use with caution — memory grows indefinitely.
    pub fn unbounded() -> Self {
        Self {
            records: Mutex::new(VecDeque::with_capacity(1024)),
            capacity: usize::MAX,
            unbounded: AtomicBool::new(true),
            dropped: AtomicU64::new(0),
        }
    }

    /// Number of records currently buffered.
    pub fn len(&self) -> usize {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Number of records dropped due to buffer overflow (bounded mode only).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Drain all records in chronological order. Clears the buffer.
    pub fn drain(&self) -> Vec<PacketRecord> {
        let mut r = self.records.lock().unwrap_or_else(|e| e.into_inner());
        // Swap out the VecDeque under the lock, drain outside.
        let capacity = r.capacity();
        let old = std::mem::replace(&mut *r, VecDeque::with_capacity(capacity));
        old.into_iter().collect()
    }

    /// Take a snapshot without clearing. Records are in chronological order.
    ///
    /// Clones all records under the lock. For large buffers in hot paths,
    /// prefer [`Self::drain`] which swaps the buffer and releases the lock immediately.
    pub fn snapshot(&self) -> Vec<PacketRecord> {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Switch to unbounded mode (existing records preserved).
    pub fn set_unbounded(&self) {
        self.unbounded.store(true, Ordering::Relaxed);
    }

    /// Switch to bounded mode, stopping unbounded growth.
    ///
    /// Capacity is fixed at construction time; this only prevents the ring
    /// buffer from growing without bound. Use [`RingBufferCapture::new`]
    /// to create a capture with a specific capacity.
    pub fn set_bounded(&self) {
        self.unbounded.store(false, Ordering::Relaxed);
    }

    fn record(&self, timestamp_us: u64, data: PacketData) {
        let record = PacketRecord { timestamp_us, data };
        // Read unbounded flag before lock — TOCTOU is harmless here:
        // worst case one record uses the old mode during a mode switch.
        let is_unbounded = self.unbounded.load(Ordering::Relaxed);
        let mut r = self.records.lock().unwrap_or_else(|e| e.into_inner());
        if is_unbounded || r.len() < self.capacity {
            r.push_back(record);
        } else {
            // Evict oldest — VecDeque pop_front is O(1).
            r.pop_front();
            r.push_back(record);
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Default for RingBufferCapture {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl WireTap for RingBufferCapture {
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
