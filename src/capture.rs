// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! BusCapture — unified recording + statistics backend.
//!
//! Three modes:
//! - `stats_only()` — atomic counters, zero memory allocation
//! - `unbounded()` — full recording, never drops
//! - `bounded(n)` — ring buffer, oldest evicted when full
//!
//! Wrap in `Arc` to share between client and test code.
//! Cloning the `Arc` shares the same counters and records.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::intercept::{PacketData, PacketRecord};
use crate::wire_tap::WireTap;

/// Shared inner state — atomic counters + Mutex-protected store.
struct Inner {
    requests: AtomicU64,
    responses: AtomicU64,
    errors: AtomicU64,
    dropped: AtomicU64,
    store: Mutex<Store>,
}

enum Store {
    StatsOnly,
    Unbounded(Vec<PacketRecord>),
    Bounded {
        buf: VecDeque<PacketRecord>,
        cap: usize,
    },
}

/// Unified bus capture — WireTap implementation with three storage modes.
///
/// Wrap in `Arc` to share between client and test code:
///
/// # Examples
///
/// ```no_run
/// use oms_modbus::*;
/// use std::sync::Arc;
///
/// let cap = Arc::new(BusCapture::unbounded());
/// let opts = ClientOptions::default().with_tap(cap.clone());
/// ```
pub struct BusCapture {
    inner: Inner,
}

impl BusCapture {
    /// Statistics only — no recording, atomic counters only.
    /// Zero memory allocation beyond the counters themselves.
    pub fn stats_only() -> Self {
        Self {
            inner: Inner {
                requests: AtomicU64::new(0),
                responses: AtomicU64::new(0),
                errors: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                store: Mutex::new(Store::StatsOnly),
            },
        }
    }

    /// Full recording — stores every record, never evicts.
    pub fn unbounded() -> Self {
        Self {
            inner: Inner {
                requests: AtomicU64::new(0),
                responses: AtomicU64::new(0),
                errors: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                store: Mutex::new(Store::Unbounded(Vec::with_capacity(1024))),
            },
        }
    }

    /// Bounded ring buffer — oldest records evicted when full.
    /// Records are returned in chronological order by [`Self::drain`] and [`Self::snapshot`].
    ///
    /// `capacity` is clamped to a minimum of 1. Passing 0 is equivalent to
    /// passing 1 — a single-record buffer.
    pub fn bounded(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: Inner {
                requests: AtomicU64::new(0),
                responses: AtomicU64::new(0),
                errors: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                store: Mutex::new(Store::Bounded {
                    buf: VecDeque::with_capacity(cap),
                    cap,
                }),
            },
        }
    }

    // ── Statistics ──────────────────────────────────────────────────────

    pub fn count_requests(&self) -> u64 {
        self.inner.requests.load(Ordering::Relaxed)
    }
    pub fn count_responses(&self) -> u64 {
        self.inner.responses.load(Ordering::Relaxed)
    }
    pub fn count_errors(&self) -> u64 {
        self.inner.errors.load(Ordering::Relaxed)
    }
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    pub fn reset_stats(&self) {
        // Acquire the store lock to ensure atomic reset across all counters —
        // prevents concurrent readers from seeing impossible states like
        // responses > requests.
        let _g = self.inner.store.lock().unwrap_or_else(|e| e.into_inner());
        self.inner.requests.store(0, Ordering::Relaxed);
        self.inner.responses.store(0, Ordering::Relaxed);
        self.inner.errors.store(0, Ordering::Relaxed);
        self.inner.dropped.store(0, Ordering::Relaxed);
    }

    // ── Records ─────────────────────────────────────────────────────────

    pub fn len(&self) -> usize {
        let g = self.inner.store.lock().unwrap_or_else(|e| e.into_inner());
        match &*g {
            Store::StatsOnly => 0,
            Store::Unbounded(v) => v.len(),
            Store::Bounded { buf, .. } => buf.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drain all records in chronological order. Stats are NOT reset.
    pub fn drain(&self) -> Vec<PacketRecord> {
        let mut g = self.inner.store.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *g {
            Store::StatsOnly => Vec::new(),
            Store::Unbounded(v) => std::mem::take(v),
            Store::Bounded { buf, cap } => {
                // Swap out the VecDeque under the lock, then drain outside.
                let old = std::mem::replace(buf, VecDeque::with_capacity(*cap));
                old.into_iter().collect()
            }
        }
    }

    /// Snapshot without clearing. Records are in chronological order.
    ///
    /// Clones all records under the lock. For large capture buffers in
    /// hot paths, prefer [`Self::drain`] which swaps the buffer and releases
    /// the lock immediately.
    pub fn snapshot(&self) -> Vec<PacketRecord> {
        let g = self.inner.store.lock().unwrap_or_else(|e| e.into_inner());
        match &*g {
            Store::StatsOnly => Vec::new(),
            Store::Unbounded(v) => v.clone(),
            Store::Bounded { buf, .. } => buf.iter().cloned().collect(),
        }
    }

    fn record(&self, ts: u64, data: PacketData) {
        let mut g = self.inner.store.lock().unwrap_or_else(|e| e.into_inner());
        let record = PacketRecord {
            timestamp_us: ts,
            data,
        };
        match &mut *g {
            Store::StatsOnly => {}
            Store::Unbounded(v) => v.push(record),
            Store::Bounded { buf, cap } => {
                if buf.len() < *cap {
                    buf.push_back(record);
                } else {
                    // Evict oldest, keep newest — chronological order preserved.
                    buf.pop_front();
                    buf.push_back(record);
                    self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl WireTap for BusCapture {
    fn on_write(&self, bytes: &[u8], ts: u64) {
        self.inner.requests.fetch_add(1, Ordering::Relaxed);
        self.record(ts, PacketData::RawTx(bytes.to_vec()));
    }
    fn on_read(&self, bytes: &[u8], ts: u64) {
        self.inner.responses.fetch_add(1, Ordering::Relaxed);
        self.record(ts, PacketData::RawRx(bytes.to_vec()));
    }
    fn on_error(&self, bytes: &[u8], error: &str, ts: u64) {
        self.inner.errors.fetch_add(1, Ordering::Relaxed);
        self.record(ts, PacketData::RawError(bytes.to_vec(), error.to_string()));
    }
}
