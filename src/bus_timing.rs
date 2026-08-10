// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Bus frame spacing — enforces minimum silence between Modbus frames.
//
// Critical for RS-485 multi-drop buses where sending too fast can crash
// low-performance slave devices. RTU/ASCII spec requires ≥3.5 character
// times of silence between frames. Users may also configure custom values
// (50ms, 100ms, etc.) for particularly slow devices.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Tracks bus activity and enforces minimum frame spacing.
///
/// **Thread-safe**: uses `AtomicU64` for lock-free timestamp updates.
/// Pass to [`crate::ClientOptions::with_bus_timing`] to activate.
///
/// # Frame spacing
///
/// 1. Every bus I/O event (read/write) calls [`touch`](BusTiming::touch)
///    to record the last activity timestamp.
/// 2. Before sending, [`wait_if_needed`](BusTiming::wait_if_needed) checks
///    if enough time has elapsed. If not, it sleeps for the remaining gap.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use oms_modbus::BusTiming;
///
/// // RTU 3.5T @ 9600 baud ≈ 4.01ms
/// let timing = BusTiming::rtu_35t(9600);
///
/// // Custom 100ms — for slow devices
/// let timing = BusTiming::custom(Duration::from_millis(100));
/// ```
#[derive(Debug)]
pub struct BusTiming {
    /// Microsecond timestamp of the last bus event.
    last_event_us: AtomicU64,
    /// Minimum spacing between frames, in microseconds.
    min_spacing_us: u64,
}

impl BusTiming {
    /// Create with a custom minimum spacing.
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use oms_modbus::BusTiming;
    /// let timing = BusTiming::custom(Duration::from_millis(50));
    /// ```
    pub fn custom(min_spacing: Duration) -> Self {
        let min_spacing_us = min_spacing.as_micros() as u64;
        Self {
            last_event_us: AtomicU64::new(0),
            min_spacing_us,
        }
    }

    /// RTU standard 3.5 character times at the given baud rate.
    ///
    /// Per *MODBUS over Serial Line Specification V1.02* §1.4:
    /// - **≤ 19200 bps**: `3.5 × 11 bits / baud_rate` (the full formula).
    /// - **> 19200 bps**: fixed minimum of **1.75 ms (1750 µs)**.
    ///
    /// Without the cap, 115200 baud would give only ~334 µs — too short for
    /// real RS-485 transceivers and low-end MCU interrupt latency. Use
    /// [`rtu_35t_raw`](Self::rtu_35t_raw) if you need the uncapped formula.
    ///
    /// | Baud   | 3.5T spacing |
    /// |--------|-------------|
    /// | 9600   | ~4.01 ms    |
    /// | 19200  | ~2.01 ms    |
    /// | 38400  | 1.75 ms     |
    /// | 115200 | 1.75 ms     |
    /// | 921600 | 1.75 ms     |
    pub fn rtu_35t(baud_rate: u32) -> Self {
        let min_spacing_us = if baud_rate > 19200 {
            1750 // spec-mandated fixed interval for high baud rates
        } else {
            (3_500_000u64 * 11) / baud_rate.max(1) as u64
        };
        Self {
            last_event_us: AtomicU64::new(0),
            min_spacing_us,
        }
    }

    /// Raw 3.5 character-time formula at any baud rate — **no 1.75 ms cap**.
    ///
    /// Unlike [`rtu_35t`](Self::rtu_35t), this always computes the raw formula,
    /// even at high baud rates where the Modbus spec recommends a fixed
    /// minimum of 1.75 ms. Baud rate 0 is silently clamped to 1.
    /// Use this only if you understand the implications for your specific hardware.
    ///
    /// ```rust
    /// use oms_modbus::BusTiming;
    /// // 115200 baud → ~334 µs (raw formula, no cap)
    /// let timing = BusTiming::rtu_35t_raw(115200);
    /// assert_eq!(timing.min_spacing().as_micros(), 334);
    /// ```
    pub fn rtu_35t_raw(baud_rate: u32) -> Self {
        let min_spacing_us = (3_500_000u64 * 11) / baud_rate.max(1) as u64;
        Self {
            last_event_us: AtomicU64::new(0),
            min_spacing_us,
        }
    }

    /// ASCII standard 3.5 character times — same timing as RTU.
    pub fn ascii_35t(baud_rate: u32) -> Self {
        Self::rtu_35t(baud_rate)
    }

    /// Record bus activity at the current instant.
    ///
    /// Call on every I/O read/write to keep the timestamp fresh.
    /// Uses a monotonic, syscall-free clock for microsecond timestamps.
    #[inline]
    pub fn touch(&self) {
        // Use max(1) so the stored value is never 0, which is reserved
        // for "no previous activity" in wait_if_needed. Without this,
        // touch + immediate wait within the same microsecond would hit
        // the first-call fast path and skip the frame spacing check.
        let now = now_micros().max(1);
        self.last_event_us.store(now, Ordering::Relaxed);
    }

    /// Wait until the minimum spacing has elapsed since the last [`touch`](Self::touch).
    ///
    /// Returns immediately (no sleep) if:
    /// - Enough time has already passed
    /// - No previous activity was recorded (first call)
    pub async fn wait_if_needed(&self) {
        let last = self.last_event_us.load(Ordering::Relaxed);
        if last == 0 {
            return; // first call — no history
        }
        let now = now_micros();
        let elapsed = now.saturating_sub(last);
        if elapsed < self.min_spacing_us {
            let remaining_us = self.min_spacing_us - elapsed;
            tokio::time::sleep(Duration::from_micros(remaining_us)).await;
        }
    }

    /// Current minimum spacing.
    pub fn min_spacing(&self) -> Duration {
        Duration::from_micros(self.min_spacing_us)
    }
}

/// Monotonic timestamp in microseconds based on `std::time::Instant`.
///
/// Uses `std::time::Instant` (not `tokio::time::Instant`) because bus timing
/// measures real hardware intervals — it should not be affected by
/// `tokio::time::pause()` in tests. Also works in non-Tokio contexts
/// (sync tests, pure-thread environments).
fn now_micros() -> u64 {
    static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let base = BASE.get_or_init(std::time::Instant::now);
    base.elapsed().as_micros() as u64
}
