// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Auto-reconnect configuration for all transports.
//!
//! Use [`.with_reconnect(max_retries, interval)`](crate::ModbusClient) to enable.

use std::time::Duration;

/// Reconnect parameters for transport clients.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectConfig {
    /// Maximum retry attempts. 0 = infinite.
    pub(crate) max_retries: u32,
    /// Fixed delay between reconnect attempts.
    /// Industrial environments require predictable timing — no exponential backoff.
    pub(crate) interval: Duration,
}

impl ReconnectConfig {
    /// Create reconnect config. `max_retries = 0` means infinite retries.
    pub fn new(max_retries: u32, interval: Duration) -> Self {
        Self {
            max_retries,
            interval,
        }
    }
    /// Maximum retry attempts. 0 = infinite retries.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
    /// Fixed delay between reconnect attempts.
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

/// Returns `true` if the error indicates a transport-level failure that
/// may be resolved by rebuilding the connection.
///
/// Only **hard transport errors** are retryable:
/// - `Connection` — TCP RST / broken pipe / connection refused
/// - `Serial` — serial port error / device disconnected
///
/// **Not retryable** (retrying won't fix them):
/// - `Timeout` — ambiguous: slow device, wrong slave ID, or silent reject
/// - `Protocol` — CRC mismatch, slave ID mismatch, PDU decode error
/// - `Exception` — Modbus exception response from the server
/// - `Other` — miscellaneous / unknown errors
#[inline]
pub fn is_retryable(e: &crate::error::ModbusError) -> bool {
    matches!(
        e,
        crate::error::ModbusError::Connection(_) | crate::error::ModbusError::Serial(_)
    )
}
