// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Channel-based async recording with user-defined sinks.
//!
//! A dedicated tokio task receives packets via an unbounded mpsc channel
//! and dispatches them to a user-provided `RecordSink`.
//! This decouples the hot I/O path from slow I/O (file writes, network sends).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::intercept::{PacketData, PacketRecord};
use crate::wire_tap::WireTap;

/// User-implementable sink for recording Modbus traffic.
///
/// Implement this trait to create custom recording backends:
/// - [`FileRecorder`](crate::monitor::FileRecorder) (built-in)
/// - Network forwarder
/// - Database writer
/// - Real-time dashboard
///
/// Methods are called sequentially from a dedicated tokio task —
/// no locking needed in your implementation.
pub trait RecordSink: Send + 'static {
    /// Called for each captured packet. Must not block — if I/O is needed,
    /// spawn it or use `tokio::task::spawn_blocking`.
    fn on_packet(&mut self, record: PacketRecord);
}

/// A WireTap that sends packets to a dedicated async task via an
/// unbounded mpsc channel. The task calls the provided [`RecordSink`].
///
/// This is the recommended way to add file, network, or database recording
/// without blocking the I/O path. Combine with `FileRecorder` for disk
/// logging, or implement [`RecordSink`] for custom backends.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use oms_modbus::monitor::{ChannelRecorder, FileRecorder};
/// use oms_modbus::*;
///
/// // File recording — attach via SniffIo in with_options
/// let recorder = FileRecorder::new("capture.log").unwrap();
/// let (tap, _handle) = ChannelRecorder::spawn(recorder);
/// let opts = ClientOptions::default().with_tap(Arc::new(tap));
/// let client = rtu::with_options(port, opts);
///
/// // Custom backend
/// struct MyRecorder;
/// impl RecordSink for MyRecorder {
///     fn on_packet(&mut self, record: PacketRecord) {
///         // send to network, write to DB, etc.
///     }
/// }
/// let (tap, _handle) = ChannelRecorder::spawn(MyRecorder);
/// ```
pub struct ChannelRecorder {
    tx: mpsc::UnboundedSender<PacketRecord>,
    dropped: Arc<AtomicU64>,
}

impl ChannelRecorder {
    /// Spawn a recording task and return a WireTap that feeds it.
    ///
    /// The returned handle keeps the task alive. Drop it to stop recording.
    pub fn spawn<S: RecordSink>(mut sink: S) -> (Self, RecorderHandle) {
        let (tx, mut rx) = mpsc::unbounded_channel::<PacketRecord>();
        let dropped = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                sink.on_packet(record);
            }
            // All senders dropped — channel is empty, task exits naturally.
        });

        (Self { tx, dropped }, RecorderHandle { handle })
    }

    /// Number of records dropped due to closed channel or backpressure.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn send(&self, timestamp_us: u64, data: PacketData) {
        let record = PacketRecord { timestamp_us, data };
        if self.tx.send(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Opaque handle to the recording task. Drop to stop recording.
pub struct RecorderHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for RecorderHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl WireTap for ChannelRecorder {
    fn on_write(&self, bytes: &[u8], ts: u64) {
        self.send(ts, PacketData::RawTx(bytes.to_vec()));
    }
    fn on_read(&self, bytes: &[u8], ts: u64) {
        self.send(ts, PacketData::RawRx(bytes.to_vec()));
    }
    fn on_error(&self, bytes: &[u8], error: &str, ts: u64) {
        self.send(ts, PacketData::RawError(bytes.to_vec(), error.to_string()));
    }
}
