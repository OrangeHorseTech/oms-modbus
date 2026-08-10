// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! File-based Modbus traffic recorder — ISO 8601 timestamps, non-blocking I/O.
//!
//! Writes captured packets to a file in human-readable format with
//! ISO 8601 timestamps and hex-dump payload. Uses `spawn_blocking`
//! for non-blocking disk I/O.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::mpsc as std_mpsc;

use crate::intercept::{PacketData, PacketRecord};
use crate::monitor::channel::RecordSink;

/// Writes Modbus traffic to a file in human-readable format.
///
/// Each packet produces one line with ISO 8601 timestamp and hex dump:
///
/// ```text
/// 2026-08-05T14:32:15.123456  TX    8B  [01 03 00 00 00 01 84 0A]
/// 2026-08-05T14:32:15.173891  RX    7B  [01 03 04 00 00 00 01 3B 5A]
/// ```
///
/// Disk I/O runs on a blocking thread — does not stall the hot path.
/// Use [`dropped`](FileRecorder::dropped) to detect data loss under extreme load.
///
/// On drop, the sender is dropped to signal the I/O thread to exit.
/// The background task uses `spawn_blocking` which has no `.await`
/// points, so `abort()` is not effective — the sender drop naturally
/// terminates the thread after any in-flight I/O completes.
pub struct FileRecorder {
    sender: std_mpsc::SyncSender<String>,
    dropped: std::sync::atomic::AtomicU64,
    _handle: tokio::task::JoinHandle<()>,
}

impl FileRecorder {
    /// Create a new file recorder. Existing file is truncated.
    ///
    /// Spawns a background `spawn_blocking` task for non-blocking disk I/O.
    /// The task exits when the `FileRecorder` is dropped (sender disconnect).
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(8192, file);
        let (tx, rx) = std_mpsc::sync_channel::<String>(4096);

        let handle = tokio::task::spawn_blocking(move || {
            let mut w = writer;
            while let Ok(line) = rx.recv() {
                if writeln!(w, "{}", line).is_err() {
                    break;
                }
            }
            let _ = w.flush();
        });

        Ok(Self {
            sender: tx,
            dropped: std::sync::atomic::AtomicU64::new(0),
            _handle: handle,
        })
    }

    /// Number of records dropped because the internal channel was full.
    ///
    /// Non-zero values indicate the disk I/O thread cannot keep up with
    /// the capture rate — increase channel capacity or reduce traffic.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn format_record(record: &PacketRecord) -> String {
        let ts = crate::intercept::format_timestamp(record.timestamp_us);
        let (dir, detail) = format_detail(record);
        format!("{ts}  {dir}  {detail}")
    }
}

impl RecordSink for FileRecorder {
    fn on_packet(&mut self, record: PacketRecord) {
        let line = Self::format_record(&record);
        if self.sender.try_send(line).is_err() {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

// ── Formatting helpers ────────────────────────────────────────────────

fn hex_str(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 3);
    use std::fmt::Write;
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        // String's fmt::Write impl never returns Err — this is infallible
        let _ = write!(s, "{b:02X}");
    }
    s
}

fn format_detail(record: &PacketRecord) -> (&'static str, String) {
    match &record.data {
        PacketData::RawTx(b) => ("TX", format!("{:>3}B  [{}]", b.len(), hex_str(b))),
        PacketData::RawRx(b) => ("RX", format!("{:>3}B  [{}]", b.len(), hex_str(b))),
        PacketData::RawError(b, e) => {
            ("ERR", format!("{:>3}B  [{}]  — {}", b.len(), hex_str(b), e))
        }
    }
}
