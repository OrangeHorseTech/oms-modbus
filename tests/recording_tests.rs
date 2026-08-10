// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for recording infrastructure — format_timestamp, days_to_ymd,
//! PacketRecord Display, Capture, TrafficStats, ChannelRecorder, RecordSink.

use std::sync::Arc;

use oms_modbus::*;

// ── format_timestamp ────────────────────────────────────────────────────

#[test]
fn format_timestamp_epoch_zero() {
    let s = format_timestamp(0);
    assert_eq!(s, "1970-01-01T00:00:00.000000");
}

#[test]
fn format_timestamp_iso8601_format() {
    // Verify the output matches ISO 8601 pattern: YYYY-MM-DDTHH:MM:SS.uuuuuu
    let s = format_timestamp(1_600_000_000_123_456); // some arbitrary timestamp
                                                     // 26 chars, T separator, colons, dashes
    assert_eq!(s.len(), 26);
    assert_eq!(&s[4..5], "-");
    assert_eq!(&s[7..8], "-");
    assert_eq!(&s[10..11], "T");
    assert_eq!(&s[13..14], ":");
    assert_eq!(&s[16..17], ":");
    assert_eq!(&s[19..20], ".");
}

#[test]
fn format_timestamp_monotonic() {
    let t1 = format_timestamp(1_000_000); // 1 second
    let t2 = format_timestamp(2_000_000); // 2 seconds
                                          // Both should have consistent format
    assert_eq!(t1.len(), 26);
    assert_eq!(t2.len(), 26);
    // t2 should be lexicographically >= t1 (same date, later time)
    assert!(t2 >= t1);
}

#[test]
fn format_timestamp_microseconds_preserved() {
    let s = format_timestamp(123_456);
    assert!(s.ends_with(".123456"));
}

#[test]
fn format_timestamp_day_rollover() {
    // 86400 seconds = 1 day, then 1 second into next day
    let s = format_timestamp(86_401_000_000);
    assert_eq!(&s[8..10], "02"); // day 2
    assert_eq!(&s[11..19], "00:00:01"); // 1 second past midnight
}

// ── PacketRecord Display ────────────────────────────────────────────────

#[test]
fn packet_record_display_tx() {
    let record = PacketRecord {
        timestamp_us: 0, // epoch zero
        data: PacketData::RawTx(vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x01]),
    };
    let s = format!("{record}");
    assert!(s.contains("[TX]"));
    assert!(s.contains("1970-01-01T00:00:00.000000"));
    assert!(s.contains("6B"));
    assert!(s.contains("[01 03 00 00 00 01]"));
}

#[test]
fn packet_record_display_rx() {
    let record = PacketRecord {
        timestamp_us: 86_400_000_000, // day 2 = 1970-01-02T00:00:00
        data: PacketData::RawRx(vec![0x01, 0x03, 0x04, 0x00, 0x42]),
    };
    let s = format!("{record}");
    assert!(s.contains("[RX]"));
    assert!(s.contains("1970-01-02T"));
    assert!(s.contains("5B"));
}

#[test]
fn packet_record_display_error() {
    let record = PacketRecord {
        timestamp_us: 1,
        data: PacketData::RawError(vec![0xFF], "timeout".into()),
    };
    let s = format!("{record}");
    assert!(s.contains("[ERR]"));
    assert!(s.contains("timeout"));
}

// ── TrafficStats WireTap impl ───────────────────────────────────────────

#[test]
fn traffic_stats_counts_via_wiretap() {
    let stats = BusCapture::stats_only();
    stats.on_write(&[1, 2, 3], 0);
    stats.on_write(&[4], 100);
    stats.on_read(&[5, 6], 200);
    stats.on_error(&[7], "err", 300);
    stats.on_error(&[8], "err2", 400);

    assert_eq!(stats.count_requests(), 2);
    assert_eq!(stats.count_responses(), 1);
    assert_eq!(stats.count_errors(), 2);
}

#[test]
fn traffic_stats_reset() {
    let stats = BusCapture::stats_only();
    stats.on_write(&[1], 0);
    stats.on_read(&[2], 1);
    stats.on_error(&[3], "e", 2);
    stats.reset_stats();
    assert_eq!(stats.count_requests(), 0);
    assert_eq!(stats.count_responses(), 0);
    assert_eq!(stats.count_errors(), 0);
}

// ── Capture WireTap + stats ─────────────────────────────────────────────

#[test]
fn capture_records_and_counts() {
    let cap = Arc::new(BusCapture::unbounded());
    cap.on_write(&[1, 2, 3], 1000);
    cap.on_read(&[4, 5], 2000);
    cap.on_error(&[6], "err", 3000);

    assert_eq!(cap.count_requests(), 1);
    assert_eq!(cap.count_responses(), 1);
    assert_eq!(cap.count_errors(), 1);
    assert_eq!(cap.len(), 3);
}

#[test]
fn capture_drain_clears() {
    let cap = Arc::new(BusCapture::unbounded());
    cap.on_write(&[1], 0);
    cap.on_read(&[2], 1);
    assert_eq!(cap.len(), 2);

    let records = cap.drain();
    assert_eq!(records.len(), 2);
    assert!(cap.is_empty());
    assert_eq!(cap.len(), 0);
}

#[test]
fn capture_is_empty_start() {
    let cap = Arc::new(BusCapture::unbounded());
    assert!(cap.is_empty());
    assert_eq!(cap.len(), 0);
}

// ── ChannelRecorder + RecordSink ────────────────────────────────────────

use std::sync::Mutex as StdMutex;

struct TestSink {
    received: Arc<StdMutex<Vec<PacketRecord>>>,
}

impl oms_modbus::monitor::RecordSink for TestSink {
    fn on_packet(&mut self, record: PacketRecord) {
        self.received.lock().unwrap().push(record);
    }
}

#[tokio::test]
async fn channel_recorder_sends_to_sink() {
    let received = Arc::new(StdMutex::new(Vec::new()));
    let sink = TestSink {
        received: received.clone(),
    };
    let (recorder, handle) = oms_modbus::monitor::ChannelRecorder::spawn(sink);

    recorder.on_write(&[1, 2, 3], 100);
    recorder.on_read(&[4, 5], 200);
    recorder.on_error(&[6], "oops", 300);

    // Give the async task time to process
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(received.lock().unwrap().len(), 3);
    drop(handle);
}

#[tokio::test]
async fn recorder_handle_drop_stops_task() {
    let received = Arc::new(StdMutex::new(Vec::new()));
    let sink = TestSink {
        received: received.clone(),
    };
    let (recorder, handle) = oms_modbus::monitor::ChannelRecorder::spawn(sink);

    recorder.on_write(&[1], 0);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(received.lock().unwrap().len(), 1);

    drop(handle); // stops the task
    drop(recorder); // drops the sender — channel closes

    // If the task exited cleanly after handle drop, no panic.
}

// ── RingBufferCapture edge cases ────────────────────────────────────────

#[test]
fn ring_buffer_snapshot_preserves_records() {
    let cap = oms_modbus::BusCapture::bounded(100);
    cap.on_write(&[1], 0);
    cap.on_read(&[2], 1);
    let snap = cap.snapshot();
    assert_eq!(snap.len(), 2);
    assert_eq!(cap.len(), 2);
}

#[test]
fn ring_buffer_drain_clears() {
    let cap = oms_modbus::BusCapture::bounded(100);
    cap.on_write(&[1], 0);
    let records = cap.drain();
    assert_eq!(records.len(), 1);
    assert!(cap.is_empty());
}

#[test]
fn ring_buffer_bounded_evicts_oldest() {
    let cap = oms_modbus::BusCapture::bounded(3);
    cap.on_write(&[1], 0);
    cap.on_write(&[2], 1);
    cap.on_write(&[3], 2);
    cap.on_write(&[4], 3);
    assert_eq!(cap.dropped(), 1);
    let records = cap.drain();
    assert_eq!(records.len(), 3);
    // Chronological order: [2] evicted, remaining [2,3,4]
    assert_eq!(records[0].timestamp_us, 1);
    assert_eq!(records[1].timestamp_us, 2);
    assert_eq!(records[2].timestamp_us, 3);
}

#[test]
fn ring_buffer_unbounded_never_drops() {
    let cap = oms_modbus::BusCapture::unbounded();
    for i in 0..1000u64 {
        cap.on_write(&[i as u8], i);
    }
    assert_eq!(cap.len(), 1000);
    assert_eq!(cap.dropped(), 0);
}

#[test]
fn ring_buffer_set_bounded_then_unbounded() {
    let cap = oms_modbus::monitor::RingBufferCapture::new(2);
    cap.on_write(&[1], 0);
    cap.on_write(&[2], 1);
    cap.on_write(&[3], 2);
    assert_eq!(cap.dropped(), 1);

    cap.set_unbounded();
    cap.on_write(&[4], 3);
    cap.on_write(&[5], 4);
    assert_eq!(cap.len(), 4);
    assert_eq!(cap.dropped(), 1);
}

// ── FileRecorder ────────────────────────────────────────────────────────

#[tokio::test]
async fn file_recorder_writes_to_file() {
    let dir = std::env::temp_dir();
    let path = dir.join("oms_modbus_test_capture.log");
    let _ = std::fs::remove_file(&path);

    {
        let file_recorder = oms_modbus::monitor::FileRecorder::new(&path).unwrap();
        let (tap, handle) = oms_modbus::monitor::ChannelRecorder::spawn(file_recorder);
        tap.on_write(&[0x01, 0x03], 1_600_000_000_000_000);
        tap.on_read(&[0x01, 0x03, 0x04], 1_600_000_001_000_000);
        drop(tap); // closes channel → task drains and flushes
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        drop(handle);
    }

    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("TX"), "should contain TX: {content}");
    assert!(content.contains("RX"), "should contain RX: {content}");
    assert!(content.contains("01 03"), "should contain hex: {content}");

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn file_recorder_starts_with_zero_dropped() {
    let dir = std::env::temp_dir();
    let path = dir.join("oms_modbus_test_dropped.log");
    let _ = std::fs::remove_file(&path);

    let recorder = oms_modbus::monitor::FileRecorder::new(&path).unwrap();
    assert_eq!(recorder.dropped(), 0);

    let _ = std::fs::remove_file(&path);
}

// ── P2-5: FileRecorder backpressure ─────────────────────────────────────

#[tokio::test]
async fn file_recorder_small_load_no_drops() {
    use oms_modbus::monitor::{ChannelRecorder, FileRecorder};
    let dir = std::env::temp_dir();
    let path = dir.join("oms_modbus_p2_backpressure.log");
    let _ = std::fs::remove_file(&path);

    let recorder = FileRecorder::new(&path).unwrap();
    let (tap, handle) = ChannelRecorder::spawn(recorder);

    // Send 100 records — well below 4096 channel capacity
    for i in 0..100u64 {
        tap.on_write(&[i as u8], i);
    }
    drop(tap);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    drop(handle);

    let content = std::fs::read_to_string(&path).unwrap();
    assert!(!content.is_empty(), "file should not be empty");
    assert!(content.contains("TX"), "should contain TX markers");
    assert!(
        content.lines().count() >= 100,
        "should have at least 100 log lines, got {}",
        content.lines().count()
    );
    let _ = std::fs::remove_file(&path);
}

// ── P2-5: ChannelRecorder dropped ───────────────────────────────────────

#[tokio::test]
async fn channel_recorder_no_drops_while_active() {
    use oms_modbus::monitor::{ChannelRecorder, RecordSink};
    struct VoidSink;
    impl RecordSink for VoidSink {
        fn on_packet(&mut self, _: PacketRecord) {}
    }
    let (tap, handle) = ChannelRecorder::spawn(VoidSink);

    // Unbounded channel — dropped counter always at 0
    assert_eq!(tap.dropped(), 0);

    for i in 0..100u64 {
        tap.on_write(&[i as u8], i);
    }
    assert_eq!(tap.dropped(), 0);
    drop(handle);
}

// ── P2-5: BusCapture boundary conditions ────────────────────────────────

#[test]
fn bus_capture_stats_only_drain_empty() {
    let cap = Arc::new(BusCapture::stats_only());
    cap.on_write(&[1, 2], 0);
    cap.on_read(&[3], 1);

    // Stats counters work
    assert_eq!(cap.count_requests(), 1);
    assert_eq!(cap.count_responses(), 1);
    // But no records are stored — drain returns empty
    let records = cap.drain();
    assert!(records.is_empty());
    assert_eq!(cap.len(), 0);
    assert!(cap.is_empty());
}

#[test]
fn bus_capture_bounded_dropped_counter() {
    // bounded(3) → capacity 3. Write 10 records → 7 dropped.
    let cap = BusCapture::bounded(3);
    for i in 0..10u64 {
        cap.on_write(&[i as u8], i);
    }
    assert_eq!(cap.dropped(), 7);
    assert_eq!(cap.len(), 3);
}

#[test]
fn bus_capture_bounded_eviction_retains_latest() {
    let cap = BusCapture::bounded(3);
    cap.on_write(&[1], 100); // evicted by 4th write
    cap.on_write(&[2], 200); // evicted by 5th write
    cap.on_write(&[3], 300);
    cap.on_write(&[4], 400);
    cap.on_write(&[5], 500);

    let records = cap.drain();
    assert_eq!(records.len(), 3);
    // Chronological order: oldest surviving first.
    assert_eq!(records[0].timestamp_us, 300);
    assert_eq!(records[1].timestamp_us, 400);
    assert_eq!(records[2].timestamp_us, 500);
    // Dropped counter confirms evictions happened.
    assert_eq!(cap.dropped(), 2, "2 writes should be evicted");
}

#[test]
fn bus_capture_bounded_drain_resets_position() {
    // Verify that drain empties the buffer, allowing fresh writes
    // without immediate eviction.
    let cap = BusCapture::bounded(3);
    cap.on_write(&[1], 10);
    cap.on_write(&[2], 20);
    let _ = cap.drain();

    // After drain, writes should fill fresh (no eviction)
    cap.on_write(&[3], 30);
    cap.on_write(&[4], 40);
    cap.on_write(&[5], 50);
    assert_eq!(cap.dropped(), 0, "fresh after drain: no evictions");
    assert_eq!(cap.len(), 3);
}

#[test]
fn bus_capture_capacity_one_no_panic() {
    let cap = BusCapture::bounded(1);
    cap.on_write(&[1], 0);
    cap.on_write(&[2], 1);
    cap.on_write(&[3], 2);

    assert_eq!(cap.dropped(), 2);
    assert_eq!(cap.len(), 1);
    let records = cap.drain();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].timestamp_us, 2);
}

#[test]
fn bus_capture_clone_shares_state() {
    let cap = Arc::new(BusCapture::bounded(10));
    let clone = cap.clone();

    cap.on_write(&[1], 100);
    cap.on_read(&[2], 200);

    // Clone sees the same counters
    assert_eq!(clone.count_requests(), 1);
    assert_eq!(clone.count_responses(), 1);
    assert_eq!(clone.len(), 2);

    // Clone writes are visible
    clone.on_write(&[3], 300);
    assert_eq!(cap.len(), 3);
    assert_eq!(cap.count_requests(), 2);
}

// ── P2-6: WireTap default no-ops ─────────────────────────────────────────

#[test]
fn custom_wiretap_default_noop_methods() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct ReadOnlyTap {
        called: AtomicBool,
    }
    impl WireTap for ReadOnlyTap {
        fn on_read(&self, _bytes: &[u8], _ts: u64) {
            self.called.store(true, Ordering::SeqCst);
        }
        // on_write and on_error use default no-ops
    }

    let tap = ReadOnlyTap {
        called: AtomicBool::new(false),
    };
    tap.on_write(&[1, 2, 3], 100); // no-op — must not panic
    tap.on_error(&[4], "fail", 200); // no-op — must not panic
    tap.on_read(&[5, 6], 300); // implemented — must fire

    assert!(tap.called.load(Ordering::SeqCst));
}

#[test]
fn packet_data_raw_error_empty_no_panic() {
    // PacketRecord with empty data and empty error string
    let record = PacketRecord {
        timestamp_us: 0,
        data: PacketData::RawError(vec![], String::new()),
    };
    let s = format!("{record}");
    assert!(s.contains("[ERR]"));
    assert!(s.contains("0B"));

    // Empty data with a message
    let record = PacketRecord {
        timestamp_us: 1,
        data: PacketData::RawError(vec![], "oops".into()),
    };
    let s = format!("{record}");
    assert!(s.contains("[ERR]"));
    assert!(s.contains("oops"));
}

// ── P2-6: TrafficStats clone ─────────────────────────────────────────────

#[test]
fn traffic_stats_clone_shares_counters() {
    let stats = TrafficStats::default();
    let clone = stats.clone();

    stats.on_write(&[1, 2], 100);
    stats.on_read(&[3], 200);

    // Clone sees the same atomic counters
    assert_eq!(clone.count_requests(), 1);
    assert_eq!(clone.count_responses(), 1);
    assert_eq!(clone.count_errors(), 0);

    // Clone writes are visible to the original
    clone.on_write(&[4], 300);
    assert_eq!(stats.count_requests(), 2);
}
