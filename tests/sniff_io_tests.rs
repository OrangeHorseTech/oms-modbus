// SPDX-License-Identifier: MIT OR Apache-2.0
//! SniffIo unit tests — passthrough, sniffing, pending, replace_inner,
//! and BusTiming integration.
//!
//! SniffIo<T> wraps any AsyncRead+AsyncWrite transport with optional
//! WireTap and BusTiming. Two modes:
//! - Passthrough (tap=None): direct delegation, zero overhead
//! - Sniffing (tap=Some): background task + mpsc channel + tokio::io::split

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::bus_timing::BusTiming;
use oms_modbus::transport::sniff_io::SniffIo;
use oms_modbus::wire_tap::WireTap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Test tap with atomic counters ──────────────────────────────────────

struct TestTap {
    tx_bytes: AtomicU64,
    tx_count: AtomicU64,
    rx_bytes: AtomicU64,
    rx_count: AtomicU64,
    errors: AtomicU64,
}

impl TestTap {
    fn new() -> Self {
        Self {
            tx_bytes: AtomicU64::new(0),
            tx_count: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            rx_count: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

impl WireTap for TestTap {
    fn on_write(&self, bytes: &[u8], _ts: u64) {
        self.tx_bytes
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        self.tx_count.fetch_add(1, Ordering::SeqCst);
    }

    fn on_read(&self, bytes: &[u8], _ts: u64) {
        self.rx_bytes
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        self.rx_count.fetch_add(1, Ordering::SeqCst);
    }

    fn on_error(&self, bytes: &[u8], _error: &str, _ts: u64) {
        self.errors.fetch_add(bytes.len() as u64, Ordering::SeqCst);
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn new_duplex_pair(buf: usize) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
    tokio::io::duplex(buf)
}

/// Write data to the remote half and verify SniffIo reads it correctly.
async fn write_remote_and_read_sniff(
    remote: &mut tokio::io::DuplexStream,
    sniff: &mut SniffIo<tokio::io::DuplexStream>,
    data: &[u8],
) -> Vec<u8> {
    remote.write_all(data).await.unwrap();
    let mut buf = vec![0u8; data.len() + 16];
    let n = tokio::time::timeout(Duration::from_secs(2), sniff.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf[..n].to_vec()
}

// ── Passthrough mode ─────────────────────────────────────────────────────

#[tokio::test]
async fn passthrough_read_roundtrip() {
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, None, None);

    let data = b"Hello, Modbus!";
    let result = write_remote_and_read_sniff(&mut remote, &mut sniff, data).await;
    assert_eq!(result, data);
}

#[tokio::test]
async fn passthrough_write_roundtrip() {
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, None, None);

    let data = b"Write via passthrough!";
    sniff.write_all(data).await.unwrap();

    let mut buf = vec![0u8; data.len() + 16];
    let n = tokio::time::timeout(Duration::from_secs(2), remote.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], data);
}

#[tokio::test]
async fn passthrough_no_background_task() {
    // Passthrough mode should not consume the inner transport via split.
    // The following read/write cycle proves the transport isn't split.
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, None, None);

    // Multiple read/write cycles on same connection
    for i in 0u8..5u8 {
        let msg = [i; 4];
        sniff.write_all(&msg).await.unwrap();

        let mut buf = [0u8; 8];
        let n = remote.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], &msg);
    }
}

// ── Sniffing mode — read path ────────────────────────────────────────────

#[tokio::test]
async fn sniffing_on_read_fires() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    let data = b"sniff this read!";
    remote.write_all(data).await.unwrap();

    // Give the background reader time to pick it up
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut buf = vec![0u8; 256];
    let n = sniff.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], data);
    assert_eq!(tap.rx_bytes.load(Ordering::SeqCst), data.len() as u64);
    assert_eq!(tap.rx_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sniffing_on_write_fires() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    let data = b"sniff this write!";
    sniff.write_all(data).await.unwrap();

    // Read back from remote to confirm delivery
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), remote.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], data);

    // on_write should have fired
    assert_eq!(tap.tx_bytes.load(Ordering::SeqCst), data.len() as u64);
    assert!(tap.tx_count.load(Ordering::SeqCst) >= 1);
}

// ── Sniffing — dual-direction traffic ────────────────────────────────────

#[tokio::test]
async fn sniffing_bidirectional_traffic() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    // Write from remote → sniff reads (triggers on_read)
    remote.write_all(b"ping").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut buf = [0u8; 16];
    let n = sniff.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"ping");

    // Write from sniff → remote reads (triggers on_write)
    sniff.write_all(b"pong").await.unwrap();
    let n = remote.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"pong");

    assert_eq!(
        tap.rx_bytes.load(Ordering::SeqCst),
        4,
        "rx: 'ping' = 4 bytes"
    );
    assert_eq!(
        tap.tx_bytes.load(Ordering::SeqCst),
        4,
        "tx: 'pong' = 4 bytes"
    );
}

// ── Pending partial reads ────────────────────────────────────────────────

#[tokio::test]
async fn sniffing_pending_partial_read() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    // Send 100 bytes; the background reader delivers one chunk
    let data = vec![0xABu8; 100];
    remote.write_all(&data).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Read only 30 bytes — 70 should stay in `pending`
    let mut small_buf = [0u8; 30];
    let n = sniff.read(&mut small_buf).await.unwrap();
    assert_eq!(n, 30);
    assert_eq!(&small_buf[..], &data[..30]);

    // Next read gets the pending remainder (70 bytes)
    let mut rest = vec![0u8; 128];
    let n = sniff.read(&mut rest).await.unwrap();
    assert_eq!(n, 70);
    assert_eq!(&rest[..70], &data[30..]);

    // Only one on_read call for the original 100-byte chunk
    assert_eq!(tap.rx_count.load(Ordering::SeqCst), 1);
    assert_eq!(tap.rx_bytes.load(Ordering::SeqCst), 100);
}

// ── Cleanup on drop ──────────────────────────────────────────────────────

/// Drop-safety test: the assertion is that the test runs to completion
/// without panicking. SniffIo spawns a background reader; dropping the
/// SniffIo must shut down the reader task cleanly (no panic, no deadlock).
#[tokio::test]
async fn sniffing_drop_cleanup_no_panic() {
    let tap = Arc::new(TestTap::new());
    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, Some(tap.clone()), None);

    // Write something to trigger background reader activity
    // then drop — must not panic
    drop(sniff);
    // Let the background task unwind
    tokio::time::sleep(Duration::from_millis(20)).await;
}

// ── BusTiming integration ────────────────────────────────────────────────

#[tokio::test]
async fn prepare_send_no_timing_returns_immediately() {
    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, None, None);

    let start = std::time::Instant::now();
    sniff.prepare_send().await;
    let elapsed = start.elapsed();
    // No timing configured → should return almost instantly
    assert!(elapsed < Duration::from_millis(50), "elapsed={elapsed:?}");
}

#[tokio::test]
async fn prepare_send_with_timing_waits() {
    let timing = Arc::new(BusTiming::custom(Duration::from_millis(80)));
    timing.touch(); // mark a recent transmission

    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, None, Some(timing.clone()));

    let start = std::time::Instant::now();
    sniff.prepare_send().await;
    let elapsed = start.elapsed();
    // Should have waited at least 80ms since last touch
    assert!(
        elapsed >= Duration::from_millis(75),
        "expected >=75ms wait, got {elapsed:?}"
    );
}

// ── replace_inner ────────────────────────────────────────────────────────

#[tokio::test]
async fn replace_inner_passthrough_to_passthrough() {
    let (mut remote1, server1) = new_duplex_pair(1024);
    let (mut remote2, server2) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server1, None, None);

    // Use original transport
    sniff.write_all(b"before").await.unwrap();
    let mut buf = [0u8; 16];
    let n = remote1.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"before");

    // Replace inner
    sniff.replace_inner(server2);

    // New transport works, old doesn't
    sniff.write_all(b"after").await.unwrap();
    let n = remote2.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"after");
}

#[tokio::test]
async fn replace_inner_preserves_tap() {
    let tap = Arc::new(TestTap::new());
    let (_remote1, server1) = new_duplex_pair(1024);
    let (mut remote2, server2) = new_duplex_pair(1024);
    let mut sniff = SniffIo::new(server1, Some(tap.clone()), None);

    sniff.replace_inner(server2);

    // Write on new transport — tap should still fire
    sniff.write_all(b"new-transport").await.unwrap();
    let mut buf = [0u8; 32];
    let _ = remote2.read(&mut buf).await.unwrap();

    assert!(tap.tx_bytes.load(Ordering::SeqCst) >= 13);
}

// ── Large transfer ───────────────────────────────────────────────────────

/// Large transfer through the sniffing path (data channel + pending).
/// The background reader reads from the transport and fans out to the
/// data channel. We wait for the writer to finish, then drain the channel.
#[tokio::test]
async fn sniffing_large_transfer() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(65536);
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    let expected_len = 8192usize;
    let write_handle = tokio::spawn(async move {
        let data = vec![0x42u8; expected_len];
        remote.write_all(&data).await.unwrap();
    });

    // Wait for the writer to finish. After this, all 8192 bytes are
    // either in the transport buffer or already in the data channel.
    write_handle.await.unwrap();

    // Give the background reader a moment to push remaining chunks.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Drain all data from the data channel (via poll_read).
    let mut total = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sniff.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => total.extend_from_slice(&buf[..n]),
            _ => break,
        }
        if total.len() >= expected_len {
            break;
        }
    }

    assert_eq!(total.len(), expected_len);
    assert_eq!(&total[..], &vec![0x42u8; expected_len][..]);
}

/// Verify the tap forwarder receives data through the tap channel and
/// calls WireTap::on_read. Write first, then let background reader +
/// tap forwarder process before checking counters.
#[tokio::test]
async fn tap_forwarder_receives_bulk() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(65536);
    let sniff = SniffIo::new(server, Some(tap.clone()), None);

    // Write all data first, then give background reader + tap forwarder
    // time to process before reading from the consumer.
    let data = vec![0xABu8; 4096];
    remote.write_all(&data).await.unwrap();
    // Drop remote so background reader sees EOF after reading all data.
    drop(remote);

    // Wait for background reader to read, fan out, and tap forwarder to process.
    // The tap channel is 256 KB — plenty for 4 KB of data.
    for _ in 0..50 {
        let rx = tap.rx_bytes.load(Ordering::SeqCst);
        if rx >= data.len() as u64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        tap.rx_bytes.load(Ordering::SeqCst),
        data.len() as u64,
        "tap should receive all bytes via tap_forwarder"
    );

    drop(sniff);
}

// ── Bounded channel — eviction & counters ────────────────────────────────

/// The transport path always involves tokio::io::split + background reader,
/// so data chunks are 4096 bytes (DEFAULT_BUF_SIZE). To test eviction we
/// set capacity smaller than a single chunk — every chunk is dropped.
#[tokio::test]
async fn data_channel_evicts_when_full() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(65536);
    // 100-byte data capacity — every 4096-byte chunk will be dropped.
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None).with_channel_capacity(100);

    let data = vec![0xCCu8; 8192];
    remote.write_all(&data).await.unwrap();
    drop(remote);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to read — all data should have been evicted (chunks > capacity).
    let mut buf = [0u8; 256];
    let result = tokio::time::timeout(Duration::from_millis(200), sniff.read(&mut buf)).await;
    // Either immediate error (channel closed with error) or timeout (queue empty).
    match result {
        Ok(Ok(n)) if n > 0 => {
            // Might get a chunk smaller than capacity — don't assert zero.
        }
        _ => { /* expected: all chunks evicted, closed channel */ }
    }

    // Dropped counter reflects the evictions.
    assert!(
        sniff.data_dropped_chunks() > 0,
        "data_dropped_chunks={}",
        sniff.data_dropped_chunks()
    );
    assert!(
        sniff.data_dropped_bytes() > 0,
        "data_dropped_bytes={}",
        sniff.data_dropped_bytes()
    );
}

/// With sufficient capacity, no drops should occur for modest traffic.
#[tokio::test]
async fn data_channel_no_drops_under_capacity() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(65536);
    // 1 MB data capacity — plenty for 8 KB.
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    let data = vec![0xDDu8; 2048];
    remote.write_all(&data).await.unwrap();
    drop(remote);
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Drain what's available.
    let mut total = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(200), sniff.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => total.extend_from_slice(&buf[..n]),
            _ => break,
        }
    }

    assert_eq!(total.len(), data.len());
    assert_eq!(total, data);
    assert_eq!(sniff.data_dropped_chunks(), 0);
    assert_eq!(sniff.data_dropped_bytes(), 0);
}

/// Passthrough mode always returns 0 for all dropped counters.
#[tokio::test]
async fn passthrough_dropped_counters_always_zero() {
    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, None, None);

    assert_eq!(sniff.data_dropped_chunks(), 0);
    assert_eq!(sniff.data_dropped_bytes(), 0);
    assert_eq!(sniff.tap_dropped_chunks(), 0);
    assert_eq!(sniff.tap_dropped_bytes(), 0);
    assert_eq!(sniff.total_dropped_bytes(), 0);
    assert_eq!(sniff.channel_capacity(), 0);
}

/// Verify default capacity constants are reasonable.
#[tokio::test]
async fn sniffing_uses_default_capacity() {
    let tap = Arc::new(TestTap::new());
    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, Some(tap.clone()), None);

    let cap = sniff.channel_capacity();
    assert_eq!(cap, 1024 * 1024, "default data channel = 1 MB");
}

/// `with_channel_capacity` wires through for sniffing mode.
#[tokio::test]
async fn with_channel_capacity_configurable() {
    let tap = Arc::new(TestTap::new());
    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, Some(tap.clone()), None).with_channel_capacity(4096);

    assert_eq!(sniff.channel_capacity(), 4096);
}

/// Tap channel has its own capacity independent of data channel.
#[tokio::test]
async fn tap_channel_independent_capacity() {
    let tap = Arc::new(TestTap::new());
    let (_remote, server) = new_duplex_pair(1024);
    let sniff = SniffIo::new(server, Some(tap.clone()), None).with_tap_channel_capacity(512);

    // Data channel still at default.
    assert_eq!(sniff.channel_capacity(), 1024 * 1024);
    // tap_dropped starts at 0.
    assert_eq!(sniff.tap_dropped_chunks(), 0);
}

// ── Close / error ordering: data before error ────────────────────────────

/// When the transport closes after sending data, the consumer must drain
/// all queued data before seeing the error. This is the regression test
/// for the poll_recv error-take ordering bug.
#[tokio::test]
async fn drain_data_before_error_on_transport_close() {
    let tap = Arc::new(TestTap::new());
    let (mut remote, server) = new_duplex_pair(65536);
    let mut sniff = SniffIo::new(server, Some(tap.clone()), None);

    // Write data, then close the transport.
    let data = vec![0xEEu8; 4096];
    remote.write_all(&data).await.unwrap();
    drop(remote);

    // Give background reader time to read + send error.
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Must be able to read the data even though the transport is closed.
    let mut total = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(200), sniff.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => total.extend_from_slice(&buf[..n]),
            _ => break,
        }
        if total.len() >= data.len() {
            break;
        }
    }

    assert_eq!(
        total.len(),
        data.len(),
        "data should be drained before EOF error"
    );
    assert_eq!(total, data);
}
