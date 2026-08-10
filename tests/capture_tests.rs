// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration tests for capture infrastructure — TrafficStats, PacketCapture,
//! RingBufferCapture.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::monitor::RingBufferCapture;
use oms_modbus::tcp::TcpServer;
use oms_modbus::*;

/// Start a TCP Modbus server on a random port, return the bound address.
async fn start_test_server(store: Arc<SlaveStore>) -> SocketAddr {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = TcpServer::bind(addr).await.unwrap();
    let bind_addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    bind_addr
}

#[tokio::test(flavor = "multi_thread")]
async fn traffic_stats_counts_requests() {
    let stats = Arc::new(BusCapture::stats_only());
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let addr = start_test_server(store).await;
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(stats.clone());
    let client = tcp::with_options(addr, opts).await.unwrap();
    assert_eq!(
        client.read_holding_registers(1, 0, 1).await.unwrap(),
        vec![42]
    );
    // WireTap hooks fire on TX and RX via SniffIo
    assert_eq!(stats.count_requests(), 1, "should count 1 request (TX)");
    assert_eq!(stats.count_responses(), 1, "should count 1 response (RX)");
    assert_eq!(stats.count_errors(), 0, "should have no errors");
}

#[tokio::test(flavor = "multi_thread")]
async fn bus_capture_records_traffic() {
    let capture = Arc::new(BusCapture::unbounded());
    let store = Arc::new(SlaveStore::with_holding_registers(&[(100, 777)]));
    let addr = start_test_server(store).await;
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(capture.clone());
    let client = tcp::with_options(addr, opts).await.unwrap();
    client.read_holding_registers(1, 100, 1).await.unwrap();
    client.write_single_register(1, 100, 999).await.unwrap();
    let packets = capture.drain();
    assert!(!packets.is_empty(), "should capture traffic");
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_ring_buffer() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let addr = start_test_server(store).await;
    let capture = Arc::new(BusCapture::bounded(100));
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(capture.clone());
    let client = tcp::with_options(addr, opts).await.unwrap();
    client.read_holding_registers(1, 0, 1).await.unwrap();
    let packets = capture.drain();
    assert!(!packets.is_empty());
}

#[test]
fn unbounded_mode() {
    let capture = BusCapture::unbounded();
    capture.on_write(&[1, 2, 3], 0);
    assert_eq!(capture.len(), 1);
    assert_eq!(capture.dropped(), 0);
}

#[test]
fn default_capacity_is_10k() {
    let capture = RingBufferCapture::default();
    assert_eq!(capture.len(), 0);
    for i in 0..12_000u64 {
        capture.on_write(&[1], i);
    }
    let len = capture.len();
    assert!(len <= 10_000, "should not exceed capacity, got {}", len);
    assert!(capture.dropped() > 0, "should have dropped some records");
}

#[test]
fn ring_buffer_zero_capacity_clamped_to_one() {
    // Zero capacity is clamped to 1 — no panic.
    let cap = BusCapture::bounded(0);
    cap.on_write(&[1], 0);
    let records = cap.drain();
    assert_eq!(records.len(), 1);
}

#[test]
fn ring_buffer_double_drain_returns_empty_second_time() {
    let cap = Arc::new(BusCapture::bounded(10));
    cap.on_write(&[1], 0);
    let first = cap.drain();
    assert_eq!(first.len(), 1);
    let second = cap.drain();
    assert!(second.is_empty());
    assert!(cap.is_empty());
}
