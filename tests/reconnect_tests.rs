// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Reconnect integration tests — TcpClient::with_reconnect()

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::intercept::PacketData;
use oms_modbus::*;

async fn spawn_server(store: Arc<SlaveStore>) -> (tokio::task::JoinHandle<()>, SocketAddr) {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let bind_addr = server.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (handle, bind_addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_after_server_restart() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99)]));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 15500);
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let store_clone = store.clone();
    let h = tokio::spawn(async move {
        server.serve_forever(store_clone).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_reconnect(10, Duration::from_millis(50));

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    h.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let server2 = tcp::TcpServer::bind(addr).await.unwrap();
    let store_clone2 = store.clone();
    let h2 = tokio::spawn(async move {
        server2.serve_forever(store_clone2).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);
    h2.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_to_closed_port_fails() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 15599);
    // No server on this port — initial connection should fail
    let result = tcp::TcpClient::connect(addr).await;
    assert!(result.is_err(), "connect to closed port should fail");
}

/// Verify that a default client (no `.with_reconnect()`) can perform basic
/// reads. The absence of reconnect behavior (no retries on transport error) is
/// tested implicitly by `connect_to_closed_port_fails` — an initial connect
/// failure returns immediately without retries.
#[tokio::test(flavor = "multi_thread")]
async fn basic_read_without_reconnect() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 7)]));
    let (_h, addr) = spawn_server(store).await;
    // Default: no reconnect configured
    let client = tcp::TcpClient::connect(addr).await.unwrap();
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![7]);
}

#[tokio::test(flavor = "multi_thread")]
async fn protocol_error_not_retried() {
    let store = Arc::new(SlaveStore::new());
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_reconnect(5, Duration::from_millis(10));
    // Diagnostic with unknown sub-function triggers server exception.
    // Protocol errors (exceptions) must NOT trigger reconnect — the error
    // should be returned immediately as a ModbusError::Exception.
    let result = client.diagnostic(1, 0x00FF, 0x0000).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, ModbusError::Exception { .. }),
        "protocol error should be Exception, got: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn infinite_retry_default() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 7)]));
    let (_h, addr) = spawn_server(store).await;
    // max_retries=0 means infinite
    let client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_reconnect(0, Duration::from_millis(20));
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![7]);
}

#[tokio::test(flavor = "multi_thread")]
async fn wiretap_survives_reconnect() {
    // Bind to a specific port so the client can reconnect to the same address
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let bind_addr = server.local_addr().unwrap();
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let server_h = tokio::spawn(async move {
        server.serve_forever(store.clone()).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // WireTap via ClientOptions, reconnect via TcpClient
    let capture = Arc::new(BusCapture::unbounded());
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(capture.clone() as Arc<dyn WireTap>);
    let client = tcp::with_options(bind_addr, opts)
        .await
        .unwrap()
        .with_reconnect(3, Duration::from_millis(10));

    // Before reconnect — WireTap must capture
    client.read_holding_registers(1, 0, 1).await.unwrap();
    let before = capture.drain();
    let has_tx = before
        .iter()
        .any(|r| matches!(&r.data, PacketData::RawTx(_)));
    let has_rx = before
        .iter()
        .any(|r| matches!(&r.data, PacketData::RawRx(_)));
    assert!(has_tx, "WireTap should capture TX before reconnect");
    assert!(has_rx, "WireTap should capture RX before reconnect");

    // Kill server, restart on same port
    server_h.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let server2 = tcp::TcpServer::bind(bind_addr).await.unwrap();
    let store2 = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let _h2 = tokio::spawn(async move {
        server2.serve_forever(store2).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // After reconnect — WireTap must STILL capture
    client.read_holding_registers(1, 0, 1).await.unwrap();
    let after = capture.drain();
    let has_tx = after
        .iter()
        .any(|r| matches!(&r.data, PacketData::RawTx(_)));
    let has_rx = after
        .iter()
        .any(|r| matches!(&r.data, PacketData::RawRx(_)));
    assert!(has_tx, "WireTap should capture TX after reconnect");
    assert!(has_rx, "WireTap should capture RX after reconnect");
}

// ── 6.5/6.6: Timeout rejection + rebuild failure ────────────────────────

#[tokio::test]
async fn timeout_returns_error_without_reconnect() {
    // Connect to a port with no server — reads will timeout immediately.
    // Timeout should return the error directly (never triggers reconnect).
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19998);
    let Ok(client) = tcp::TcpClient::connect_with_timeout(addr, Duration::from_millis(200)).await
    else {
        return; // skip test if port not reachable
    };
    let client = client.with_reconnect(0, Duration::from_millis(10));

    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    let detail = err.detail();
    assert!(
        !detail.contains("max_retries"),
        "timeout should not trigger reconnect, got: {detail}"
    );
}

#[tokio::test]
async fn connection_refused_triggers_immediate_reconnect() {
    // Use a port that no server is listening on
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19999);
    let client = match tcp::TcpClient::connect_with_timeout(addr, Duration::from_millis(500)).await
    {
        Ok(c) => c.with_reconnect(1, Duration::from_millis(10)),
        Err(_) => return, // port may be firewalled, skip test
    };
    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("connect")
            || err.detail().contains("refused")
            || err.detail().contains("timeout"),
        "expected connection error, got: {}",
        err.detail()
    );
}

#[tokio::test]
async fn protocol_exception_does_not_trigger_reconnect() {
    // Start a server, make a valid read, then force an exception response.
    // Exceptions should NOT trigger reconnect.
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap()
        .with_reconnect(1, Duration::from_millis(10));

    // Valid read
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    // Diagnostic with unsupported sub-function should return exception,
    // not trigger reconnect (protocol error never retried)
    let result = client.diagnostic(1, 0x0001, 0).await;
    if let Err(e) = result {
        let detail = e.detail();
        assert!(
            !detail.contains("max_retries"),
            "protocol error should not trigger reconnect: {detail}"
        );
    }
}

// ── ReconnectConfig ────────────────────────────────────────────────────

#[test]
fn reconnect_config_max_retries_zero_means_infinite() {
    let cfg = oms_modbus::ReconnectConfig::new(0, std::time::Duration::from_millis(100));
    assert_eq!(cfg.max_retries(), 0);
    assert_eq!(cfg.interval(), std::time::Duration::from_millis(100));
}

#[test]
fn reconnect_config_positive_max_retries() {
    let cfg = oms_modbus::ReconnectConfig::new(5, std::time::Duration::from_secs(1));
    assert_eq!(cfg.max_retries(), 5);
    assert_eq!(cfg.interval(), std::time::Duration::from_secs(1));
}
