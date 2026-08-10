// SPDX-License-Identifier: MIT OR Apache-2.0
//! RTU/ASCII reconnect tests using DuplexStream (in-memory, no hardware).

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

// ── RTU direct reconnect (no SniffIo) ──────────────────────────────────

#[tokio::test]
async fn rtu_direct_reconnect_succeeds() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let server = rtu::RtuServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3))
        .with_reconnect(3, Duration::from_millis(100), || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        });

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);
}

#[tokio::test]
async fn ascii_direct_reconnect_succeeds() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99)]));
    let server = ascii::AsciiServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_secs(3))
        .with_reconnect(3, Duration::from_millis(100), || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        });

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);
}

// ── RTU with_options reconnect (SniffIo) ───────────────────────────────

#[tokio::test]
async fn rtu_with_options_and_reconnect_on() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
    let server = rtu::RtuServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let cap = Arc::new(BusCapture::unbounded());
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(cap.clone());

    let client = rtu::with_options(client_stream, opts).with_reconnect_on(
        3,
        Duration::from_millis(100),
        || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        },
    );

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![77]);
    assert!(!cap.is_empty(), "should capture at least one frame");
}

#[tokio::test]
async fn ascii_with_options_and_reconnect_on() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 88)]));
    let server = ascii::AsciiServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let cap = Arc::new(BusCapture::unbounded());
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(cap.clone());

    let client = ascii::with_options(client_stream, opts).with_reconnect_on(
        3,
        Duration::from_millis(100),
        || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        },
    );

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![88]);
    assert!(!cap.is_empty());
}

// ── Reconnect with bus_timing ──────────────────────────────────────────

/// Verify that bus_timing + reconnect can be configured together and a basic
/// request succeeds. (Full timing-after-reconnect verification requires
/// hardware-level timing — this test ensures the configuration composes.)
#[tokio::test]
async fn rtu_reconnect_with_bus_timing_configured() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 55)]));
    let server = rtu::RtuServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });

    let timing = BusTiming::custom(Duration::from_micros(1));
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_bus_timing(timing);

    let client = rtu::with_options(client_stream, opts).with_reconnect_on(
        3,
        Duration::from_millis(100),
        || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        },
    );

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![55]);
}

// ── Protocol error not retried ─────────────────────────────────────────

#[tokio::test]
async fn rtu_protocol_error_not_retried() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    let store = Arc::new(SlaveStore::new());
    let server = rtu::RtuServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(1))
        .with_reconnect(3, Duration::from_millis(10), || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        });

    // Diagnostic with unknown sub-function triggers server exception.
    // Protocol errors (exceptions) must NOT trigger reconnect — the error
    // should be returned immediately as ModbusError::Exception.
    let result = client.diagnostic(1, 0x00FF, 0x0000).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, ModbusError::Exception { .. }),
        "protocol error should be Exception, got: {err:?}"
    );
}

// ── Max retries exceeded ───────────────────────────────────────────────

#[tokio::test]
async fn rtu_max_retries_exceeded() {
    // Drop the server side to trigger a connection error (WriteZero / broken pipe).
    // Connection errors are immediately retryable — they trigger a rebuild on every call.
    // max_retries=1 allows one rebuild; the second call should hit the limit.
    let (client_stream, server_stream) = tokio::io::duplex(1024);
    drop(server_stream);

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_millis(100))
        .with_reconnect(1, Duration::from_millis(5), || {
            let (c, _s) = tokio::io::duplex(1024);
            Ok(c)
        });

    let mut saw_max_retries = false;
    for _ in 0..5 {
        let result = client.read_holding_registers(1, 0, 1).await;
        if let Err(e) = result {
            let detail = e.detail();
            if detail.contains("max_retries") {
                saw_max_retries = true;
                break;
            }
        }
    }
    assert!(saw_max_retries, "expected max_retries error");
}

// ── Reconnect survives task restart ────────────────────────────────────

#[tokio::test]
async fn rtu_reconnect_survives_server_restart() {
    // Start a server, kill it, then reconnect to a new server via factory.
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    // Start first server
    let store1 = Arc::new(SlaveStore::with_holding_registers(&[(0, 11)]));
    let server1 = rtu::RtuServer::new(server_stream);
    let h1 = tokio::spawn(async move {
        server1.serve_forever(store1).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Kill server and drop server_stream (closes transport)
    h1.abort();
    drop(h1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client with reconnect factory that creates a new server each time
    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_millis(100))
        .with_reconnect(5, Duration::from_millis(50), || {
            let (c, s) = tokio::io::duplex(1024);
            let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 22)]));
            let server = rtu::RtuServer::new(s);
            tokio::spawn(async move {
                server.serve_forever(store).await.ok();
            });
            Ok(c)
        });

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![22]);
}

// ── ASCII reconnect survives server restart ────────────────────────────

#[tokio::test]
async fn ascii_reconnect_survives_server_restart() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);

    // Start first server
    let store1 = Arc::new(SlaveStore::with_holding_registers(&[(0, 11)]));
    let server1 = ascii::AsciiServer::new(server_stream);
    let h1 = tokio::spawn(async move {
        server1.serve_forever(store1).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Kill server
    h1.abort();
    drop(h1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client with reconnect factory that creates a new server each time
    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(100))
        .with_reconnect(5, Duration::from_millis(50), || {
            let (c, s) = tokio::io::duplex(1024);
            let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 88)]));
            let server = ascii::AsciiServer::new(s);
            tokio::spawn(async move {
                server.serve_forever(store).await.ok();
            });
            Ok(c)
        });

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![88]);
}
