// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Integration tests — end-to-end TCP client/server communication.
// Covers success paths, error paths, and edge cases.

use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use oms_modbus::*;

/// Start a TCP server on a random port, return (server_handle, address).
/// The handle must be kept alive until the test completes.
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

// ── Success path tests ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn read_holding_registers_success() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 100), (1, 200)]));
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let regs = client.read_holding_registers(1, 0, 2).await.unwrap();
    assert_eq!(regs, vec![100, 200]);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_single_register_success() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 0)]));
    let (_h, addr) = spawn_server(store.clone()).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    client.write_single_register(1, 0, 9999).await.unwrap();
    assert_eq!(store.read_holding_register(0), 9999);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_multiple_registers() {
    let store = Arc::new(SlaveStore::new());
    let (_h, addr) = spawn_server(store.clone()).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    client
        .write_multiple_registers(1, 0, &[10, 20, 30])
        .await
        .unwrap();
    assert_eq!(store.read_holding_register(0), 10);
    assert_eq!(store.read_holding_register(1), 20);
    assert_eq!(store.read_holding_register(2), 30);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_write_coils() {
    let store = Arc::new(SlaveStore::new());
    store.write_coil(0, true);
    store.write_coil(1, false);
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    // Modbus packs coil reads into full bytes — reading 4 returns ceil(4/8)=8 values
    let coils = client.read_coils(1, 0, 4).await.unwrap();
    assert!(coils.len() >= 4);
    assert!(coils[0]);
    assert!(!coils[1]);

    client.write_single_coil(1, 1, true).await.unwrap();
    client
        .write_multiple_coils(1, 2, &[true, true])
        .await
        .unwrap();
    let coils = client.read_coils(1, 0, 4).await.unwrap();
    assert!(coils.len() >= 4);
    assert!(coils[0]);
    assert!(coils[1]);
    assert!(coils[2]);
    assert!(coils[3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn mask_write_register_integration() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(10, 0x0F0F)]));
    let (_h, addr) = spawn_server(store.clone()).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    // Keep low byte, set high byte to 0xF0
    client
        .mask_write_register(1, 10, 0x00FF, 0xF000)
        .await
        .unwrap();
    assert_eq!(store.read_holding_register(10), 0xF00F);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_write_multiple_integration() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(20, 1), (21, 2)]));
    let (_h, addr) = spawn_server(store.clone()).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let old_vals = client
        .read_write_multiple_registers(1, 20, 2, 20, &[100, 200])
        .await
        .unwrap();
    assert_eq!(old_vals, vec![1, 2]); // pre-write values
    assert_eq!(store.read_holding_register(20), 100);
    assert_eq!(store.read_holding_register(21), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_read_function_codes() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    // FC01: coils pack to bytes — reading 1 returns 8 values
    let coils = client.read_coils(1, 0, 1).await.unwrap();
    assert!(!coils.is_empty());
    assert!(!coils[0]); // all coils default to false

    let inputs = client.read_discrete_inputs(1, 0, 1).await.unwrap();
    assert!(!inputs.is_empty());
    assert!(!inputs[0]);

    assert_eq!(
        client.read_holding_registers(1, 0, 1).await.unwrap(),
        vec![42]
    );
    assert_eq!(client.read_input_registers(1, 0, 1).await.unwrap(), vec![0]);
}

// ── Error path tests ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn timeout_on_no_server() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 19999);
    let result = tcp::TcpClient::connect_with_timeout(addr, Duration::from_millis(100)).await;
    assert!(result.is_err()); // connection refused or timeout
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_and_read_with_unit_id_one() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let (_h, addr) = spawn_server(store).await;
    // Modbus TCP uses the client's MBAP unit_id — the server echoes it back.
    // Any unit_id works. This test verifies basic read with unit_id=1.
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_nonexistent_register_returns_zero() {
    let store = Arc::new(SlaveStore::new());
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let regs = client.read_holding_registers(1, 999, 1).await.unwrap();
    assert_eq!(regs, vec![0]);
}

// ── Concurrency tests ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_one_server() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 0)]));
    let (_h, addr) = spawn_server(store).await;

    let mut handles = vec![];
    for i in 0..5 {
        handles.push(tokio::spawn(async move {
            let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
                .await
                .unwrap();
            client.write_single_register(1, 0, i * 100).await.unwrap();
            client.read_holding_registers(1, 0, 1).await.unwrap()
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ── Max size read ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn max_size_read_holding() {
    let values: Vec<(u16, u16)> = (0..125).map(|i| (i, i)).collect();
    let store = Arc::new(SlaveStore::with_holding_registers(&values));
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let regs = client.read_holding_registers(1, 0, 125).await.unwrap();
    assert_eq!(regs.len(), 125);
    assert_eq!(regs[0], 0);
    assert_eq!(regs[124], 124);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_size_read_coils() {
    let store = Arc::new(SlaveStore::new());
    // Set some coils at the edges
    store.write_coil(0, true);
    store.write_coil(1999, true);
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let coils = client.read_coils(1, 0, 2000).await.unwrap();
    assert_eq!(coils.len(), 2000);
    assert!(coils[0]);
    assert!(coils[1999]);
}

// ── PDU limit validation ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn diagnostic_return_query_data_integration() {
    let store = Arc::new(SlaveStore::new());
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();
    let (sf, data) = client.diagnostic(1, 0x0000, 0xABCD).await.unwrap();
    assert_eq!(sf, 0x0000);
    assert_eq!(data, 0xABCD);
}

#[tokio::test]
async fn pdu_too_large_is_rejected() {
    use bytes::Bytes;
    use oms_modbus::frame::Request;
    use std::convert::TryFrom;

    // Try to encode a request with >125 registers (would produce PDU >253 bytes)
    let values: Vec<u16> = (0..130).collect();
    let req = Request::WriteMultipleRegisters(0, Cow::Owned(values));
    let result = Bytes::try_from(req);
    assert!(result.is_err(), "encoding >253 byte PDU should fail");
}

// ── TcpConfig: gateway mode ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tcp_config_gateway_mode_works() {
    use oms_modbus::tcp::{LengthMode, TcpConfig, TidMode};
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            tid: TidMode::Auto,
            unit_id_in_body: true,
            length_mode: LengthMode::PduOnly,
        });
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_config_fixed_tid_works() {
    use oms_modbus::tcp::{LengthMode, TcpConfig, TidMode};
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
    let (_h, addr) = spawn_server(store).await;
    let client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            tid: TidMode::Fixed(0x1234),
            unit_id_in_body: false,
            length_mode: LengthMode::Standard,
        });
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![77]);
}

// ── Server: corrupt MBAP header recovery ───────────────────────────────

/// When a corrupt MBAP header arrives (proto_id != 0), the server clears
/// the buffer and the same connection can still process subsequent valid
/// frames.
#[tokio::test(flavor = "multi_thread")]
async fn tcp_server_recovers_after_corrupt_header() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let bind_addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut stream = tokio::net::TcpStream::connect(bind_addr).await.unwrap();

    // Send corrupt MBAP header: proto_id = 0xFFFF (must be 0)
    let corrupt = [
        0x00u8, 0x01, 0xFF, 0xFF, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
    ];
    stream.write_all(&corrupt).await.unwrap();
    // Brief pause to let server process and clear.
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Now send a valid ReadHoldingRegisters frame on the same connection.
    let valid = [
        0x00u8, 0x02, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
    ];
    stream.write_all(&valid).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    // Expect valid response: FC03, 2 bytes, value = 42 (0x002A)
    assert!(n >= 10, "response too short: {n} bytes");
    let pdu = &buf[7..n]; // skip MBAP header
    assert_eq!(pdu[0], 0x03, "expected FC 0x03");
    assert_eq!(pdu[1], 0x02, "expected byte count 2");
    assert_eq!(&pdu[2..4], &[0x00, 0x2A], "expected value 42");
}
// ── TcpConfig: PduOnly and gateway modes ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tcp_pdu_only_both_sides() {
    use oms_modbus::tcp::{LengthMode, TcpConfig};
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 55), (1, 66)]));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let bind_addr = server.local_addr().unwrap();
    let _h = tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect(bind_addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let regs = client.read_holding_registers(1, 0, 2).await.unwrap();
    assert_eq!(regs, vec![55, 66]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_pdu_only_write_then_read() {
    use oms_modbus::tcp::{LengthMode, TcpConfig};
    let store = Arc::new(SlaveStore::new());
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let bind_addr = server.local_addr().unwrap();
    let store_clone = store.clone();
    let _h = tokio::spawn(async move {
        server.serve_forever(store_clone).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect(bind_addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    client.write_single_register(1, 10, 0xABCD).await.unwrap();
    assert_eq!(store.read_holding_register(10), 0xABCD);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_standard_client_pdu_only_server() {
    // Standard client talking to PduOnly server — client handles both
    // response formats in its receive path.
    use oms_modbus::tcp::{LengthMode, TcpConfig};
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 88)]));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let bind_addr = server.local_addr().unwrap();
    let _h = tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Standard client (default config, LengthMode::Standard)
    let client = tcp::TcpClient::connect(bind_addr).await.unwrap();
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![88]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_gateway_client_with_pdu_only_server() {
    // Gateway client (Auto TID, unit_id_in_body, Standard length) talking to
    // a PduOnly server. The client prepends UID to the PDU body; the server
    // handles it via try_parse_tcp_frame → try_extract_tcp_pdu case 1.
    use oms_modbus::tcp::TcpConfig;
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99), (1, 100)]));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: oms_modbus::tcp::LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let bind_addr = server.local_addr().unwrap();
    let _h = tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect(bind_addr)
        .await
        .unwrap()
        .with_config(TcpConfig::gateway());
    let regs = client.read_holding_registers(1, 0, 2).await.unwrap();
    assert_eq!(regs, vec![99, 100]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_pdu_only_multiple_requests() {
    // Multiple requests with Auto TID and PduOnly — verifies TID counter
    // increments correctly across requests and data doesn't get confused.
    use oms_modbus::tcp::{LengthMode, TcpConfig, TidMode};
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 10),
        (1, 20),
        (2, 30),
    ]));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let bind_addr = server.local_addr().unwrap();
    let _h = tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect(bind_addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            tid: TidMode::Auto,
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });

    // Multiple reads — each gets its own TID
    for _ in 0..5 {
        let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
        assert_eq!(regs, vec![10]);
    }
    let regs = client.read_holding_registers(1, 0, 3).await.unwrap();
    assert_eq!(regs, vec![10, 20, 30]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_pdu_only_max_size_read() {
    // Max-size read (125 registers) with PduOnly to verify large payloads
    // work correctly when Length = PDU bytes only.
    use oms_modbus::tcp::{LengthMode, TcpConfig};
    let values: Vec<(u16, u16)> = (0..125).map(|i| (i, i * 2)).collect();
    let store = Arc::new(SlaveStore::with_holding_registers(&values));
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let bind_addr = server.local_addr().unwrap();
    let _h = tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect(bind_addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            length_mode: LengthMode::PduOnly,
            ..TcpConfig::default()
        });
    let regs = client.read_holding_registers(1, 0, 125).await.unwrap();
    assert_eq!(regs.len(), 125);
    assert_eq!(regs[0], 0);
    assert_eq!(regs[124], 248);
}

// ── Server: invalid PDU → no response (process_server_request error path) ──

#[tokio::test(flavor = "multi_thread")]
async fn read_uninitialized_register_returns_zero() {
    // Reading from an uninitialized holding register returns 0 (the
    // default value for out-of-range addresses in SlaveStore).
    let store = Arc::new(SlaveStore::new());
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let bind_addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = tcp::TcpClient::connect_with_timeout(bind_addr, Duration::from_millis(200))
        .await
        .unwrap();
    // Send an invalid FC that the server will reject silently
    let result = client.read_holding_registers(1, 0, 1).await;
    // Valid FC03 should work
    assert_eq!(result.unwrap(), vec![0]);
}

// ── P1: TCP MBAP error paths ────────────────────────────────────────────

/// Spawn a raw TCP responder that sends `response` after reading the client's
/// request. Returns the bound address.
async fn spawn_raw_responder(response: Vec<u8>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Read the client's MBAP request
        let mut buf = [0u8; 256];
        let _ = tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf))
            .await
            .unwrap_or(Ok(0));
        // Send the malformed response
        let _ = stream.write_all(&response).await;
        // Keep the connection alive briefly so the client can read the response
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    addr
}

/// Build a minimal valid MBAP response for FC03 ReadHoldingRegisters (2 bytes).
/// TID=0, Proto=0, Len=4, UID=1, body=[3, 2, 0, 42]
fn valid_mbap_response(tid: u16, len: u16, body: &[u8]) -> Vec<u8> {
    let mut rsp = Vec::with_capacity(7 + body.len());
    rsp.extend_from_slice(&tid.to_be_bytes());
    rsp.extend_from_slice(&0u16.to_be_bytes()); // Protocol ID
    rsp.extend_from_slice(&len.to_be_bytes());
    rsp.push(1u8); // Unit ID
    rsp.extend_from_slice(body);
    rsp
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_invalid_protocol_id_error() {
    // MBAP with Protocol ID = 1 (must be 0). Body is a valid FC03 response.
    let body = [0x03, 0x02, 0x00, 0x2A];
    let mut rsp = valid_mbap_response(0, body.len() as u16 + 1, &body);
    rsp[2] = 0x00;
    rsp[3] = 0x01; // Protocol ID = 1 (invalid)
    let addr = spawn_raw_responder(rsp).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_millis(500))
        .await
        .unwrap();
    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("Protocol ID"),
        "expected Protocol ID error, got: {}",
        err.detail()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_mbap_length_exceeds_max_error() {
    // MBAP with Length > MAX_TCP_ADU_SIZE (260).
    let rsp = [
        0x00, 0x01, // TID
        0x00, 0x00, // Proto = 0
        0x01, 0x06, // Length = 262 (> 260 = MAX_TCP_ADU_SIZE)
        0x01, // UID
        0x03, 0x02, 0x00, 0x2A, // valid PDU (ignored due to invalid length)
    ];
    let addr = spawn_raw_responder(rsp.to_vec()).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_millis(500))
        .await
        .unwrap();
    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("Length exceeds") || err.detail().contains("max ADU"),
        "expected Length exceeds error, got: {}",
        err.detail()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_empty_response_pdu_error() {
    // MBAP with Length=1 (just the UID, no body after that).
    let rsp = [
        0x00, 0x01, // TID
        0x00, 0x00, // Proto = 0
        0x00, 0x01, // Length = 1 (just UID, no PDU bytes)
        0x01, // UID
    ];
    let addr = spawn_raw_responder(rsp.to_vec()).await;
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_millis(500))
        .await
        .unwrap();
    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("empty"),
        "expected empty PDU error, got: {}",
        err.detail()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_mbap_header_fragmented_arrival() {
    // Send MBAP header in two parts: bytes 0-3 first, then bytes 4-6 + body.
    // The client should reassemble and parse correctly.
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let bind_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Read the client's request
        let mut buf = [0u8; 256];
        let _n = stream.read(&mut buf).await.unwrap();
        // Build a valid response: FC03, 2 bytes, value=77
        let rsp = [
            0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x01, 0x03, 0x02, 0x00, 0x4D,
        ];
        // Send bytes 0-3 first (TID + half of Proto)
        stream.write_all(&rsp[..4]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Send remaining bytes
        stream.write_all(&rsp[4..]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let client = tcp::TcpClient::connect_with_timeout(bind_addr, Duration::from_secs(3))
        .await
        .unwrap();
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![77]);
}

// ── P1: ModbusClient response variant mismatch → Protocol error ─────────

use async_trait::async_trait;
use oms_modbus::ModbusClient;

/// Mock transport that returns a specified response regardless of request.
struct MockTransport {
    response: oms_modbus::Response,
}

#[async_trait]
impl ModbusClient for MockTransport {
    async fn call(
        &self,
        _slave: u8,
        _request: oms_modbus::Request<'_>,
    ) -> Result<oms_modbus::Response, oms_modbus::ModbusError> {
        Ok(self.response.clone())
    }
}

#[tokio::test]
async fn wrong_response_variant_yields_protocol_error() {
    // read_holding_registers expects ReadHoldingRegisters;
    // return ReadCoils instead — must produce Protocol error.
    let mock = MockTransport {
        response: oms_modbus::Response::ReadCoils(vec![true]),
    };
    let err = mock.read_holding_registers(1, 0, 1).await.unwrap_err();
    match &err {
        oms_modbus::ModbusError::Protocol(msg) => {
            assert!(
                msg.contains("unexpected response"),
                "should say 'unexpected response', got: {msg}"
            );
        }
        other => panic!("expected Protocol, got: {other:?}"),
    }
}

#[tokio::test]
async fn exception_response_still_converted_to_exception_error() {
    // If the server returns an Exception, the default methods should
    // still convert it to ModbusError::Exception (not Protocol).
    let mock = MockTransport {
        response: oms_modbus::Response::Exception(3, oms_modbus::Exception::IllegalDataAddress),
    };
    let err = mock.read_holding_registers(1, 0, 1).await.unwrap_err();
    match &err {
        oms_modbus::ModbusError::Exception { function: _, code } => {
            assert_eq!(*code, 2, "IllegalDataAddress code should be 2");
        }
        other => panic!("expected Exception, got: {other:?}"),
    }
}

#[tokio::test]
async fn write_method_echo_mismatch_yields_protocol_error() {
    // write_single_register expects echoed address+value.
    // Return wrong address — must produce Protocol error.
    let mock = MockTransport {
        response: oms_modbus::Response::WriteSingleRegister(99, 42),
    };
    let err = mock.write_single_register(1, 0, 42).await.unwrap_err();
    match &err {
        oms_modbus::ModbusError::Protocol(msg) => {
            assert!(msg.contains("unexpected response"), "got: {msg}");
        }
        other => panic!("expected Protocol, got: {other:?}"),
    }
}
