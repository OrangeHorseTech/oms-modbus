// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! README example code — every snippet in README.md is extracted from this file.
//!
//! Each `#[test]` function below corresponds to a code block in the README.
//! The function body IS the code block content. A build script or tool
//! extracts these bodies and inserts them at `<!-- @readme:NAME -->` markers
//! in README.md.
//!
//! **Rule:** If a snippet doesn't compile here, it won't appear in the README.
//! This guarantees the README code examples are always correct.

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::monitor::*;
use oms_modbus::*;

// ═══════════════════════════════════════════════════════════════════════════════
// @readme quick_start
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn quick_start() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    // ── Start a TCP server on a random port ──────────────────────
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let bind_addr = server.local_addr().unwrap();

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1234), (1, 5678)]));
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Connect and exchange data ─────────────────────────────────
    let client = tcp::TcpClient::connect_with_timeout(bind_addr, Duration::from_secs(3))
        .await
        .unwrap();

    let regs = client.read_holding_registers(1, 0, 2).await.unwrap();
    assert_eq!(regs, vec![1234, 5678]);

    client.write_single_register(1, 0, 9999).await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme client
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "requires a Modbus TCP server at 192.168.1.10:502"]
async fn client_master() {
    // TCP
    let addr = "192.168.1.10:502".parse().unwrap();
    let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .unwrap();

    // RTU — serial port
    // let port = tokio_serial::SerialStream::open(&config)?;
    // let client = rtu::RtuClient::with_timeout(port, Duration::from_secs(3));

    // RTU/ASCII with monitoring, timing, reconnect — via ClientOptions
    // let opts = ClientOptions::default()
    //     .with_timeout(Duration::from_secs(3))
    //     .with_tap(BusCapture::unbounded());
    // let client = rtu::with_options(port, opts);

    // ── Standard function codes ──────────────────────────────────────
    client.read_coils(1, 0, 10).await.unwrap();
    client.read_discrete_inputs(1, 0, 4).await.unwrap();
    client.read_holding_registers(1, 100, 5).await.unwrap();
    client.read_input_registers(1, 0, 2).await.unwrap();

    client.write_single_coil(1, 10, true).await.unwrap();
    client.write_single_register(1, 100, 42).await.unwrap();
    client
        .write_multiple_coils(1, 20, &[true, false, true])
        .await
        .unwrap();
    client
        .write_multiple_registers(1, 200, &[1, 2, 3])
        .await
        .unwrap();

    // ── Advanced ─────────────────────────────────────────────────────
    client
        .mask_write_register(1, 10, 0x00FF, 0xFF00)
        .await
        .unwrap();
    let _old = client
        .read_write_multiple_registers(1, 0, 4, 10, &[100, 200])
        .await
        .unwrap();
    let (_sf, _data) = client.diagnostic(1, 0x0000, 0xABCD).await.unwrap();

    // ── Share via Arc ────────────────────────────────────────────────
    let _shared: Arc<dyn ModbusClient> = Arc::new(client);
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme server
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn server_slave() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 100), (1, 200)]));
    store.write_coil(0, true);

    // TCP
    let addr = "127.0.0.1:0".parse().unwrap();
    let server = tcp::TcpServer::bind(addr).await.unwrap();
    let _bind_addr = server.local_addr().unwrap();
    // server.serve_forever(store).await?;  // would block forever

    // RTU/ASCII with bus timing — uses in-memory duplex for testing
    let (serial_port, _server_end) = tokio::io::duplex(64);
    let rtu_server =
        rtu::RtuServer::new(serial_port).with_bus_timing(Arc::new(BusTiming::rtu_35t(9600)));
    tokio::spawn(async move {
        rtu_server.serve_forever(store).await.ok();
    });
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme bus_monitoring
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn bus_monitoring() {
    let (port, _server) = tokio::io::duplex(64);

    // ── In-memory recording ──────────────────────────────────────────
    let cap = Arc::new(BusCapture::unbounded()); // memory buffer + stats
    let _ring = BusCapture::bounded(10_000); // bounded ring buffer

    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(cap.clone());
    let _client = rtu::with_options(port, opts);

    // client.read_holding_registers(1, 0, 5).await?;
    // println!("requests: {}  errors: {}", cap.count_requests(), cap.count_errors());
    // for record in cap.drain() {
    //     println!("{record}");   // [TX] 2026-08-05T14:32:15.123456    8B  [01 03 ...]
    // }

    // ── File recording (async, non-blocking) ─────────────────────────
    let recorder = FileRecorder::new("traffic.log").unwrap();
    let (tap, _handle) = ChannelRecorder::spawn(recorder);

    let _opts = ClientOptions::default().with_tap(Arc::new(tap));
    // let client = rtu::with_options(port, opts);
    // ... traffic is written to traffic.log in real time ...

    // ── Custom backend (network, database, dashboard) ────────────────
    struct MyRecorder;
    impl RecordSink for MyRecorder {
        fn on_packet(&mut self, _record: PacketRecord) {
            // Called sequentially from a dedicated tokio task
        }
    }
    let (_tap, _handle) = ChannelRecorder::spawn(MyRecorder);
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme reconnect
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "requires a Modbus TCP server at 127.0.0.1:502"]
async fn auto_reconnect() {
    // TCP — address-based (built-in)
    let addr = "127.0.0.1:502".parse().unwrap();
    let _client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_reconnect(0, Duration::from_millis(100)); // 0 = infinite

    // RTU/ASCII — factory-based (USB serial resilience)
    // let client = rtu::RtuClient::with_timeout(port, timeout)
    //     .with_reconnect(3, Duration::from_millis(100), || {
    //         tokio_serial::SerialStream::open(&config) // reopen after disconnect
    //     });

    // With ClientOptions + with_reconnect_on (SniffIo re-wrapped automatically)
    let (port, _server) = tokio::io::duplex(64);
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(Arc::new(BusCapture::unbounded()));
    let _client =
        rtu::with_options(port, opts).with_reconnect_on(0, Duration::from_millis(100), || {
            let (c, _s) = tokio::io::duplex(64);
            Ok(c)
        });
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme bus_timing
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn bus_timing() {
    let (port, _server) = tokio::io::duplex(64);

    // RTU 3.5T at 9600 baud (~4 ms)
    let _timing = BusTiming::rtu_35t(9600);

    // Per spec: >19200 baud → fixed 1.75 ms minimum
    let _timing = BusTiming::rtu_35t(115200); // → 1.75 ms, not ~334 µs

    // Raw formula (no cap) — for custom hardware
    let _timing = BusTiming::rtu_35t_raw(115200); // → ~334 µs

    // Custom spacing for slow devices
    let _timing = BusTiming::custom(Duration::from_millis(50));

    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_bus_timing(BusTiming::rtu_35t(9600));
    let _client = rtu::with_options(port, opts);
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme error_handling
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn error_handling() {
    // Simulate a client call returning an error for demonstration
    let result: Result<Vec<u16>, ModbusError> = Err(ModbusError::timeout("RTU recv timed out"));

    match result {
        Ok(regs) => println!("{regs:?}"),
        Err(e) => {
            eprintln!("{}", e); // "TIMEOUT" — short, UI-friendly
            log::error!("{}", e.detail()); // "TIMEOUT: RTU recv timed out"
            println!("{}", e.label()); // "TIMEOUT"

            match &e {
                ModbusError::Exception {
                    function: _,
                    code: _,
                } => { /* handle */ }
                ModbusError::Timeout(_) => { /* retry */ }
                ModbusError::Serial(_) => { /* port error */ }
                _ => {}
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme tcp_config
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "requires a Modbus TCP server at 127.0.0.1:502"]
async fn advanced_tcp() {
    use oms_modbus::tcp::{LengthMode, TcpConfig, TidMode};

    let addr = "127.0.0.1:502".parse().unwrap();

    // Gateway mode — auto TID + UID in PDU body (RTU-over-TCP gateways)
    let _client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_gateway_mode();

    // Full custom config
    let _client = tcp::TcpClient::connect(addr)
        .await
        .unwrap()
        .with_config(TcpConfig {
            tid: TidMode::Auto,                // auto-increment transaction ID
            unit_id_in_body: true,             // UID also in PDU body
            length_mode: LengthMode::Standard, // Length includes byte-6 UID
        });
}

// ═══════════════════════════════════════════════════════════════════════════════
// @readme transport_flex
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "requires a TCP server at 127.0.0.1:1234"]
async fn transport_flexibility() {
    use tokio::net::TcpStream;

    // RTU over TCP — RTU frames on a TCP connection
    let tcp = TcpStream::connect("127.0.0.1:1234").await.unwrap();
    let _rtu = rtu::RtuClient::with_timeout(tcp, Duration::from_secs(3));
    // let regs = _rtu.read_holding_registers(1, 0, 5).await?;

    // ASCII over TCP — ASCII frames on a TCP connection
    let tcp = TcpStream::connect("127.0.0.1:1234").await.unwrap();
    let _ascii = ascii::AsciiClient::with_timeout(tcp, Duration::from_secs(3));

    // In-memory channel — perfect for testing (zero hardware)
    let (client, _server) = tokio::io::duplex(1024);
    let _rtu = rtu::RtuClient::with_timeout(client, Duration::from_secs(3));

    // With capture — uses SniffIo internally
    let (_any, _server) = tokio::io::duplex(64);
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(Arc::new(BusCapture::unbounded()));
    let _rtu = rtu::with_options(_any, opts); // capture works anywhere
}
