# OMS Modbus

[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange?logo=rust)](https://www.rust-lang.org) [![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT) [![crates.io](https://img.shields.io/crates/v/oms-modbus?label=crates.io)](https://crates.io/crates/oms-modbus) [![docs](https://img.shields.io/docsrs/oms-modbus)](https://docs.rs/oms-modbus) [![tests](https://img.shields.io/badge/tests-passing-brightgreen)](.)

> ⚠️ **Preview Release** — v0.2.0 is a preview. The API may change based on
> user feedback. We welcome issues, PRs, and suggestions.

**High-performance, transport-generic Modbus library in pure Rust.**

Complete Master (client) and Slave (server) for TCP, RTU, and ASCII —
all through a single unified API. 11 function codes. Generic transport
(any `AsyncRead + AsyncWrite` — serial, TCP, Unix sockets, pipes).
Passive `WireTap` bus monitor with microsecond timestamps. Configurable
TCP MBAP framing for non-standard devices. Auto-reconnect with USB
serial resilience. Spec-compliant RS-485 bus timing.

Async-first. Zero panics. **One API, every transport.**

---

## Features

| Category | Details |
|----------|---------|
| **Protocols** | TCP (MBAP), RTU (CRC-16), ASCII (LRC). Full client + server for each. |
| **Generic Transport** | RTU and ASCII work over **any** `AsyncRead + AsyncWrite` — serial ports, TCP streams, Unix sockets, pipes, in-memory channels. One `RtuClient` / `AsciiClient` for all. |
| **TCP MBAP Config** | Full control over MBAP framing via `TcpConfig`: transaction ID (fixed / auto-increment), unit ID placement (header-only / PDU body), length calculation (standard / PDU-only). Gateway mode preset for TCP-to-RTU bridges. |
| **Bus Monitor** | Passive `WireTap` at the I/O boundary — like a hardware logic analyzer. Microsecond ISO 8601 timestamps (hybrid `Instant` + `SystemTime`, one syscall at init). Memory ring buffer, channel-based async dispatch, file recorder, custom backends via `RecordSink`. |
| **Reconnect** | Built-in for all transports. TCP: address-based. RTU/ASCII: factory closure for USB serial resilience, auto re-wraps in `SniffIo`. Fixed-interval (industrial-predictable). |
| **Bus Timing** | Enforces ≥3.5T silence between RTU/ASCII frames. Per Modbus Serial Spec V1.02 §1.4 (>19200 baud → fixed 1.75 ms). Raw-formula mode for custom hardware. Server-side timing support. |
| **Custom Service** | Implement the `Service` trait to bridge real hardware, transform values, enforce access control, or simulate devices. See `dynamic_server` example. |
| **Function Codes** | FC01–06, FC08, FC15–16, FC22–23 — all 11 with write+readback verification. Diagnostic sub-functions 0x00, 0x0A–0x0E. |
| **Slave Store** | `RwLock<Vec<u16>>` — O(1) indexed access. 125 consecutive registers = single lock + memcpy. |
| **Error Handling** | Structured `ModbusError` — short `Display`, full `detail()`, machine-readable `label()`. No string parsing. |
| **Panic-Free** | Zero `.unwrap()` in production code. Poison-safe mutexes. |
| **Zero-Copy** | `Bytes`-based PDU encoding. TCP codec uses reference-counted buffers. |
| **Lean** | `tokio` + `tokio-serial` + `tokio-util` + `bytes` + `futures-util` + `async-trait` + `thiserror` + `log`. 8 runtime deps, no transient dependencies. |

## Installation

```toml
[dependencies]
oms-modbus = "0.2"
```

No feature flags — everything included. TCP, RTU, ASCII, server, capture, monitoring, and timing are all available by default. One dependency, all three protocols.

## Quick Start

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

// ── Start a TCP server on a random port ──────────────────────
let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
let server = tcp::TcpServer::bind(addr).await.unwrap();
let bind_addr = server.local_addr().unwrap();

let store = Arc::new(SlaveStore::with_holding_registers(&[
    (0, 1234),
    (1, 5678),
]));
tokio::spawn(async move { server.serve_forever(store).await.ok(); });
tokio::time::sleep(Duration::from_millis(50)).await;

// ── Connect and exchange data ─────────────────────────────────
let client = tcp::TcpClient::connect_with_timeout(
    bind_addr,
    Duration::from_secs(3),
)
.await
.unwrap();

let regs = client.read_holding_registers(1, 0, 2).await.unwrap();
assert_eq!(regs, vec![1234, 5678]);

client.write_single_register(1, 0, 9999).await.unwrap();
```

## Usage

### Client (Master)

All function codes work the same way across TCP, RTU, and ASCII:

```rust
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
client.mask_write_register(1, 10, 0x00FF, 0xFF00).await.unwrap();
let _old = client
    .read_write_multiple_registers(1, 0, 4, 10, &[100, 200])
    .await
    .unwrap();
let (_sf, _data) = client.diagnostic(1, 0x0000, 0xABCD).await.unwrap();

// ── Share via Arc ────────────────────────────────────────────────
let _shared: Arc<dyn ModbusClient> = Arc::new(client);
```

### Server (Slave)

```rust
let store = Arc::new(SlaveStore::with_holding_registers(&[
    (0, 100),
    (1, 200),
]));
store.write_coil(0, true);

// TCP
let addr = "127.0.0.1:0".parse().unwrap();
let server = tcp::TcpServer::bind(addr).await.unwrap();
let _bind_addr = server.local_addr().unwrap();
// server.serve_forever(store).await?;  // would block forever

// RTU/ASCII with bus timing — uses in-memory duplex for testing
let (serial_port, _server_end) = tokio::io::duplex(64);
let rtu_server = rtu::RtuServer::new(serial_port)
    .with_bus_timing(Arc::new(BusTiming::rtu_35t(9600)));
tokio::spawn(async move { rtu_server.serve_forever(store).await.ok(); });
```

Implement the `Service` trait for custom backends (hardware bridge, access control, value transformation) — see `examples/custom_service.rs`.

### Bus Monitoring

`WireTap` is a passive observer attached to the transport. Implement it for custom backends, or use the built-in recorders:

```rust
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
```

Timestamps are ISO 8601 (`2026-08-05T14:32:15.123456`) — real wall-clock time via a hybrid `Instant` + `SystemTime` clock. One syscall at init, zero thereafter.

### Auto-Reconnect

```rust
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
let _client = rtu::with_options(port, opts).with_reconnect_on(
    0,
    Duration::from_millis(100),
    || {
        let (c, _s) = tokio::io::duplex(64);
        Ok(c)
    },
);
```

- `max_retries = 0` → infinite retry
- Fixed interval (no exponential backoff — industrial environments require predictable timing)
- Protocol errors (CRC mismatch, exception responses) are **not** retried
- Only transport errors (timeout, connection, serial) trigger reconnect

### Bus Timing

```rust
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
```

### Error Handling

```rust
// Simulate a client call returning an error for demonstration
let result: Result<Vec<u16>, ModbusError> = Err(ModbusError::timeout("RTU recv timed out"));

match result {
    Ok(regs) => println!("{regs:?}"),
    Err(e) => {
        eprintln!("{}", e); // "TIMEOUT" — short, UI-friendly
        log::error!("{}", e.detail()); // "TIMEOUT: RTU recv timed out"
        println!("{}", e.label()); // "TIMEOUT"

        match &e {
            ModbusError::Exception { function: _, code: _ } => { /* handle */ }
            ModbusError::Timeout(_) => { /* retry */ }
            ModbusError::Serial(_) => { /* port error */ }
            _ => {}
        }
    }
}
```

### Advanced TCP Configuration

`TcpConfig` gives full control over MBAP framing for non-standard devices:

```rust
use oms_modbus::tcp::{TcpConfig, TidMode, LengthMode};

let addr = "127.0.0.1:502".parse().unwrap();

// Gateway mode — auto TID + UID in PDU body (RTU-over-TCP gateways)
let _client = tcp::TcpClient::connect(addr)
    .await
    .unwrap()
    .with_gateway_mode();

// Full custom config
let _client = tcp::TcpClient::connect(addr).await.unwrap().with_config(
    TcpConfig {
        tid: TidMode::Auto,                // auto-increment transaction ID
        unit_id_in_body: true,             // UID also in PDU body
        length_mode: LengthMode::Standard, // Length includes byte-6 UID
    },
);
```

Three configuration axes:

| Axis | Values | Default |
|------|--------|---------|
| `tid` | `Fixed(u16)` / `Auto` | `Fixed(0)` |
| `unit_id_in_body` | PDU body starts with UID | `false` |
| `length_mode` | `Standard` (1+PDU) / `PduOnly` | `Standard` |

### Transport Flexibility

All protocol clients are generic over `T: AsyncRead + AsyncWrite`.
The same `RtuClient` or `AsciiClient` works with any byte stream:

```rust
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
```

**Transport Matrix:**

| Transport | RTU | ASCII | TCP (MBAP) | SniffIo |
|-----------|-----|-------|------------|---------|
| Serial port | ✅ | ✅ | — | ✅ |
| TCP stream | ✅ | ✅ | ✅ | ✅ |
| DuplexStream | ✅ | ✅ | — | ✅ |
| Unix socket | ✅ | ✅ | — | ✅ |
| Custom I/O | ✅ | ✅ | — | ✅ |

See `examples/generic_transport.rs` and `examples/tcp_client_gateway.rs`.

## Testing

```bash
# All tests (440+ passed, zero failures)
cargo test

# Specific suites
cargo test --test spec_compliance        # CRC, LRC, PDU, MBAP
cargo test --test frame_tests            # PDU + Response + FunctionCode
cargo test --test slave_store_tests      # Register/coil CRUD
cargo test --test recording_tests        # BusCapture, TrafficStats, RingBuf
cargo test --test capture_tests          # BusCapture edge cases
cargo test --test error_tests            # ModbusError + Exception + ReconnectConfig
cargo test --test codec_tests            # RTU/ASCII encode/decode
cargo test --test codec_resilience       # Fragmentation, noise, recovery
cargo test --test integration_tests      # TCP end-to-end
cargo test --test options_tests          # ClientOptions builder
cargo test --test bus_timing_tests       # BusTiming frame spacing
cargo test --test sniff_io_tests         # SniffIo passthrough + capture
cargo test --test reconnect_tests        # TCP reconnect
cargo test --test serial_reconnect_tests # RTU/ASCII reconnect
cargo test --test server_hook_tests      # ServerHook + HookedService
cargo test --test rtu_server_tests       # RTU server
cargo test --test ascii_server_tests     # ASCII server

# Hardware tests (require COM20↔COM21 virtual serial pair)
cargo test --test serial_live_test -- --ignored --test-threads=1

# No hardware? All core tests run with zero setup.
cargo test --lib --tests
```

### Benchmarks

```bash
cargo bench
```

Representative results from a developer workstation (Intel Core i7-6800K @ 3.40 GHz, Windows 10, DDR4 32 GB, Rust 1.97). Run `cargo bench` on your target hardware for numbers relevant to your deployment.

| Benchmark | Time |
|-----------|------|
| CRC-16 (6-byte RTU frame) | 16 ns |
| CRC-16 (256 bytes) | 668 ns |
| PDU decode (ReadHolding) | 39 ns |
| PDU encode (ReadHolding) | 241 ns |
| PDU encode (123 registers) | 1.6 µs |
| PDU encode response (125 registers) | 1.6 µs |
| RTU codec encode (6 bytes) | 112 ns |
| RTU codec decode (8-byte frame) | 243 ns |
| ASCII codec encode (6 bytes) | 177 ns |
| ASCII codec decode (19-byte frame) | 446 ns |

Run on your own hardware — `cargo bench` produces detailed HTML reports under
`target/criterion/`.

## Examples

```bash
# TCP (no hardware needed)
cargo run --example tcp_server
cargo run --example tcp_client
cargo run --example tcp_client_gateway   # TcpConfig: gateway mode, custom MBAP
cargo run --example tcp_client_auto_tid  # Auto-increment transaction ID

# RTU / ASCII (no hardware needed — in-memory duplex)
cargo run --example rtu_server           # RTU server with in-memory transport
cargo run --example generic_transport    # Any AsyncRead+AsyncWrite transport

# Monitoring
cargo run --example bus_monitor          # Bus-level SniffIo monitoring
cargo run --example file_recorder        # Real-time file logging
cargo run --example custom_tap           # Custom WireTap implementation

# Advanced
cargo run --example all_function_codes   # All 11 function codes
cargo run --example custom_service       # Custom Service trait
cargo run --example dynamic_server       # Live-updating register values
cargo run --example reconnect            # TCP auto-reconnect
cargo run --example reconnect_serial     # RTU/ASCII auto-reconnect
cargo run --example bus_timing           # RS-485 frame spacing
cargo run --example server_with_hook     # ServerHook middleware
```

See `examples/` for the full list.

## Architecture

```
src/
├── frame.rs             PDU encode/decode (Request, Response, Exception)
├── error.rs             ModbusError — structured, label + detail + Display
├── client.rs            ModbusClient trait — single required method: call()
├── server.rs            Service trait + ServerHook + HookedService
├── slave_store.rs       In-memory register/coil store (RwLock)
├── bus_timing.rs        Minimum frame spacing for RS-485 buses
├── capture.rs           BusCapture — WireTap impl with recording + stats
├── options.rs           ClientOptions — unified builder for all transports
├── intercept.rs         PacketRecord, PacketData, TrafficStats
├── wire_tap.rs          WireTap trait — passive bus observer (3 hooks)
├── reconnect.rs         ReconnectConfig + is_retryable()
├── codec.rs             CRC-16 + LRC + frame encoding
├── transport/
│   ├── tcp.rs           TcpClient + TcpServer + TcpConfig (MBAP)
│   ├── rtu.rs           RtuClient + RtuServer (generic transport)
│   ├── ascii.rs         AsciiClient + AsciiServer (generic transport)
│   ├── send_recv.rs     Shared drain/reconnect/process helpers
│   └── sniff_io.rs      SniffIo<T> — WireTap attachment + BusTiming
└── monitor/
    ├── mod.rs           Module declarations
    ├── channel.rs       ChannelRecorder + RecordSink trait
    ├── ring_buffer.rs   RingBufferCapture (bounded/unbounded)
    └── file_recorder.rs FileRecorder (ISO 8601, non-blocking)
```

### Data Flow

```
┌──────────┐    Request     ┌──────────────┐    Raw Bytes    ┌──────────┐
│  Client  │ ──────────────► │  send_recv   │ ──────────────► │  SniffIo │
│ (Master) │                 │  (protocol)  │                 │  (proxy) │
│          │ ◄────────────── │              │ ◄────────────── │          │
└──────────┘    Response     └──────────────┘    Raw Bytes    └────┬─────┘
                                                                   │
                                            ┌──────────────────────┤
                                            │  WireTap (passive)   │
                                            │  on_write / on_read  │
                                            │  on_error            │
                                            └──────────┬───────────┘
                                                       │
                                          ┌────────────┴───────────┐
                                          │  BusCapture /          │
                                          │  ChannelRecorder /     │
                                          │  FileRecorder          │
                                          └────────────────────────┘

┌──────────┐    Request     ┌──────────────┐
│  Server  │ ◄────────────── │  Service     │──► SlaveStore
│ (Slave)  │ ──────────────► │  (trait)     │    (default)
└──────────┘    Response     └──────────────┘
```

Core principle: **`WireTap` attaches to `SniffIo<T>` at the physical transport boundary.**
Neither the client nor the server participates in capture — it's a pure passive observer.


## Why OMS Modbus

OMS Modbus was built to support a next-generation industrial diagnostic
IDE with requirements not commonly addressed by Modbus libraries:

- **All three protocols in one API** — TCP, RTU, and ASCII share the same
  client and server patterns. Switch transports without rewriting application logic.
- **Passive bus monitoring** — `WireTap` captures raw bytes at the I/O
  boundary with microsecond timestamps, like a hardware logic analyzer.
  Neither the client nor the server participates in capture.
- **Spec-compliant bus timing** — the Modbus Serial Spec mandates a fixed 1.75 ms
  silent interval above 19200 baud. `BusTiming` implements this explicitly,
  protecting low-performance RS-485 devices from frame collisions.
- **Panic-free production paths** — zero `.unwrap()` in library code.
  Poison-safe mutexes, checked arithmetic, validated PDU sizes.
- **Generic transport** — RTU and ASCII work over any `AsyncRead + AsyncWrite`,
  enabling RTU-over-TCP, in-memory testing, and custom I/O backends.

## Contact & Links

**OrangeHorse Electronic Technology Co., Ltd.** — industrial communication
and diagnostic solutions.

| Channel | Link |
|---------|------|
| 🌐 Website | [orangehorsetech.com](https://orangehorsetech.com) |
| 💼 LinkedIn | [OrangeHorseTech](https://linkedin.com/company/orangehorsetech) |
| 📧 Email | [github@orangehorsetech.com](mailto:github@orangehorsetech.com) |

## License

Licensed under either of [MIT License](LICENSE-MIT) or [Apache License 2.0](LICENSE-APACHE), at your option.
