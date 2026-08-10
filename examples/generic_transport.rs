// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Generic Transport — RTU/ASCII over any byte stream
//!
//! `RtuClient` and `AsciiClient` are generic over `T: AsyncRead + AsyncWrite`.
//! This means ANY byte transport works — TCP, serial ports, Unix sockets,
//! pipes, in-memory channels, or your own custom I/O.
//!
//! Demonstrates RTU and ASCII protocols over TCP and DuplexStream, with
//! BusCapture monitoring. Self-contained: no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example generic_transport
//! ```
//!
//! ## Transport Matrix
//!
//! | Transport | RTU | ASCII | Notes |
//! |-----------|-----|-------|-------|
//! | Serial port | `RtuClient::with_timeout(port, timeout)` | Same | Real hardware |
//! | TCP | `RtuClient::with_timeout(TcpStream, timeout)` | Same | RTU/ASCII-over-Ethernet |
//! | DuplexStream | `RtuClient::with_timeout(duplex, timeout)` | Same | In-memory testing |
//! | Unix socket | `RtuClient::with_timeout(unix, timeout)` | Same | Local IPC |

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Generic Transport Demo ═══\n");

    // ================================================================
    // 1. RTU over TCP — real TCP connection, RTU framing
    // ================================================================

    println!("[1/4] RTU over TCP — RTU frames on a real TCP connection");
    {
        // Start RTU server on a TCP listener
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let bind_addr = listener.local_addr()?;

        let store = Arc::new(SlaveStore::with_holding_registers(&[
            (0, 1234),
            (1, 5678),
            (2, 100),
        ]));
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            rtu::RtuServer::new(stream).serve_forever(store).await.ok();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let cap = Arc::new(BusCapture::unbounded());
        let stream = tokio::net::TcpStream::connect(bind_addr).await?;
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_tap(cap.clone());
        let client = rtu::with_options(stream, opts);

        let regs = client.read_holding_registers(1, 0, 3).await?;
        println!("   FC03 → {regs:?}");
        println!(
            "   Capture: {} req, {} rsp",
            cap.count_requests(),
            cap.count_responses()
        );
        println!("   → RtuClient on a TcpStream — no serial port needed\n");
    }

    // ================================================================
    // 2. ASCII over TCP — real TCP connection, ASCII framing
    // ================================================================

    println!("[2/4] ASCII over TCP — ASCII frames on a real TCP connection");
    {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let bind_addr = listener.local_addr()?;

        let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1234), (1, 5678)]));
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            ascii::AsciiServer::new(stream)
                .serve_forever(store)
                .await
                .ok();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let cap = Arc::new(BusCapture::unbounded());
        let stream = tokio::net::TcpStream::connect(bind_addr).await?;
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_tap(cap.clone());
        let client = ascii::with_options(stream, opts);

        let regs = client.read_holding_registers(1, 0, 2).await?;
        println!("   FC03 → {regs:?}");
        println!(
            "   Capture: {} req, {} rsp",
            cap.count_requests(),
            cap.count_responses()
        );
        println!("   → AsciiClient on a TcpStream — ASCII-over-Ethernet is common in industry\n");
    }

    // ================================================================
    // 3. RTU over DuplexStream — in-memory testing
    // ================================================================

    println!("[3/4] RTU over DuplexStream — zero-setup in-memory test");
    {
        let (client_stream, server_stream) = tokio::io::duplex(1024);
        let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42), (1, 99)]));
        let server = rtu::RtuServer::new(server_stream);
        tokio::spawn(async move {
            server.serve_forever(store).await.ok();
        });

        let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));
        let regs = client.read_holding_registers(1, 0, 2).await?;
        println!("   FC03 → {regs:?}");
        println!("   → DuplexStream is ideal for unit tests and CI\n");
    }

    // ================================================================
    // 4. Protocol comparison — ASCII frame formatting
    // ================================================================

    println!("[4/4] Protocol overhead comparison");
    {
        let (client_stream, server_stream) = tokio::io::duplex(1024);
        let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
        let server = ascii::AsciiServer::new(server_stream);
        tokio::spawn(async move {
            server.serve_forever(store).await.ok();
        });

        let cap = Arc::new(BusCapture::unbounded());
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_tap(cap.clone());
        let client = ascii::with_options(client_stream, opts);
        client.read_holding_registers(1, 0, 1).await?;

        println!("   ASCII capture (raw hex text):");
        for pkt in cap.drain() {
            println!("     {pkt}");
        }
    }

    // ================================================================
    // Summary
    // ================================================================

    println!("\n═══ Summary ═══");
    println!("  ┌──────────┬───────────┬────────────────────┐");
    println!("  │ Protocol │ Overhead  │ 6B PDU frame        │");
    println!("  ├──────────┼───────────┼────────────────────┤");
    println!("  │ TCP      │ 7B MBAP   │ 13 bytes            │");
    println!("  │ RTU      │ 2B CRC    │ 8 bytes             │");
    println!("  │ ASCII    │ 2× hex    │ ~19 bytes           │");
    println!("  └──────────┴───────────┴────────────────────┘");
    println!();
    println!("Key point: OMS Modbus doesn't care about the transport layer.");
    println!("Serial, TCP, Unix sockets, pipes, in-memory channels,");
    println!("custom I/O — they all work the same way.");
    Ok(())
}
