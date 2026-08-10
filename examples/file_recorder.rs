// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # File Recorder — capture Modbus traffic to disk
//!
//! Demonstrates capture backends for persistent traffic recording:
//!
//! | Backend | Behavior | When to use |
//! |---------|----------|-------------|
//! | `BusCapture::bounded(N)` | In-memory ring buffer | Short sessions, drain at end |
//! | `FileRecorder` | Streams to file via dedicated task | Long sessions, real-time persistence |
//! | `ChannelRecorder` | Adapter: WireTap → channel → sink | Bridge custom sinks to BusCapture |
//!
//! Self-contained: uses in-memory TCP, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example file_recorder
//! ```
//!
//! Output files:
//! - `capture_ring.log` — ring buffer dump (drained at end)
//! - `capture_file.log` — real-time stream from FileRecorder

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::monitor::{ChannelRecorder, FileRecorder};
use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — File Recorder Demo ═══\n");

    // ================================================================
    // Setup: local TCP server
    // ================================================================

    println!("[1/4] Starting server …");

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 1234),
        (1, 5678),
        (2, 100),
    ]));
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("   ✓ Server on {bind_addr}\n");

    // ================================================================
    // 1. Ring Buffer — in-memory, drain at end
    // ================================================================

    println!("[2/4] BusCapture::bounded(100) — ring buffer in memory");
    let ring = Arc::new(BusCapture::bounded(100));
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(ring.clone());
    let client = tcp::with_options(bind_addr, opts).await?;

    for i in 0..5 {
        client.read_holding_registers(1, 0, 3).await?;
        client
            .write_single_register(1, 0, (i * 1000) as u16)
            .await?;
    }
    println!("   Captured {} records (capacity: 100)", ring.len());
    println!("   Dropped: {}\n", ring.dropped());

    // ================================================================
    // 2. FileRecorder — real-time file output
    // ================================================================

    println!("[3/4] FileRecorder — real-time streaming to disk");
    let recorder = FileRecorder::new("capture_file.log")?;
    let (tap, _handle) = ChannelRecorder::spawn(recorder);

    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(Arc::new(tap));
    let client = tcp::with_options(bind_addr, opts).await?;

    for i in 0..5 {
        client.read_holding_registers(1, 0, 3).await?;
        client
            .write_single_register(1, 0, (i * 2000) as u16)
            .await?;
    }
    println!("   Wrote to capture_file.log (real-time)\n");

    // ================================================================
    // 4. Drain ring buffer to file
    // ================================================================

    println!("[4/4] Draining ring buffer to capture_ring.log …");
    let packets = ring.drain();
    if !packets.is_empty() {
        use std::io::Write;
        let mut f = std::fs::File::create("capture_ring.log")?;
        for pkt in &packets {
            let ts_ms = pkt.timestamp_us as f64 / 1000.0;
            let line = match &pkt.data {
                PacketData::RawTx(b) => {
                    format!("-> {:>12.3}ms TX {}B {:02X?}\n", ts_ms, b.len(), b)
                }
                PacketData::RawRx(b) => {
                    format!("<- {:>12.3}ms RX {}B {:02X?}\n", ts_ms, b.len(), b)
                }
                PacketData::RawError(b, e) => {
                    format!("!! {:>12.3}ms ERR {}B — {}\n", ts_ms, b.len(), e)
                }
                _ => continue,
            };
            f.write_all(line.as_bytes())?;
        }
        println!("   Dumped {} records to capture_ring.log\n", packets.len());
    }

    // ================================================================
    // Summary
    // ================================================================

    println!("═══ Summary ═══");
    println!("  Output files:");
    println!(
        "    capture_ring.log  — ring buffer dump ({packets} records)",
        packets = packets.len()
    );
    println!("    capture_file.log  — real-time stream from FileRecorder");
    println!();
    println!("  ✓ BusCapture::bounded(100)  — ring buffer, constant memory");
    println!("  ✓ FileRecorder              — dedicated task, non-blocking I/O");
    println!("  ✓ ChannelRecorder           — bridge any sink to WireTap");
    Ok(())
}
