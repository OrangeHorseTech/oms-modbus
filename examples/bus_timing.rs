// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Bus Timing — minimum frame spacing for RS-485 buses
//!
//! On multi-drop RS-485 buses, sending Modbus frames too quickly can crash
//! low-performance slave devices. `BusTiming` enforces a minimum silence
//! period between frames to prevent this.
//!
//! Two modes:
//! 1. `BusTiming::rtu_35t(baud)` — standard 3.5 character times (RTU/ASCII)
//! 2. `BusTiming::custom(duration)` — fixed delay for slow devices
//!
//! Self-contained: uses in-memory TCP, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example bus_timing
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Bus Timing Demo ═══\n");

    // ================================================================
    // Setup: local TCP server
    // ================================================================

    println!("[1/4] Starting server …");

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("   ✓ Server on {bind_addr}\n");

    // ================================================================
    // Mode 1: Without bus timing (default)
    // ================================================================

    println!("[2/4] Without BusTiming — baseline speed");
    {
        let client = tcp::TcpClient::connect(bind_addr).await?;
        let start = tokio::time::Instant::now();
        for _ in 0..10 {
            client.read_holding_registers(1, 0, 1).await?;
        }
        let elapsed = start.elapsed();
        println!("   10 reads: {elapsed:?}");
        println!("   → No enforced gap — may crash slow RS-485 devices\n");
    }

    // ================================================================
    // Mode 2: RTU 3.5T spacing (standard for RS-485)
    // ================================================================

    println!("[3/4] BusTiming::rtu_35t(9600) — standard RS-485 spacing");
    {
        let timing = BusTiming::rtu_35t(9600);
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_bus_timing(timing);
        let client = tcp::with_options(bind_addr, opts).await?;

        let start = tokio::time::Instant::now();
        for _ in 0..10 {
            client.read_holding_registers(1, 0, 1).await?;
        }
        let elapsed = start.elapsed();
        println!("   10 reads: {elapsed:?}");
        println!("   Expected minimum: ~40ms (3.5T ≈ 4.01ms @ 9600 × 10)\n");
    }

    // ================================================================
    // Mode 3: Custom spacing (for very slow devices)
    // ================================================================

    println!("[4/4] BusTiming::custom(50ms) — fixed delay for slow devices");
    {
        let timing = BusTiming::custom(Duration::from_millis(50));
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_bus_timing(timing);
        let client = tcp::with_options(bind_addr, opts).await?;

        let start = tokio::time::Instant::now();
        for _ in 0..5 {
            client.read_holding_registers(1, 0, 1).await?;
        }
        let elapsed = start.elapsed();
        println!("   5 reads: {elapsed:?}");
        println!("   Expected minimum: ~250ms (50ms × 5)\n");
    }

    // ================================================================
    // RTU 3.5T spacing reference table
    // ================================================================

    println!("─── RTU 3.5T Spacing Reference ───");
    println!("  | Baud   | 3.5T spacing |");
    println!("  |--------|-------------|");
    for &baud in &[9600, 19200, 38400, 57600, 115200] {
        let t = BusTiming::rtu_35t(baud);
        println!("  | {:>6} | {:?} |", baud, t.min_spacing());
    }

    println!("\n═══ Summary ═══");
    println!("  ✓ Without timing   — fastest, may crash RS-485 slaves");
    println!("  ✓ rtu_35t(9600)    — safe default for RS-485");
    println!("  ✓ custom(50ms)     — for unusually slow devices");
    Ok(())
}
