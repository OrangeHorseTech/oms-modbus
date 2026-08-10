// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Custom WireTap — implement your own bus-level monitor
//!
//! Demonstrates how to implement the `WireTap` trait for custom monitoring:
//! count bytes transferred, log only errors, attach to any transport client
//! via `ClientOptions::with_tap()`.
//!
//! Self-contained: uses in-memory TCP, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example custom_tap
//! ```
//!
//! ## When to use
//!
//! | Approach | Use case |
//! |----------|----------|
//! | `BusCapture` | Built-in: stats + packet records, drain for analysis |
//! | Custom `WireTap` | Specialized: stream to external system, custom aggregation |
//!
//! All `WireTap` methods have default no-op implementations — implement
//! only the hooks you need: `on_write()`, `on_read()`, `on_error()`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

// ── Custom WireTap: counts bytes + logs only errors ────────────────────────

/// A custom tap that tracks byte counts per direction and logs only errors.
/// Useful when you need lightweight monitoring without storing every packet.
struct ByteCounter {
    tx_bytes: AtomicU64,
    rx_bytes: AtomicU64,
    error_count: AtomicU64,
}

impl ByteCounter {
    fn new() -> Self {
        Self {
            tx_bytes: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
        }
    }

    fn tx(&self) -> u64 {
        self.tx_bytes.load(Ordering::Relaxed)
    }
    fn rx(&self) -> u64 {
        self.rx_bytes.load(Ordering::Relaxed)
    }
    fn errors(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
}

impl WireTap for ByteCounter {
    fn on_write(&self, bytes: &[u8], _ts: u64) {
        self.tx_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }

    fn on_read(&self, bytes: &[u8], _ts: u64) {
        self.rx_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }

    fn on_error(&self, bytes: &[u8], error: &str, _ts: u64) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        // Log only errors — skip normal traffic to keep output clean
        eprintln!("  WireTap error ({} bytes): {error}", bytes.len());
    }
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Custom WireTap Demo ═══\n");

    // ================================================================
    // Setup: local TCP server
    // ================================================================

    println!("[1/3] Starting server …");

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 100)]));
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("   ✓ Server on {bind_addr}\n");

    // ================================================================
    // Attach custom WireTap via ClientOptions
    // ================================================================

    println!("[2/3] Connecting client with ByteCounter tap …");

    let tap = Arc::new(ByteCounter::new());
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(tap.clone());
    let client = tcp::with_options(bind_addr, opts).await?;
    println!("   ✓ Client connected\n");

    // ================================================================
    // Run traffic — success frames are silent, only errors logged
    // ================================================================

    println!("[3/3] Running Modbus traffic …\n");

    // Normal reads/writes — no errors, no log output from WireTap
    client.read_holding_registers(1, 0, 1).await?;
    client.write_single_register(1, 0, 42).await?;
    println!("─── Results ───");
    println!("  TX bytes transmitted: {}", tap.tx());
    println!("  RX bytes received:    {}", tap.rx());
    println!("  Errors captured:      {}", tap.errors());
    println!("  (No error output above — successful frames are NOT logged)\n");

    // Trigger an error: read from non-existent slave
    println!("─── Triggering error (slave=247) ───");
    let _ = client.read_holding_registers(247, 0, 1).await;
    println!("  Errors captured: {}  ← incremented\n", tap.errors());

    println!("═══ Summary ═══");
    println!("  ✓ ByteCounter::on_write  — count every transmitted byte");
    println!("  ✓ ByteCounter::on_read   — count every received byte");
    println!("  ✓ ByteCounter::on_error  — log only failures, not successes");
    println!();
    println!("Key point: WireTap hooks are driven by SniffIo at the I/O boundary.");
    println!("Only implement the hooks you need — default no-ops cover the rest.");
    Ok(())
}
