// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # TCP Auto-Reconnect — transparent connection recovery
//!
//! Demonstrates `TcpClient::with_reconnect()` for transparent reconnection on
//! connection loss. The client retries failed requests with a fresh TCP
//! connection — callers use the same `read_holding_registers()` API and
//! don't need to handle reconnection logic.
//!
//! Self-contained: uses in-memory TCP, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example reconnect
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — TCP Reconnect Demo ═══\n");

    // ================================================================
    // Setup: local TCP server
    // ================================================================

    println!("[1/2] Starting server …");

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
    // Connect with reconnect enabled
    // ================================================================

    println!("[2/2] Connecting with auto-reconnect …");

    // max_retries=0 → infinite retries. 3 → at most 3 attempts.
    let client = tcp::TcpClient::connect(bind_addr)
        .await?
        .with_reconnect(0, Duration::from_millis(100)); // infinite

    println!("   ✓ Connected with reconnect (infinite retries, 100ms backoff)");
    println!("   Polling register 0 every 500ms. Kill server to test reconnection.\n");

    // ================================================================
    // Poll loop
    // ================================================================

    let mut i = 0u64;
    loop {
        i += 1;
        match client.read_holding_registers(1, 0, 1).await {
            Ok(regs) => {
                if i % 10 == 1 || i <= 3 {
                    println!("  [{i:>4}] ✓ Holding[0] = {:5}", regs[0]);
                }
            }
            Err(e) => eprintln!("  [{i:>4}] ✗ {e}"),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
