// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Bus Monitor Example
//!
//! Demonstrates passive bus-level monitoring using `BusCapture`. Shows how to
//! capture raw Modbus traffic, analyze it by packet type, and extract
//! statistics — all without modifying the client or server logic.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example bus_monitor
//! ```
//!
//! Self-contained: starts an in-memory TCP server, runs traffic, then
//! prints the capture report. No hardware required.
//!
//! ## Monitoring modes
//!
//! | Mode | Constructor | Behavior |
//! |------|------------|----------|
//! | Stats only | `BusCapture::stats_only()` | Counts requests/errors, no records |
//! | Unbounded | `BusCapture::unbounded()` | Records every frame, never drops |
//! | Bounded | `BusCapture::bounded(1000)` | Ring buffer, evicts oldest when full |
//!
//! ## What this shows
//!
//! - `BusCapture` — the unified capture backend (stats + records, implements `WireTap`)
//! - `ClientOptions::with_tap()` — attaching capture to any transport client
//! - `drain()` — consuming captured records
//! - `count_*()` — lightweight statistics (no records consumed)
//! - Packet analysis: filtering by direction, function code, errors

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Bus Monitor ═══\n");

    // ================================================================
    // 1. Start a local TCP server (target to monitor)
    // ================================================================

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;

    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 100),
        (1, 200),
        (2, 300),
        (5, 500),
    ]));
    store.write_coil(0, true);
    store.write_coil(1, false);

    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("[1/4] TCP server started on {bind_addr}");

    // ================================================================
    // 2. Create BusCapture — the monitoring backend
    // ================================================================

    // BusCapture::unbounded() — records every frame, never drops.
    // Use bounded(N) for production (ring buffer, constant memory).
    // Use stats_only() when you only need counters, not packet data.
    let cap = Arc::new(BusCapture::unbounded());

    println!("[2/4] BusCapture created (unbounded mode)");

    // ================================================================
    // 3. Attach capture to a client via ClientOptions
    // ================================================================

    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(cap.clone());

    let client = tcp::with_options(bind_addr, opts).await?;

    println!("[3/4] Client connected with capture attached\n");

    // ================================================================
    // 4. Generate traffic — mix of reads, writes, success, and error
    // ================================================================

    println!("[4/4] Generating Modbus traffic …\n");

    // ── Successful reads ────────────────────────────────────────────
    println!("─── Reads ───");
    let regs = client.read_holding_registers(1, 0, 3).await?;
    println!("  FC03 Holding[0..2] = {regs:?}");

    let bits = client.read_coils(1, 0, 4).await?;
    println!("  FC01 Coils[0..3]   = {bits:?}");

    client.read_discrete_inputs(1, 0, 2).await?;
    println!("  FC02 DiscreteIn    OK");

    client.read_input_registers(1, 0, 1).await?;
    println!("  FC04 InputReg      OK\n");

    // ── Successful writes ───────────────────────────────────────────
    println!("─── Writes ───");
    client.write_single_register(1, 0, 7777).await?;
    println!("  FC06 WriteSingle   OK");

    client
        .write_multiple_registers(1, 10, &[111, 222, 333])
        .await?;
    println!("  FC16 WriteMulti     OK");

    client.write_single_coil(1, 0, false).await?;
    println!("  FC05 WriteCoil      OK\n");

    // ── Errors (captured but not retried) ───────────────────────────
    println!("─── Errors (these appear in capture) ───");
    // Reading from a non-existent slave — timeout (captured as error)
    let err = client.read_holding_registers(247, 0, 1).await;
    match err {
        Err(e) => println!("  FC03 slave=247  → ✗ {e}"),
        Ok(_) => println!("  FC03 slave=247  → (unexpected success)"),
    }

    // Out-of-range address (depends on server implementation)
    let err = client.read_holding_registers(1, 60000, 1).await;
    match err {
        Err(e) => println!("  FC03 addr=60000 → ✗ {e}\n"),
        Ok(regs) => println!("  FC03 addr=60000 → {regs:?} (server returned zeros)\n"),
    }

    // ================================================================
    // 5. Statistics — lightweight counters (no records consumed)
    // ================================================================

    println!("═══ Capture Statistics ═══");
    println!(" Requests:  {}", cap.count_requests());
    println!(" Responses: {}", cap.count_responses());
    println!(" Errors:    {}", cap.count_errors());
    println!(" Dropped:   {}  (zero = no records lost)", cap.dropped());
    println!();

    // ================================================================
    // 6. Packet analysis — drain and categorize records
    // ================================================================

    let packets = cap.drain();
    println!("─── Packet Analysis ({:>3} records) ───", packets.len());

    // Count by direction
    let tx_count = packets
        .iter()
        .filter(|p| matches!(p.data, PacketData::RawTx(_)))
        .count();
    let rx_count = packets
        .iter()
        .filter(|p| matches!(p.data, PacketData::RawRx(_)))
        .count();
    let err_count = packets
        .iter()
        .filter(|p| matches!(p.data, PacketData::RawError(..)))
        .count();
    println!("  TX (requests):  {tx_count}");
    println!("  RX (responses): {rx_count}");
    if err_count > 0 {
        println!("  Errors:         {err_count}");
    }

    // ================================================================
    // 7. Print all captured records
    // ================================================================

    println!("\n─── Capture Dump ───");
    for pkt in &packets {
        let prefix = match &pkt.data {
            PacketData::RawTx(_) => "[TX]",
            PacketData::RawRx(_) => "[RX]",
            PacketData::RawError(..) => "[ERR]",
            _ => "[???]",
        };
        println!("  {pkt}  {prefix}");
    }

    // ================================================================
    // 8. Summary
    // ================================================================

    println!("\n═══ Monitor Summary ═══");
    println!("  BusCapture records: {}", packets.len());
    println!(
        "  Success rate:       {}/{}",
        cap.count_responses(),
        cap.count_requests()
    );
    println!();
    println!("Integrate with any transport:");
    println!("  let cap = Arc::new(BusCapture::unbounded());");
    println!("  let opts = ClientOptions::default().with_tap(cap.clone());");
    println!("  let client = tcp::with_options(addr, opts).await?;");
    println!("  // ... run traffic ...");
    println!("  for pkt in cap.drain() {{ println!(\"{{pkt}}\"); }}");

    Ok(())
}
