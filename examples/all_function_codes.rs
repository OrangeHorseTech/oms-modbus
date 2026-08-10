// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # All Modbus Function Codes — complete FC reference
//!
//! Demonstrates every standard Modbus function code supported by OMS Modbus:
//!
//! | FC | Name | Operation |
//! |----|------|-----------|
//! | 01 | Read Coils | Read digital outputs |
//! | 02 | Read Discrete Inputs | Read digital inputs |
//! | 03 | Read Holding Registers | Read analog outputs |
//! | 04 | Read Input Registers | Read analog inputs |
//! | 05 | Write Single Coil | Set one digital output |
//! | 06 | Write Single Register | Set one analog output |
//! | 15 | Write Multiple Coils | Set multiple digital outputs |
//! | 16 | Write Multiple Registers | Set multiple analog outputs |
//! | 22 | Mask Write Register | Atomic AND/OR mask |
//! | 23 | Read/Write Multiple Registers | Atomic read + write |
//!
//! Self-contained: starts its own TCP server, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example all_function_codes
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — All Function Codes Demo ═══\n");

    // ================================================================
    // Setup: TCP server with pre-populated data
    // ================================================================

    println!("[1/2] Starting TCP server …");

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;

    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 100),   // Holding[0] — for write tests
        (1, 200),   // Holding[1]
        (10, 500),  // Holding[10] — for mask write test
        (20, 1000), // Holding[20] — for read/write multiple test
        (21, 2000),
        (22, 3000),
    ]));
    store.write_coil(0, true);
    store.write_coil(1, false);
    store.write_coil(2, true);

    let svc = store.clone();
    tokio::spawn(async move {
        server.serve_forever(svc).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let cap = Arc::new(BusCapture::unbounded());
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(3))
        .with_tap(cap.clone());
    let client = tcp::with_options(bind_addr, opts).await?;
    println!("   ✓ Server on {bind_addr}\n");

    // ================================================================
    // All function codes
    // ================================================================

    println!("[2/2] Demonstrating all function codes …\n");

    // ── FC 01: Read Coils ──────────────────────────────────────────────
    println!("─── FC01 Read Coils ───");
    let coils = client.read_coils(1, 0, 8).await?;
    println!("  Coils[0..8] = {coils:?}");

    // ── FC 02: Read Discrete Inputs ────────────────────────────────────
    println!("\n─── FC02 Read Discrete Inputs ───");
    let inputs = client.read_discrete_inputs(1, 0, 4).await?;
    println!("  DiscreteInputs[0..4] = {inputs:?}");

    // ── FC 03: Read Holding Registers ──────────────────────────────────
    println!("\n─── FC03 Read Holding Registers ───");
    let regs = client.read_holding_registers(1, 0, 3).await?;
    println!("  Holding[0..3] = {regs:?}");

    // ── FC 04: Read Input Registers ────────────────────────────────────
    println!("\n─── FC04 Read Input Registers ───");
    let regs = client.read_input_registers(1, 0, 2).await?;
    println!("  InputReg[0..2] = {regs:?}");

    // ── FC 05: Write Single Coil ───────────────────────────────────────
    println!("\n─── FC05 Write Single Coil ───");
    println!("  Before: Coil[1] = {}", coils[1]);
    client.write_single_coil(1, 1, true).await?;
    let coils = client.read_coils(1, 0, 3).await?;
    println!("  After:  Coil[1] = {}", coils[1]);

    // ── FC 06: Write Single Register ───────────────────────────────────
    println!("\n─── FC06 Write Single Register ───");
    client.write_single_register(1, 0, 7777).await?;
    let regs = client.read_holding_registers(1, 0, 1).await?;
    println!("  Holding[0] = {} (was 100, now 7777)", regs[0]);

    // ── FC 15: Write Multiple Coils ────────────────────────────────────
    println!("\n─── FC15 Write Multiple Coils ───");
    client
        .write_multiple_coils(1, 0, &[true, true, false, false])
        .await?;
    let coils = client.read_coils(1, 0, 4).await?;
    println!("  Coils[0..4] = {coils:?}");

    // ── FC 16: Write Multiple Registers ────────────────────────────────
    println!("\n─── FC16 Write Multiple Registers ───");
    client
        .write_multiple_registers(1, 0, &[111, 222, 333])
        .await?;
    let regs = client.read_holding_registers(1, 0, 3).await?;
    println!("  Holding[0..3] = {regs:?}");

    // ── FC 22: Mask Write Register ─────────────────────────────────────
    println!("\n─── FC22 Mask Write Register ───");
    println!(
        "  Before: Holding[10] = {}",
        store.read_holding_register(10)
    );
    client.mask_write_register(1, 10, 0x00FF, 0xFF00).await?;
    let regs = client.read_holding_registers(1, 10, 1).await?;
    println!(
        "  After:  Holding[10] = {:#06X} (AND=0x00FF, OR=0xFF00)",
        regs[0]
    );

    // ── FC 23: Read/Write Multiple Registers ───────────────────────────
    println!("\n─── FC23 Read/Write Multiple Registers ───");
    let read_back = client
        .read_write_multiple_registers(1, 20, 4, 20, &[100, 200, 300])
        .await?;
    println!("  Old values [20..23] = {read_back:?}");
    let regs = client.read_holding_registers(1, 20, 4).await?;
    println!("  New values [20..23] = {regs:?}");

    // ================================================================
    // Capture summary
    // ================================================================

    println!("\n═══ Capture Summary ═══");
    println!("  Requests:  {}", cap.count_requests());
    println!("  Responses: {}", cap.count_responses());
    println!("  Errors:    {}", cap.count_errors());

    println!("\n═══ Summary ═══");
    println!("  ✓ All 10 standard function codes demonstrated");
    println!(
        "  ✓ BusCapture monitoring: {}/{} success rate",
        cap.count_responses(),
        cap.count_requests()
    );
    Ok(())
}
