// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # TCP Client Example (Gateway Mode)
//!
//! Demonstrates a TCP client configured for TCP-to-RTU gateway mode:
//! auto-increment TID + Unit ID repeated in the PDU body. This is the
//! standard configuration for talking through a Modbus TCP/RTU gateway.
//! Uses BusCapture for in-memory packet capture — compare the MBAP
//! headers with the standard and auto-TID examples.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example tcp_client_gateway
//! ```
//!
//! Requires: Modbus TCP device or gateway at the configured address.

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ================================================================
    // Configuration — adjust these values for your device and setup
    // ================================================================

    // ── TCP connection ─────────────────────────────────────────────
    let host = "127.0.0.1"; // Server IP or hostname
    let port: u16 = 502; // Modbus TCP port (default: 502)

    // ── Device ─────────────────────────────────────────────────────
    let slave_id: u8 = 1; // Slave / Unit ID (1–247)
    let start_addr: u16 = 0; // Starting address
    let quantity: u16 = 10; // Number of items (addresses 0–9)
    let loop_count: usize = 3; // How many times to repeat the read loop

    // ── Timeout ────────────────────────────────────────────────────
    let timeout = Duration::from_secs(3); // Per-request timeout

    // ================================================================
    // Setup
    // ================================================================

    println!("═══ OMS Modbus — TCP Client (Gateway Mode) ═══\n");
    println!("Protocol: TCP — MBAP header + PDU");
    println!("MBAP config: auto TID + UID in body (TCP-to-RTU gateway)");
    println!("Host: {host}:{port} | Unit ID: {slave_id}");
    println!("Start addr: {start_addr} | Quantity: {quantity}");
    println!("Loops: {loop_count} | Timeout: {} s\n", timeout.as_secs());

    // 1. Parse address
    println!("[1/3] Building client options …");
    let addr: std::net::SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| format!("Invalid address {host}:{port}: {e}"))?;
    let cap = Arc::new(BusCapture::unbounded()); // Never drops a record
    let opts = ClientOptions::default()
        .with_timeout(timeout)
        .with_tap(cap.clone());
    println!(
        "   ✓ BusCapture unbounded | Timeout: {} s\n",
        timeout.as_secs()
    );

    // 2. Connect to server with gateway config
    println!("[2/3] Connecting to {addr} …");
    let client = tcp::with_options(addr, opts)
        .await
        .map_err(|e| {
            format!(
                "Cannot connect to {addr}: {e}\n\
                             Check: ① server is running  ② firewall allows port {port}"
            )
        })?
        .with_config(tcp::TcpConfig::gateway());
    println!("   ✓ Connected (gateway mode)\n");

    // ================================================================
    // 3. Read loop — each iteration fires four function codes
    // ================================================================
    println!("[3/3] Starting read loop …\n");

    for round in 1..=loop_count {
        println!("─── Round {round}/{loop_count} ───");

        // FC01 — Read Coils
        match client.read_coils(slave_id, start_addr, quantity).await {
            Ok(bits) => {
                let on = bits.iter().filter(|&&b| b).count();
                println!(
                    "  FC01 Coils       [{start_addr}..{}] → ON:{on} / OFF:{}",
                    start_addr + quantity - 1,
                    quantity as usize - on,
                );
            }
            Err(e) => println!("  FC01 Coils       ✗ {e}"),
        }

        // FC02 — Read Discrete Inputs
        match client
            .read_discrete_inputs(slave_id, start_addr, quantity)
            .await
        {
            Ok(bits) => {
                let on = bits.iter().filter(|&&b| b).count();
                println!(
                    "  FC02 Discrete In  [{start_addr}..{}] → ON:{on} / OFF:{}",
                    start_addr + quantity - 1,
                    quantity as usize - on,
                );
            }
            Err(e) => println!("  FC02 Discrete In  ✗ {e}"),
        }

        // FC03 — Read Holding Registers
        match client
            .read_holding_registers(slave_id, start_addr, quantity)
            .await
        {
            Ok(regs) => println!(
                "  FC03 Holding Reg [{start_addr}..{}] → {regs:?}",
                start_addr + quantity - 1,
            ),
            Err(e) => println!("  FC03 Holding Reg ✗ {e}"),
        }

        // FC04 — Read Input Registers
        match client
            .read_input_registers(slave_id, start_addr, quantity)
            .await
        {
            Ok(regs) => println!(
                "  FC04 Input Reg   [{start_addr}..{}] → {regs:?}",
                start_addr + quantity - 1,
            ),
            Err(e) => println!("  FC04 Input Reg   ✗ {e}"),
        }

        println!(); // blank line between rounds
    }

    // ================================================================
    // Capture summary
    // ================================================================
    println!("═══ Capture Stats ═══");
    println!(
        "Requests: {}  Responses: {}  Errors: {}  Dropped: {}",
        cap.count_requests(),
        cap.count_responses(),
        cap.count_errors(),
        cap.dropped(),
    );

    let packets = cap.drain();
    if !packets.is_empty() {
        println!("\n─── Capture Records ({} total) ───", packets.len());
        for pkt in &packets {
            println!("  {pkt}");
        }
    }

    println!("\nDone.");
    Ok(())
}
