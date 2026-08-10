// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # RTU Serial Server Example
//!
//! Demonstrates a Modbus RTU server that listens on a serial port and
//! responds to client requests. Uses `SlaveStore` for in-memory register
//! storage with BusTiming for spec-compliant frame spacing.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example rtu_server
//! ```
//!
//! Requires: a serial port (real COM port or virtual null-modem pair).
//! Edit `port_name` and `baud_rate` below to match your setup.
//!
//! ## What this shows
//!
//! - `RtuServer::new()` — create an RTU server from a serial transport
//! - `with_bus_timing()` — enforce minimum frame spacing (3.5T)
//! - `serve_forever()` — process requests in a continuous loop
//! - Pre-populated `SlaveStore` with test data
//! - Recovering from framing errors without crashing

use std::sync::Arc;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ================================================================
    // Configuration — adjust these values for your setup
    // ================================================================

    // ── Serial port ────────────────────────────────────────────────
    let port_name = "COM20"; // Windows: "COM3" / Linux: "/dev/ttyUSB0"
    let baud_rate = 9600; // Baud rate (9600, 19200, 38400, …)

    // ================================================================
    // 1. Open the serial port
    // ================================================================

    println!("═══ OMS Modbus — RTU Serial Server ═══\n");
    println!("Port: {port_name} | Baud: {baud_rate} | Format: 8N1\n");

    println!("[1/4] Opening {port_name} …");
    let port = tokio_serial::SerialStream::open(
        &tokio_serial::new(port_name, baud_rate)
            .data_bits(tokio_serial::DataBits::Eight)
            .parity(tokio_serial::Parity::None)
            .stop_bits(tokio_serial::StopBits::One)
            .flow_control(tokio_serial::FlowControl::None),
    )
    .map_err(|e| {
        format!(
            "Cannot open {port_name}: {e}\n\
             Check: ① port exists  ② not locked by another program"
        )
    })?;
    println!("   ✓ Port opened (8N1, no flow control)\n");

    // ================================================================
    // 2. Configure bus timing
    // ================================================================

    println!("[2/4] Configuring bus timing …");
    let bus_timing = Arc::new(BusTiming::rtu_35t(baud_rate)); // 3.5T silence @ 9600 ≈ 3.65 ms
    println!(
        "   ✓ 3.5T ≈ {:.2} ms @ {baud_rate} baud\n",
        bus_timing.min_spacing().as_secs_f64() * 1000.0,
    );

    // ================================================================
    // 3. Prepare the register store
    // ================================================================

    println!("[3/4] Initializing SlaveStore …");

    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 1234), // Temperature ×10  (12.34 °C)
        (1, 5678), // Humidity ×10     (56.78 %)
        (2, 100),  // Pressure         (100 kPa)
        (3, 42),   // Status
        (10, 500), // Setpoint 1
        (11, 600), // Setpoint 2
        (20, 0),   // Counter — writable by client
    ]));

    store.write_input_register(0, 2400); // Analog input 0  (24.00 V)
    store.write_input_register(1, 150); // Analog input 1  (1.50 A)

    store.write_coil(0, true); // Output 0  ON
    store.write_coil(1, false); // Output 1  OFF
    store.write_coil(2, true); // Output 2  ON

    store.write_discrete_input(0, true); // DI 0 — sensor active
    store.write_discrete_input(1, false); // DI 1 — sensor idle

    println!("   ✓ Store ready: 7 holding regs, 2 input regs, 3 coils, 2 DIs\n");

    // ================================================================
    // 4. Create server and serve forever
    // ================================================================

    println!("[4/4] Creating RTU server …");

    // RtuServer takes ownership of the serial port.
    // bus_timing is shared with the server's internal frame timer.
    let server = rtu::RtuServer::new(port).with_bus_timing(bus_timing);

    println!("   ✓ Server ready on {port_name}\n");
    println!("─── Register Map ───");
    println!("  Holding[ 0.. 3] = [1234, 5678, 100, 42]");
    println!("  Holding[10..11] = [500, 600]  (setpoints)");
    println!("  Holding[20]     = 0           (counter)");
    println!("  Input[0..1]     = [2400, 150] (analog inputs)");
    println!("  Coils[0..2]     = ON, OFF, ON");
    println!("  DI[0..1]        = ON, OFF\n");
    println!("Serving … (press Ctrl+C to stop)\n");

    // `serve_forever` blocks indefinitely, processing one request at a time.
    // Modbus RTU is half-duplex — only one master talks at a time on RS-485.
    // The server recovers from framing errors (CRC mismatch, garbage bytes)
    // without crashing — invalid frames are silently discarded.
    server.serve_forever(store).await?;

    Ok(())
}
