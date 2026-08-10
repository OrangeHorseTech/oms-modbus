// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # TCP Server Example
//!
//! Demonstrates a Modbus TCP server with pre-populated register data.
//! Uses `SlaveStore` for in-memory storage and `serve_forever()` for
//! concurrent multi-client support.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example tcp_server
//! ```
//!
//! Then connect with any Modbus TCP client (or `tcp_client` example).
//! Press Ctrl+C to stop.
//!
//! ## What this shows
//!
//! - `TcpServer::bind()` — bind to a local address
//! - `SlaveStore` — pre-populated coils + holding/input registers
//! - `serve_forever()` — concurrent multi-client accept loop
//! - All 13 standard Modbus function codes supported

use std::net::SocketAddr;
use std::sync::Arc;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ================================================================
    // Configuration
    // ================================================================

    let host = "127.0.0.1"; // Listen address
    let port: u16 = 502; // Modbus TCP port (0 = OS picks a free port)

    // ================================================================
    // 1. Bind the TCP listener
    // ================================================================

    println!("═══ OMS Modbus — TCP Server ═══\n");

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    println!("[1/3] Binding to {addr} …");
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;
    println!("   ✓ Listening on {bind_addr}\n");

    // ================================================================
    // 2. Prepare the register store
    // ================================================================

    println!("[2/3] Initializing SlaveStore …");

    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 1234),  // Temperature ×10  (12.34 °C)
        (1, 5678),  // Humidity ×10     (56.78 %)
        (2, 100),   // Pressure         (100 kPa)
        (3, 42),    // Status code
        (10, 500),  // Setpoint 1
        (11, 600),  // Setpoint 2
        (20, 1000), // Counter (read/write by client)
    ]));

    // Pre-set input registers (read-only from client perspective)
    store.write_input_register(0, 3300); // Voltage ×100  (33.00 V)
    store.write_input_register(1, 1250); // Current ×100  (12.50 A)
    store.write_input_register(2, 5000); // Power         (5000 W)

    // Pre-set coils (digital outputs)
    store.write_coil(0, true); // Pump    ON
    store.write_coil(1, false); // Valve   OFF
    store.write_coil(2, true); // Heater  ON
    store.write_coil(3, true); // Fan     ON

    // Pre-set discrete inputs
    store.write_discrete_input(0, true); // Limit switch 1 — closed
    store.write_discrete_input(1, false); // Limit switch 2 — open
    store.write_discrete_input(2, true); // Emergency stop — OK

    println!("   ✓ Registers, coils, and discrete inputs ready\n");

    // ================================================================
    // 3. Print register map
    // ================================================================

    println!("┌─ Register Map ───────────────────────────────────────┐");
    println!("│ Holding Registers (read/write):                     │");
    println!("│   [  0] = 1234   Temperature ×10 (12.34 °C)        │");
    println!("│   [  1] = 5678   Humidity ×10    (56.78 %)         │");
    println!("│   [  2] = 100    Pressure        (100 kPa)         │");
    println!("│   [  3] = 42     Status code                       │");
    println!("│   [ 10] = 500    Setpoint 1                        │");
    println!("│   [ 11] = 600    Setpoint 2                        │");
    println!("│   [ 20] = 1000   Counter                           │");
    println!("│                                                     │");
    println!("│ Input Registers (read-only):                        │");
    println!("│   [  0] = 3300   Voltage ×100   (33.00 V)          │");
    println!("│   [  1] = 1250   Current ×100   (12.50 A)          │");
    println!("│   [  2] = 5000   Power          (5000 W)           │");
    println!("│                                                     │");
    println!("│ Coils (digital outputs):                            │");
    println!("│   [0] = ON   Pump                                   │");
    println!("│   [1] = OFF  Valve                                  │");
    println!("│   [2] = ON   Heater                                 │");
    println!("│   [3] = ON   Fan                                    │");
    println!("│                                                     │");
    println!("│ Discrete Inputs:                                    │");
    println!("│   [0] = ON   Limit switch 1                         │");
    println!("│   [1] = OFF  Limit switch 2                         │");
    println!("│   [2] = ON   Emergency stop OK                      │");
    println!("└─────────────────────────────────────────────────────┘\n");

    // ================================================================
    // 4. Serve forever
    // ================================================================

    println!("[3/3] Serving … (press Ctrl+C to stop)\n");
    println!("Connect with: cargo run --example tcp_client");
    println!("Or:           cargo run --example tcp_client_auto_tid");
    println!("Or any Modbus TCP client → {bind_addr}\n");

    server.serve_forever(store).await?;

    Ok(())
}
