// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Serial Reconnect — RTU/ASCII with factory-based reconnect
//!
//! Demonstrates `RtuClient::with_reconnect()` and `AsciiClient::with_reconnect()`
//! using DuplexStream (in-memory) — no serial hardware needed.
//!
//! The factory closure is called on each reconnect attempt to create a fresh
//! transport. This pattern handles USB serial ports that disappear/reappear
//! with different device names.
//!
//! Self-contained: uses tokio DuplexStream, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example reconnect_serial
//! ```

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Serial Reconnect Demo ═══\n");

    // ================================================================
    // Mode 1: RTU reconnect with factory
    // ================================================================

    println!("[1/2] RTU reconnect over DuplexStream");
    {
        let (client_stream, server_stream) = tokio::io::duplex(1024);

        // Start RTU server on one end
        let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
        let server = rtu::RtuServer::new(server_stream);
        tokio::spawn(async move {
            server.serve_forever(store).await.ok();
        });

        // Client with reconnect — factory rebuilds transport on failure
        let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3))
            .with_reconnect(3, Duration::from_millis(100), || {
                // In a real application, this would reopen the serial port:
                //   serialport::new("/dev/ttyUSB0", 9600).open()
                let (c, _s) = tokio::io::duplex(1024);
                Ok(c)
            });

        let regs = client.read_holding_registers(1, 0, 1).await?;
        println!("   RTU read → {regs:?}\n");
    }

    // ================================================================
    // Mode 2: ASCII reconnect with factory
    // ================================================================

    println!("[2/2] ASCII reconnect over DuplexStream");
    {
        let (client_stream, server_stream) = tokio::io::duplex(1024);

        let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99)]));
        let server = ascii::AsciiServer::new(server_stream);
        tokio::spawn(async move {
            server.serve_forever(store).await.ok();
        });

        let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_secs(3))
            .with_reconnect(3, Duration::from_millis(100), || {
                // Real usage: serialport::new("/dev/ttyUSB0", 9600).open()
                let (c, _s) = tokio::io::duplex(1024);
                Ok(c)
            });

        let regs = client.read_holding_registers(1, 0, 1).await?;
        println!("   ASCII read → {regs:?}\n");
    }

    // ================================================================
    // Summary
    // ================================================================

    println!("═══ Summary ═══");
    println!("  ✓ RTU reconnect  — with_reconnect(3, 100ms, factory)");
    println!("  ✓ ASCII reconnect — same API as RTU, just swap client type");
    println!();
    println!("Real-world factory pattern:");
    println!("  client.with_reconnect(3, Duration::from_millis(100), || {{");
    println!("      serialport::new(\"/dev/ttyUSB0\", 9600)");
    println!("          .timeout(Duration::from_millis(100))");
    println!("          .open()");
    println!("  }});");
    println!();
    println!("The factory is the key to USB serial resilience. When the USB adapter");
    println!("unplugs and replugs, the factory re-opens the port on each attempt —");
    println!("completely transparent to the caller's read_holding_registers().");
    Ok(())
}
