// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Dynamic Server — live-updating register values
//!
//! Demonstrates a Modbus TCP server where register values change over time,
//! simulating a real industrial device. Implements the `Service` trait directly
//! instead of using `SlaveStore`, with live values like uptime counter,
//! temperature (sine wave), and a heartbeat coil.
//!
//! Self-contained: uses in-memory TCP, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example dynamic_server
//! ```
//!
//! Then connect with any Modbus client:
//! ```bash
//! cargo run --example tcp_client
//! ```
//!
//! ## Register Map
//!
//! | Register | Description |
//! |----------|-------------|
//! | Holding[0] | Uptime counter (seconds since start) |
//! | Holding[1] | Temperature ×10, sine wave (20.0–30.0 °C) |
//! | InputReg[0] | Simulated ADC reading, changes each read |
//! | Coil[0] | Heartbeat, toggles every 5 seconds |
//! | DiscreteIn[0] | Overheat flag: ON when temp > 25.0 °C |

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use oms_modbus::*;

// ── Dynamic Service — live data, no SlaveStore needed ──────────────────────

/// A service that generates live-updating register values.
///
/// Implements `Service` directly — no `SlaveStore` needed.
/// Use this pattern to bridge to real hardware or simulate devices.
#[derive(Clone)]
struct DynamicDevice {
    start_time: Instant,
    read_count: Arc<AtomicU64>,
}

impl DynamicDevice {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            read_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Simulated temperature ×10: oscillates between 20.0 °C and 30.0 °C
    fn temperature(&self) -> u16 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let temp = 25.0 + 5.0 * (elapsed * 0.5).sin();
        (temp * 10.0) as u16
    }

    /// Simulated ADC reading — pseudo-random but deterministic
    fn adc_reading(&self) -> u16 {
        let count = self.read_count.fetch_add(1, Ordering::Relaxed);
        ((count.wrapping_mul(1103515245).wrapping_add(12345) >> 16) & 0x3FF) as u16
    }
}

#[async_trait]
impl Service for DynamicDevice {
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
        let uptime = self.start_time.elapsed().as_secs() as u16;

        Ok(match request {
            Request::ReadHoldingRegisters(addr, cnt) => {
                let mut regs = Vec::with_capacity(cnt as usize);
                for i in 0..cnt {
                    let a = addr.wrapping_add(i);
                    regs.push(match a {
                        0 => uptime,             // uptime counter
                        1 => self.temperature(), // temperature ×10
                        _ => 0,
                    });
                }
                Response::ReadHoldingRegisters(regs)
            }

            Request::ReadInputRegisters(_addr, cnt) => {
                let mut regs = Vec::with_capacity(cnt as usize);
                for _ in 0..cnt {
                    regs.push(self.adc_reading());
                }
                Response::ReadInputRegisters(regs)
            }

            Request::ReadCoils(addr, cnt) => {
                let elapsed = self.start_time.elapsed().as_secs();
                let heartbeat = (elapsed / 5) % 2 == 0;
                let mut bits = Vec::with_capacity(cnt as usize);
                for i in 0..cnt {
                    let a = addr.wrapping_add(i);
                    bits.push(a == 0 && heartbeat);
                }
                Response::ReadCoils(bits)
            }

            Request::ReadDiscreteInputs(addr, cnt) => {
                let temp = self.temperature();
                let overheat = temp > 250; // > 25.0 °C
                let mut bits = Vec::with_capacity(cnt as usize);
                for i in 0..cnt {
                    let a = addr.wrapping_add(i);
                    bits.push(a == 0 && overheat);
                }
                Response::ReadDiscreteInputs(bits)
            }

            // Allow writes to holding registers for testing
            Request::WriteSingleRegister(addr, _value) => {
                Response::WriteSingleRegister(addr, _value)
            }
            Request::WriteMultipleRegisters(addr, values) => {
                Response::WriteMultipleRegisters(addr, values.len() as u16)
            }

            _ => return Err(Exception::IllegalFunction),
        })
    }
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Dynamic Server Demo ═══\n");

    // ================================================================
    // Setup: bind server
    // ================================================================

    println!("[1/3] Binding TCP server …");

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;
    println!("   ✓ Server on {bind_addr}\n");

    // ================================================================
    // Print register map
    // ================================================================

    println!("[2/3] Register map:");
    println!("  Holding[0]  = uptime counter (seconds since start)");
    println!("  Holding[1]  = temperature ×10, sine wave (20.0–30.0 °C)");
    println!("  InputReg[0] = simulated ADC reading (changes each read)");
    println!("  Coil[0]     = heartbeat (toggles every 5 seconds)");
    println!("  DiscreteIn[0] = overheat flag (ON when temp > 25.0 °C)");
    println!();
    println!("  Connect with: cargo run --example tcp_client");

    // ================================================================
    // Monitor thread: print live values every 2 seconds
    // ================================================================

    let device = DynamicDevice::new();
    let monitor = device.clone();
    let start = Instant::now();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let elapsed = start.elapsed().as_secs_f64();
            let temp = 25.0 + 5.0 * (elapsed * 0.5).sin();
            let adc = monitor.adc_reading();
            println!(
                "  [live] uptime={:.0}s  temp={:.1}°C  adc={}  overheat={}",
                elapsed,
                temp,
                adc,
                temp > 25.0,
            );
        }
    });

    // ================================================================
    // Serve forever
    // ================================================================

    println!("\n[3/3] Serving … (press Ctrl+C to stop)\n");
    server.serve_forever(device).await?;
    Ok(())
}
