// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # ServerHook Demo — logging, access control, dynamic computation
//!
//! Demonstrates three realistic `ServerHook` patterns composed with
//! `HookedService`. All hooks run in-process against a local TCP server.
//!
//! ## Patterns shown
//!
//! | Pattern | What it does |
//! |---------|-------------|
//! | **LoggingHook** | Prints every request/response pair with timing |
//! | **AccessControlHook** | Rejects writes from non-admin slaves |
//! | **ComputeHook** | Returns dynamically-computed values (simulates Lua) |
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example server_with_hook
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use oms_modbus::*;

// ═══════════════════════════════════════════════════════════════════════════════
// Pattern 1: LoggingHook — records every request/response with timing
// ═══════════════════════════════════════════════════════════════════════════════

/// A hook that logs every call — ideal for debugging and auditing.
#[derive(Clone)]
struct LoggingHook {
    call_count: Arc<AtomicU64>,
}

#[async_trait]
impl ServerHook for LoggingHook {
    async fn before_call(&self, slave: u8, request: &Request<'_>) -> Option<Response> {
        let n = self.call_count.fetch_add(1, Ordering::Relaxed);
        println!("  [{n:04}] ← slave={slave} {:?}", request);
        None // always pass through to inner service
    }

    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        match &result {
            Ok(rsp) => println!("       → {:?}", rsp),
            Err(ex) => println!("       → Exception: {:?}", ex),
        }
        result
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pattern 2: AccessControlHook — rejects writes from unauthorized slaves
// ═══════════════════════════════════════════════════════════════════════════════

/// Blocks write requests from slaves not in the allowed list.
/// Read requests pass through regardless of slave ID.
#[derive(Clone)]
struct AccessControlHook {
    admin_slaves: Vec<u8>,
}

#[async_trait]
impl ServerHook for AccessControlHook {
    async fn before_call(&self, slave: u8, request: &Request<'_>) -> Option<Response> {
        let is_write = matches!(
            request,
            Request::WriteSingleRegister(..)
                | Request::WriteMultipleRegisters(..)
                | Request::WriteSingleCoil(..)
                | Request::WriteMultipleCoils(..)
                | Request::MaskWriteRegister(..)
                | Request::ReadWriteMultipleRegisters(..)
        );

        if is_write && !self.admin_slaves.contains(&slave) {
            let fc = request.function_code().value();
            println!("  🚫 Access denied: slave={slave} attempted write FC={fc}");
            return Some(Response::Exception(fc, Exception::IllegalFunction));
        }
        None
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pattern 3: ComputeHook — dynamic value generation (Lua/script simulation)
// ═══════════════════════════════════════════════════════════════════════════════

/// A hook that computes register values on-the-fly instead of reading
/// from a static store. This simulates what a Lua scripting hook would do:
/// evaluate expressions at read time based on external state.
#[derive(Clone)]
struct ComputeHook {
    /// Simulated "external sensor" reading — changes each read.
    sensor_value: Arc<std::sync::Mutex<u16>>,
}

impl ComputeHook {
    fn new(initial: u16) -> Self {
        Self {
            sensor_value: Arc::new(std::sync::Mutex::new(initial)),
        }
    }
}

#[async_trait]
impl ServerHook for ComputeHook {
    async fn before_call(&self, slave: u8, request: &Request<'_>) -> Option<Response> {
        match request {
            // Address 100–109: "computed area" — running counter
            Request::ReadHoldingRegisters(addr, qty) if *addr >= 100 && *addr < 110 => {
                let mut val = self.sensor_value.lock().unwrap();
                *val = val.wrapping_add(1);
                let regs: Vec<u16> = (0..*qty).map(|i| (*val).wrapping_add(i)).collect();
                Some(Response::ReadHoldingRegisters(regs))
            }
            // Address 200: "device info" — returns device serial number
            Request::ReadHoldingRegisters(addr, qty) if *addr == 200 => {
                // Simulate: serial number = 0x4F4D_5300 + slave (OM-S-<addr>)
                let base: u16 = 0x4F4D; // "OM"
                let mut regs = vec![base, 0x5300 + slave as u16]; // "S" + addr
                regs.resize(*qty as usize, 0);
                Some(Response::ReadHoldingRegisters(regs))
            }
            _ => None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — ServerHook Demo ═══\n");
    println!("This example demonstrates three hook patterns:\n");
    println!("  ① Logging     — prints every request/response");
    println!("  ② AccessCtrl  — blocks writes from non-admin slaves");
    println!("  ③ Compute     — dynamic register values (Lua-like)\n");

    // ================================================================
    // Setup: TCP server + layered hooks
    // ================================================================

    // 1. Bind TCP server
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;

    // 2. Inner store — physical register data (addresses 0-99)
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 1234), // Temperature  (12.34 °C)
        (1, 5678), // Humidity     (56.78 %)
        (2, 100),  // Pressure     (100 kPa)
        (3, 42),   // Status
    ]));

    // 3. Layer hooks: Logging → AccessControl → Compute → Store
    //    ┌─────────────────────────────────────┐
    //    │ LoggingHook  (outermost: sees all)  │
    //    │  ┌───────────────────────────────┐  │
    //    │  │ AccessControlHook             │  │
    //    │  │  ┌─────────────────────────┐  │  │
    //    │  │  │ ComputeHook             │  │  │
    //    │  │  │  ┌───────────────────┐  │  │  │
    //    │  │  │  │ SlaveStore (data) │  │  │  │
    //    │  │  │  └───────────────────┘  │  │  │
    //    │  │  └─────────────────────────┘  │  │
    //    │  └───────────────────────────────┘  │
    //    └─────────────────────────────────────┘
    let logging = LoggingHook {
        call_count: Arc::new(AtomicU64::new(0)),
    };
    let access = AccessControlHook {
        admin_slaves: vec![1],
    }; // only slave 1 can write
    let compute = ComputeHook::new(1000);

    type L2 = HookedService<Arc<SlaveStore>, ComputeHook>;
    type L1 = HookedService<L2, AccessControlHook>;
    let hooked: HookedService<L1, LoggingHook> = HookedService::new(
        HookedService::new(HookedService::new(store, compute), access),
        logging,
    );

    // 4. Spawn server
    tokio::spawn(async move {
        server.serve_forever(hooked).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Server listening on {bind_addr}\n");

    // ================================================================
    // Demo: Run requests through the hooked server
    // ================================================================

    let client = tcp::TcpClient::connect_with_timeout(bind_addr, Duration::from_secs(3)).await?;

    // ── Demo 1: Read from static store (LoggingHook fires) ──────────
    println!("─── Demo ①: Logging (watch the [NNNN] log lines) ───");
    let regs = client.read_holding_registers(1, 0, 3).await?;
    println!("  Result: {regs:?}\n");

    // ── Demo 2: Read from compute area (ComputeHook short-circuits) ─
    println!("─── Demo ②: Dynamic Compute (address 100..109) ───");
    for _ in 0..3 {
        let regs = client.read_holding_registers(1, 100, 2).await?;
        println!("  Computed[100..101] = {regs:?}  ← increments each read");
    }
    println!();

    // ── Demo 3: Read device info (ComputeHook address 200) ──────────
    println!("─── Demo ③: Device Info (address 200) ───");
    let info = client.read_holding_registers(1, 200, 4).await?;
    println!("  DeviceInfo = {info:?}\n");

    // ── Demo 4: Write as admin slave 1 (allowed) ────────────────────
    println!("─── Demo ④: Write as admin slave 1 (allowed) ───");
    client.write_single_register(1, 0, 9999).await?;
    let regs = client.read_holding_registers(1, 0, 1).await?;
    println!("  Holding[0] = {regs:?}  ← write succeeded\n");

    // ── Demo 5: Write as non-admin slave 2 (blocked by AccessControl) ─
    println!("─── Demo ⑤: Write as slave 2 (BLOCKED) ───");
    match client.write_single_register(2, 0, 5555).await {
        Ok(()) => println!("  Unexpected: write succeeded"),
        Err(e) => println!("  ✗ {e}  ← correctly blocked by AccessControlHook\n"),
    }

    // ── Demo 6: Read as non-admin slave 2 (still allowed) ────────────
    println!("─── Demo ⑥: Read as slave 2 (allowed — reads not blocked) ───");
    match client.read_holding_registers(2, 0, 1).await {
        Ok(regs) => println!("  Holding[0] = {regs:?}\n"),
        Err(e) => println!("  ✗ {e}\n"),
    }

    // ================================================================
    // Summary
    // ================================================================
    println!("═══ Summary ═══");
    println!("  ✓ LoggingHook      — recorded every request/response (see [NNNN] above)");
    println!("  ✓ ComputeHook      — short-circuited reads to addresses 100-109, 200");
    println!("  ✓ AccessCtrlHook   — blocked write from slave 2, allowed slave 1");
    println!();
    println!("Integrate your own hook: implement ServerHook, wrap with HookedService.");
    println!("Real-world examples: Lua script execution, database lookup, MQTT bridge.");

    Ok(())
}
