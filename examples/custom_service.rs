// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! # Custom Service — implement your own Modbus server backend
//!
//! Demonstrates two approaches to custom server logic:
//!
//! | Pattern | How | Use case |
//! |---------|-----|----------|
//! | `Service` trait (this example) | Implement `call()` directly | Full control |
//! | `ServerHook` (`server_with_hook`) | Wrap an existing service | Intercept/transform |
//!
//! This example shows implementing the `Service` trait directly:
//! - `LoggingService` — logs every request/response, delegates to SlaveStore
//! - `ReadOnlyService` — rejects all write operations
//!
//! Self-contained: uses in-memory TCP, no hardware needed.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example custom_service
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use oms_modbus::*;

// ── Pattern 1: LoggingService ─────────────────────────────────────────────

/// A service that logs every Modbus request/response to stdout before
/// delegating to an inner `Arc<SlaveStore>`. Same pattern works for
/// bridging to real PLC hardware, databases, or external APIs.
#[derive(Clone)]
struct LoggingService {
    inner: Arc<SlaveStore>,
    name: String,
}

impl LoggingService {
    fn new(name: impl Into<String>, inner: Arc<SlaveStore>) -> Self {
        Self {
            name: name.into(),
            inner,
        }
    }
}

#[async_trait]
impl Service for LoggingService {
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
        println!("  [{}] ← {:?}", self.name, request);

        let response = self.inner.call(request).await;

        match &response {
            Ok(rsp) => println!("  [{}] → {:?}", self.name, rsp),
            Err(ex) => println!("  [{}] → Exception: {:?}", self.name, ex),
        }
        response
    }
}

// ── Pattern 2: ReadOnlyService ────────────────────────────────────────────

/// A service that allows only read operations. Any write request returns
/// `IllegalFunction` exception. Swap with LoggingService to test.
#[derive(Clone)]
#[allow(dead_code)]
struct ReadOnlyService {
    inner: Arc<SlaveStore>,
}

#[async_trait]
impl Service for ReadOnlyService {
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
        match &request {
            // Allow all read operations
            Request::ReadCoils(..)
            | Request::ReadDiscreteInputs(..)
            | Request::ReadHoldingRegisters(..)
            | Request::ReadInputRegisters(..) => self.inner.call(request).await,
            // Reject all write operations
            _ => Err(Exception::IllegalFunction),
        }
    }
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══ OMS Modbus — Custom Service Demo ═══\n");

    // ================================================================
    // Setup: TCP server + custom logging service
    // ================================================================

    println!("[1/3] Starting server with custom service …");

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = tcp::TcpServer::bind(addr).await?;
    let bind_addr = server.local_addr()?;

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1234), (1, 5678)]));

    // Wrap store with LoggingService — logs every request as it happens
    let service = LoggingService::new("TCP-Slave-1", store.clone());

    // Alternative: uncomment to test read-only mode
    // let service = ReadOnlyService { inner: store.clone() };

    tokio::spawn(async move {
        server.serve_forever(service).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("   ✓ Server on {bind_addr}\n");

    // ================================================================
    // Run operations — watch the logs!
    // ================================================================

    println!("[2/3] Connecting client …");
    let client = tcp::TcpClient::connect_with_timeout(bind_addr, Duration::from_secs(3)).await?;
    println!("   ✓ Connected\n");

    println!("[3/3] Running operations — see logs above ↓\n");

    // ── Read — LoggingService prints: ← ReadHoldingRegisters → [1234, 5678]
    println!("─── Read ───");
    let regs = client.read_holding_registers(1, 0, 2).await?;
    println!("  Result: {regs:?}\n");

    // ── Write — LoggingService prints: ← WriteSingleRegister → WriteSingleRegister
    println!("─── Write ───");
    client.write_single_register(1, 0, 9999).await?;
    println!("  Result: wrote 9999 to Holding[0] (see log above)\n");

    // ── Read back — verify write persisted
    println!("─── Read Back ───");
    let regs = client.read_holding_registers(1, 0, 2).await?;
    println!("  Result: {regs:?}\n");

    println!("═══ Summary ═══");
    println!("  ✓ LoggingService → full Service impl, delegates to SlaveStore");
    println!("  ✓ ReadOnlyService → blocks all writes with IllegalFunction");
    println!();
    println!("When to implement Service directly vs. ServerHook:");
    println!("  Service    — full control: bridge to hardware, virtual device");
    println!("  ServerHook — intercept: logging, ACL, transform (see server_with_hook)");
    Ok(())
}
