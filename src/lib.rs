// SPDX-License-Identifier: MIT OR Apache-2.0

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

//! OMS Modbus — high-performance, transport-generic Modbus library in pure Rust.
//!
//! Complete Master (client) and Slave (server) for TCP, RTU, and ASCII —
//! all through a single unified API. Passive bus-level [`WireTap`] with
//! microsecond timestamps, auto-reconnect with USB serial resilience,
//! and spec-compliant RS-485 bus timing.
//!
//! # Quick start
//!
//! ```no_run
//! use oms_modbus::*;
//! use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
//! let server = tcp::TcpServer::bind(addr).await?;
//! let bind_addr = server.local_addr()?;
//! let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1234)]));
//! tokio::spawn(async move { server.serve_forever(store).await.ok(); });
//!
//! let client = tcp::TcpClient::connect_with_timeout(
//!     bind_addr, Duration::from_secs(3),
//! ).await?;
//! let regs = client.read_holding_registers(1, 0, 1).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Key types
//!
//! | Type | Description |
//! |------|-------------|
//! | [`ModbusClient`] | Master (client) trait — TCP, RTU, ASCII share this API |
//! | [`SlaveStore`] | Default Slave (server) — in-memory register/coil store |
//! | [`Service`] | Trait for custom server backends |
//! | [`ClientOptions`] | Builder: timeout, capture, reconnect, bus timing |
//! | [`BusCapture`] | Recording + statistics — implements [`WireTap`] |
//! | [`WireTap`] | Passive bus observer — implement for custom capture |
//! | [`BusTiming`] | RS-485 frame spacing — per Modbus Serial Spec |
//! | [`ModbusError`] | Structured error — `Display`, `detail()`, `label()` |
//!
//! # Examples
//!
//! See the [examples directory](https://github.com/orangehorsetech/oms-modbus/tree/master/examples)
//! for 14 runnable examples covering TCP, RTU, ASCII, monitoring, reconnect,
//! custom services, and bus timing.

pub mod bus_timing;
pub mod capture;
pub mod client;
pub mod codec;
pub mod error;
pub mod frame;
pub mod options;
pub mod server;
pub use codec::calculate_crc;
pub use codec::calculate_lrc;
pub mod intercept;
pub mod monitor;
pub mod reconnect;
pub mod slave_store;
pub mod transport;
pub mod wire_tap;
pub use reconnect::ReconnectConfig;

pub use bus_timing::BusTiming;
pub use capture::BusCapture;
pub use client::ModbusClient;
pub use error::ModbusError;
pub use frame::{Exception, ExceptionResponse, FunctionCode, Request, Response};
pub use intercept::{format_timestamp, PacketCapture, PacketData, PacketRecord, TrafficStats};
pub use options::ClientOptions;
pub use server::Service;
pub use server::{HookedService, ServerHook};
pub use slave_store::SlaveStore;
pub use wire_tap::{now_micros, WireTap};

pub mod rtu {
    pub use crate::transport::rtu::{with_options, RtuClient, RtuServer};
}
pub mod ascii {
    pub use crate::transport::ascii::{with_options, AsciiClient, AsciiServer};
}
pub mod tcp {
    pub use crate::transport::tcp::{
        with_options, LengthMode, TcpClient, TcpConfig, TcpServer, TidMode,
    };
}
