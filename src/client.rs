// SPDX-License-Identifier: MIT OR Apache-2.0
//! Modbus client (master) trait.

use async_trait::async_trait;

pub use crate::error::ModbusError;
use crate::frame::{Request, Response};

/// Convert a response that didn't match the expected variant into the
/// appropriate error: `Exception` for exception responses, `Protocol`
/// for unexpected success variants.
#[inline]
fn unexpected_response(rsp: Response) -> ModbusError {
    match rsp {
        Response::Exception(fc, ex) => ModbusError::exception(fc, u8::from(ex)),
        other => ModbusError::protocol(format!("unexpected response: {other:?}")),
    }
}

/// Transport-independent Modbus client interface.
///
/// Each transport implements [`call`](ModbusClient::call) — the only required
/// method. All specific methods (`read_coils`, `write_single_register`, etc.)
/// have default implementations that delegate to `call`.
///
/// `slave` is a per-request parameter — a single client instance handles
/// the whole RS-485 bus, addressing different slaves on each call.
///
/// # Examples
///
/// ```no_run
/// use oms_modbus::*;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // TCP
/// let client = tcp::TcpClient::connect_with_timeout(
///     "192.168.1.10:502".parse()?, Duration::from_secs(3),
/// ).await?;
///
/// // RTU serial
/// # let port = tokio::io::duplex(64).0; // placeholder
/// let client = rtu::RtuClient::with_timeout(port, Duration::from_secs(3));
///
/// // All function codes work the same way
/// let regs = client.read_holding_registers(1, 0, 5).await?;
/// client.write_single_register(1, 0, 42).await?;
///
/// // Share via Arc
/// let shared: std::sync::Arc<dyn ModbusClient> = std::sync::Arc::new(client);
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait ModbusClient: Send + Sync {
    /// Send a raw Modbus request to the given slave and return the response.
    /// Transports must implement this; all other methods delegate to it.
    async fn call(&self, slave: u8, request: Request<'_>) -> Result<Response, ModbusError>;

    // ── Default implementations (delegate to `call`) ──────────────────────

    /// Read coils (FC01) — returns the ON/OFF status of discrete outputs.
    async fn read_coils(
        &self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<Vec<bool>, ModbusError> {
        match self.call(slave, Request::ReadCoils(addr, quantity)).await? {
            Response::ReadCoils(bits) => Ok(bits),
            other => Err(unexpected_response(other)),
        }
    }
    /// Read discrete inputs (FC02) — returns the ON/OFF status of digital inputs.
    async fn read_discrete_inputs(
        &self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<Vec<bool>, ModbusError> {
        match self
            .call(slave, Request::ReadDiscreteInputs(addr, quantity))
            .await?
        {
            Response::ReadDiscreteInputs(bits) => Ok(bits),
            other => Err(unexpected_response(other)),
        }
    }
    /// Read holding registers (FC03) — returns the contents of read/write registers.
    async fn read_holding_registers(
        &self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        match self
            .call(slave, Request::ReadHoldingRegisters(addr, quantity))
            .await?
        {
            Response::ReadHoldingRegisters(regs) => Ok(regs),
            other => Err(unexpected_response(other)),
        }
    }
    /// Read input registers (FC04) — returns the contents of read-only registers.
    async fn read_input_registers(
        &self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        match self
            .call(slave, Request::ReadInputRegisters(addr, quantity))
            .await?
        {
            Response::ReadInputRegisters(regs) => Ok(regs),
            other => Err(unexpected_response(other)),
        }
    }
    /// Write a single coil (FC05) — set a discrete output ON (`true`) or OFF (`false`).
    async fn write_single_coil(
        &self,
        slave: u8,
        addr: u16,
        value: bool,
    ) -> Result<(), ModbusError> {
        match self
            .call(slave, Request::WriteSingleCoil(addr, value))
            .await?
        {
            Response::WriteSingleCoil(a, v) if a == addr && v == value => Ok(()),
            other => Err(unexpected_response(other)),
        }
    }
    /// Write a single holding register (FC06).
    async fn write_single_register(
        &self,
        slave: u8,
        addr: u16,
        value: u16,
    ) -> Result<(), ModbusError> {
        match self
            .call(slave, Request::WriteSingleRegister(addr, value))
            .await?
        {
            Response::WriteSingleRegister(a, v) if a == addr && v == value => Ok(()),
            other => Err(unexpected_response(other)),
        }
    }
    /// Write multiple coils (FC15).
    async fn write_multiple_coils(
        &self,
        slave: u8,
        addr: u16,
        values: &[bool],
    ) -> Result<(), ModbusError> {
        let req = Request::WriteMultipleCoils(addr, std::borrow::Cow::Borrowed(values));
        match self.call(slave, req).await? {
            Response::WriteMultipleCoils(a, n) if a == addr && n as usize == values.len() => Ok(()),
            other => Err(unexpected_response(other)),
        }
    }
    /// Write multiple holding registers (FC16).
    async fn write_multiple_registers(
        &self,
        slave: u8,
        addr: u16,
        values: &[u16],
    ) -> Result<(), ModbusError> {
        let req = Request::WriteMultipleRegisters(addr, std::borrow::Cow::Borrowed(values));
        match self.call(slave, req).await? {
            Response::WriteMultipleRegisters(a, n) if a == addr && n as usize == values.len() => {
                Ok(())
            }
            other => Err(unexpected_response(other)),
        }
    }

    /// Write a single register using a mask (FC 22).
    async fn mask_write_register(
        &self,
        slave: u8,
        addr: u16,
        and_mask: u16,
        or_mask: u16,
    ) -> Result<(), ModbusError> {
        match self
            .call(slave, Request::MaskWriteRegister(addr, and_mask, or_mask))
            .await?
        {
            Response::MaskWriteRegister(a, and, or)
                if a == addr && and == and_mask && or == or_mask =>
            {
                Ok(())
            }
            other => Err(unexpected_response(other)),
        }
    }

    /// Send a Diagnostic request (FC 08).
    async fn diagnostic(
        &self,
        slave: u8,
        sub_function: u16,
        data: u16,
    ) -> Result<(u16, u16), ModbusError> {
        match self
            .call(slave, Request::Diagnostic(sub_function, data))
            .await?
        {
            Response::Diagnostic(sf, d) => Ok((sf, d)),
            other => Err(unexpected_response(other)),
        }
    }

    /// Read and write multiple registers in a single transaction (FC 23).
    async fn read_write_multiple_registers(
        &self,
        slave: u8,
        read_addr: u16,
        read_qty: u16,
        write_addr: u16,
        values: &[u16],
    ) -> Result<Vec<u16>, ModbusError> {
        let req = Request::ReadWriteMultipleRegisters(
            read_addr,
            read_qty,
            write_addr,
            std::borrow::Cow::Borrowed(values),
        );
        match self.call(slave, req).await? {
            Response::ReadWriteMultipleRegisters(regs) => Ok(regs),
            other => Err(unexpected_response(other)),
        }
    }
}
