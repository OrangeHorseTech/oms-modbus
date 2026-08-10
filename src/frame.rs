// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Modbus frame types — Request, Response, Exception, FunctionCode.

use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{Error, ErrorKind};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Maximum PDU size per MODBUS Application Protocol V1.1b3 §4.1.
/// The PDU is function_code (1 byte) + data (max 252 bytes) = 253 bytes.
pub const MAX_PDU_SIZE: usize = 253;

/// Maximum number of coils per request per MODBUS Application Protocol V1.1b3.
/// Functions: ReadCoils (FC=1), WriteMultipleCoils (FC=15).
pub const MAX_COILS: u16 = 2000;

/// Maximum number of registers per request per MODBUS Application Protocol V1.1b3.
/// Functions: ReadHoldingRegisters (FC=3), ReadInputRegisters (FC=4),
/// WriteMultipleRegisters (FC=16), ReadWriteMultipleRegisters (FC=23).
pub const MAX_REGISTERS: u16 = 125;

/// Safe conversion from `usize` to `u8`, returning an error on overflow.
/// Used for byte-count fields in Modbus frames where counts must fit in u8.
#[inline]
fn safe_u8(val: usize, context: &str) -> Result<u8, Error> {
    u8::try_from(val).map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            format!("{context}: value {val} exceeds u8 range"),
        )
    })
}

/// Validate that a response's payload size will fit within the Modbus PDU limit.
/// Called by `encode_response_into` before encoding to catch oversized payloads.
fn validate_response_size(rsp: &Response) -> Result<(), Error> {
    let byte_count = match rsp {
        Response::ReadCoils(bits) | Response::ReadDiscreteInputs(bits) => bits.len().div_ceil(8),
        Response::ReadHoldingRegisters(regs)
        | Response::ReadInputRegisters(regs)
        | Response::ReadWriteMultipleRegisters(regs) => regs
            .len()
            .checked_mul(2)
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "response too large"))?,
        _ => return Ok(()), // fixed-size responses always fit
    };
    safe_u8(byte_count, "response byte count")?;
    Ok(())
}

// ── Function Code ─────────────────────────────────────────────────────────

/// Modbus function code (1-127 for standard, 128-255 for exception responses).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FunctionCode(u8);

impl FunctionCode {
    #[inline]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }
    #[inline]
    pub const fn value(self) -> u8 {
        self.0
    }
}

/// Returns `true` if `b` is a known Modbus function code or its exception
/// variant (standard FC + 0x80). Used by server-side frame parsers to
/// distinguish valid frames from noise on the wire.
///
/// Known standard function codes: 1–6, 8, 15, 16, 22, 23.
/// Exception variants: standard + 0x80 (bit 7 set).
#[inline]
pub(crate) fn is_known_function_code(b: u8) -> bool {
    // Strip exception bit (bit 7) and check standard range.
    let base = b & 0x7F;
    matches!(base, 1..=6 | 8 | 15 | 16 | 22 | 23)
}

// ── Address & Quantity ────────────────────────────────────────────────────

/// Modbus register/coil address (0–65535, 0-based per spec).
pub type Address = u16;
/// Number of registers or coils to read/write.
pub type Quantity = u16;

// ── Request ───────────────────────────────────────────────────────────────

/// A Modbus request PDU — one variant per standard function code.
///
/// Use [`Request::function_code`] to get the numeric FC, and
/// [`encode_request_into`] or [`TryFrom`]`<`[`Bytes`]`>` to serialize.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Request<'a> {
    ReadCoils(Address, Quantity),
    ReadDiscreteInputs(Address, Quantity),
    ReadHoldingRegisters(Address, Quantity),
    ReadInputRegisters(Address, Quantity),
    WriteSingleCoil(Address, bool),
    WriteSingleRegister(Address, u16),
    WriteMultipleCoils(Address, Cow<'a, [bool]>),
    WriteMultipleRegisters(Address, Cow<'a, [u16]>),
    ReadWriteMultipleRegisters(Address, Quantity, Address, Cow<'a, [u16]>),
    MaskWriteRegister(Address, u16, u16),
    /// Diagnostic (FC 08). First `u16` is sub-function code (e.g. 0x0000 =
    /// Return Query Data, 0x000A = Clear Counters, 0x000B-0x000E = counter
    /// reads). Second `u16` is the data field.
    Diagnostic(u16, u16),
    Disconnect,
}

impl Request<'_> {
    /// The Modbus function code for this request variant.
    pub const fn function_code(&self) -> FunctionCode {
        use Request::*;
        match self {
            ReadCoils(..) => FunctionCode::new(1),
            ReadDiscreteInputs(..) => FunctionCode::new(2),
            ReadHoldingRegisters(..) => FunctionCode::new(3),
            ReadInputRegisters(..) => FunctionCode::new(4),
            WriteSingleCoil(..) => FunctionCode::new(5),
            WriteSingleRegister(..) => FunctionCode::new(6),
            ReadWriteMultipleRegisters(..) => FunctionCode::new(23),
            WriteMultipleCoils(..) => FunctionCode::new(15),
            WriteMultipleRegisters(..) => FunctionCode::new(16),
            MaskWriteRegister(..) => FunctionCode::new(22),
            Diagnostic(..) => FunctionCode::new(8),
            Disconnect => FunctionCode::new(0),
        }
    }

    /// Convert any borrowed data to `'static` owned data. Useful for storing
    /// requests across await points or sending them to another task.
    pub fn into_owned(self) -> Request<'static> {
        match self {
            Request::ReadCoils(a, q) => Request::ReadCoils(a, q),
            Request::ReadDiscreteInputs(a, q) => Request::ReadDiscreteInputs(a, q),
            Request::ReadHoldingRegisters(a, q) => Request::ReadHoldingRegisters(a, q),
            Request::ReadInputRegisters(a, q) => Request::ReadInputRegisters(a, q),
            Request::WriteSingleCoil(a, v) => Request::WriteSingleCoil(a, v),
            Request::WriteSingleRegister(a, v) => Request::WriteSingleRegister(a, v),
            Request::WriteMultipleCoils(a, v) => {
                Request::WriteMultipleCoils(a, Cow::Owned(v.into_owned()))
            }
            Request::WriteMultipleRegisters(a, v) => {
                Request::WriteMultipleRegisters(a, Cow::Owned(v.into_owned()))
            }
            Request::ReadWriteMultipleRegisters(a, q, w, d) => {
                Request::ReadWriteMultipleRegisters(a, q, w, Cow::Owned(d.into_owned()))
            }
            Request::MaskWriteRegister(a, b, c) => Request::MaskWriteRegister(a, b, c),
            Request::Diagnostic(sf, d) => Request::Diagnostic(sf, d),
            Request::Disconnect => Request::Disconnect,
        }
    }
}

// ── Response ──────────────────────────────────────────────────────────────

/// A Modbus response PDU — one variant per function code, plus [`Exception`].
///
/// Use [`Response::function_code`] to get the numeric FC (MSB set for exceptions),
/// and [`encode_response_into`] or [`From`]`<`[`Bytes`]`>` to serialize.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Response {
    ReadCoils(Vec<bool>),
    ReadDiscreteInputs(Vec<bool>),
    ReadHoldingRegisters(Vec<u16>),
    ReadInputRegisters(Vec<u16>),
    WriteSingleCoil(Address, bool),
    WriteSingleRegister(Address, u16),
    WriteMultipleCoils(Address, u16),
    WriteMultipleRegisters(Address, u16),
    ReadWriteMultipleRegisters(Vec<u16>),
    MaskWriteRegister(Address, u16, u16),
    Diagnostic(u16, u16),
    Exception(u8, Exception),
}

impl Response {
    /// The Modbus function code for this response.
    /// Returns `fc | 0x80` for [`Exception`](Response::Exception) variants.
    pub const fn function_code(&self) -> FunctionCode {
        use Response::*;
        match self {
            ReadCoils(..) => FunctionCode::new(1),
            ReadDiscreteInputs(..) => FunctionCode::new(2),
            ReadHoldingRegisters(..) => FunctionCode::new(3),
            ReadInputRegisters(..) => FunctionCode::new(4),
            WriteSingleCoil(..) => FunctionCode::new(5),
            WriteSingleRegister(..) => FunctionCode::new(6),
            WriteMultipleCoils(..) => FunctionCode::new(15),
            WriteMultipleRegisters(..) => FunctionCode::new(16),
            ReadWriteMultipleRegisters(..) => FunctionCode::new(23),
            MaskWriteRegister(..) => FunctionCode::new(22),
            Diagnostic(..) => FunctionCode::new(8),
            Exception(fc, _) => FunctionCode::new(*fc | 0x80),
        }
    }
}

// ── Response → ModbusError conversion ─────────────────────────────────────

impl From<Response> for crate::error::ModbusError {
    /// Convert a `Response` to a `ModbusError`.
    ///
    /// - `Response::Exception` → `ModbusError::Exception`
    /// - Everything else        → `ModbusError::Protocol` (unexpected success response
    ///   in an error context — prefer [`unexpected_response`](crate::client)
    ///   for direct handling in `ModbusClient` default methods)
    fn from(rsp: Response) -> Self {
        match rsp {
            Response::Exception(fc, ex) => crate::error::ModbusError::exception(fc, u8::from(ex)),
            other => crate::error::ModbusError::protocol(format!(
                "unexpected success response in error context: {other:?}"
            )),
        }
    }
}

// ── Exception ─────────────────────────────────────────────────────────────

/// Standard Modbus exception codes (1–8, 10–11) plus [`Custom(u8)`](Exception::Custom).
///
/// Implements `From<u8>` and `Into<u8>` for wire-format conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[repr(u8)]
#[non_exhaustive]
pub enum Exception {
    #[error("Illegal function")]
    IllegalFunction = 1,
    #[error("Illegal data address")]
    IllegalDataAddress = 2,
    #[error("Illegal data value")]
    IllegalDataValue = 3,
    #[error("Server device failure")]
    ServerDeviceFailure = 4,
    #[error("Acknowledge")]
    Acknowledge = 5,
    #[error("Server device busy")]
    ServerDeviceBusy = 6,
    #[error("Negative acknowledge")]
    NegativeAcknowledge = 7,
    #[error("Memory parity error")]
    MemoryParityError = 8,
    #[error("Gateway path unavailable")]
    GatewayPathUnavailable = 10,
    #[error("Gateway target device failed to respond")]
    GatewayTargetDeviceFailedToRespond = 11,
    #[error("Custom({0})")]
    Custom(u8),
}

impl From<u8> for Exception {
    fn from(code: u8) -> Self {
        match code {
            1 => Exception::IllegalFunction,
            2 => Exception::IllegalDataAddress,
            3 => Exception::IllegalDataValue,
            4 => Exception::ServerDeviceFailure,
            5 => Exception::Acknowledge,
            6 => Exception::ServerDeviceBusy,
            7 => Exception::NegativeAcknowledge,
            8 => Exception::MemoryParityError,
            10 => Exception::GatewayPathUnavailable,
            11 => Exception::GatewayTargetDeviceFailedToRespond,
            n => Exception::Custom(n),
        }
    }
}

impl From<Exception> for u8 {
    fn from(e: Exception) -> u8 {
        match e {
            Exception::IllegalFunction => 1,
            Exception::IllegalDataAddress => 2,
            Exception::IllegalDataValue => 3,
            Exception::ServerDeviceFailure => 4,
            Exception::Acknowledge => 5,
            Exception::ServerDeviceBusy => 6,
            Exception::NegativeAcknowledge => 7,
            Exception::MemoryParityError => 8,
            Exception::GatewayPathUnavailable => 10,
            Exception::GatewayTargetDeviceFailedToRespond => 11,
            Exception::Custom(n) => n,
        }
    }
}

// ── Exception Response ────────────────────────────────────────────────────

/// A decoded Modbus exception response: function code and exception code.
///
/// Converts to [`ModbusError`](crate::ModbusError) via `From`.
#[derive(Debug, Clone, Error)]
#[error("Modbus exception {exception:?} for function {function:?}")]
pub struct ExceptionResponse {
    pub function: FunctionCode,
    pub exception: Exception,
}

impl From<ExceptionResponse> for crate::error::ModbusError {
    fn from(er: ExceptionResponse) -> Self {
        crate::error::ModbusError::exception(er.function.value(), u8::from(er.exception))
    }
}

// ── PDU Serialization ─────────────────────────────────────────────────────
//
// These are the ONLY conversion functions for Request ↔ Bytes and
// Response ↔ Bytes.  Everything else goes through these.
//
// For zero-copy framing, transports should call `encode_request_into` /
// `encode_response_into` directly with a reused `BytesMut` buffer instead of
// round-tripping through `Bytes`.

/// Encode a request PDU into an existing buffer (no intermediate allocation).
///
/// # Errors
///
/// Returns an error if the PDU exceeds the Modbus spec limit of 253 bytes.
pub fn encode_request_into(req: &Request<'_>, buf: &mut BytesMut) -> Result<(), Error> {
    let start = buf.len();
    encode_request(req, buf)?;
    let pdu_len = buf.len() - start;
    if pdu_len > MAX_PDU_SIZE {
        buf.truncate(start);
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("PDU size {pdu_len} exceeds Modbus limit of {MAX_PDU_SIZE}"),
        ));
    }
    Ok(())
}

/// Encode a response PDU into an existing buffer (no intermediate allocation).
///
/// # Errors
///
/// Returns an error if the response byte count overflows a `u8` or the
/// resulting PDU exceeds the Modbus spec limit of 253 bytes.
pub fn encode_response_into(rsp: &Response, buf: &mut BytesMut) -> Result<(), Error> {
    // Validate byte counts before encoding to catch oversized responses early.
    // The Modbus spec guarantees these bounds (≤2000 coils → ≤250 bytes,
    // ≤125 registers → ≤250 bytes), but we validate here for defense in depth.
    validate_response_size(rsp)?;
    let start = buf.len();
    encode_response(rsp, buf)?;
    let pdu_len = buf.len() - start;
    if pdu_len > MAX_PDU_SIZE {
        buf.truncate(start);
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("PDU size {pdu_len} exceeds Modbus limit of {MAX_PDU_SIZE}"),
        ));
    }
    Ok(())
}

impl<'a> TryFrom<Request<'a>> for Bytes {
    type Error = Error;
    fn try_from(req: Request<'a>) -> Result<Self, Self::Error> {
        let mut buf = BytesMut::new();
        encode_request_into(&req, &mut buf)?;
        Ok(buf.freeze())
    }
}

impl TryFrom<Bytes> for Request<'static> {
    type Error = Error;
    fn try_from(mut bytes: Bytes) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "empty PDU"));
        }
        let fc = bytes[0];
        bytes.advance(1);
        decode_request(fc, &mut bytes)
    }
}

/// Converts a [`Response`] into its wire-format PDU bytes.
///
/// Uses [`encode_response_into`] internally for validation. If the
/// response violates the Modbus spec (e.g., `>2000` coils or `>125`
/// registers), an empty `Bytes` is returned rather than truncating or
/// panicking. Prefer [`encode_response_into`] directly when you need
/// error feedback.
impl From<Response> for Bytes {
    /// Converts a [`Response`] into its wire-format PDU bytes.
    ///
    /// Uses [`encode_response_into`] internally for validation. If the
    /// response violates the Modbus spec (e.g., `>2000` coils or `>125`
    /// registers), an empty `Bytes` is returned rather than truncating or
    /// panicking. When you need error feedback on oversized payloads, use
    /// [`encode_response_into`] directly — it returns `Result` so encoding
    /// failures are surfaced rather than silently discarded.
    fn from(rsp: Response) -> Self {
        let mut buf = BytesMut::new();
        let _ = encode_response_into(&rsp, &mut buf);
        buf.freeze()
    }
}

impl TryFrom<Bytes> for Response {
    type Error = Error;
    fn try_from(mut bytes: Bytes) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "empty PDU"));
        }
        let fc = bytes[0];
        // Check for exception response (function code + 0x80)
        if fc & 0x80 != 0 {
            if bytes.len() < 2 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "truncated exception PDU",
                ));
            }
            let exception_code = bytes[1];
            return Ok(Response::Exception(
                fc & 0x7f,
                Exception::from(exception_code),
            ));
        }
        bytes.advance(1);
        decode_response(fc, &mut bytes)
    }
}

// ── Encoders ──────────────────────────────────────────────────────────────

#[inline]
fn push_u16(buf: &mut BytesMut, v: u16) {
    buf.put_u16(v);
}

fn encode_request(req: &Request<'_>, buf: &mut BytesMut) -> Result<(), Error> {
    // Disconnect sends nothing on the wire
    if matches!(req, Request::Disconnect) {
        return Ok(());
    }
    buf.put_u8(req.function_code().value());
    match req {
        Request::ReadCoils(addr, qty)
        | Request::ReadDiscreteInputs(addr, qty)
        | Request::ReadHoldingRegisters(addr, qty)
        | Request::ReadInputRegisters(addr, qty) => {
            push_u16(buf, *addr);
            push_u16(buf, *qty);
        }
        Request::WriteSingleCoil(addr, value) => {
            push_u16(buf, *addr);
            buf.put_u16(if *value { 0xFF00 } else { 0x0000 });
        }
        Request::WriteSingleRegister(addr, value) => {
            push_u16(buf, *addr);
            push_u16(buf, *value);
        }
        Request::WriteMultipleCoils(addr, values) => {
            push_u16(buf, *addr);
            push_u16(buf, values.len() as u16);
            let byte_count = safe_u8(values.len().div_ceil(8), "coil byte count")?;
            buf.put_u8(byte_count);
            for chunk in values.chunks(8) {
                let mut byte = 0u8;
                for (i, &v) in chunk.iter().enumerate() {
                    if v {
                        byte |= 1 << i;
                    }
                }
                buf.put_u8(byte);
            }
        }
        Request::WriteMultipleRegisters(addr, values) => {
            push_u16(buf, *addr);
            push_u16(buf, values.len() as u16);
            let byte_count = safe_u8(values.len() * 2, "write multiple reg byte count")?;
            buf.put_u8(byte_count);
            for &v in values.iter() {
                push_u16(buf, v);
            }
        }
        Request::ReadWriteMultipleRegisters(read_addr, read_qty, write_addr, data) => {
            push_u16(buf, *read_addr);
            push_u16(buf, *read_qty);
            push_u16(buf, *write_addr);
            push_u16(buf, data.len() as u16);
            let byte_count = safe_u8(data.len() * 2, "read-write reg byte count")?;
            buf.put_u8(byte_count);
            for &v in data.iter() {
                push_u16(buf, v);
            }
        }
        Request::MaskWriteRegister(addr, and_mask, or_mask) => {
            push_u16(buf, *addr);
            push_u16(buf, *and_mask);
            push_u16(buf, *or_mask);
        }
        Request::Diagnostic(sf, data) => {
            push_u16(buf, *sf);
            push_u16(buf, *data);
        }
        // Disconnect is a virtual request — produces no bytes on the wire.
        // The caller (AsciiClient::send_recv) checks for it before calling encode.
        Request::Disconnect => {}
    }
    Ok(())
}

fn decode_request(fc: u8, data: &mut Bytes) -> Result<Request<'static>, Error> {
    // Macro to read u16 with bounds check — never panics on malformed data
    macro_rules! read_u16 {
        ($data:expr) => {{
            if $data.remaining() < 2 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "truncated PDU"));
            }
            $data.get_u16()
        }};
    }
    macro_rules! read_u8 {
        ($data:expr) => {{
            if $data.remaining() < 1 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "truncated PDU"));
            }
            $data.get_u8()
        }};
    }

    Ok(match fc {
        1 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            if qty == 0 || qty > MAX_COILS {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadCoils: qty {qty} not in 1..={MAX_COILS}"),
                ));
            }
            if addr as u32 + qty as u32 > 0x10000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadCoils: addr {addr} + qty {qty} exceeds 0xFFFF"),
                ));
            }
            Request::ReadCoils(addr, qty)
        }
        2 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            if qty == 0 || qty > MAX_COILS {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadDiscreteInputs: qty {qty} not in 1..={MAX_COILS}"),
                ));
            }
            if addr as u32 + qty as u32 > 0x10000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadDiscreteInputs: addr {addr} + qty {qty} exceeds 0xFFFF"),
                ));
            }
            Request::ReadDiscreteInputs(addr, qty)
        }
        3 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            if qty == 0 || qty > MAX_REGISTERS {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadHoldingRegisters: qty {qty} not in 1..={MAX_REGISTERS}"),
                ));
            }
            if addr as u32 + qty as u32 > 0x10000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadHoldingRegisters: addr {addr} + qty {qty} exceeds 0xFFFF"),
                ));
            }
            Request::ReadHoldingRegisters(addr, qty)
        }
        4 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            if qty == 0 || qty > MAX_REGISTERS {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadInputRegisters: qty {qty} not in 1..={MAX_REGISTERS}"),
                ));
            }
            if addr as u32 + qty as u32 > 0x10000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("ReadInputRegisters: addr {addr} + qty {qty} exceeds 0xFFFF"),
                ));
            }
            Request::ReadInputRegisters(addr, qty)
        }
        5 => {
            let addr = read_u16!(data);
            let raw = read_u16!(data);
            let val = match raw {
                0xFF00 => true,
                0x0000 => false,
                other => return Err(Error::new(ErrorKind::InvalidData,
                    format!("WriteSingleCoil: invalid coil value {other:#06X}, expected 0xFF00 or 0x0000"))),
            };
            Request::WriteSingleCoil(addr, val)
        }
        6 => {
            let addr = read_u16!(data);
            let val = read_u16!(data);
            Request::WriteSingleRegister(addr, val)
        }
        15 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            if qty == 0 || qty > MAX_COILS {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("WriteMultipleCoils: qty {qty} not in 1..={MAX_COILS}"),
                ));
            }
            if addr as u32 + qty as u32 > 0x10000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("WriteMultipleCoils: addr {addr} + qty {qty} exceeds 0xFFFF"),
                ));
            }
            let qty = qty as usize;
            let byte_count = read_u8!(data) as usize;
            let expected_byte_count = qty.div_ceil(8);
            if byte_count != expected_byte_count {
                return Err(Error::new(ErrorKind::InvalidData,
                    format!("WriteMultipleCoils: byte_count {byte_count} != ceil(qty/8) ({expected_byte_count})")));
            }
            if data.remaining() < byte_count {
                return Err(Error::new(ErrorKind::UnexpectedEof, "truncated coil data"));
            }
            let mut values = Vec::with_capacity(byte_count * 8);
            for _ in 0..byte_count {
                let byte = data.get_u8();
                for i in 0..8 {
                    values.push(byte & (1 << i) != 0);
                    if values.len() >= qty {
                        break;
                    }
                }
            }
            values.truncate(qty);
            Request::WriteMultipleCoils(addr, Cow::Owned(values))
        }
        16 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            if qty == 0 || qty > MAX_REGISTERS {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("WriteMultipleRegisters: qty {qty} not in 1..={MAX_REGISTERS}"),
                ));
            }
            if addr as u32 + qty as u32 > 0x10000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("WriteMultipleRegisters: addr {addr} + qty {qty} exceeds 0xFFFF"),
                ));
            }
            let qty = qty as usize;
            let byte_count = read_u8!(data) as usize;
            if byte_count != qty * 2 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "WriteMultipleRegisters: byte_count {byte_count} != qty*2 ({})",
                        qty * 2
                    ),
                ));
            }
            if data.remaining() < qty * 2 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated register data",
                ));
            }
            let mut values = Vec::with_capacity(qty);
            for _ in 0..qty {
                values.push(data.get_u16());
            }
            Request::WriteMultipleRegisters(addr, Cow::Owned(values))
        }
        22 => {
            let addr = read_u16!(data);
            let and_mask = read_u16!(data);
            let or_mask = read_u16!(data);
            Request::MaskWriteRegister(addr, and_mask, or_mask)
        }
        23 => {
            let read_addr = read_u16!(data);
            let read_qty = read_u16!(data);
            if read_qty == 0 || read_qty > MAX_REGISTERS {
                return Err(Error::new(ErrorKind::InvalidData,
                    format!("ReadWriteMultipleRegisters: read_qty {read_qty} not in 1..={MAX_REGISTERS}")));
            }
            if read_addr as u32 + read_qty as u32 > 0x10000 {
                return Err(Error::new(ErrorKind::InvalidData,
                    format!("ReadWriteMultipleRegisters: read_addr {read_addr} + read_qty {read_qty} exceeds 0xFFFF")));
            }
            let write_addr = read_u16!(data);
            let write_qty = read_u16!(data);
            if write_qty == 0 || write_qty > MAX_REGISTERS {
                return Err(Error::new(ErrorKind::InvalidData,
                    format!("ReadWriteMultipleRegisters: write_qty {write_qty} not in 1..={MAX_REGISTERS}")));
            }
            if write_addr as u32 + write_qty as u32 > 0x10000 {
                return Err(Error::new(ErrorKind::InvalidData,
                    format!("ReadWriteMultipleRegisters: write_addr {write_addr} + write_qty {write_qty} exceeds 0xFFFF")));
            }
            let write_qty = write_qty as usize;
            let byte_count = read_u8!(data) as usize;
            if byte_count != write_qty * 2 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "ReadWriteMultipleRegisters: byte_count {byte_count} != write_qty*2 ({})",
                        write_qty * 2
                    ),
                ));
            }
            if data.remaining() < write_qty * 2 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated R/W register data",
                ));
            }
            let mut values = Vec::with_capacity(write_qty);
            for _ in 0..write_qty {
                values.push(data.get_u16());
            }
            Request::ReadWriteMultipleRegisters(read_addr, read_qty, write_addr, Cow::Owned(values))
        }
        8 => {
            let sf = read_u16!(data);
            let d = read_u16!(data);
            Request::Diagnostic(sf, d)
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unknown function code: {fc:#04X}"),
            ))
        }
    })
}

fn encode_response(rsp: &Response, buf: &mut BytesMut) -> Result<(), Error> {
    match rsp {
        Response::ReadCoils(bits) | Response::ReadDiscreteInputs(bits) => {
            let byte_count = safe_u8(bits.len().div_ceil(8), "coil byte count")?;
            buf.put_u8(rsp.function_code().value());
            buf.put_u8(byte_count);
            for chunk in bits.chunks(8) {
                let mut byte = 0u8;
                for (i, &v) in chunk.iter().enumerate() {
                    if v {
                        byte |= 1 << i;
                    }
                }
                buf.put_u8(byte);
            }
        }
        Response::ReadHoldingRegisters(regs) | Response::ReadInputRegisters(regs) => {
            buf.put_u8(rsp.function_code().value());
            buf.put_u8(safe_u8(regs.len() * 2, "register byte count")?);
            for &v in regs {
                push_u16(buf, v);
            }
        }
        Response::ReadWriteMultipleRegisters(regs) => {
            buf.put_u8(rsp.function_code().value());
            buf.put_u8(safe_u8(regs.len() * 2, "register byte count")?);
            for &v in regs {
                push_u16(buf, v);
            }
        }
        Response::WriteSingleCoil(addr, val) => {
            buf.put_u8(rsp.function_code().value());
            push_u16(buf, *addr);
            buf.put_u16(if *val { 0xFF00 } else { 0x0000 });
        }
        Response::WriteSingleRegister(addr, val) => {
            buf.put_u8(rsp.function_code().value());
            push_u16(buf, *addr);
            push_u16(buf, *val);
        }
        Response::WriteMultipleCoils(addr, qty) => {
            buf.put_u8(rsp.function_code().value());
            push_u16(buf, *addr);
            push_u16(buf, *qty);
        }
        Response::WriteMultipleRegisters(addr, qty) => {
            buf.put_u8(rsp.function_code().value());
            push_u16(buf, *addr);
            push_u16(buf, *qty);
        }
        Response::MaskWriteRegister(addr, and_mask, or_mask) => {
            buf.put_u8(rsp.function_code().value());
            push_u16(buf, *addr);
            push_u16(buf, *and_mask);
            push_u16(buf, *or_mask);
        }
        Response::Diagnostic(sf, data) => {
            buf.put_u8(rsp.function_code().value());
            push_u16(buf, *sf);
            push_u16(buf, *data);
        }
        Response::Exception(fc, exception) => {
            buf.put_u8(*fc | 0x80);
            buf.put_u8(u8::from(*exception));
        }
    }
    Ok(())
}

fn decode_response(fc: u8, data: &mut Bytes) -> Result<Response, Error> {
    macro_rules! read_u16 {
        ($data:expr) => {{
            if $data.remaining() < 2 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated response PDU",
                ));
            }
            $data.get_u16()
        }};
    }
    macro_rules! read_u8 {
        ($data:expr) => {{
            if $data.remaining() < 1 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated response PDU",
                ));
            }
            $data.get_u8()
        }};
    }

    Ok(match fc {
        1 | 2 => {
            let byte_count = read_u8!(data) as usize;
            if data.remaining() < byte_count {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated coil response",
                ));
            }
            let mut bits = Vec::with_capacity(byte_count * 8);
            for _ in 0..byte_count {
                let byte = data.get_u8();
                for i in 0..8 {
                    bits.push(byte & (1 << i) != 0);
                }
            }
            if fc == 1 {
                Response::ReadCoils(bits)
            } else {
                Response::ReadDiscreteInputs(bits)
            }
        }
        3 | 4 => {
            let byte_count = read_u8!(data) as usize;
            if byte_count % 2 != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("register response: byte_count {byte_count} is not even"),
                ));
            }
            if data.remaining() < byte_count {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated register response",
                ));
            }
            let mut regs = Vec::with_capacity(byte_count / 2);
            for _ in 0..(byte_count / 2) {
                regs.push(data.get_u16());
            }
            if fc == 3 {
                Response::ReadHoldingRegisters(regs)
            } else {
                Response::ReadInputRegisters(regs)
            }
        }
        5 => {
            let addr = read_u16!(data);
            let val = read_u16!(data) == 0xFF00;
            Response::WriteSingleCoil(addr, val)
        }
        6 => {
            let addr = read_u16!(data);
            let val = read_u16!(data);
            Response::WriteSingleRegister(addr, val)
        }
        15 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            Response::WriteMultipleCoils(addr, qty)
        }
        16 => {
            let addr = read_u16!(data);
            let qty = read_u16!(data);
            Response::WriteMultipleRegisters(addr, qty)
        }
        22 => {
            let addr = read_u16!(data);
            let and_mask = read_u16!(data);
            let or_mask = read_u16!(data);
            Response::MaskWriteRegister(addr, and_mask, or_mask)
        }
        23 => {
            let byte_count = read_u8!(data) as usize;
            if byte_count % 2 != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "ReadWriteMultipleRegisters response: byte_count {byte_count} is not even"
                    ),
                ));
            }
            if data.remaining() < byte_count {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated R/W response",
                ));
            }
            let mut regs = Vec::with_capacity(byte_count / 2);
            for _ in 0..(byte_count / 2) {
                regs.push(data.get_u16());
            }
            Response::ReadWriteMultipleRegisters(regs)
        }
        8 => {
            let sf = read_u16!(data);
            let d = read_u16!(data);
            Response::Diagnostic(sf, d)
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unknown response function code: {fc}"),
            ))
        }
    })
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_function_codes_accepted() {
        // Standard FCs: 1-6, 8, 15, 16, 22, 23
        for fc in [1u8, 2, 3, 4, 5, 6, 8, 15, 16, 22, 23] {
            assert!(
                is_known_function_code(fc),
                "standard FC {fc} should be known"
            );
        }
        // Exception variants: standard + 0x80
        for fc in [
            0x81u8, 0x82, 0x83, 0x84, 0x85, 0x86, 0x88, 0x8F, 0x90, 0x96, 0x97,
        ] {
            assert!(
                is_known_function_code(fc),
                "exception FC 0x{fc:02X} should be known"
            );
        }
    }

    #[test]
    fn unknown_function_codes_rejected() {
        for fc in [0u8, 7, 9, 14, 18, 24, 0x80, 0x87, 0xFF] {
            assert!(
                !is_known_function_code(fc),
                "unknown FC {fc} should NOT be known"
            );
        }
    }
}
