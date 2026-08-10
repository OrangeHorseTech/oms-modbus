// SPDX-License-Identifier: MIT OR Apache-2.0
//! Modbus ASCII client and server — generic over transport.
//!
//! ASCII frames are hex-encoded with `:` prefix and CRLF suffix, making them
//! human-readable but ~2× larger than RTU at the same baud rate. Works with
//! any `AsyncRead + AsyncWrite` byte transport.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use oms_modbus::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // In-memory duplex — works with any AsyncRead + AsyncWrite
//! let (client_port, server_port) = tokio::io::duplex(1024);
//!
//! // ── Server ──────────────────────────────────────────────────
//! let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
//! let server = ascii::AsciiServer::new(server_port);
//! tokio::spawn(async move { server.serve_forever(store).await.ok(); });
//!
//! // ── Client ──────────────────────────────────────────────────
//! let client = ascii::AsciiClient::with_timeout(client_port, Duration::from_secs(3));
//! let regs = client.read_holding_registers(1, 0, 1).await?;
//! # Ok(())
//! # }
//! ```
//!
//! For bus-level capture and timing, use [`with_options`] which wraps the
//! transport in [`SniffIo`] automatically.

use std::fmt::Debug;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::bus_timing::BusTiming;
use crate::client::ModbusClient;
use crate::codec;
use crate::error::ModbusError;
use crate::error::*;
use crate::frame::{Request, Response};
use crate::options::ClientOptions;
use crate::transport::send_recv;
use crate::transport::sniff_io::SniffIo;
use crate::transport::MAX_ADU_SIZE;
use crate::WireTap;

/// Default server read timeout for ASCII — generous 100ms to account for
/// slow hex encoding (each byte = 2 hex chars + colon + CRLF overhead).
const DEFAULT_ASCII_SERVER_TIMEOUT_US: u64 = 100_000;

/// Reconnect factory function type: creates a new transport on reconnect.
type ReconnectFactory<T> = (
    crate::reconnect::ReconnectConfig,
    Box<dyn Fn() -> io::Result<T> + Send + Sync>,
);

/// Modbus ASCII client — `Send + Sync + Clone` via internal `Mutex`.
///
/// Works with any `AsyncRead + AsyncWrite` byte transport. ASCII frames are
/// hex-encoded with `:` prefix and CRLF suffix, making them human-readable
/// but ~2× larger than RTU at the same baud rate.
///
/// For bus-level capture, use [`with_options`] which wraps the transport in
/// [`SniffIo`] automatically.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use oms_modbus::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let (client_port, _server) = tokio::io::duplex(1024);
/// let client = ascii::AsciiClient::with_timeout(client_port, Duration::from_secs(3));
/// let regs = client.read_holding_registers(1, 0, 5).await?;
/// # Ok(())
/// # }
/// ```
pub struct AsciiClient<T> {
    inner: Mutex<AsciiInner<T>>,
    timeout: Duration,
    reconnect: Option<ReconnectFactory<T>>,
    /// Preserved for reconnect rebuilds — active timing lives in SniffIo.
    bus_timing: Option<Arc<BusTiming>>,
    /// Preserved for reconnect rebuilds — active tap lives in SniffIo.
    tap: Option<Arc<dyn WireTap>>,
}

struct AsciiInner<T> {
    stream: SniffIo<T>,
    write_buf: BytesMut,
    read_buf: BytesMut,
}

impl<T> Debug for AsciiClient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AsciiClient");
        d.field("timeout", &self.timeout);
        if let Some((cfg, _)) = &self.reconnect {
            d.field("reconnect_max_retries", &cfg.max_retries());
            d.field("reconnect_interval", &cfg.interval());
        }
        d.finish()
    }
}

impl<T> AsciiClient<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Create an ASCII client with a 5-second default timeout.
    pub fn new(transport: T) -> Self {
        Self::with_timeout(transport, Duration::from_secs(5))
    }

    /// Create an ASCII client with an explicit request timeout.
    pub fn with_timeout(transport: T, timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(AsciiInner {
                stream: SniffIo::new(transport, None, None),
                write_buf: BytesMut::with_capacity(MAX_ADU_SIZE),
                read_buf: BytesMut::with_capacity(MAX_ADU_SIZE),
            }),
            timeout,
            reconnect: None,
            bus_timing: None,
            tap: None,
        }
    }

    /// Enable auto-reconnect with a factory that creates the FULL transport.
    ///
    /// For `AsciiClient<SniffIo<T>>`, prefer `with_reconnect_on` which
    /// accepts a raw-transport factory and handles SniffIo wrapping.
    pub fn with_reconnect<F>(mut self, max_retries: u32, backoff: Duration, factory: F) -> Self
    where
        F: Fn() -> io::Result<T> + Send + Sync + 'static,
    {
        self.reconnect = Some((
            crate::reconnect::ReconnectConfig::new(max_retries, backoff),
            Box::new(factory),
        ));
        self
    }

    /// ── ASCII receive: CRLF-delimited ────────────────────────────────────
    ///
    /// Accumulates bytes until `\r\n` is found, then decodes hex + validates
    /// LRC once. No per-byte scanning beyond the CRLF check.
    async fn send_recv(
        &self,
        slave_id: u8,
        request: &Request<'_>,
    ) -> Result<Response, ModbusError> {
        if *request == Request::Disconnect {
            // Disconnect is a client-initiated close request, not a protocol
            // error — Other is the appropriate category (no Connection/Protocol match).
            return Err(ModbusError::Other("Disconnected".into()));
        }

        let mut inner = self.inner.lock().await;
        let inner = &mut *inner;

        // ── Pre-send: drain stale data (all protocols, half-duplex) ──────
        let mut scratch = [0u8; MAX_ADU_SIZE];
        send_recv::drain_stale_data(&mut inner.stream, &mut scratch).await?;

        // ── Prepare bus timing ────────────────────────────────────────────
        inner.stream.prepare_send().await;

        // ── Send ──────────────────────────────────────────────────────────
        send_recv::send_frame(
            &mut inner.stream,
            &mut inner.write_buf,
            slave_id,
            self.timeout,
            request,
            codec::encode_ascii_frame_into,
        )
        .await?;

        // ── Receive: accumulate until CRLF ────────────────────────────────
        inner.read_buf.clear();
        let deadline = Instant::now() + self.timeout;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                if let Some(result) = try_parse_ascii_response(&inner.read_buf, slave_id) {
                    return result;
                }
                return Err(ModbusError::timeout("ASCII_RECV_TIMEOUT"));
            }

            match tokio::time::timeout(remaining, inner.stream.read_buf(&mut inner.read_buf)).await
            {
                Ok(Ok(0)) => return Err(ModbusError::connection("connection closed")),
                Ok(Ok(_)) => {
                    if let Some(result) = try_parse_ascii_response(&inner.read_buf, slave_id) {
                        return result;
                    }
                }
                Ok(Err(e)) => return Err(ModbusError::from(e)),
                Err(_elapsed) => {
                    if let Some(result) = try_parse_ascii_response(&inner.read_buf, slave_id) {
                        return result;
                    }
                    return Err(ModbusError::timeout("ASCII_RECV_TIMEOUT"));
                }
            }
        }
    }
}

// ── ASCII frame parsing (inline, not in codec) ──────────────────────────

const ASCII_START: u8 = b':';
const CR: u8 = b'\r';
const LF: u8 = b'\n';

/// Try to parse a response from the buffer. Returns:
/// - `Some(Ok(response))` if a valid frame for `slave_id` was found
/// - `Some(Err(e))` if a frame was found but slave_id mismatched or PDU decoding failed
/// - `None` if no complete valid frame could be parsed
fn try_parse_ascii_response(buf: &[u8], slave_id: u8) -> Option<Result<Response, ModbusError>> {
    let (rsp_slave, pdu, _consumed) = try_parse_ascii(buf)?;
    if rsp_slave != slave_id {
        return Some(Err(ModbusError::protocol(format!(
            "{SLAVE_ID_MISMATCH} {slave_id}, got {rsp_slave}"
        ))));
    }
    Some(
        Response::try_from(pdu)
            .map_err(|e| ModbusError::protocol(format!("{PDU_DECODE_ERROR} {e}"))),
    )
}

/// Scan `buf` for the first valid ASCII Modbus frame, skipping any
/// garbage CRLF-terminated lines encountered along the way.
///
/// A "garbage line" is any CRLF-delimited segment that does not start with
/// `:`, contains non-hex characters, has odd hex length, fails LRC
/// validation, or is too short to contain a slave address + function code.
///
/// Returns `None` when no CRLF is found or only garbage lines remain.
/// The returned `consumed` includes all skipped garbage bytes so the
/// caller can `buf.advance(consumed)` and loop to parse the next frame.
fn try_parse_ascii(buf: &[u8]) -> Option<(u8, Bytes, usize)> {
    let mut offset = 0;

    loop {
        let remaining = buf.get(offset..)?;
        // Find the next CRLF-terminated line.
        let crlf_pos = remaining.windows(2).position(|w| w == [CR, LF])?;
        let line_end = offset + crlf_pos + 2; // first byte after CRLF
        let content = &buf[offset..line_end - 2]; // bytes before CRLF

        // Only attempt decode when the line starts with ':'.
        if !content.is_empty() && content[0] == ASCII_START {
            let hex_chars = &content[1..];
            if hex_chars.len() % 2 == 0 {
                let mut bytes = Vec::with_capacity(hex_chars.len() / 2);
                let mut ok = true;
                for chunk in hex_chars.chunks(2) {
                    match hex_decode_byte(chunk[0], chunk[1]) {
                        Ok(b) => bytes.push(b),
                        Err(_) => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok && bytes.len() >= 2 {
                    let (data, lrc_byte) = bytes.split_at(bytes.len() - 1);
                    if lrc_byte[0] == codec::calculate_lrc(data) && data.len() >= 2 {
                        return Some((data[0], Bytes::copy_from_slice(&data[1..]), line_end));
                    }
                }
            }
        }

        // This line is garbage — skip past it and try the next one.
        offset = line_end;
    }
}

fn hex_decode_byte(hi: u8, lo: u8) -> io::Result<u8> {
    fn nibble(c: u8) -> io::Result<u8> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid hex character",
            )),
        }
    }
    Ok((nibble(hi)? << 4) | nibble(lo)?)
}

// ── Reconnect for SniffIo-wrapped clients ───────────────────────────────

impl<T> AsciiClient<SniffIo<T>>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Enable auto-reconnect with a factory that creates the RAW transport.
    ///
    /// The client automatically re-wraps the new transport in `SniffIo`
    /// with the same timing as the original.
    pub fn with_reconnect_on<F>(mut self, max_retries: u32, backoff: Duration, factory: F) -> Self
    where
        F: Fn() -> io::Result<T> + Send + Sync + 'static,
    {
        let timing = self.bus_timing.clone();
        let tap = self.tap.clone();
        self.reconnect = Some((
            crate::reconnect::ReconnectConfig::new(max_retries, backoff),
            Box::new(move || {
                let raw = factory()?;
                Ok(SniffIo::new(raw, tap.clone(), timing.clone()))
            }),
        ));
        self
    }
}

/// Create an ASCII client with optional capture and reconnect.
///
/// Wraps the transport in [`SniffIo`] so that the configured [`WireTap`]
/// can observe raw bus traffic. If no tap is configured, `SniffIo` passes
/// through with near-zero overhead.
pub fn with_options<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    transport: T,
    opts: ClientOptions,
) -> AsciiClient<SniffIo<T>> {
    let tap = opts.tap().cloned();
    let timing = opts.bus_timing.clone();
    let mut stream = SniffIo::new(transport, tap, timing.clone());
    if let Some(cap) = opts.data_channel_capacity {
        stream = stream.with_channel_capacity(cap);
    }
    if let Some(cap) = opts.tap_channel_capacity {
        stream = stream.with_tap_channel_capacity(cap);
    }
    let mut raw = AsciiClient::with_timeout(stream, opts.timeout);
    raw.bus_timing = timing;
    raw.tap = opts.tap().cloned();
    raw
}

#[async_trait]
impl<T> ModbusClient for AsciiClient<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    async fn call(&self, slave: u8, request: Request<'_>) -> Result<Response, ModbusError> {
        let request = request.into_owned();
        let slave_id = slave;
        let timing = self.bus_timing.clone();

        send_recv::run_with_reconnect(
            self.reconnect.as_ref().map(|(cfg, _)| cfg),
            || self.send_recv(slave_id, &request),
            || async {
                let mut inner = self.inner.lock().await;
                if let Some((_, factory)) = &self.reconnect {
                    if let Ok(new) = factory() {
                        inner.stream = SniffIo::new(new, self.tap.clone(), timing.clone());
                        inner.write_buf.clear();
                        inner.read_buf.clear();
                        return true;
                    }
                }
                false
            },
            ModbusError::serial,
        )
        .await
    }
}

// ── ASCII Server ────────────────────────────────────────────────────────

/// Modbus ASCII server over any byte transport (serial, DuplexStream, etc.).
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use oms_modbus::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let (port, _server_side) = tokio::io::duplex(64);
/// let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
///
/// let server = ascii::AsciiServer::new(port);
/// tokio::spawn(async move { server.serve_forever(store).await.ok(); });
/// # Ok(())
/// # }
/// ```
pub struct AsciiServer<T> {
    transport: T,
    bus_timing: Option<Arc<BusTiming>>,
    /// Per-frame read timeout — when no CRLF arrives within this window,
    /// accumulated bytes are flushed. Defaults to `BusTiming::min_spacing()`
    /// when set, or 100 ms otherwise.
    read_timeout: Option<Duration>,
}

impl<T> AsciiServer<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            bus_timing: None,
            read_timeout: None,
        }
    }

    /// Enforce bus timing on the server side (optional).
    pub fn with_bus_timing(mut self, timing: Arc<BusTiming>) -> Self {
        self.bus_timing = Some(timing);
        self
    }

    /// Set a custom per-frame read timeout. When this timeout expires
    /// without receiving a complete CRLF-terminated frame, accumulated
    /// bytes in the buffer are discarded.
    ///
    /// Default: [`BusTiming::min_spacing`] if bus timing is configured,
    /// otherwise 100 ms.
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Run the server loop until the transport closes.
    pub async fn serve_forever<S>(self, service: S) -> std::io::Result<()>
    where
        S: crate::server::Service + Send + Sync + 'static,
    {
        let timing = self.bus_timing.clone();
        let mut stream = SniffIo::new(self.transport, None, timing.clone());
        let mut buf = BytesMut::with_capacity(MAX_ADU_SIZE);
        let mut rsp_buf = BytesMut::with_capacity(MAX_ADU_SIZE);
        let mut frame_buf = BytesMut::with_capacity(MAX_ADU_SIZE);

        // Resolve read timeout: explicit user setting > BusTiming > default.
        let read_timeout = self
            .read_timeout
            .or_else(|| timing.as_ref().map(|t| t.min_spacing()))
            .unwrap_or(Duration::from_micros(DEFAULT_ASCII_SERVER_TIMEOUT_US));

        loop {
            let mut tmp = [0u8; MAX_ADU_SIZE];
            if buf.is_empty() {
                match stream.read(&mut tmp).await {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    Err(_) => break,
                }
            } else {
                match tokio::time::timeout(read_timeout, stream.read(&mut tmp)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                    Ok(Err(_)) => break,
                    Err(_elapsed) => {
                        buf.clear();
                        continue;
                    }
                }
            }
            while let Some((slave_id, pdu, consumed)) = try_parse_ascii(&buf) {
                buf.advance(consumed);
                if let Some(rsp_data) =
                    send_recv::process_server_request(&pdu, slave_id, &service, &mut rsp_buf).await
                {
                    stream.prepare_send().await;
                    frame_buf.clear();
                    codec::encode_ascii_frame_into(&rsp_data, &mut frame_buf);
                    if stream.write_all(&frame_buf).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a CRLF-terminated ASCII Modbus frame from raw bytes.
    /// Computes LRC = wrapping_neg(sum of bytes) and hex-encodes everything.
    fn make_ascii_frame(bytes: &[u8]) -> Vec<u8> {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let lrc = bytes
            .iter()
            .fold(0u8, |acc, &b| acc.wrapping_add(b))
            .wrapping_neg();
        let mut frame = vec![b':'];
        for &b in bytes.iter().chain(std::iter::once(&lrc)) {
            frame.push(HEX[(b >> 4) as usize]);
            frame.push(HEX[(b & 0x0F) as usize]);
        }
        frame.extend_from_slice(b"\r\n");
        frame
    }

    // ── try_parse_ascii ─────────────────────────────────────────────────

    #[test]
    fn empty_buffer() {
        assert!(try_parse_ascii(&[]).is_none());
    }

    #[test]
    fn no_crlf_terminator() {
        let data = b":010300000001FB\r";
        assert!(try_parse_ascii(data).is_none());
    }

    #[test]
    fn cr_without_lf() {
        let data = b":010300000001FB\rX";
        assert!(try_parse_ascii(data).is_none());
    }

    #[test]
    fn valid_read_holding_request() {
        let frame = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let (slave, pdu, consumed) = try_parse_ascii(&frame).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 3);
        assert_eq!(pdu.len(), 5);
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn no_colon_prefix_invalid() {
        let data = b"010300000001FB\r\n";
        assert!(try_parse_ascii(data).is_none());
    }

    #[test]
    fn bad_hex_characters() {
        let data = b":01G300000001XX\r\n";
        assert!(try_parse_ascii(data).is_none());
    }

    #[test]
    fn odd_hex_length() {
        let valid = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let mut data = b":01030\r\n".to_vec();
        data.extend_from_slice(&valid);
        let (slave, pdu, _) = try_parse_ascii(&data).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 3);
    }

    #[test]
    fn bad_lrc_recovery() {
        let mut bad = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let crlf_pos = bad.windows(2).position(|w| w == b"\r\n").unwrap();
        bad[crlf_pos - 2] = b'F';
        bad[crlf_pos - 1] = b'F';
        let good = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let mut data = bad;
        data.extend_from_slice(&good);
        let (slave, pdu, consumed) = try_parse_ascii(&data).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 3);
        assert!(consumed > 20);
    }

    #[test]
    fn too_short_after_hex_decode() {
        let valid = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let mut data = b":01\r\n".to_vec();
        data.extend_from_slice(&valid);
        let (slave, pdu, _) = try_parse_ascii(&data).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 3);
    }

    #[test]
    fn lrc_validates_correctly() {
        let frame = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let (slave, pdu, _) = try_parse_ascii(&frame).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 3);
    }

    #[test]
    fn lowercase_hex_accepted() {
        let frame = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let lower: Vec<u8> = frame
            .iter()
            .map(|&b| if b.is_ascii_uppercase() { b + 32 } else { b })
            .collect();
        let (slave, pdu, _) = try_parse_ascii(&lower).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 3);
    }

    #[test]
    fn multiple_valid_frames_parses_first_only() {
        let first = make_ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let second = make_ascii_frame(&[0x01, 0x03, 0x00, 0x01, 0x00, 0x01]);
        let mut data = first.clone();
        data.extend_from_slice(&second);
        let (slave, pdu, consumed) = try_parse_ascii(&data).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[3], 0);
        assert_eq!(consumed, first.len());
    }

    // ── hex_decode_byte ─────────────────────────────────────────────────

    #[test]
    fn hex_decode_all_nibbles() {
        assert_eq!(hex_decode_byte(b'0', b'0').unwrap(), 0x00);
        assert_eq!(hex_decode_byte(b'0', b'1').unwrap(), 0x01);
        assert_eq!(hex_decode_byte(b'1', b'0').unwrap(), 0x10);
        assert_eq!(hex_decode_byte(b'F', b'F').unwrap(), 0xFF);
        assert_eq!(hex_decode_byte(b'f', b'f').unwrap(), 0xFF);
        assert_eq!(hex_decode_byte(b'A', b'a').unwrap(), 0xAA);
    }

    #[test]
    fn hex_decode_invalid_high_nibble() {
        assert!(hex_decode_byte(b'G', b'0').is_err());
    }

    #[test]
    fn hex_decode_invalid_low_nibble() {
        assert!(hex_decode_byte(b'0', b'G').is_err());
    }
}
