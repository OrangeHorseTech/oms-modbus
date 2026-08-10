// SPDX-License-Identifier: MIT OR Apache-2.0
//! Modbus TCP client and server with MBAP framing.
//!
//! # Quick start
//!
//! ```no_run
//! use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//! use std::sync::Arc;
//! use std::time::Duration;
//! use oms_modbus::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // ── Server ──────────────────────────────────────────────────
//! let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 502);
//! let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1234)]));
//! let server = tcp::TcpServer::bind(addr).await?;
//! tokio::spawn(async move { server.serve_forever(store).await.ok(); });
//!
//! // ── Client ──────────────────────────────────────────────────
//! let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3)).await?;
//! let regs = client.read_holding_registers(1, 0, 1).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Advanced MBAP configuration
//!
//! Use [`TcpConfig`] for non-standard devices and RTU-over-TCP gateways:
//!
//! ```no_run
//! use oms_modbus::tcp::{TcpConfig, TidMode, LengthMode};
//! # use std::net::SocketAddr;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let addr = "127.0.0.1:502".parse()?;
//! let client = tcp::TcpClient::connect(addr).await?.with_config(TcpConfig {
//!     tid: TidMode::Auto,                // auto-increment transaction ID
//!     unit_id_in_body: true,             // UID also in PDU body
//!     length_mode: LengthMode::Standard,
//! });
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::panic::AssertUnwindSafe;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::FutureExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::bus_timing::BusTiming;
use crate::client::ModbusClient;
use crate::error::ModbusError;
use crate::error::*;
use crate::frame::{Request, Response};
use crate::options::ClientOptions;
use crate::transport::send_recv;
use crate::transport::sniff_io::SniffIo;
use crate::transport::{MAX_ADU_SIZE, MAX_TCP_ADU_SIZE, MBAP_HEADER_SIZE, MBAP_PREFIX_SIZE};
use crate::wire_tap::WireTap;

// ── TCP configuration ───────────────────────────────────────────────────

/// Transaction ID generation mode.
#[derive(Debug)]
pub enum TidMode {
    /// Always use the same value (default: 0).
    Fixed(u16),
    /// Auto-increment on each request, wrapping at u16::MAX.
    /// Starts from 1.
    Auto,
}

impl Clone for TidMode {
    fn clone(&self) -> Self {
        match self {
            TidMode::Fixed(v) => TidMode::Fixed(*v),
            TidMode::Auto => TidMode::Auto,
        }
    }
}

/// MBAP Length field calculation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LengthMode {
    /// Standard per spec: Length = 1 (UID at byte 6) + PDU bytes.
    /// Compatible with tokio-modbus and most devices.
    Standard,
    /// Length = PDU bytes only (excludes UID at byte 6).
    /// Some non-standard devices use this mode.
    PduOnly,
}

/// TCP-specific MBAP framing configuration.
///
/// Controls how the MBAP header is built. Defaults match the
/// Modbus TCP specification (TID=0, UID in header only, Standard length).
#[derive(Debug, Clone)]
pub struct TcpConfig {
    pub tid: TidMode,
    /// If true, the unit_id is also written as the first byte of the PDU body
    /// (in addition to MBAP header byte 6). Required by some TCP-to-RTU gateways.
    pub unit_id_in_body: bool,
    /// How the MBAP Length field is calculated.
    pub length_mode: LengthMode,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            tid: TidMode::Fixed(0),
            unit_id_in_body: false,
            length_mode: LengthMode::Standard,
        }
    }
}

impl TcpConfig {
    /// Standard Modbus TCP: TID=0, UID in MBAP header only, Standard length.
    pub fn standard() -> Self {
        Self::default()
    }

    /// Gateway-friendly: auto-increment TID from 1, UID in PDU body, Standard length.
    pub fn gateway() -> Self {
        Self {
            tid: TidMode::Auto,
            unit_id_in_body: true,
            length_mode: LengthMode::Standard,
        }
    }
}

// ── TCP frame encoding / decoding ────────────────────────────────────────

fn next_tid(mode: &TidMode, counter: &AtomicU16) -> u16 {
    match mode {
        TidMode::Fixed(v) => *v,
        TidMode::Auto => counter.fetch_add(1, Ordering::Relaxed),
    }
}

/// Encode [slave_id, pdu...] into a TCP MBAP frame.
pub fn encode_tcp_frame(data: &[u8], buf: &mut BytesMut, config: &TcpConfig, tid: u16) {
    let (unit_id, pdu) = if data.is_empty() {
        (1u8, &[] as &[u8])
    } else {
        (data[0], &data[1..])
    };
    let extra = if config.unit_id_in_body { 1u16 } else { 0u16 };
    let len = match config.length_mode {
        LengthMode::Standard => pdu.len() as u16 + 1 + extra,
        LengthMode::PduOnly => pdu.len() as u16 + extra,
    };
    buf.put_u16(tid);
    buf.put_u16(0); // Protocol ID
    buf.put_u16(len);
    buf.put_u8(unit_id);
    if config.unit_id_in_body {
        buf.put_u8(unit_id);
    }
    buf.extend_from_slice(pdu);
}

/// Try to parse one TCP frame from a byte buffer.
///
/// Returns `Some((tid, slave_id, pdu, bytes_consumed))` if MBAP header is complete
/// and frame data is sufficient. The `tid` (Transaction Identifier) is extracted
/// from the MBAP header for the server to echo back in its response.
/// Validates Protocol ID == 0 and max frame size.
///
/// Handles both normal frames (PDU byte 0 = function code) and `unit_id_in_body`
/// Check whether the first 7 bytes of `buf` contain a corrupt MBAP header.
///
/// Returns `false` when there aren't enough bytes yet or the header looks
/// valid (proto_id == 0, payload within range).  Returns `true` only when a
/// full header is present but structurally invalid — this is the signal to
/// discard the buffer rather than wait for more data.
fn is_tcp_header_corrupt(buf: &[u8]) -> bool {
    if buf.len() < MBAP_HEADER_SIZE {
        return false; // need more data
    }
    let proto_id = u16::from_be_bytes([buf[2], buf[3]]);
    let payload_len = u16::from_be_bytes([buf[4], buf[5]]) as usize;
    proto_id != 0 || !(1..=MAX_TCP_ADU_SIZE).contains(&payload_len)
}

/// Safety cap: clear the buffer if it grows beyond 4× the maximum legal ADU.
/// A well-formed stream should never exceed this; oversized buffers indicate
/// a corrupted length field or a peer that isn't speaking Modbus TCP.
const MAX_TCP_BUF_BYTES: usize = MAX_TCP_ADU_SIZE * 4; // 1040 bytes

/// frames (PDU byte 0 = duplicate slave ID, byte 1 = function code). When the
/// first body byte matches the MBAP Unit ID, both interpretations are tested
/// and the one that parses as a valid [`Request`] is returned.
pub fn try_parse_tcp_frame(buf: &[u8]) -> Option<(u16, u8, Bytes, usize)> {
    if buf.len() < MBAP_HEADER_SIZE {
        return None;
    }
    let tid = u16::from_be_bytes([buf[0], buf[1]]);
    let payload_len = u16::from_be_bytes([buf[4], buf[5]]) as usize;
    let proto_id = u16::from_be_bytes([buf[2], buf[3]]);
    if proto_id != 0 || payload_len > MAX_TCP_ADU_SIZE {
        return None;
    }

    let unit_id = buf[6];
    for &fl in &[
        MBAP_HEADER_SIZE + payload_len,
        MBAP_PREFIX_SIZE + payload_len,
    ] {
        // Guard: when payload_len < 1, fl can be < MBAP_HEADER_SIZE,
        // which would panic on buf[MBAP_HEADER_SIZE..fl].
        if fl > MBAP_HEADER_SIZE && buf.len() >= fl {
            let body = &buf[MBAP_HEADER_SIZE..fl];
            if body.is_empty() {
                continue;
            }
            return try_extract_tcp_pdu(tid, unit_id, body, fl);
        }
    }
    None
}

/// Given a TCP frame body (bytes after the MBAP header), try to
/// extract the PDU. Handles both normal and `unit_id_in_body` framing.
///
/// Priority: when byte 0 matches the MBAP Unit ID AND byte 1 is also a valid
/// function code, prefer the `unit_id_in_body` interpretation (skip byte 0).
/// This is correct because:
/// - `unit_id_in_body` clients put the slave ID in byte 0, which is always in
///   the valid FC range (1–247). Function codes are also 1–127. When both
///   interpretations parse as a valid Request, we must prefer unit_id_in_body.
/// - Normal clients have byte 0 = FC. The Unit ID in the MBAP header (byte 6)
///   is independent. The only time byte 0 == unit_id AND byte 1 is a valid FC
///   is when a unit_id_in_body client sent the frame.
fn try_extract_tcp_pdu(
    tid: u16,
    unit_id: u8,
    body: &[u8],
    consumed: usize,
) -> Option<(u16, u8, Bytes, usize)> {
    // Case 1: unit_id_in_body — byte 0 matches MBAP UID and byte 1 is a valid FC.
    if body.len() >= 2 && body[0] == unit_id && crate::frame::is_known_function_code(body[1]) {
        let pdu = Bytes::copy_from_slice(&body[1..]);
        if crate::frame::Request::try_from(pdu.clone()).is_ok() {
            return Some((tid, body[0], pdu, consumed));
        }
    }

    // Case 2: normal framing — byte 0 is the function code.
    // When body[0] == unit_id but Case 1 fell through, we're in an
    // ambiguous situation: body[0] could be a UID byte (unit_id_in_body
    // with unknown FC) or a normal FC that coincidentally equals the UID.
    // Try parsing as a Request; if it fails, reject rather than guessing.
    if crate::frame::is_known_function_code(body[0]) {
        let pdu = Bytes::copy_from_slice(body);
        if body[0] == unit_id && Request::try_from(pdu.clone()).is_err() {
            return None;
        }
        // Even if Request::try_from fails, return the PDU — the caller will
        // log the invalid request rather than dropping bytes.
        return Some((tid, unit_id, pdu, consumed));
    }

    None
}

// ── TCP client ──────────────────────────────────────────────────────────

/// Modbus TCP client — `Send + Sync + Clone` via internal `Mutex`.
///
/// Call `.with_reconnect(max, backoff)` to enable auto-reconnect on transport
/// errors. Without it, transport errors are returned directly (no retry).
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use oms_modbus::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let addr = "192.168.1.10:502".parse()?;
/// let client = tcp::TcpClient::connect_with_timeout(addr, Duration::from_secs(3)).await?;
/// let regs = client.read_holding_registers(1, 0, 10).await?;
/// client.write_single_register(1, 0, 42).await?;
/// # Ok(())
/// # }
/// ```
pub struct TcpClient {
    inner: Mutex<TcpInner>,
    addr: SocketAddr,
    timeout: Duration,
    reconnect: Option<crate::reconnect::ReconnectConfig>,
    tcp_config: TcpConfig,
    tcp_counter: AtomicU16,
    /// Preserved for reconnect rebuilds — active tap lives in SniffIo.
    tap: Option<Arc<dyn WireTap>>,
    /// Preserved for reconnect rebuilds — active timing lives in SniffIo.
    bus_timing: Option<Arc<BusTiming>>,
}

struct TcpInner {
    stream: SniffIo<TcpStream>,
    write_buf: BytesMut,
    read_buf: BytesMut,
}

impl std::fmt::Debug for TcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("TcpClient");
        d.field("addr", &self.addr);
        d.field("timeout", &self.timeout);
        if let Some(cfg) = &self.reconnect {
            d.field("reconnect_max_retries", &cfg.max_retries());
            d.field("reconnect_interval", &cfg.interval());
        }
        d.finish()
    }
}

impl TcpClient {
    /// Connect to a Modbus TCP server (default 5-second timeout).
    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        Self::connect_with_timeout(addr, Duration::from_secs(5)).await
    }

    /// Connect with an explicit connection timeout.
    pub async fn connect_with_timeout(
        addr: SocketAddr,
        timeout: Duration,
    ) -> std::io::Result<Self> {
        Self::connect_with_config(addr, timeout, TcpConfig::default()).await
    }

    async fn connect_with_config(
        addr: SocketAddr,
        timeout: Duration,
        tcp_config: TcpConfig,
    ) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            inner: Mutex::new(TcpInner {
                stream: SniffIo::new(stream, None, None),
                write_buf: BytesMut::with_capacity(MAX_ADU_SIZE),
                read_buf: BytesMut::with_capacity(MAX_ADU_SIZE),
            }),
            addr,
            timeout,
            reconnect: None,
            tcp_config: tcp_config.clone(),
            tcp_counter: AtomicU16::new(1),
            tap: None,
            bus_timing: None,
        })
    }

    /// Set MBAP framing configuration.
    ///
    /// Controls transaction ID, unit ID placement, and length calculation.
    /// Use [`TcpConfig::gateway`] for RTU-over-TCP gateways.
    pub fn with_config(mut self, config: TcpConfig) -> Self {
        self.tcp_config = config;
        self
    }

    /// Shortcut for gateway mode: auto TID, UID in body, standard length.
    pub fn with_gateway_mode(self) -> Self {
        self.with_config(TcpConfig::gateway())
    }

    /// Enable auto-reconnect on transport errors.
    /// `max_retries = 0` means infinite retry. Protocol errors are not retried.
    pub fn with_reconnect(mut self, max_retries: u32, backoff: Duration) -> Self {
        self.reconnect = Some(crate::reconnect::ReconnectConfig::new(max_retries, backoff));
        self
    }

    /// ── TCP receive: two-phase MBAP ─────────────────────────────────────
    ///
    /// Phase 1: read MBAP header, parse Length field.
    /// Phase 2: read remaining PDU bytes.
    /// No CRC — TCP checksum handles integrity.
    async fn send_recv(
        &self,
        slave_id: u8,
        request: &Request<'_>,
    ) -> Result<Response, ModbusError> {
        let mut inner = self.inner.lock().await;
        let inner = &mut *inner;

        // TCP is full-duplex, but stale bytes from a previous response or
        // unsolicited server data can accumulate in the receive buffer.
        // Drain before sending so the next read returns the correct response.
        let mut scratch = [0u8; MAX_ADU_SIZE];
        send_recv::drain_stale_data(&mut inner.stream, &mut scratch).await?;

        // ── Send ────────────────────────────────────────────────────────
        let tcp_cfg = self.tcp_config.clone();
        let tid = next_tid(&tcp_cfg.tid, &self.tcp_counter);
        send_recv::send_frame(
            &mut inner.stream,
            &mut inner.write_buf,
            slave_id,
            self.timeout,
            request,
            |data, buf| encode_tcp_frame(data, buf, &tcp_cfg, tid),
        )
        .await?;

        // ── Receive ──────────────────────────────────────────────────────
        inner.read_buf.clear();
        let deadline = Instant::now() + self.timeout;

        // Phase 1: read MBAP header (7 bytes)
        send_recv::read_at_least(
            &mut inner.stream,
            &mut inner.read_buf,
            deadline,
            MBAP_HEADER_SIZE,
        )
        .await?;

        if inner.read_buf.len() < MBAP_HEADER_SIZE {
            return Err(ModbusError::timeout(TCP_RECV_TIMEOUT));
        }

        let proto_id = u16::from_be_bytes([inner.read_buf[2], inner.read_buf[3]]);
        if proto_id != 0 {
            return Err(ModbusError::protocol("TCP: invalid Protocol ID"));
        }

        let payload_len = u16::from_be_bytes([inner.read_buf[4], inner.read_buf[5]]) as usize;
        if payload_len > MAX_TCP_ADU_SIZE {
            return Err(ModbusError::protocol("TCP: MBAP Length exceeds max ADU"));
        }

        // Phase 2: read PDU bytes.
        // The MBAP Length field is ambiguous:
        //   Standard (LengthMode::Standard): Length = PDU bytes + 1 (includes UID)
        //   Non-standard (LengthMode::PduOnly): Length = PDU bytes only
        // Frame sizes: min = MBAP_PREFIX_SIZE + payload_len (standard), max = MBAP_HEADER_SIZE + payload_len (PduOnly)
        let min_total = MBAP_PREFIX_SIZE + payload_len;
        send_recv::read_at_least(&mut inner.stream, &mut inner.read_buf, deadline, min_total)
            .await?;

        // If we only have min_total bytes, one more byte may be needed for PduOnly.
        let max_total = MBAP_HEADER_SIZE + payload_len;
        if inner.read_buf.len() < max_total {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                match tokio::time::timeout(
                    remaining.min(Duration::from_millis(200)),
                    inner.stream.read(&mut scratch),
                )
                .await
                {
                    Ok(Ok(n)) if n > 0 => {
                        inner.read_buf.extend_from_slice(&scratch[..n]);
                    }
                    _ => {}
                }
            }
        }

        let available = inner.read_buf.len();
        let mut pdu = if available >= max_total {
            Bytes::copy_from_slice(&inner.read_buf[MBAP_HEADER_SIZE..max_total])
        } else if available >= min_total {
            Bytes::copy_from_slice(&inner.read_buf[MBAP_HEADER_SIZE..min_total])
        } else {
            return Err(ModbusError::timeout(TCP_RECV_TIMEOUT));
        };

        if pdu.is_empty() {
            return Err(ModbusError::protocol(TCP_EMPTY_RESP));
        }

        // Strip unit_id_in_body prefix if configured — the MBAP header
        // already carries the Unit ID, but gateway devices may echo it
        // as the first byte of the body.
        if tcp_cfg.unit_id_in_body && pdu.len() > 1 && pdu[0] == slave_id {
            pdu = pdu.slice(1..);
        }

        // TCP does NOT verify slave_id — Unit ID in MBAP is informational
        Response::try_from(pdu)
            .map_err(|e| ModbusError::protocol(format!("{PDU_DECODE_ERROR} {e}")))
    }
}

#[async_trait]
impl ModbusClient for TcpClient {
    async fn call(&self, slave: u8, request: Request<'_>) -> Result<Response, ModbusError> {
        let request = request.into_owned();
        let slave_id = slave;

        send_recv::run_with_reconnect(
            self.reconnect.as_ref(),
            || self.send_recv(slave_id, &request),
            || async {
                let mut inner = self.inner.lock().await;
                if let Ok(stream) = TcpStream::connect(self.addr).await {
                    stream.set_nodelay(true).ok();
                    inner.stream = SniffIo::new(stream, self.tap.clone(), self.bus_timing.clone());
                    inner.write_buf.clear();
                    inner.read_buf.clear();
                    true
                } else {
                    false
                }
            },
            ModbusError::connection,
        )
        .await
    }
}

/// Create a TCP client with optional capture and reconnect.
pub async fn with_options(addr: SocketAddr, opts: ClientOptions) -> std::io::Result<TcpClient> {
    let tap = opts.tap().cloned();
    let timing = opts.bus_timing.clone();
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let mut sniff = SniffIo::new(stream, tap, timing);
    if let Some(cap) = opts.data_channel_capacity {
        sniff = sniff.with_channel_capacity(cap);
    }
    if let Some(cap) = opts.tap_channel_capacity {
        sniff = sniff.with_tap_channel_capacity(cap);
    }
    Ok(TcpClient {
        inner: Mutex::new(TcpInner {
            stream: sniff,
            write_buf: BytesMut::with_capacity(MAX_ADU_SIZE),
            read_buf: BytesMut::with_capacity(MAX_ADU_SIZE),
        }),
        addr,
        timeout: opts.timeout,
        reconnect: opts.reconnect,
        tcp_config: TcpConfig::default(),
        tcp_counter: AtomicU16::new(1),
        tap: opts.tap().cloned(),
        bus_timing: opts.bus_timing.clone(),
    })
}

// ── TCP server ──────────────────────────────────────────────────────────

/// Modbus TCP server. Spawns one tokio task per connection.
///
/// # Example
///
/// ```no_run
/// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
/// use std::sync::Arc;
/// use oms_modbus::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 502);
/// let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1234)]));
/// let server = tcp::TcpServer::bind(addr).await?;
/// println!("Listening on {}", server.local_addr()?);
/// // server.serve_forever(store).await?;  // blocks forever
/// # Ok(())
/// # }
/// ```
pub struct TcpServer {
    listener: tokio::net::TcpListener,
    tcp_config: TcpConfig,
}

impl TcpServer {
    /// Bind to a TCP address and prepare to accept connections.
    pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            tcp_config: TcpConfig::default(),
        })
    }

    /// Set the MBAP framing configuration for response encoding.
    ///
    /// Default is [`TcpConfig::standard`]. Use [`TcpConfig::gateway`] for
    /// RTU-over-TCP gateways, or construct a custom config with
    /// [`LengthMode::PduOnly`] for non-standard devices.
    pub fn with_config(mut self, config: TcpConfig) -> Self {
        self.tcp_config = config;
        self
    }

    /// The socket address this server is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept connections indefinitely, processing each with the given service.
    ///
    /// Handler panics are caught via [`futures_util::FutureExt::catch_unwind`]
    /// and logged rather than silently swallowed.
    pub async fn serve_forever<S>(self, service: S) -> std::io::Result<()>
    where
        S: crate::server::Service + Send + Sync + Clone + 'static,
    {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            log::info!("TCP server: new connection from {}", addr);
            let svc = service.clone();
            let tcp_cfg = self.tcp_config.clone();
            tokio::spawn(async move {
                let result = AssertUnwindSafe(handle_tcp_connection(stream, svc, tcp_cfg))
                    .catch_unwind()
                    .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        log::warn!("TCP server: connection {} error: {}", addr, e)
                    }
                    Err(_) => {
                        log::error!("TCP server: handler task panicked");
                    }
                }
            });
        }
    }
}

async fn handle_tcp_connection<S>(
    stream: TcpStream,
    service: S,
    tcp_cfg: TcpConfig,
) -> std::io::Result<()>
where
    S: crate::server::Service + Send + Sync + 'static,
{
    let mut sniff = SniffIo::new(stream, None, None);
    let mut buf = BytesMut::with_capacity(MAX_ADU_SIZE);
    let mut rsp_buf = BytesMut::with_capacity(MAX_ADU_SIZE);
    let mut frame_buf = BytesMut::with_capacity(MAX_ADU_SIZE);
    loop {
        let mut tmp = [0u8; MAX_ADU_SIZE];
        match sniff.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                while let Some((tid, slave_id, pdu, consumed)) = try_parse_tcp_frame(&buf) {
                    if let Some(rsp_data) =
                        send_recv::process_server_request(&pdu, slave_id, &service, &mut rsp_buf)
                            .await
                    {
                        frame_buf.clear();
                        encode_tcp_frame(&rsp_data, &mut frame_buf, &tcp_cfg, tid);
                        if sniff.write_all(&frame_buf).await.is_err() {
                            return Ok(());
                        }
                    }
                    let _ = buf.split_to(consumed);
                }
                // If unparseable bytes remain and the header is corrupt
                // (or the buffer is absurdly large), discard them so the
                // next valid frame isn't contaminated.
                if is_tcp_header_corrupt(&buf) || buf.len() > MAX_TCP_BUF_BYTES {
                    buf.clear();
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_parse_tcp_frame_empty_buffer() {
        assert!(try_parse_tcp_frame(&[]).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_incomplete_header() {
        // Less than 7 bytes (MBAP_HEADER_SIZE)
        assert!(try_parse_tcp_frame(&[0x00, 0x01, 0x00]).is_none());
        assert!(try_parse_tcp_frame(&[0x00, 0x01, 0x00, 0x00, 0x00, 0x01]).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_nonzero_protocol_id() {
        // Proto ID = 0x0001 (must be 0)
        let buf = [0x00, 0x01, 0x00, 0x01, 0x00, 0x02, 0x01, 0x03, 0x00];
        assert!(try_parse_tcp_frame(&buf).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_payload_too_large() {
        // payload_len > MAX_TCP_ADU_SIZE (260)
        let mut buf = [0u8; MBAP_HEADER_SIZE + 1];
        buf[0] = 0x00;
        buf[1] = 0x01; // TID
        buf[2] = 0x00;
        buf[3] = 0x00; // Proto = 0
        buf[4] = 0x01;
        buf[5] = 0x05; // Length = 261 (> 260)
        buf[6] = 0x01; // UID
        buf[7] = 0x03; // FC (body)
        assert!(try_parse_tcp_frame(&buf).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_payload_len_zero() {
        // payload_len = 0: fl values are 7 and 6, both ≤ MBAP_HEADER_SIZE.
        // Guard prevents panic, returns None (invalid MBAP frame).
        let buf = [0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01];
        assert!(try_parse_tcp_frame(&buf).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_payload_len_one_no_pdu() {
        // payload_len = 1 → body is just [UID], not a valid FC.
        // UID=0x46 is NOT a known function code → try_extract_tcp_pdu returns None.
        let buf = [0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x46, 0x46];
        assert!(try_parse_tcp_frame(&buf).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_unknown_function_code() {
        // MBAP header complete, body exists, but byte 0 is NOT a known FC (0x46)
        let buf = [
            0x00, 0x01, // TID
            0x00, 0x00, // Proto = 0
            0x00, 0x02, // Length = 2
            0x01, // UID
            0x46, 0x00, // Unknown FC 0x46
        ];
        assert!(try_parse_tcp_frame(&buf).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_valid_minimal() {
        // Minimal valid frame: ReadCoils request
        let buf = [
            0x00, 0x01, // TID
            0x00, 0x00, // Proto = 0
            0x00, 0x05, // Length = 5
            0x01, // UID
            0x01, 0x00, 0x00, 0x00, 0x01, // FC=1, addr=0, count=1
        ];
        let (tid, slave, pdu, consumed) = try_parse_tcp_frame(&buf).unwrap();
        assert_eq!(tid, 1);
        assert_eq!(slave, 1);
        assert_eq!(pdu.len(), 5);
        assert_eq!(consumed, 12);
    }

    #[test]
    fn try_parse_tcp_frame_with_unit_id_in_body() {
        // Gateway-style: UID in body byte 0 matches MBAP UID
        // Body: [UID=1, FC=3, addr_hi, addr_lo, count_hi, count_lo]
        let buf = [
            0x00, 0x01, // TID
            0x00, 0x00, // Proto = 0
            0x00, 0x06, // Length = 6
            0x01, // UID in MBAP
            0x01, 0x03, 0x00, 0x00, 0x00, 0x01, // body: UID=1, FC=3, addr=0, count=1
        ];
        let (tid, slave, pdu, _) = try_parse_tcp_frame(&buf).unwrap();
        assert_eq!(tid, 1);
        // Slave should come from body[0], PDU should skip the duplicate UID
        assert_eq!(slave, 1);
        // PDU is body[1..] = [FC, addr, count]
        assert_eq!(pdu[0], 0x03);
        assert_eq!(pdu.len(), 5);
    }

    #[test]
    fn try_parse_tcp_frame_pdu_only_length() {
        // PduOnly: Length = 5 (PDU only, no +1 for UID)
        // min_total = MBAP_PREFIX_SIZE + 5 = 11, max_total = MBAP_HEADER_SIZE + 5 = 12
        // So second iteration of the `for` loop finds the match.
        let buf = [
            0x00, 0x01, // TID
            0x00, 0x00, // Proto = 0
            0x00, 0x05, // Length = 5 (PduOnly)
            0x01, // UID
            0x03, 0x00, 0x00, 0x00, 0x01, // PDU: FC=3, addr=0, count=1
        ];
        let (tid, slave, pdu, consumed) = try_parse_tcp_frame(&buf).unwrap();
        assert_eq!(tid, 1);
        assert_eq!(slave, 1);
        assert_eq!(pdu.len(), 5);
        // consumed = MBAP_HEADER_SIZE + payload_len = 12 (full frame)
        assert_eq!(consumed, 12);
    }

    #[test]
    fn try_parse_tcp_frame_not_enough_data() {
        // Correct MBAP header with payload_len=5, but buffer only has 10 bytes
        // (MBAP_HEADER_SIZE + 3 < MBAP_HEADER_SIZE + 5)
        let buf = [
            0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 0x01, // MBAP (7 bytes)
            0x03, 0x00, 0x00, // Only 3 PDU bytes (need 5)
        ];
        assert!(try_parse_tcp_frame(&buf).is_none());
    }

    #[test]
    fn try_parse_tcp_frame_max_valid_length() {
        // payload_len = MAX_TCP_ADU_SIZE (260) → valid size
        let mut buf = vec![0u8; MBAP_HEADER_SIZE + 260];
        buf[0] = 0x00;
        buf[1] = 0x01; // TID
        buf[2] = 0x00;
        buf[3] = 0x00; // Proto = 0
        buf[4] = 0x01;
        buf[5] = 0x04; // Length = 260
        buf[6] = 0x01; // UID
        buf[7] = 0x03; // FC=3 (first byte of body, ensures known FC)
        assert!(try_parse_tcp_frame(&buf).is_some());
    }
}
