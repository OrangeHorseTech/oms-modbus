// SPDX-License-Identifier: MIT OR Apache-2.0
//! Modbus RTU client and server — generic over transport.
//!
//! Works with any `AsyncRead + AsyncWrite` byte transport, including
//! serial ports and TCP (RTU over TCP).

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
use crate::transport::{CRC_SIZE, MAX_ADU_SIZE, MIN_RTU_FRAME};
use crate::WireTap;

/// Default T3.5 inter-frame timeout for RTU server when `BusTiming` is not
/// configured. 5 ms ≈ T3.5 @ 9600 baud (4.01 ms), rounded up for margin.
const DEFAULT_RTU_T35_US: u64 = 5_000;

/// Reconnect factory for direct transport.
type ReconnectFactory<T> = (
    crate::reconnect::ReconnectConfig,
    Box<dyn Fn() -> io::Result<T> + Send + Sync>,
);

/// Modbus RTU client — `Send + Sync + Clone` via internal `Mutex`.
///
/// Call `.with_reconnect(max, backoff, factory)` to enable auto-reconnect
/// on transport errors (e.g. USB serial unplugged). The factory is called to
/// create a new transport instance on each reconnect attempt.
///
/// For bus-level capture, use [`with_options`] which wraps the transport in
/// [`SniffIo`] automatically.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use oms_modbus::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // In-memory duplex — works with serial, TCP, Unix sockets too
/// let (client_port, server_port) = tokio::io::duplex(1024);
///
/// // With capture + timing via ClientOptions
/// let cap = Arc::new(BusCapture::unbounded());
/// let opts = ClientOptions::default()
///     .with_timeout(Duration::from_secs(3))
///     .with_tap(cap.clone())
///     .with_bus_timing(BusTiming::rtu_35t(9600));
/// let client = rtu::with_options(client_port, opts);
///
/// let regs = client.read_holding_registers(1, 0, 5).await?;
/// # Ok(())
/// # }
/// ```
pub struct RtuClient<T> {
    inner: Mutex<RtuInner<T>>,
    timeout: Duration,
    reconnect: Option<ReconnectFactory<T>>,
    /// Preserved for reconnect rebuilds — active timing lives in SniffIo.
    bus_timing: Option<Arc<BusTiming>>,
    /// Preserved for reconnect rebuilds — active tap lives in SniffIo.
    tap: Option<Arc<dyn WireTap>>,
}

struct RtuInner<T> {
    stream: SniffIo<T>,
    write_buf: BytesMut,
    read_buf: BytesMut,
}

impl<T> Debug for RtuClient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RtuClient");
        d.field("timeout", &self.timeout);
        if let Some((cfg, _)) = &self.reconnect {
            d.field("reconnect_max_retries", &cfg.max_retries());
            d.field("reconnect_interval", &cfg.interval());
        }
        d.finish()
    }
}

impl<T> RtuClient<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Create an RTU client with a 5-second default timeout.
    pub fn new(transport: T) -> Self {
        Self::with_timeout(transport, Duration::from_secs(5))
    }

    /// Create an RTU client with an explicit request timeout.
    pub fn with_timeout(transport: T, timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(RtuInner {
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
    /// For `RtuClient<SniffIo<T>>`, prefer `with_reconnect_on` which
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

    /// ── RTU receive: length-based ────────────────────────────────────────
    ///
    /// The client knows what request it sent → it knows the expected response
    /// frame length. Reads exactly that many bytes (with timeout), then
    /// validates CRC once. No per-byte CRC scanning.
    async fn send_recv(
        &self,
        slave_id: u8,
        request: &Request<'_>,
    ) -> Result<Response, ModbusError> {
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
            codec::encode_rtu_frame,
        )
        .await?;

        // ── Receive: read expected frame length ───────────────────────────
        let expected_pdu = expected_rtu_pdu_len(request);
        let expected_frame = expected_pdu.map(|p| p + 1 + CRC_SIZE); // slave_id(1) + pdu + crc
        let min_frame: usize = MIN_RTU_FRAME; // slave_id(1) + fc|0x80(1) + code(1) + crc(2)

        inner.read_buf.clear();
        let deadline = Instant::now() + self.timeout;
        let target = expected_frame.unwrap_or(min_frame);

        // Read until we have `target` bytes or timeout
        send_recv::read_at_least(&mut inner.stream, &mut inner.read_buf, deadline, target).await?;

        // Timeout may leave us with fewer than target bytes. Validate CRC
        // on whatever we have (≥5 bytes minimum).
        if inner.read_buf.len() < min_frame {
            let got = inner.read_buf.len();
            if got == 0 {
                return Err(ModbusError::timeout(RTU_RECV_TIMEOUT));
            }
            return Err(ModbusError::timeout(format!(
                "RTU short frame ({got} bytes, need at least {min_frame})"
            )));
        }

        // Validate CRC on accumulated data
        let buf = &inner.read_buf;
        let crc_received = u16::from_le_bytes([buf[buf.len() - 2], buf[buf.len() - 1]]);
        let crc_calc = codec::calculate_crc(&buf[..buf.len() - 2]);
        if crc_received != crc_calc {
            return Err(ModbusError::protocol("RTU CRC mismatch"));
        }

        let frame = &buf[..buf.len() - 2]; // strip CRC
        if frame.len() < 2 {
            return Err(ModbusError::protocol("RTU frame too short"));
        }

        let rsp_slave = frame[0];
        let pdu = Bytes::copy_from_slice(&frame[1..]);

        if rsp_slave != slave_id {
            return Err(ModbusError::protocol(format!(
                "{SLAVE_ID_MISMATCH} {slave_id}, got {rsp_slave}"
            )));
        }

        Response::try_from(pdu)
            .map_err(|e| ModbusError::protocol(format!("{PDU_DECODE_ERROR} {e}")))
    }
}

// ── Expected RTU response PDU length ────────────────────────────────────

fn expected_rtu_pdu_len(request: &Request<'_>) -> Option<usize> {
    use Request::*;
    match request {
        // PDU for read responses: FC(1) + byte_count(1) + data
        ReadCoils(_, count) | ReadDiscreteInputs(_, count) => {
            Some(2 + (*count as usize).div_ceil(8))
        }
        ReadHoldingRegisters(_, count) | ReadInputRegisters(_, count) => {
            Some(2 + *count as usize * 2)
        }
        WriteSingleCoil(_, _)
        | WriteSingleRegister(_, _)
        | WriteMultipleCoils(_, _)
        | WriteMultipleRegisters(_, _) => Some(5),
        ReadWriteMultipleRegisters(_, count, _, _) => Some(2 + *count as usize * 2),
        MaskWriteRegister(_, _, _) => Some(7),
        Diagnostic(_, _) => Some(5),
        Disconnect => None,
    }
}

// ── Reconnect for SniffIo-wrapped clients ───────────────────────────────

impl<T> RtuClient<SniffIo<T>>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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

#[async_trait]
impl<T> ModbusClient for RtuClient<T>
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

/// Create an RTU client with optional capture and reconnect via SniffIo.
pub fn with_options<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    transport: T,
    opts: ClientOptions,
) -> RtuClient<SniffIo<T>> {
    let tap = opts.tap().cloned();
    let timing = opts.bus_timing.clone();
    let mut stream = SniffIo::new(transport, tap, timing.clone());
    if let Some(cap) = opts.data_channel_capacity {
        stream = stream.with_channel_capacity(cap);
    }
    if let Some(cap) = opts.tap_channel_capacity {
        stream = stream.with_tap_channel_capacity(cap);
    }
    let mut raw = RtuClient::with_timeout(stream, opts.timeout);
    raw.bus_timing = timing;
    raw.tap = opts.tap().cloned();
    raw
}

// ── RTU Server ──────────────────────────────────────────────────────────

/// Modbus RTU server over any byte transport (serial, DuplexStream, etc.).
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
/// let server = rtu::RtuServer::new(port)
///     .with_bus_timing(Arc::new(BusTiming::rtu_35t(9600)));
/// tokio::spawn(async move { server.serve_forever(store).await.ok(); });
/// # Ok(())
/// # }
/// ```
pub struct RtuServer<T> {
    transport: T,
    bus_timing: Option<Arc<BusTiming>>,
    /// Per-frame read timeout — when the bus is silent for this long,
    /// accumulated bytes are flushed as a broken frame. Defaults to
    /// `BusTiming::min_spacing()` (T3.5) when set, or 5 ms otherwise.
    read_timeout: Option<Duration>,
}

impl<T> RtuServer<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Create an RTU server on the given byte transport.
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

    /// Set a custom per-frame read timeout for detecting broken/partial
    /// frames. When this timeout expires without receiving new bytes,
    /// any unparseable data in the buffer is discarded.
    ///
    /// Default: [`BusTiming::min_spacing`] if bus timing is configured,
    /// otherwise 5 ms (≈ T3.5 @ 9600 baud).
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Process requests indefinitely until the transport closes.
    ///
    /// Frame boundaries are detected by inter-byte timeout (3.5T silence).
    /// Bytes are accumulated until the bus goes quiet, then CRC is validated.
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
            .unwrap_or(Duration::from_micros(DEFAULT_RTU_T35_US));

        loop {
            let mut tmp = [0u8; MAX_ADU_SIZE];
            if buf.is_empty() {
                // No pending data — wait indefinitely for the next frame.
                match stream.read(&mut tmp).await {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    Err(_) => break,
                }
            } else {
                // Have accumulated bytes — wait T3.5 for more, then decide
                // whether the frame is complete or broken.
                match tokio::time::timeout(read_timeout, stream.read(&mut tmp)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                    Ok(Err(_)) => break,
                    Err(_elapsed) => {
                        // T3.5 silence — the accumulated bytes are a
                        // complete (or broken) frame. Try one last parse
                        // before discarding; if CRC matches, process it.
                        // Otherwise clear and wait for the next clean frame
                        // (libmodbus flush equivalent).
                        if try_parse_server_rtu(&buf).is_none() {
                            buf.clear();
                        }
                        continue;
                    }
                }
            }
            // Try to parse complete frames from buf.
            while let Some((slave_id, pdu, consumed)) = try_parse_server_rtu(&buf) {
                buf.advance(consumed);
                if let Some(rsp_data) =
                    send_recv::process_server_request(&pdu, slave_id, &service, &mut rsp_buf).await
                {
                    stream.prepare_send().await;
                    frame_buf.clear();
                    codec::encode_rtu_frame(&rsp_data, &mut frame_buf);
                    if stream.write_all(&frame_buf).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

/// Server-side RTU frame parsing: validate CRC, check FC validity.
///
/// Returns `(slave_id, pdu_bytes, total_consumed)` for a valid frame, or
/// `None` if the buffer doesn't contain a recognizable Modbus RTU frame.
///
/// Unknown function codes are silently discarded (no Exception 0x01
/// response). This is a deliberate choice: on noisy RS-485 buses, a CRC-16
/// collision with random bytes that happen to decode as an unknown FC would
/// generate spurious exception responses, adding unnecessary bus traffic.
/// Many industrial Modbus devices follow the same practice.  Configure
/// `read_timeout` to control how long the server waits before flushing
/// unparseable bytes via the T3.5 silence window.
fn try_parse_server_rtu(buf: &[u8]) -> Option<(u8, Bytes, usize)> {
    if buf.len() < MIN_RTU_FRAME {
        return None;
    }
    let crc_received = u16::from_le_bytes([buf[buf.len() - 2], buf[buf.len() - 1]]);
    let crc_calc = codec::calculate_crc(&buf[..buf.len() - 2]);
    if crc_received != crc_calc {
        return None;
    }
    let frame = &buf[..buf.len() - 2];
    if frame.len() < 2 {
        return None;
    }
    let fc = frame[1];
    if !crate::frame::is_known_function_code(fc) {
        return None;
    }
    Some((
        frame[0],
        Bytes::copy_from_slice(&frame[1..]),
        buf.len(), // consumed = frame + crc
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── expected_rtu_pdu_len ────────────────────────────────────────────

    #[test]
    fn read_coils_pdu_len() {
        assert_eq!(expected_rtu_pdu_len(&Request::ReadCoils(1, 1)), Some(3));
        assert_eq!(expected_rtu_pdu_len(&Request::ReadCoils(1, 8)), Some(3));
        assert_eq!(expected_rtu_pdu_len(&Request::ReadCoils(1, 9)), Some(4));
    }

    #[test]
    fn read_discrete_inputs_pdu_len() {
        assert_eq!(
            expected_rtu_pdu_len(&Request::ReadDiscreteInputs(1, 2000)),
            Some(2 + 250)
        );
    }

    #[test]
    fn read_holding_registers_pdu_len() {
        assert_eq!(
            expected_rtu_pdu_len(&Request::ReadHoldingRegisters(1, 1)),
            Some(4)
        );
        assert_eq!(
            expected_rtu_pdu_len(&Request::ReadHoldingRegisters(1, 125)),
            Some(252)
        );
    }

    #[test]
    fn read_input_registers_pdu_len() {
        assert_eq!(
            expected_rtu_pdu_len(&Request::ReadInputRegisters(1, 125)),
            Some(252)
        );
    }

    #[test]
    fn write_single_coil_pdu_len() {
        assert_eq!(
            expected_rtu_pdu_len(&Request::WriteSingleCoil(1, true)),
            Some(5)
        );
    }

    #[test]
    fn write_single_register_pdu_len() {
        assert_eq!(
            expected_rtu_pdu_len(&Request::WriteSingleRegister(1, 0x1234)),
            Some(5)
        );
    }

    #[test]
    fn write_multiple_coils_pdu_len() {
        let coils: std::borrow::Cow<'_, [bool]> = std::borrow::Cow::Owned(vec![true; 10]);
        assert_eq!(
            expected_rtu_pdu_len(&Request::WriteMultipleCoils(1, coils)),
            Some(5)
        );
    }

    #[test]
    fn write_multiple_registers_pdu_len() {
        let regs: std::borrow::Cow<'_, [u16]> = std::borrow::Cow::Owned(vec![0; 10]);
        assert_eq!(
            expected_rtu_pdu_len(&Request::WriteMultipleRegisters(1, regs)),
            Some(5)
        );
    }

    #[test]
    fn read_write_multiple_registers_pdu_len() {
        let regs: std::borrow::Cow<'_, [u16]> = std::borrow::Cow::Owned(vec![0; 3]);
        assert_eq!(
            expected_rtu_pdu_len(&Request::ReadWriteMultipleRegisters(1, 3, 1, regs)),
            Some(8)
        );
    }

    #[test]
    fn mask_write_register_pdu_len() {
        assert_eq!(
            expected_rtu_pdu_len(&Request::MaskWriteRegister(1, 0x0000, 0xFFFF)),
            Some(7)
        );
    }

    #[test]
    fn diagnostic_pdu_len() {
        // Diagnostic(sub_function, data) — both u16
        assert_eq!(
            expected_rtu_pdu_len(&Request::Diagnostic(0x0000, 0x0000)),
            Some(5)
        );
    }

    #[test]
    fn disconnect_pdu_len_is_none() {
        assert_eq!(expected_rtu_pdu_len(&Request::Disconnect), None);
    }

    // ── try_parse_server_rtu ────────────────────────────────────────────

    #[test]
    fn server_parse_empty_buffer() {
        assert!(try_parse_server_rtu(&[]).is_none());
    }

    #[test]
    fn server_parse_too_short() {
        assert!(try_parse_server_rtu(&[0x01, 0x03, 0x00, 0x00]).is_none());
    }

    #[test]
    fn server_parse_valid_read_holding_response() {
        // slave=1, FC=3, byte_count=2, data=[0, 0]
        let mut frame = vec![0x01, 0x03, 0x02, 0x00, 0x00];
        let crc = crate::codec::calculate_crc(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());
        let (slave, pdu, consumed) = try_parse_server_rtu(&frame).unwrap();
        assert_eq!(slave, 1);
        assert_eq!(pdu[0], 0x03);
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn server_parse_crc_mismatch() {
        let frame = [0x01, 0x03, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF];
        assert!(try_parse_server_rtu(&frame).is_none());
    }

    #[test]
    fn server_parse_unknown_function_code_rejected() {
        // Valid CRC but FC=0x46 (unknown) → silently discarded
        let mut frame = vec![0x01, 0x46, 0x00, 0x00, 0x01];
        let crc = crate::codec::calculate_crc(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());
        assert!(try_parse_server_rtu(&frame).is_none());
    }

    #[test]
    fn server_parse_zero_length_frame_after_crc_removal() {
        // Frame with only slave_id + CRC → after CRC strip, < 2 bytes
        let frame = [0x01, 0x00, 0x00];
        assert!(try_parse_server_rtu(&frame).is_none());
    }
}
