// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Shared send/recv helpers for all Modbus transports.
//!
//! Each transport client implements its own receive logic (RTU = length-based,
//! TCP = MBAP-based, ASCII = delimiter-based), but the send path and pre-send
//! drain are shared. This module also houses the reconnect loop and server-side
//! request processor.

use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ModbusError;
use crate::error::*;
use crate::frame::{self, Request, Response};

// ── Pre-send drain (all protocols) ─────────────────────────────────────────

// ── Drain timing constants ──────────────────────────────────────────

/// Per-read silence threshold in the drain loop. 10 ms covers T3.5 at
/// ≥4800 baud and T1.5 at ≥2400 baud — enough to detect an inter-frame
/// gap while responding quickly.
const DRAIN_QUIET_MS: u64 = 10;

/// Max total drain duration. If data keeps arriving for this long,
/// the bus is genuinely busy. 200 ms is the worst-case stale frame
/// at 9600 baud (~190 bytes drained), but at 200 ms the client is
/// still responsive.
const DRAIN_MAX_MS: u64 = 200;

/// Drain stale data from the read buffer before sending a request.
///
/// All Modbus protocols are half-duplex: only one device transmits at a time.
/// Before the client sends, it must ensure the bus is quiet. This function:
///
/// 1. Checks if bytes are immediately available (quick poll, ~1ms).
/// 2. If data arrives, drains until the bus goes quiet or the drain deadline.
/// 3. If data keeps arriving for the full drain deadline → "bus busy" error.
/// 4. If the bus goes quiet → OK (stale data drained, proceed to send).
///
/// The drain uses internal timeouts independent of the client's receive
/// timeout. A busy bus is detected in at most [`DRAIN_MAX_MS`] rather than
/// blocking for the full request timeout. Each per-read wait is capped at
/// [`DRAIN_QUIET_MS`] — just long enough to detect an inter-frame gap.
///
/// # Design note
///
/// Before v0.4, this took a `timeout` parameter (the client's logical receive
/// timeout, typically seconds). A genuinely busy bus would block the caller
/// for the full duration. The fixed cap avoids that: draining is a bus-busy
/// *probe*, not part of the request/response cycle. If the bus is truly
/// congested the client should back off early and let the reconnect or
/// application layer decide what to do next.
///
/// BusTiming is updated automatically by SniffIo.
pub(crate) async fn drain_stale_data<S>(
    stream: &mut S,
    scratch: &mut [u8],
) -> Result<(), ModbusError>
where
    S: AsyncRead + Unpin,
{
    // Quick poll: if no data is immediately available, the bus is quiet.
    // Use a 1 ms timeout — best-effort on Windows (~15 ms timer granularity),
    // but still avoids the full drain timeout on a clean bus.
    const QUICK_POLL_MS: u64 = 1;
    let quick = Duration::from_millis(QUICK_POLL_MS);
    match tokio::time::timeout(quick, stream.read(scratch)).await {
        Ok(Ok(0)) => return Err(ModbusError::connection("connection closed")),
        Ok(Ok(_)) => {
            // Data arrived — the bus has stale data. Switch to drain loop.
        }
        Ok(Err(e)) => return Err(ModbusError::from(e)),
        Err(_) => {
            // No data within quick poll → bus is quiet.
            return Ok(());
        }
    }

    // Data arrived in the quick poll. Continue draining until the bus
    // goes quiet (per-read silence) or the drain deadline expires.
    let per_read = Duration::from_millis(DRAIN_QUIET_MS);
    let deadline = Instant::now() + Duration::from_millis(DRAIN_MAX_MS);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ModbusError::connection(
                "bus busy: data received during quiet period",
            ));
        }
        // Cap each read to the per-read silence threshold so a
        // continuously-busy bus is detected quickly rather than after
        // the full request timeout.
        let wait = remaining.min(per_read);
        match tokio::time::timeout(wait, stream.read(scratch)).await {
            Ok(Ok(0)) => return Err(ModbusError::connection("connection closed")),
            Ok(Ok(_)) => {
                // Data still arriving — keep draining.
            }
            Ok(Err(e)) => return Err(ModbusError::from(e)),
            Err(_) => {
                // Bus went quiet — stale data drained.
                return Ok(());
            }
        }
    }
}

// ── Shared send ────────────────────────────────────────────────────────────

/// Encode and send a Modbus request. All three transports share this path.
///
/// 1. Clear write_buf, encode PDU (slave_id + request).
/// 2. Call `encode_fn` to add transport framing (CRC/LRC/MBAP).
/// 3. Write the framed bytes to the stream with timeout.
pub(crate) async fn send_frame<S, E>(
    stream: &mut S,
    write_buf: &mut BytesMut,
    slave_id: u8,
    timeout: Duration,
    request: &Request<'_>,
    mut encode_fn: E,
) -> Result<(), ModbusError>
where
    S: AsyncWrite + Unpin,
    E: FnMut(&[u8], &mut BytesMut),
{
    write_buf.clear();
    write_buf.put_u8(slave_id);
    frame::encode_request_into(request, write_buf)
        .map_err(|e| ModbusError::protocol(format!("{PDU_ENCODE_ERROR} {e}")))?;

    let send_buf = write_buf.split();
    encode_fn(&send_buf, write_buf);
    let frame_bytes = write_buf.split().freeze();

    tokio::time::timeout(timeout, stream.write_all(&frame_bytes))
        .await
        .map_err(|_| ModbusError::timeout(SEND_TIMEOUT))?
        .map_err(ModbusError::from)?;

    Ok(())
}

// ── Receive helpers ────────────────────────────────────────────────────────

/// Read at least `min` bytes into `read_buf` with a timeout, zero-copy.
///
/// Uses [`AsyncReadExt::read_buf`] to read directly into `BytesMut`'s spare
/// capacity — no intermediate scratch buffer or per-byte copy.
///
/// Returns the number of bytes read once `min` is reached, or on timeout/EOF.
/// If the inner timeout fires but some data has already been accumulated, the
/// partial read is returned (the caller validates CRC/size). This handles
/// exception responses which are shorter than the expected normal response.
pub(crate) async fn read_at_least<S>(
    stream: &mut S,
    read_buf: &mut BytesMut,
    deadline: Instant,
    min: usize,
) -> Result<usize, ModbusError>
where
    S: AsyncRead + Unpin,
{
    while read_buf.len() < min {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.read_buf(read_buf)).await {
            Ok(Ok(0)) => {
                if read_buf.is_empty() {
                    return Err(ModbusError::connection(CONN_CLOSED));
                }
                break; // partial frame — caller validates
            }
            Ok(Ok(_)) => {} // bytes already in read_buf
            Ok(Err(e)) => return Err(ModbusError::from(e)),
            Err(_) => {
                // Inner timeout — if we already have data, return it.
                // Exception responses are shorter than normal responses,
                // and the expected `min` is for the normal case.
                if !read_buf.is_empty() {
                    break;
                }
                return Err(ModbusError::timeout(RECV_TIMEOUT));
            }
        }
    }
    Ok(read_buf.len())
}

// ── Shared reconnect loop ─────────────────────────────────────────────────

use crate::reconnect::ReconnectConfig;
use std::future::Future;

/// Shared reconnect loop for TCP, RTU, and ASCII transports.
///
/// - `reconnect`: reconnect config, or `None` to send once without retry.
/// - `send`: closure that executes one send_recv attempt.
/// - `rebuild`: async closure that rebuilds the transport. Returns `true` on success.
/// - `exceeded_err`: builds the "max retries exceeded" error for this transport.
///
/// # Reconnect classification
///
/// - `Connection` / `Serial`: triggers immediate reconnect (hard transport failure).
/// - **Only physical-layer failures are retried.** Timeout, protocol errors, and
///   Modbus exceptions are returned directly without triggering a reconnect.
///
/// # Rebuild failure guard
///
/// If the transport rebuild itself fails 3 consecutive times, returns an error
/// rather than looping indefinitely.
pub(crate) async fn run_with_reconnect<Fut, S, R, RFut, E>(
    reconnect: Option<&ReconnectConfig>,
    mut send: S,
    mut rebuild: R,
    exceeded_err: E,
) -> Result<Response, ModbusError>
where
    Fut: Future<Output = Result<Response, ModbusError>>,
    S: FnMut() -> Fut,
    R: FnMut() -> RFut,
    RFut: Future<Output = bool>,
    E: FnOnce(String) -> ModbusError,
{
    let Some(cfg) = reconnect else {
        return send().await;
    };

    let mut failures = 0u32;
    let mut rebuild_failures = 0u32;
    loop {
        match send().await {
            Ok(rsp) => return Ok(rsp),
            Err(e) => {
                if !crate::reconnect::is_retryable(&e) {
                    return Err(e);
                }

                failures += 1;
                if cfg.max_retries() > 0 && failures > cfg.max_retries() {
                    return Err(exceeded_err(format!(
                        "reconnect max_retries({}) exceeded: {}",
                        cfg.max_retries(),
                        e.detail()
                    )));
                }

                tokio::time::sleep(cfg.interval()).await;

                if !rebuild().await {
                    rebuild_failures += 1;
                    if rebuild_failures >= 3 {
                        return Err(exceeded_err(
                            "reconnect: transport rebuild failed 3 consecutive times".into(),
                        ));
                    }
                    continue;
                }
                rebuild_failures = 0;
            }
        }
    }
}

// ── Shared server request processing ────────────────────────────────────────

/// Process a single Modbus request on the server side: decode, call service,
/// encode response. Returns the framed bytes ready to send, or `None` if the
/// request was invalid or encoding failed.
pub(crate) async fn process_server_request<S>(
    raw: &Bytes,
    slave_id: u8,
    service: &S,
    rsp_buf: &mut BytesMut,
) -> Option<Bytes>
where
    S: crate::server::Service + Send + Sync,
{
    // Broadcast (slave_id == 0): per Modbus spec, all slaves act on the
    // request but NONE respond. Returning None prevents any response.
    if slave_id == 0 {
        return None;
    }
    if raw.is_empty() {
        return None;
    }
    let request = match Request::try_from(raw.clone()) {
        Ok(req) => req,
        Err(e) => {
            log::warn!("server: invalid request from slave {}: {}", slave_id, e);
            return None;
        }
    };
    let req_fc = request.function_code().value();
    let response = crate::server::context::SLAVE_ID
        .scope(slave_id, async { service.call(request).await })
        .await;
    let response = match response {
        Ok(rsp) => rsp,
        Err(ex) => {
            log::warn!("server: exception from slave {}: {:?}", slave_id, ex);
            Response::Exception(req_fc, ex)
        }
    };
    rsp_buf.clear();
    rsp_buf.put_u8(slave_id);
    if frame::encode_response_into(&response, rsp_buf).is_err() {
        log::warn!(
            "server: response PDU exceeds max size for slave {}",
            slave_id
        );
        return None;
    }
    Some(rsp_buf.split().freeze())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    // ── drain_stale_data tests ──────────────────────────────────────────

    /// A mock AsyncRead that always returns data — never goes quiet.
    struct NeverQuiet;

    impl AsyncRead for NeverQuiet {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let to_fill = buf.remaining().min(16);
            let fill_data = [0xAAu8; 16];
            buf.put_slice(&fill_data[..to_fill]);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn drain_stale_data_clean_bus_returns_ok() {
        let (mut client, _server) = tokio::io::duplex(64);
        let mut scratch = [0u8; 256];
        let result = drain_stale_data(&mut client, &mut scratch).await;
        assert!(result.is_ok(), "clean bus should return Ok immediately");
    }

    #[tokio::test]
    async fn drain_stale_data_bus_busy_returns_error() {
        let mut stream = NeverQuiet;
        let mut scratch = [0u8; 256];
        let err = drain_stale_data(&mut stream, &mut scratch)
            .await
            .unwrap_err();
        let detail = err.detail();
        assert!(
            detail.contains("bus busy"),
            "expected 'bus busy', got: {detail}"
        );
        assert!(
            matches!(err, ModbusError::Connection(_)),
            "bus-busy should be a Connection error (retryable), got: {err:?}"
        );
    }

    #[tokio::test]
    async fn drain_stale_data_connection_closed() {
        let (mut client, server) = tokio::io::duplex(64);
        drop(server); // close the write half
        let mut scratch = [0u8; 256];
        let err = drain_stale_data(&mut client, &mut scratch)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ModbusError::Connection(_)),
            "expected Connection error, got: {err:?}"
        );
    }

    // ── run_with_reconnect tests ────────────────────────────────────────

    #[tokio::test]
    async fn rebuild_fails_three_consecutive_times() {
        let cfg = ReconnectConfig::new(5, Duration::from_millis(1));
        let result = run_with_reconnect(
            Some(&cfg),
            || async { Err(ModbusError::connection("test")) },
            || async { false },
            ModbusError::connection,
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            err.detail().contains("3 consecutive times"),
            "expected rebuild failure, got: {}",
            err.detail()
        );
    }

    #[tokio::test]
    async fn rebuild_counter_resets_on_success() {
        let rebuild_count = AtomicU32::new(0);
        let cfg = ReconnectConfig::new(5, Duration::from_millis(1));
        // rebuild alternates false→true→false→true; never 3 consecutive fails.
        // After 6 send failures, max_retries=5 is exceeded, proving rebuild
        // counter never reached 3 (otherwise rebuild error would come first).
        let result = run_with_reconnect(
            Some(&cfg),
            || async { Err(ModbusError::connection("test")) },
            || {
                let n = rebuild_count.fetch_add(1, Ordering::SeqCst);
                async move { n % 2 == 0 } // even → rebuild succeeds, odd → fails
            },
            ModbusError::connection,
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            err.detail().contains("max_retries"),
            "expected max_retries error (rebuild counter reset), got: {}",
            err.detail()
        );
    }

    #[tokio::test]
    async fn send_succeeds_after_rebuild() {
        let first_call = AtomicBool::new(true);
        let cfg = ReconnectConfig::new(3, Duration::from_millis(1));
        let result = run_with_reconnect(
            Some(&cfg),
            || {
                let is_first = first_call.swap(false, Ordering::SeqCst);
                async move {
                    if is_first {
                        Err(ModbusError::connection("test"))
                    } else {
                        Ok(Response::ReadHoldingRegisters(vec![42]))
                    }
                }
            },
            || async { true },
            ModbusError::connection,
        )
        .await;
        let rsp = result.unwrap();
        assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42]));
    }

    #[tokio::test]
    async fn max_retries_zero_rebuild_fails_three_times() {
        // max_retries=0 means infinite retries — but the rebuild failure
        // guard (3 consecutive fails) still protects against infinite loops.
        let cfg = ReconnectConfig::new(0, Duration::from_millis(1));
        let result = run_with_reconnect(
            Some(&cfg),
            || async { Err(ModbusError::connection("test")) },
            || async { false },
            ModbusError::connection,
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            err.detail().contains("3 consecutive times"),
            "expected rebuild failure guard, got: {}",
            err.detail()
        );
    }

    #[tokio::test]
    async fn non_retryable_error_returned_immediately() {
        // Protocol errors (not retryable) skip the reconnect loop entirely.
        let cfg = ReconnectConfig::new(10, Duration::from_millis(1));
        let result = run_with_reconnect(
            Some(&cfg),
            || async { Err(ModbusError::protocol("bad CRC")) },
            || async { true },
            ModbusError::connection,
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            matches!(err, ModbusError::Protocol(_)),
            "expected Protocol error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn no_reconnect_passthrough() {
        // Without reconnect config, send result is returned directly
        // (no reconnect loop, no rebuild).
        let result = run_with_reconnect(
            None,
            || async { Ok(Response::ReadHoldingRegisters(vec![99])) },
            || async { true },
            ModbusError::connection,
        )
        .await;
        let rsp = result.unwrap();
        assert_eq!(rsp, Response::ReadHoldingRegisters(vec![99]));
    }
}
