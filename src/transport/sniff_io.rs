// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! `SniffIo<T>` — transparent I/O proxy with optional WireTap and BusTiming.
//!
//! When a WireTap is attached, SniffIo spawns a background reader that
//! continuously reads from the inner transport. Each chunk is fanned out
//! via `Bytes::clone()` (reference-count bump, zero-copy) to two independent
//! bounded channels:
//!
//! - **Data channel** (1 MB default): consumed by `poll_read` — the Modbus
//!   protocol path. Evicts oldest chunks when full.
//! - **Tap channel** (256 KB default): consumed by a dedicated `tap_forwarder`
//!   task that calls `WireTap::on_read()`. Slow user taps never block the
//!   background reader or poll_read.
//!
//! When no WireTap is attached (tap = None), SniffIo is a pure passthrough
//! with near-zero overhead — no background task, no channel, no allocation.
//!
//! BusTiming is integrated: all read/write operations automatically call
//! `timing.touch()`. Callers invoke `prepare_send()` before writing to
//! enforce frame spacing (async wait_if_needed).

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::bus_timing::BusTiming;
use crate::wire_tap::{now_micros, WireTap};

const DEFAULT_BUF_SIZE: usize = 4096;

/// Default data channel capacity: 1 MB.
///
/// For normal Modbus request-response cycles this is far larger than needed.
/// Evictions only occur when the consumer (`poll_read`) stops draining.
pub const DEFAULT_DATA_CHANNEL_CAPACITY: usize = 1024 * 1024; // 1 MB

/// Default tap channel capacity: 256 KB.
///
/// Large enough to absorb bursts while the tap forwarder task catches up,
/// small enough to bound memory in pure-monitoring scenarios.
pub const DEFAULT_TAP_CHANNEL_CAPACITY: usize = 256 * 1024; // 256 KB

// ── SniffChannel — bounded byte-level ring buffer ──────────────────────────

/// Bounded channel for forwarding bytes between tasks. Drop-oldest semantics:
/// when total buffered bytes exceed `capacity`, oldest chunks are evicted.
/// A single chunk larger than capacity is dropped entirely.
///
/// # Thread safety
///
/// Producer and consumer access the inner state through `std::sync::Mutex`.
/// Critical sections are VecDeque push/pop + counter updates — no I/O or
/// await points inside the lock.
struct SniffChannel {
    inner: Mutex<SniffChannelInner>,
}

struct SniffChannelInner {
    queue: VecDeque<Bytes>,
    total_bytes: usize,
    capacity: usize,
    dropped_chunks: u64,
    dropped_bytes: u64,
    closed: bool,
    error: Option<io::Error>,
    waker: Option<Waker>,
}

impl SniffChannel {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(SniffChannelInner {
                queue: VecDeque::new(),
                total_bytes: 0,
                capacity,
                dropped_chunks: 0,
                dropped_bytes: 0,
                closed: false,
                error: None,
                waker: None,
            }),
        }
    }

    // ── Producer API ───────────────────────────────────────────────────

    /// Push a chunk. Evicts oldest chunks if total would exceed capacity.
    /// If the chunk alone exceeds capacity, drop it entirely.
    /// Always succeeds — never blocks.
    fn send(&self, chunk: Bytes) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        while inner.total_bytes + chunk.len() > inner.capacity {
            if let Some(old) = inner.queue.pop_front() {
                inner.total_bytes -= old.len();
                inner.dropped_chunks += 1;
                inner.dropped_bytes += old.len() as u64;
            } else {
                // Single chunk exceeds capacity — drop it.
                inner.dropped_chunks += 1;
                inner.dropped_bytes += chunk.len() as u64;
                return;
            }
        }

        let chunk_len = chunk.len();
        inner.queue.push_back(chunk);
        inner.total_bytes += chunk_len;

        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }

    /// Signal a fatal transport error. Wakes the consumer so it can
    /// observe the error.
    fn send_error(&self, error: io::Error) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.error = Some(error);
        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }

    /// Update the capacity. If current buffered bytes exceed the new
    /// capacity, evict oldest chunks until within limit.
    fn set_capacity(&self, new_cap: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.capacity = new_cap;
        while inner.total_bytes > new_cap {
            if let Some(old) = inner.queue.pop_front() {
                inner.total_bytes -= old.len();
                inner.dropped_chunks += 1;
                inner.dropped_bytes += old.len() as u64;
            } else {
                break;
            }
        }
    }

    /// Check whether the consumer side has closed the channel.
    fn is_closed(&self) -> bool {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).closed
    }

    // ── Consumer API ──────────────────────────────────────────────────

    /// Close the channel from the consumer side. Wakes any waiting consumer.
    fn close(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.closed = true;
        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }

    /// Synchronous poll — for `AsyncRead::poll_read` integration.
    ///
    /// Returns:
    /// - `Ok(Some(data))` — got a chunk
    /// - `Ok(None)` — queue empty, waker registered, caller should return `Poll::Pending`
    /// - `Err(e)` — transport error or channel closed
    fn poll_recv(&self, cx: &mut Context<'_>) -> io::Result<Option<Bytes>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Drain queued data first — the background reader may have sent
        // data then signaled EOF; we must deliver the data before the error.
        if let Some(data) = inner.queue.pop_front() {
            inner.total_bytes -= data.len();
            return Ok(Some(data));
        }

        if let Some(e) = inner.error.take() {
            return Err(e);
        }

        if inner.closed {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "channel closed",
            ));
        }

        inner.waker = Some(cx.waker().clone());
        Ok(None)
    }

    /// Async receive — for the tap forwarder task.
    ///
    /// Returns `None` when the channel is closed and empty.
    async fn recv(&self) -> Option<Bytes> {
        use std::future::poll_fn;
        poll_fn(|cx| match self.poll_recv(cx) {
            Ok(Some(data)) => Poll::Ready(Some(data)),
            Ok(None) => Poll::Pending,
            Err(_) => Poll::Ready(None),
        })
        .await
    }

    // ── Stats ─────────────────────────────────────────────────────────

    fn capacity(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .capacity
    }

    fn dropped_chunks(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .dropped_chunks
    }

    fn dropped_bytes(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .dropped_bytes
    }
}

// ── SniffIo ────────────────────────────────────────────────────────────────

/// Transparent I/O proxy — captures traffic through an optional WireTap,
/// enforces bus timing through an optional BusTiming.
///
/// Two modes:
/// - **Passthrough** (tap = None): full transport, zero overhead.
/// - **Sniffing** (tap = Some): ReadHalf → background reader → fan-out to
///   data channel (for `poll_read`) and tap channel (for `tap_forwarder`).
///
/// BusTiming (if set) is applied automatically:
/// - `touch()` after every successful read/write
/// - `prepare_send()` must be called before writing (async wait_if_needed)
pub struct SniffIo<T> {
    tap: Option<Arc<dyn WireTap>>,
    buf_size: usize,
    data_capacity: usize,
    tap_capacity: usize,
    timing: Option<Arc<BusTiming>>,
    state: SniffState<T>,
}

enum SniffState<T> {
    /// Both read and write go through the full transport (no split).
    Passthrough(T),
    /// Data channel → `poll_read`. Tap channel → `tap_forwarder` task.
    /// `pending` holds data that arrived but didn't fit into the last
    /// caller's `ReadBuf`.
    Sniffing {
        data_channel: Arc<SniffChannel>,
        tap_channel: Arc<SniffChannel>,
        writer: tokio::io::WriteHalf<T>,
        /// Background reader task lifetime. Checked in `poll_read` to
        /// detect panic / unexpected termination — prevents silent hang.
        reader_handle: tokio::task::JoinHandle<()>,
        _tap_handle: tokio::task::JoinHandle<()>,
        pending: Option<Bytes>,
    },
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> SniffIo<T> {
    /// Wrap a transport with optional WireTap and BusTiming.
    ///
    /// Pass `None` for tap to disable capture (passthrough mode).
    /// Pass `None` for timing to skip bus frame spacing.
    pub fn new(inner: T, tap: Option<Arc<dyn WireTap>>, timing: Option<Arc<BusTiming>>) -> Self {
        match tap {
            Some(t) => Self::start_sniffing(
                inner,
                t,
                timing,
                DEFAULT_BUF_SIZE,
                DEFAULT_DATA_CHANNEL_CAPACITY,
                DEFAULT_TAP_CHANNEL_CAPACITY,
            ),
            None => Self {
                tap: None,
                buf_size: DEFAULT_BUF_SIZE,
                data_capacity: DEFAULT_DATA_CHANNEL_CAPACITY,
                tap_capacity: DEFAULT_TAP_CHANNEL_CAPACITY,
                timing,
                state: SniffState::Passthrough(inner),
            },
        }
    }

    /// Set the background reader's internal buffer size (default: 4096).
    pub fn with_buf_size(mut self, size: usize) -> Self {
        self.buf_size = size;
        self
    }

    /// Set the data channel capacity in bytes (default: 1 MB).
    /// Has no effect in passthrough mode.
    pub fn with_channel_capacity(mut self, bytes: usize) -> Self {
        self.data_capacity = bytes;
        if let SniffState::Sniffing { data_channel, .. } = &self.state {
            data_channel.set_capacity(bytes);
        }
        self
    }

    /// Set the tap channel capacity in bytes (default: 256 KB).
    /// Has no effect in passthrough mode.
    pub fn with_tap_channel_capacity(mut self, bytes: usize) -> Self {
        self.tap_capacity = bytes;
        if let SniffState::Sniffing { tap_channel, .. } = &self.state {
            tap_channel.set_capacity(bytes);
        }
        self
    }

    /// Number of chunks evicted from the data channel. 0 in passthrough.
    pub fn data_dropped_chunks(&self) -> u64 {
        match &self.state {
            SniffState::Sniffing { data_channel, .. } => data_channel.dropped_chunks(),
            SniffState::Passthrough(_) => 0,
        }
    }

    /// Number of bytes evicted from the data channel. 0 in passthrough.
    pub fn data_dropped_bytes(&self) -> u64 {
        match &self.state {
            SniffState::Sniffing { data_channel, .. } => data_channel.dropped_bytes(),
            SniffState::Passthrough(_) => 0,
        }
    }

    /// Number of chunks evicted from the tap channel. 0 in passthrough.
    pub fn tap_dropped_chunks(&self) -> u64 {
        match &self.state {
            SniffState::Sniffing { tap_channel, .. } => tap_channel.dropped_chunks(),
            SniffState::Passthrough(_) => 0,
        }
    }

    /// Number of bytes evicted from the tap channel. 0 in passthrough.
    pub fn tap_dropped_bytes(&self) -> u64 {
        match &self.state {
            SniffState::Sniffing { tap_channel, .. } => tap_channel.dropped_bytes(),
            SniffState::Passthrough(_) => 0,
        }
    }

    /// Sum of data + tap evicted bytes. Quick health check.
    pub fn total_dropped_bytes(&self) -> u64 {
        self.data_dropped_bytes() + self.tap_dropped_bytes()
    }

    /// Data channel capacity. 0 in passthrough.
    pub fn channel_capacity(&self) -> usize {
        match &self.state {
            SniffState::Sniffing { data_channel, .. } => data_channel.capacity(),
            SniffState::Passthrough(_) => 0,
        }
    }

    fn start_sniffing(
        inner: T,
        tap: Arc<dyn WireTap>,
        timing: Option<Arc<BusTiming>>,
        buf_size: usize,
        data_capacity: usize,
        tap_capacity: usize,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(inner);
        let data_channel = Arc::new(SniffChannel::new(data_capacity));
        let tap_channel = Arc::new(SniffChannel::new(tap_capacity));

        let reader_handle = tokio::spawn(active_reader(
            read_half,
            data_channel.clone(),
            tap_channel.clone(),
            buf_size,
        ));
        let tap_handle = tokio::spawn(tap_forwarder(tap_channel.clone(), tap.clone()));

        Self {
            tap: Some(tap),
            buf_size,
            data_capacity,
            tap_capacity,
            timing,
            state: SniffState::Sniffing {
                data_channel,
                tap_channel,
                writer: write_half,
                reader_handle,
                _tap_handle: tap_handle,
                pending: None,
            },
        }
    }

    /// Wait for bus timing spacing before sending. No-op if no timing configured.
    pub fn prepare_send(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        let timing = self.timing.clone();
        async move {
            if let Some(t) = timing {
                t.wait_if_needed().await;
            }
        }
    }

    /// Replace the inner transport. Preserves tap, timing, and capacity
    /// settings. Old background reader and tap forwarder exit when their
    /// channels are closed.
    pub fn replace_inner(&mut self, new: T) {
        let tap = self.tap.clone();
        let timing = self.timing.clone();
        let buf = self.buf_size;
        let data_cap = self.data_capacity;
        let tap_cap = self.tap_capacity;
        *self = match tap {
            Some(t) => Self::start_sniffing(new, t, timing, buf, data_cap, tap_cap),
            None => Self {
                tap: None,
                buf_size: buf,
                data_capacity: data_cap,
                tap_capacity: tap_cap,
                timing,
                state: SniffState::Passthrough(new),
            },
        };
    }
}

// ── Drop — signal channels to exit ─────────────────────────────────────────

impl<T> Drop for SniffIo<T> {
    fn drop(&mut self) {
        if let SniffState::Sniffing {
            data_channel,
            tap_channel,
            reader_handle,
            _tap_handle,
            ..
        } = &self.state
        {
            // Close channels first so background tasks see termination
            data_channel.close();
            tap_channel.close();
            // Abort tasks at their next .await point. Channels are already
            // closed so any in-flight send() calls are harmless.
            reader_handle.abort();
            _tap_handle.abort();
        }
    }
}

// ── Background reader — fans out to data + tap channels ───────────────────

async fn active_reader<T: AsyncRead + Unpin>(
    mut read_half: tokio::io::ReadHalf<T>,
    data_channel: Arc<SniffChannel>,
    tap_channel: Arc<SniffChannel>,
    buf_size: usize,
) {
    let mut buf = bytes::BytesMut::with_capacity(buf_size);
    loop {
        match read_half.read_buf(&mut buf).await {
            Ok(0) => {
                data_channel.send_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "transport closed",
                ));
                // NOTE: do NOT close tap_channel here — the tap forwarder
                // may still be draining queued chunks. Only SniffIo::drop
                // closes the tap channel.
                break;
            }
            Ok(_n) => {
                let chunk = buf.split().freeze();
                // Fan out — Bytes::clone is refcount bump (Arc), zero-copy.
                data_channel.send(chunk.clone());
                tap_channel.send(chunk);
                if data_channel.is_closed() {
                    break;
                }
            }
            Err(e) => {
                data_channel.send_error(e);
                break;
            }
        }
    }
}

// ── Tap forwarder — independent task, slow user taps don't block reader ───

async fn tap_forwarder(channel: Arc<SniffChannel>, tap: Arc<dyn WireTap>) {
    while let Some(chunk) = channel.recv().await {
        tap.on_read(&chunk, now_micros());
    }
}

// ── AsyncRead ──────────────────────────────────────────────────────────────

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncRead for SniffIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = match &mut self.state {
            SniffState::Passthrough(inner) => Pin::new(inner).poll_read(cx, buf),
            SniffState::Sniffing {
                data_channel,
                reader_handle,
                pending,
                ..
            } => {
                // Drain leftover from the previous partial read.
                if let Some(p) = pending {
                    let n = p.len().min(buf.remaining());
                    buf.put_slice(&p[..n]);
                    if n < p.len() {
                        *pending = Some(p.slice(n..));
                    } else {
                        *pending = None;
                    }
                    return Poll::Ready(Ok(()));
                }
                // Try to get a chunk from the data channel.
                match data_channel.poll_recv(cx) {
                    Ok(Some(data)) => {
                        let n = data.len().min(buf.remaining());
                        buf.put_slice(&data[..n]);
                        if n < data.len() {
                            *pending = Some(data.slice(n..));
                        }
                        Poll::Ready(Ok(()))
                    }
                    Ok(None) => {
                        // If the background reader task has terminated
                        // (panic or unexpected exit) without sending an
                        // error, surface the fault instead of hanging.
                        if reader_handle.is_finished() {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "background reader terminated",
                            )));
                        }
                        Poll::Pending
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
        };
        if let Poll::Ready(Ok(())) = &result {
            if !buf.filled().is_empty() {
                if let Some(t) = &self.timing {
                    t.touch();
                }
            }
        }
        result
    }
}

// ── AsyncWrite ─────────────────────────────────────────────────────────────

impl<T: AsyncWrite + AsyncRead + Unpin> AsyncWrite for SniffIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = match &mut self.state {
            SniffState::Passthrough(inner) => Pin::new(inner).poll_write(cx, buf),
            SniffState::Sniffing { writer, .. } => Pin::new(writer).poll_write(cx, buf),
        };
        if let Poll::Ready(Ok(n)) = &result {
            if *n > 0 {
                if let Some(tap) = &self.tap {
                    tap.on_write(&buf[..*n], now_micros());
                }
                if let Some(t) = &self.timing {
                    t.touch();
                }
            }
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            SniffState::Passthrough(inner) => Pin::new(inner).poll_flush(cx),
            SniffState::Sniffing { writer, .. } => Pin::new(writer).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            SniffState::Passthrough(inner) => Pin::new(inner).poll_shutdown(cx),
            SniffState::Sniffing { writer, .. } => Pin::new(writer).poll_shutdown(cx),
        }
    }
}
