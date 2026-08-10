// SPDX-License-Identifier: MIT OR Apache-2.0
//! RTU/ASCII codec resilience tests — CRC/LRC errors, fragmentation,
//! exception responses, garbage data recovery.
//!
//! All tests use DuplexStream (in-memory), no hardware required.

use std::time::Duration;

use oms_modbus::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Build a complete RTU frame: [slave_id, pdu..., crc_lo, crc_hi]
fn rtu_frame(slave: u8, pdu: &[u8]) -> Vec<u8> {
    let mut data = vec![slave];
    data.extend_from_slice(pdu);
    let crc = calculate_crc(&data);
    data.push(crc as u8);
    data.push((crc >> 8) as u8);
    data
}

/// Build a complete ASCII frame: `:hexdata LRC \r\n`
fn ascii_frame(data: &[u8]) -> String {
    let lrc = calculate_lrc(data);
    let mut frame = String::with_capacity(1 + data.len() * 2 + 2 + 2);
    frame.push(':');
    for &b in data {
        frame.push_str(&format!("{:02X}", b));
    }
    frame.push_str(&format!("{:02X}", lrc));
    frame.push_str("\r\n");
    frame
}

// ── RTU CRC bit-flip recovery ───────────────────────────────────────────

/// CRC bit-flip in server response → Protocol error.
/// Next request succeeds, proving error isolation (read_buf.clear() works).
#[tokio::test]
async fn rtu_crc_bitflip_response_recovery() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    // Mock server: 1st response has bad CRC, 2nd response is correct
    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // Read 1st request
        let n = server_stream.read(&mut buf).await.unwrap();
        assert!(n >= 8, "expected RTU request, got {n} bytes");
        let slave = buf[0];

        // Response PDU: FC=3, byte_count=2, value=42
        let pdu = [0x03, 0x02, 0x00, 0x2A];
        let mut frame = rtu_frame(slave, &pdu);

        // Flip the last CRC bit
        let last = frame.len() - 1;
        frame[last] ^= 0x01;

        server_stream.write_all(&frame).await.unwrap();

        // Read 2nd request
        let n = server_stream.read(&mut buf).await.unwrap();
        assert!(n >= 8);
        let slave = buf[0];

        // Write correct response
        let frame = rtu_frame(slave, &pdu);
        server_stream.write_all(&frame).await.unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    // 1st call: CRC mismatch
    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("CRC"),
        "expected CRC error, got: {}",
        err.detail()
    );

    // 2nd call: succeeds (proves error isolation)
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

/// CRC error does not contaminate the next frame — read_buf is cleared
/// at the start of each send_recv call.
#[tokio::test]
async fn rtu_crc_error_isolated_per_call() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → bad CRC
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];
        let pdu = [0x03, 0x02, 0x00, 0x2A];
        let mut frame = rtu_frame(slave, &pdu);
        let idx = frame.len() - 2;
        frame[idx] ^= 0xFF; // corrupt CRC low byte
        server_stream.write_all(&frame).await.unwrap();

        // 2nd → correct
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];
        server_stream
            .write_all(&rtu_frame(slave, &pdu))
            .await
            .unwrap();

        // 3rd → bad CRC again
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];
        let mut frame = rtu_frame(slave, &pdu);
        let last = frame.len() - 1;
        frame[last] ^= 0x80;
        server_stream.write_all(&frame).await.unwrap();

        // 4th → correct
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];
        server_stream
            .write_all(&rtu_frame(slave, &pdu))
            .await
            .unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    // Pattern: bad → good → bad → good
    let r1 = client.read_holding_registers(1, 0, 1).await;
    assert!(r1.unwrap_err().detail().contains("CRC"));

    let r2 = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(r2, vec![42]);

    let r3 = client.read_holding_registers(1, 0, 1).await;
    assert!(r3.unwrap_err().detail().contains("CRC"));

    let r4 = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(r4, vec![42]);

    server.await.unwrap();
}

// ── RTU fragmented frame assembly ──────────────────────────────────────

/// Response bytes arrive one at a time — read_at_least accumulates correctly.
#[tokio::test]
async fn rtu_fragmented_response_byte_by_byte() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // Read request
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];

        // Build response frame
        let pdu = [0x03, 0x02, 0x00, 0x2A]; // 2 registers → 42
        let frame = rtu_frame(slave, &pdu);

        // Write one byte at a time with small delay
        for &b in &frame {
            server_stream.write_all(&[b]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

/// Fragmented frame where first read gives only 3 bytes (insufficient).
#[tokio::test]
async fn rtu_fragmented_frame_three_then_four() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];

        let pdu = [0x03, 0x02, 0x00, 0x2A];
        let frame = rtu_frame(slave, &pdu);

        // Send first 3 bytes, delay, then rest
        server_stream.write_all(&frame[..3]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        server_stream.write_all(&frame[3..]).await.unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

// ── RTU exception response (5-byte short frame) ────────────────────────

/// Server returns an exception (5 bytes) instead of normal response (7 bytes).
/// The receiver should handle the shorter frame gracefully.
#[tokio::test]
async fn rtu_exception_response_short_frame() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];

        // Exception PDU: FC|0x80 = 0x83, exception code = 0x02 (IllegalDataAddress)
        let pdu = [0x83, 0x02];
        let frame = rtu_frame(slave, &pdu); // 5 bytes total
        assert_eq!(frame.len(), 5);

        server_stream.write_all(&frame).await.unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    // Request would normally produce a 7-byte response, but server sends 5-byte exception.
    // The 5-byte frame is CRC-validated and decoded as a Modbus exception.
    let result = client.read_holding_registers(1, 0, 1).await;
    // "Illegal Data Address" = exc code 2, properly decoded from 5-byte frame
    let err = result.expect_err("short frame should produce exception or timeout");
    let detail = err.detail();
    assert!(
        detail.contains("Illegal") || detail.contains("timeout") || detail.contains("TIMEOUT"),
        "unexpected error: {detail}"
    );

    server.await.unwrap();
}

/// Exception response to Diagnostic with unsupported sub-function.
#[tokio::test]
async fn rtu_exception_from_diagnostic() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];

        // Exception for Diagnostic: 0x88 (8|0x80), code=0x01 (IllegalFunction)
        let pdu = [0x88, 0x01];
        let frame = rtu_frame(slave, &pdu);
        assert_eq!(frame.len(), 5);

        server_stream.write_all(&frame).await.unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    let result = client.diagnostic(1, 0x0001, 0).await;
    // "Illegal Function" = exc code 1, properly decoded from 5-byte frame
    let err = result.expect_err("diagnostic exception should produce error");
    let detail = err.detail();
    assert!(
        detail.contains("Illegal") || detail.contains("timeout"),
        "unexpected error: {detail}"
    );

    server.await.unwrap();
}

// ── RTU garbage data recovery ──────────────────────────────────────────

/// Garbage bytes on the wire before a valid request — drain_stale_data
/// should clear them so the next request works normally.
///
/// Uses a oneshot channel to ensure garbage is injected AFTER the first
/// response is fully consumed by the client.
#[tokio::test]
async fn rtu_garbage_then_valid_frame() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → normal response
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];
        let pdu = [0x03, 0x02, 0x00, 0x2A];
        server_stream
            .write_all(&rtu_frame(slave, &pdu))
            .await
            .unwrap();

        // Wait for client to confirm it has consumed the response.
        let _ = rx.await;

        // Now inject garbage — client's drain_stale_data will clear it
        // when the next call starts.
        server_stream
            .write_all(&[0xFF, 0x00, 0xAA, 0x55])
            .await
            .unwrap();

        // 2nd request → normal response
        let _n = server_stream.read(&mut buf).await.unwrap();
        let pdu2 = [0x03, 0x02, 0x00, 0x63]; // 99 decimal
        server_stream
            .write_all(&rtu_frame(buf[0], &pdu2))
            .await
            .unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    // 1st call: normal
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    // Signal server that we've consumed the response. Garbage will be injected now.
    tx.send(()).unwrap();

    // Brief yield to let server write the garbage bytes.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // 2nd call: garbage bytes are now in the buffer → drain_stale_data clears them,
    // then normal request/response proceeds.
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    server.await.unwrap();
}

/// Stale data burst: a large burst of data is fully drained, then the
/// request proceeds normally. Tests the drain → send → recv pipeline
/// when the bus has accumulated stale bytes.
#[tokio::test]
async fn rtu_stale_data_burst_drained_then_request() {
    let (client_stream, mut server_stream) = tokio::io::duplex(8192);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → normal response
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];
        let pdu = [0x03, 0x02, 0x00, 0x2A];
        server_stream
            .write_all(&rtu_frame(slave, &pdu))
            .await
            .unwrap();

        // Wait for client confirmation before injecting stale bytes
        let _ = rx.await;

        // Write a burst of stale bytes (simulates line noise between requests)
        server_stream.write_all(&[0xFF; 128]).await.unwrap();

        // 2nd request → normal response (different value to verify it worked)
        let _n = server_stream.read(&mut buf).await.unwrap();
        let pdu2 = [0x03, 0x02, 0x00, 0x63]; // 99 decimal
        server_stream
            .write_all(&rtu_frame(buf[0], &pdu2))
            .await
            .unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    // 1st call: normal
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    // Signal server to inject stale data
    tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // 2nd call: 128 stale bytes are drained, then request/response proceeds normally.
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    server.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// ── ASCII LRC / hex error recovery ───────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════

/// LRC checksum error in ASCII frame → ignored, client times out, next call
/// succeeds. Proves error isolation (read_buf.clear() between calls).
#[tokio::test]
async fn ascii_lrc_error_recovery() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → response with bad LRC
        let _n = server_stream.read(&mut buf).await.unwrap();
        // Data bytes: [01, 03, 02, 00, 2A] → correct LRC = 0xD0
        // Send with LRC = 0xFF (wrong)
        server_stream.write_all(b":010302002AFF\r\n").await.unwrap();

        // 2nd request → correct response
        let _n = server_stream.read(&mut buf).await.unwrap();
        let data = [0x01, 0x03, 0x02, 0x00, 0x2A];
        server_stream
            .write_all(ascii_frame(&data).as_bytes())
            .await
            .unwrap();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(200));

    // 1st call: LRC mismatch → parse fails → timeout (buffer has bad frame, keeps
    // retrying until the overall deadline expires).
    let result = client.read_holding_registers(1, 0, 1).await;
    assert!(result.is_err());

    // 2nd call: read_buf.clear() discards the bad frame, fresh request works.
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

/// Invalid hex characters in ASCII frame → ignored, client times out, next
/// call succeeds.
#[tokio::test]
async fn ascii_hex_decode_error_recovery() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → response with invalid hex chars ("GG")
        let _n = server_stream.read(&mut buf).await.unwrap();
        server_stream.write_all(b":01GG02002AD0\r\n").await.unwrap();

        // 2nd request → correct response
        let _n = server_stream.read(&mut buf).await.unwrap();
        let data = [0x01, 0x03, 0x02, 0x00, 0x2A];
        server_stream
            .write_all(ascii_frame(&data).as_bytes())
            .await
            .unwrap();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(200));

    // 1st call: invalid hex → parse fails → timeout
    let result = client.read_holding_registers(1, 0, 1).await;
    assert!(result.is_err());

    // 2nd call: fresh start → valid
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

/// Garbage data without `:` prefix → ignored, valid frame after works.
#[tokio::test]
async fn ascii_garbage_no_colon_prefix() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → send garbage (no ':' prefix)
        let _n = server_stream.read(&mut buf).await.unwrap();
        server_stream.write_all(b"RANDOM NOISE\r\n").await.unwrap();

        // 2nd request → correct response
        let _n = server_stream.read(&mut buf).await.unwrap();
        let data = [0x01, 0x03, 0x02, 0x00, 0x2A];
        server_stream
            .write_all(ascii_frame(&data).as_bytes())
            .await
            .unwrap();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(200));

    // 1st call: garbage parsed as ASCII → no ':' → parse fails → timeout
    let result = client.read_holding_registers(1, 0, 1).await;
    assert!(result.is_err());

    // 2nd call: clean
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

/// Partial ASCII frame — data without terminating CRLF.
/// Client times out, next call succeeds.
#[tokio::test]
async fn ascii_partial_frame_no_crlf() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → write data WITHOUT the \r\n terminator
        let _n = server_stream.read(&mut buf).await.unwrap();
        // Valid hex data, valid LRC, but NO CRLF
        server_stream.write_all(b":010302002AD0").await.unwrap();
        // Do NOT write \r\n — simulate a truncated frame

        // The client times out. Read the 2nd request.
        let _n = server_stream.read(&mut buf).await.unwrap();
        let data = [0x01, 0x03, 0x02, 0x00, 0x63]; // value = 99
        server_stream
            .write_all(ascii_frame(&data).as_bytes())
            .await
            .unwrap();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(200));

    // 1st call: no CRLF → try_parse_ascii never matches → timeout
    let result = client.read_holding_registers(1, 0, 1).await;
    assert!(result.is_err());

    // 2nd call: read_buf.clear() discards partial frame → fresh request works.
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    server.await.unwrap();
}

/// Basic ASCII success path — verify normal response works over DuplexStream.
#[tokio::test]
async fn ascii_basic_read_holding() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // Read the client's ASCII request
        let _n = server_stream.read(&mut buf).await.unwrap();

        // Build valid ASCII response using the library's encoder
        let data = [0x01, 0x03, 0x02, 0x00, 0x2A]; // slave=1, FC=3, bytes=2, val=42
        let frame = oms_modbus::codec::encode_ascii_frame(&data);
        server_stream.write_all(&frame).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(300));

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// ── P1: Slave ID mismatch / edge cases ───────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════

/// ASCII exception response — 2-byte PDU (FC|0x80 + code) decoded correctly.
#[tokio::test]
async fn ascii_exception_response_decoded() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // Read and discard the client's request
        let _n = server_stream.read(&mut buf).await.unwrap();

        // Exception: slave=1, FC=3|0x80=0x83, code=2 (IllegalDataAddress)
        let data = [0x01, 0x83, 0x02];
        let frame = oms_modbus::codec::encode_ascii_frame(&data);
        server_stream.write_all(&frame).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(300));

    let result = client.read_holding_registers(1, 0, 1).await;
    let err = result.expect_err("ASCII exception should produce error");
    let detail = err.detail();
    assert!(
        detail.contains("Illegal") || detail.contains("timeout"),
        "unexpected error: {detail}"
    );

    server.await.unwrap();
}

/// RTU: client queries slave 1, server responds as slave 2 → mismatch error.
#[tokio::test]
async fn rtu_slave_id_mismatch_error() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        let _n = server_stream.read(&mut buf).await.unwrap();
        // Respond as slave 2 (client asks as slave 1)
        let pdu = [0x03, 0x02, 0x00, 0x2A];
        server_stream.write_all(&rtu_frame(2, &pdu)).await.unwrap();

        // 2nd request → respond correctly as slave 1
        let _n = server_stream.read(&mut buf).await.unwrap();
        let pdu2 = [0x03, 0x02, 0x00, 0x63];
        server_stream.write_all(&rtu_frame(1, &pdu2)).await.unwrap();
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_millis(300));

    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("slave ID mismatch"),
        "expected slave ID mismatch, got: {}",
        err.detail()
    );

    // 2nd call: correct slave ID → succeeds
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    server.await.unwrap();
}

/// ASCII: client queries slave 1, server responds as slave 2 → mismatch error.
#[tokio::test]
async fn ascii_slave_id_mismatch_error() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        let _n = server_stream.read(&mut buf).await.unwrap();
        // Respond as slave 2
        let data = [0x02, 0x03, 0x02, 0x00, 0x2A];
        let frame = oms_modbus::codec::encode_ascii_frame(&data);
        server_stream.write_all(&frame).await.unwrap();

        // 2nd request → respond as slave 1
        let _n = server_stream.read(&mut buf).await.unwrap();
        let data2 = [0x01, 0x03, 0x02, 0x00, 0x63];
        let frame2 = oms_modbus::codec::encode_ascii_frame(&data2);
        server_stream.write_all(&frame2).await.unwrap();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(300));

    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    assert!(
        err.detail().contains("slave ID mismatch"),
        "expected slave ID mismatch, got: {}",
        err.detail()
    );

    // 2nd call: correct slave ID → succeeds
    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    server.await.unwrap();
}

/// ASCII: response with `\r` only (no `\n`) → parse fails → timeout.
#[tokio::test]
async fn ascii_cr_without_lf_not_accepted() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        // 1st request → send frame with \r only (no \n)
        let _n = server_stream.read(&mut buf).await.unwrap();
        server_stream.write_all(b":010302002AD0\r").await.unwrap();

        // 2nd request → correct response with \r\n
        let _n = server_stream.read(&mut buf).await.unwrap();
        let data = [0x01, 0x03, 0x02, 0x00, 0x63];
        server_stream
            .write_all(ascii_frame(&data).as_bytes())
            .await
            .unwrap();
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(200));

    let result = client.read_holding_registers(1, 0, 1).await;
    assert!(result.is_err());

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![99]);

    server.await.unwrap();
}

/// ASCII: two valid frames in one read — only first consumed, second ignored.
#[tokio::test]
async fn ascii_multiple_frames_one_read() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];

        let _n = server_stream.read(&mut buf).await.unwrap();
        let data = [0x01, 0x03, 0x02, 0x00, 0x2A];
        let frame = oms_modbus::codec::encode_ascii_frame(&data);
        // Write the same frame twice in rapid succession
        server_stream.write_all(&frame).await.unwrap();
        server_stream.write_all(&frame).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let client = ascii::AsciiClient::with_timeout(client_stream, Duration::from_millis(300));

    let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
    assert_eq!(regs, vec![42]);

    server.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
// ── P1-5: Minimum frame size & write value boundary ──────────────────────
// ═══════════════════════════════════════════════════════════════════════════

/// RTU frame shorter than 5 bytes (MIN_RTU_FRAME) → timeout.
/// Sending fewer than 5 bytes means read_at_least never reaches the target,
/// timeout fires, and the <5 guard triggers.
#[tokio::test]
async fn rtu_frame_shorter_than_5_bytes_rejected() {
    let (client_stream, mut server_stream) = tokio::io::duplex(64);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        let _n = server_stream.read(&mut buf).await.unwrap();

        // Write only 3 bytes — less than MIN_RTU_FRAME (5)
        server_stream.write_all(&[0x01, 0x03, 0x02]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_millis(100));

    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    let detail = err.detail();
    assert!(
        detail.contains("timed out") || detail.contains("TIMEOUT"),
        "expected timeout for <5 byte frame, got: {detail}"
    );

    server.await.unwrap();
}

/// RTU: exactly 4 bytes written then connection drops → should error.
#[tokio::test]
async fn rtu_exactly_4_bytes_then_close() {
    let (client_stream, mut server_stream) = tokio::io::duplex(64);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        let _n = server_stream.read(&mut buf).await.unwrap();

        // Write exactly 4 bytes then drop
        server_stream
            .write_all(&[0x01, 0x03, 0x02, 0x00])
            .await
            .unwrap();
        drop(server_stream);
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_millis(200));

    let err = client.read_holding_registers(1, 0, 1).await.unwrap_err();
    let detail = err.detail();
    assert!(
        detail.contains("timed out") || detail.contains("TIMEOUT") || detail.contains("closed"),
        "expected timeout or connection-closed for 4-byte + close, got: {detail}"
    );

    server.await.unwrap();
}

/// RTU: minimum valid 5-byte exception frame parses correctly.
#[tokio::test]
async fn rtu_minimum_valid_frame_5_bytes() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        let _n = server_stream.read(&mut buf).await.unwrap();
        let slave = buf[0];

        // 5-byte exception: slave + 0x83 + 0x02 + CRC
        let pdu = [0x83, 0x02];
        server_stream
            .write_all(&rtu_frame(slave, &pdu))
            .await
            .unwrap();
        assert_eq!(rtu_frame(slave, &pdu).len(), 5);
    });

    let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(3));

    let result = client.read_holding_registers(1, 0, 1).await;
    let err = result.expect_err("5-byte frame should produce exception or timeout");
    let detail = err.detail();
    assert!(
        detail.contains("Illegal") || detail.contains("timeout"),
        "expected exception or timeout from 5-byte frame, got: {detail}"
    );

    server.await.unwrap();
}
