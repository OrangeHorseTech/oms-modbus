// SPDX-License-Identifier: MIT OR Apache-2.0
//! ASCII Server offline tests — serve_forever correctness via tokio::io::duplex.
//!
//! AsciiServer<T> is generic over T: AsyncRead + AsyncWrite, so duplex works
//! perfectly. Tests cover: valid request/response roundtrip, LRC error
//! silent discard, bad hex reject, no-colon reject, no-CRLF hang, fragmented
//! arrival, service exceptions, lowercase hex, and boundary frame sizes.

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::codec;
use oms_modbus::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Helpers ──────────────────────────────────────────────────────────────

/// Build a complete ASCII frame: `:hexdataLRC\r\n`.
fn ascii_frame(data: &[u8]) -> Vec<u8> {
    codec::encode_ascii_frame(data)
}

/// Decode a single hex nibble.
fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'A'..=b'F' => c - b'A' + 10,
        b'a'..=b'f' => c - b'a' + 10,
        _ => panic!("not a hex character: {c:#04X}"),
    }
}

/// Validate ASCII response frame format and extract (slave, PDU).
///
/// Checks: starts with `:`, ends with `\r\n`, even hex count, LRC valid.
fn parse_ascii_response(raw: &[u8]) -> (u8, Vec<u8>) {
    assert!(raw.len() >= 7, "response too short: {} bytes", raw.len());
    assert_eq!(raw[0], b':', "ASCII response must start with ':'");
    let cr_pos = raw.len() - 2;
    assert_eq!(&raw[cr_pos..], b"\r\n", "ASCII response must end with CRLF");

    let hex_section = &raw[1..cr_pos];
    assert!(
        hex_section.len() % 2 == 0,
        "hex section must have even length"
    );

    let mut bytes = Vec::with_capacity(hex_section.len() / 2);
    for chunk in hex_section.chunks(2) {
        bytes.push((hex_val(chunk[0]) << 4) | hex_val(chunk[1]));
    }

    // Last byte is LRC
    let (lrc_byte, data) = bytes.split_last().unwrap();
    let calc = codec::calculate_lrc(data);
    assert_eq!(*lrc_byte, calc, "LRC mismatch in response");

    assert!(data.len() >= 2, "frame data too short");
    (data[0], data[1..].to_vec())
}

/// Spawn ASCII server on a duplex half, return the client half.
fn spawn_ascii_server(store: Arc<SlaveStore>) -> tokio::io::DuplexStream {
    let (client_stream, server_stream) = tokio::io::duplex(2048);
    let server = ascii::AsciiServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    client_stream
}

// ── Basic request/response ───────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_read_holding_registers() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_ascii_server(store);

    // ReadHoldingRegisters: FC=3, addr=0, count=1
    let req = ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (slave, pdu) = parse_ascii_response(&buf[..n]);
    assert_eq!(slave, 1);
    // PDU = FC(1) + byte_count(1) + 2 data bytes
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x2A]);
}

#[tokio::test]
async fn serve_forever_write_single_register() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 0)]));
    let mut stream = spawn_ascii_server(store.clone());

    // Write value 999 (0x03E7) to register 0
    let req = ascii_frame(&[0x01, 0x06, 0x00, 0x00, 0x03, 0xE7]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    // WriteSingleRegister echoes: [FC, addr_hi, addr_lo, val_hi, val_lo]
    assert_eq!(pdu, vec![0x06, 0x00, 0x00, 0x03, 0xE7]);

    assert_eq!(store.read_holding_register(0), 999);
}

#[tokio::test]
async fn serve_forever_diagnostic_return_query() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_ascii_server(store);

    // Diagnostic sub-function 0x0000 = Return Query Data
    let req = ascii_frame(&[0x01, 0x08, 0x00, 0x00, 0xAB, 0xCD]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    // Diagnostic echo: [FC, sub_hi, sub_lo, data_hi, data_lo]
    assert_eq!(pdu, vec![0x08, 0x00, 0x00, 0xAB, 0xCD]);
}

#[tokio::test]
async fn serve_forever_multiple_function_codes() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_ascii_server(store);

    for (fc, data) in [
        // ReadCoils: FC=1, addr=0, count=8
        // [slave=1, FC=1, addr_hi=0, addr_lo=0, count_hi=0, count_lo=8]
        (1, vec![0x01, 0x01, 0x00, 0x00, 0x00, 0x08]),
        // ReadDiscreteInputs: FC=2
        (2, vec![0x01, 0x02, 0x00, 0x00, 0x00, 0x08]),
        // ReadInputRegisters: FC=4
        (4, vec![0x01, 0x04, 0x00, 0x00, 0x00, 0x01]),
        // WriteSingleCoil: FC=5, addr=0, ON
        (5, vec![0x01, 0x05, 0x00, 0x00, 0xFF, 0x00]),
        // WriteMultipleCoils: FC=15, addr=0, count=8
        (15, vec![0x01, 0x0F, 0x00, 0x00, 0x00, 0x08, 0x01, 0xFF]),
    ] {
        let req = ascii_frame(&data);
        stream.write_all(&req).await.unwrap();

        let mut buf = [0u8; 512];
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let (_slave, pdu) = parse_ascii_response(&buf[..n]);
        assert_eq!(
            pdu[0], fc,
            "FC {fc}: expected response FC {fc:#04X}, got {:#04X}",
            pdu[0]
        );
    }
}

// ── Error handling ───────────────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_lrc_error_silent_discard() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_ascii_server(store);

    // Manually construct a frame with bad LRC (= 0x00 instead of correct value)
    // Correct: :010300000001FB  → wrong: :01030000000100
    let bad_frame = b":01030000000100\r\n".to_vec();
    stream.write_all(&bad_frame).await.unwrap();

    // Server silently discards — no response
    let mut buf = [0u8; 512];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "LRC error frame must produce no response (timeout)"
    );
}

#[tokio::test]
async fn serve_forever_bad_hex_characters_no_response() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_ascii_server(store);

    // ZZ is not valid hex
    let bad_frame = b":ZZ03\r\n".to_vec();
    stream.write_all(&bad_frame).await.unwrap();

    let mut buf = [0u8; 512];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "bad hex chars must produce no response (timeout)"
    );
}

#[tokio::test]
async fn serve_forever_no_colon_prefix_no_response() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_ascii_server(store);

    // Data without ':' prefix
    stream.write_all(b"010300000001FB\r\n").await.unwrap();

    let mut buf = [0u8; 512];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "no-colon data must produce no response (timeout)"
    );
}

#[tokio::test]
async fn serve_forever_no_crlf_terminator_no_response() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_ascii_server(store);

    // Valid hex + LRC, but no \r\n terminator
    stream.write_all(b":010300000001FB").await.unwrap();

    // Server blocks waiting for CRLF → timeout
    let mut buf = [0u8; 512];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "no-CRLF data must produce no response (timeout)"
    );
}

#[tokio::test]
async fn serve_forever_service_exception() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_ascii_server(store);

    // Diagnostic with unsupported sub-function (0x00FF) triggers
    // Exception::IllegalDataValue from SlaveStore
    let req = ascii_frame(&[0x01, 0x08, 0x00, 0xFF, 0x00, 0x00]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    // Exception response: FC | 0x80 = 0x88
    assert_eq!(
        pdu[0], 0x88,
        "expected exception FC 0x88, got {:#04X}",
        pdu[0]
    );
    assert_eq!(
        pdu[1], 0x03,
        "expected IllegalDataValue (0x03), got {:#04X}",
        pdu[1]
    );
}

// ── Byte arrival patterns ────────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_fragmented_arrival() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
    let mut stream = spawn_ascii_server(store);

    // Build a ReadHoldingRegisters frame and write it byte-by-byte
    let frame = ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
    for &byte in &frame {
        stream.write_all(&[byte]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x4D]); // value = 77
}

#[tokio::test]
async fn serve_forever_lowercase_hex_parsed() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99)]));
    let mut stream = spawn_ascii_server(store);

    // Construct a lowercase ASCII frame manually
    // ReadHoldingRegisters slave=1, addr=0, count=1
    let data = &[0x01, 0x03, 0x00, 0x00, 0x00, 0x01];
    let lrc = codec::calculate_lrc(data);
    let lower = format!(":010300000001{:02x}\r\n", lrc);
    stream.write_all(lower.as_bytes()).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    // The parser accepts lowercase → valid response
    assert_eq!(pdu[0], 0x03, "parser should accept lowercase hex");
    assert_eq!(pdu[1], 0x02);
}

// ── Boundary sizes ───────────────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_max_size_frame() {
    // 125 registers → PDU = 252 bytes → ASCII frame ~511 bytes
    let store = Arc::new(SlaveStore::with_holding_registers(
        &(0..125u16).map(|i| (i, i)).collect::<Vec<_>>(),
    ));
    let mut stream = spawn_ascii_server(store);

    let req = ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 125u8]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    assert_eq!(pdu[0], 0x03);
    assert_eq!(pdu[1], 250, "125 registers × 2 = 250 bytes byte_count");
    assert_eq!(pdu.len(), 252, "PDU: FC + byte_count + 250 data bytes");
}

// ── WriteMultipleRegisters ──────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_write_multiple_registers() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 0),
        (1, 0),
        (2, 0),
    ]));
    let mut stream = spawn_ascii_server(store.clone());

    // Write Multiple Registers: FC=16, addr=0, count=3, byte_count=6
    let req = ascii_frame(&[
        0x01, 0x10, 0x00, 0x00, 0x00, 0x03, 0x06, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03,
    ]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    // Response: FC=16, addr=0, count=3
    assert_eq!(pdu, vec![0x10, 0x00, 0x00, 0x00, 0x03]);

    assert_eq!(store.read_holding_register(0), 1);
    assert_eq!(store.read_holding_register(1), 2);
    assert_eq!(store.read_holding_register(2), 3);
}

/// After a CRLF-less partial frame is flushed by timeout, the next valid
/// frame on the same connection is processed correctly.
#[tokio::test]
async fn serve_forever_recovers_after_no_crlf() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
    let mut stream = spawn_ascii_server(store);

    // Send hex data without CRLF — server can't parse, accumulates bytes.
    stream.write_all(b":010300000001FB").await.unwrap();

    // Wait for timeout to flush the unterminated data.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now send a complete valid frame.
    let valid = ascii_frame(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
    stream.write_all(&valid).await.unwrap();

    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_ascii_response(&buf[..n]);
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x4D]); // value = 77
}
