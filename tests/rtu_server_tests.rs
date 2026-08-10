// SPDX-License-Identifier: MIT OR Apache-2.0
//! RTU Server offline tests — serve_forever correctness via tokio::io::duplex.
//!
//! RtuServer<T> is generic over T: AsyncRead + AsyncWrite, so duplex works
//! perfectly. Tests cover: valid request/response round-trip, CRC error
//! silent discard, unknown FC reject, fragmented arrival, service exceptions,
//! garbage recovery, and boundary frame sizes.

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::codec::calculate_crc;
use oms_modbus::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Helpers ──────────────────────────────────────────────────────────────

/// Build a complete RTU frame: [slave_id, pdu..., crc_lo, crc_hi]
fn rtu_frame(slave: u8, pdu: &[u8]) -> Vec<u8> {
    let mut data = vec![slave];
    data.extend_from_slice(pdu);
    let crc = calculate_crc(&data);
    data.push(crc as u8);
    data.push((crc >> 8) as u8);
    data
}

/// Validate RTU response frame: check CRC, extract slave + PDU.
fn parse_rtu_response(raw: &[u8]) -> (u8, Vec<u8>) {
    assert!(raw.len() >= 5, "response too short: {} bytes", raw.len());
    let crc_received = u16::from_le_bytes([raw[raw.len() - 2], raw[raw.len() - 1]]);
    let crc_calc = calculate_crc(&raw[..raw.len() - 2]);
    assert_eq!(
        crc_received, crc_calc,
        "CRC mismatch in response: received {crc_received:#06X}, calculated {crc_calc:#06X}"
    );
    let frame = &raw[..raw.len() - 2];
    (frame[0], frame[1..].to_vec())
}

/// Spawn RTU server on a duplex half, return the client half.
fn spawn_rtu_server(store: Arc<SlaveStore>) -> tokio::io::DuplexStream {
    let (client_stream, server_stream) = tokio::io::duplex(1024);
    let server = rtu::RtuServer::new(server_stream);
    tokio::spawn(async move {
        server.serve_forever(store).await.ok();
    });
    client_stream
}

// ── Basic request/response ───────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_read_holding_registers() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_rtu_server(store);

    // RTU frame: slave=1, FC=3, addr=0, count=1
    let req = rtu_frame(1, &[0x03, 0x00, 0x00, 0x00, 0x01]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (slave, pdu) = parse_rtu_response(&buf[..n]);
    assert_eq!(slave, 1);
    // PDU = FC(1) + byte_count(1) + 2 data bytes
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x2A]);
}

#[tokio::test]
async fn serve_forever_write_single_register() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 0)]));
    let mut stream = spawn_rtu_server(store.clone());

    // Write value 999 (0x03E7) to register 0
    let req = rtu_frame(1, &[0x06, 0x00, 0x00, 0x03, 0xE7]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    // WriteSingleRegister echoes: [FC, addr_hi, addr_lo, val_hi, val_lo]
    assert_eq!(pdu, vec![0x06, 0x00, 0x00, 0x03, 0xE7]);

    // Verify store was actually modified
    assert_eq!(store.read_holding_register(0), 999);
}

#[tokio::test]
async fn serve_forever_multiple_function_codes() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 100)]));
    let mut stream = spawn_rtu_server(store);

    // Use one duplex connection for multiple requests to one server
    for (fc, pdu, expected_fc) in [
        // ReadCoils: FC=1, addr=0, count=8 → response has byte_count=1
        (1, vec![0x01, 0x00, 0x00, 0x00, 0x08], 0x01u8),
        // ReadDiscreteInputs: FC=2
        (2, vec![0x02, 0x00, 0x00, 0x00, 0x08], 0x02u8),
        // ReadInputRegisters: FC=4
        (4, vec![0x04, 0x00, 0x00, 0x00, 0x01], 0x04u8),
        // WriteSingleCoil: FC=5, addr=0, ON
        (5, vec![0x05, 0x00, 0x00, 0xFF, 0x00], 0x05u8),
        // WriteMultipleCoils: FC=15, addr=0, count=8
        (15, vec![0x0F, 0x00, 0x00, 0x00, 0x08, 0x01, 0xFF], 0x0Fu8),
    ] {
        let req = rtu_frame(1, &pdu);
        stream.write_all(&req).await.unwrap();

        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let (_slave, rsp_pdu) = parse_rtu_response(&buf[..n]);
        assert_eq!(
            rsp_pdu[0], expected_fc,
            "FC {fc}: expected response FC {expected_fc:#04X}, got {:#04X}",
            rsp_pdu[0]
        );
    }
}

// ── Error handling ───────────────────────────────────────────────────────

#[tokio::test]
async fn serve_forever_crc_error_silent_discard() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_rtu_server(store);

    // Frame with deliberately wrong CRC
    let pdu = [0x03, 0x00, 0x00, 0x00, 0x01];
    let mut bad_frame = vec![1u8];
    bad_frame.extend_from_slice(&pdu);
    bad_frame.push(0xFF); // wrong CRC lo
    bad_frame.push(0xFF); // wrong CRC hi
    stream.write_all(&bad_frame).await.unwrap();

    // Server should produce NO response for CRC error (silent discard).
    // Bad frame data is flushed after T3.5 timeout — server continues cleanly.
    let mut buf = [0u8; 256];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "CRC error frame must produce no response (timeout)"
    );
}

#[tokio::test]
async fn serve_forever_unknown_function_code() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_rtu_server(store);

    // Valid CRC, but FC=0x46 is not a known function code
    let req = rtu_frame(1, &[0x46, 0x00, 0x00]);
    stream.write_all(&req).await.unwrap();

    // Server rejects at parser level → no response
    let mut buf = [0u8; 256];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "unknown FC frame must produce no response (timeout)"
    );
}

#[tokio::test]
async fn serve_forever_service_exception() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_rtu_server(store);

    // Diagnostic with unsupported sub-function (0x00FF) triggers
    // Exception::IllegalDataValue from SlaveStore
    let req = rtu_frame(1, &[0x08, 0x00, 0xFF, 0x00, 0x00]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
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
async fn serve_forever_fragmented_frame_arrival() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
    let mut stream = spawn_rtu_server(store);

    // Build a ReadHoldingRegisters frame and write it byte-by-byte
    let frame = rtu_frame(1, &[0x03, 0x00, 0x00, 0x00, 0x01]);
    for &byte in &frame {
        stream.write_all(&[byte]).await.unwrap();
        // Small delay to simulate real serial byte arrival
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x4D]); // value = 77
}

#[tokio::test]
async fn serve_forever_diagnostic_request() {
    let store = Arc::new(SlaveStore::new());
    let mut stream = spawn_rtu_server(store);

    // 5-byte exception frame: slave=1, FC=0x83, code=0x02
    let frame = rtu_frame(1, &[0x83, 0x02]);
    assert_eq!(frame.len(), 5);

    // Send a valid request first (Diagnostic)
    let req = rtu_frame(1, &[0x08, 0x00, 0x00, 0xAB, 0xCD]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    assert_eq!(pdu.len(), 5, "5-byte PDU expected for Diagnostic echo");
    assert_eq!(pdu[0], 0x08);
}

#[tokio::test]
async fn serve_forever_max_size_frame() {
    // 125 registers read → request PDU = 5 bytes, response PDU = 252 bytes
    // RTU frame = 1 (slave) + 252 + 2 (CRC) = 255 bytes (within MAX_ADU_SIZE=256)
    let _regs: Vec<u16> = vec![0xABCD; 125];
    let store = Arc::new(SlaveStore::with_holding_registers(
        &(0..125u16).map(|i| (i, i)).collect::<Vec<_>>(),
    ));
    let mut stream = spawn_rtu_server(store);

    let req = rtu_frame(1, &[0x03, 0x00, 0x00, 0x00, 125u8]);
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 300];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    assert_eq!(pdu[0], 0x03);
    assert_eq!(pdu[1], 250, "125 registers × 2 = 250 bytes byte_count");
    assert_eq!(pdu.len(), 252, "PDU: FC + byte_count + 250 data bytes");
}

// ── Recovery from bad data ───────────────────────────────────────────────

/// Partial frame (incomplete, no CRC) — server accumulates but never matches
/// CRC, so no response is sent. Verifies no panic/crash.
#[tokio::test]
async fn serve_forever_partial_frame_no_crash() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_rtu_server(store);

    // Send only first 3 bytes of a frame (no CRC)
    stream.write_all(&[0x01, 0x03, 0x00]).await.unwrap();

    // Server accumulates bytes but CRC never matches for a partial frame.
    // After T3.5 timeout, the buffer is cleared — no response is sent.
    // Expect timeout (server blocks waiting for more data, then flushes).
    let mut buf = [0u8; 256];
    let result = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "partial frame should produce no response (timeout)"
    );
}

/// Garbage bytes don't crash the server. The T3.5 timeout flushes
/// unparseable data, so the same connection recovers and processes the
/// next valid frame cleanly.
#[tokio::test]
async fn serve_forever_garbage_bytes_no_crash() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let mut stream = spawn_rtu_server(store);

    // Send garbage bytes — no valid CRC
    stream
        .write_all(&[0xFF, 0x00, 0xAA, 0x55, 0x11])
        .await
        .unwrap();

    // Wait for T3.5 timeout to flush garbage from server buffer.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Same connection — send a valid frame, must get a response.
    let valid = rtu_frame(1, &[0x03, 0x00, 0x00, 0x00, 0x01]);
    stream.write_all(&valid).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x2A]);
}

/// After a partial frame is flushed by T3.5 timeout, the next valid frame
/// on the same connection is processed correctly — no garbage contamination.
#[tokio::test]
async fn serve_forever_recovers_after_partial_frame() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 77)]));
    let mut stream = spawn_rtu_server(store);

    // Send only the first 3 bytes of a frame — no CRC, not parseable.
    stream.write_all(&[0x01, 0x03, 0x00]).await.unwrap();

    // Wait for T3.5 timeout to flush the partial bytes.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Now send a complete valid frame.
    let valid = rtu_frame(1, &[0x03, 0x00, 0x00, 0x00, 0x01]);
    stream.write_all(&valid).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    assert_eq!(pdu, vec![0x03, 0x02, 0x00, 0x4D]); // value = 77
}

// ── WriteMultipleRegisters server-side ───────────────────────────────────

#[tokio::test]
async fn serve_forever_write_multiple_registers() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 0),
        (1, 0),
        (2, 0),
    ]));
    let mut stream = spawn_rtu_server(store.clone());

    // Write Multiple Registers: FC=16, addr=0, count=3, byte_count=6, values=[1,2,3]
    let req = rtu_frame(
        1,
        &[
            0x10, 0x00, 0x00, 0x00, 0x03, 0x06, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03,
        ],
    );
    stream.write_all(&req).await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let (_slave, pdu) = parse_rtu_response(&buf[..n]);
    // Response: FC=16, addr=0, count=3
    assert_eq!(pdu, vec![0x10, 0x00, 0x00, 0x00, 0x03]);

    // Verify store was modified
    assert_eq!(store.read_holding_register(0), 1);
    assert_eq!(store.read_holding_register(1), 2);
    assert_eq!(store.read_holding_register(2), 3);
}
