// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for `ModbusError` — display, detail, code, and I/O error mapping.

use oms_modbus::ModbusError;

#[test]
fn serial_error_display_is_concise() {
    let err = ModbusError::serial("failed to open serial port COM1: system error.");
    assert_eq!(err.to_string(), "PORT ERROR");
}

#[test]
fn serial_error_detail_is_full() {
    let err = ModbusError::serial("failed to open serial port COM1: system error.");
    assert_eq!(
        err.detail(),
        "PORT ERROR: failed to open serial port COM1: system error."
    );
}

#[test]
fn connection_error_display_is_concise() {
    let err = ModbusError::connection("TCP connect to 127.0.0.1:502: Connection refused");
    assert_eq!(err.to_string(), "CONNECTION ERROR");
    assert_eq!(
        err.detail(),
        "CONNECTION ERROR: TCP connect to 127.0.0.1:502: Connection refused"
    );
}

#[test]
fn timeout_error_display_is_concise() {
    let err = ModbusError::timeout("ASCII recv timed out");
    assert_eq!(err.to_string(), "TIMEOUT");
    assert_eq!(err.detail(), "TIMEOUT: ASCII recv timed out");
}

#[test]
fn protocol_error_display_is_concise() {
    let err = ModbusError::protocol("PDU decode: invalid length 0");
    assert_eq!(err.to_string(), "PROTOCOL ERROR");
    assert_eq!(err.detail(), "PROTOCOL ERROR: PDU decode: invalid length 0");
}

#[test]
fn exception_error_display_is_human_readable() {
    let err = ModbusError::exception(4, 1);
    assert_eq!(err.to_string(), "Illegal Function (code=1)");
}

#[test]
fn exception_error_detail_has_fc_context() {
    let err = ModbusError::exception(4, 2);
    assert_eq!(err.detail(), "Illegal Data Address (FC=4, code=2)");
}

#[test]
fn from_string_becomes_other_and_displays_raw_message() {
    let err = ModbusError::from("something went wrong".to_string());
    assert_eq!(err.to_string(), "something went wrong");
    assert_eq!(err.detail(), "something went wrong");
}

#[test]
fn io_error_timeout_kind_maps_to_timeout() {
    let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
    let err = ModbusError::from(io_err);
    assert!(matches!(err, ModbusError::Timeout(_)));
}

#[test]
fn io_error_connection_refused_maps_to_connection() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
    let err = ModbusError::from(io_err);
    assert!(matches!(err, ModbusError::Connection(_)));
}

#[test]
fn io_error_broken_pipe_maps_to_connection() {
    let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
    let err = ModbusError::from(io_err);
    assert!(matches!(err, ModbusError::Connection(_)));
}

#[test]
fn io_error_other_kinds_become_other() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
    let err = ModbusError::from(io_err);
    assert!(matches!(err, ModbusError::Other(_)));
}

#[test]
fn error_labels_are_consistent() {
    assert_eq!(ModbusError::no_error().label(), "OK");
    assert_eq!(ModbusError::no_error().to_string(), "OK");
    assert_eq!(ModbusError::no_error().detail(), "OK");
    assert_eq!(ModbusError::timeout("x").label(), "TIMEOUT");
    assert_eq!(ModbusError::connection("x").label(), "CONNECTION ERROR");
    assert_eq!(ModbusError::protocol("x").label(), "PROTOCOL ERROR");
    assert_eq!(ModbusError::exception(3, 2).label(), "MODBUS EXCEPTION");
    assert_eq!(ModbusError::serial("x").label(), "PORT ERROR");
    assert_eq!(ModbusError::other("x").label(), "ERROR");
}

// ── Exception::Custom variant ──────────────────────────────────────────

#[test]
fn exception_custom_roundtrip() {
    let exc = oms_modbus::Exception::Custom(99);
    assert_eq!(u8::from(exc), 99);
    let back = oms_modbus::Exception::from(99u8);
    assert_eq!(back, oms_modbus::Exception::Custom(99));
}

#[test]
fn exception_custom_edge_values() {
    // Valid exception codes 1-11 are mapped to named variants;
    // everything else is Custom.
    for code in [0u8, 9, 12, 100, 255] {
        let exc = oms_modbus::Exception::from(code);
        assert!(matches!(exc, oms_modbus::Exception::Custom(_)));
        assert_eq!(u8::from(exc), code);
    }
}

// ── From<Response> for ModbusError ─────────────────────────────────────

#[test]
fn response_exception_converts_to_modbus_error() {
    let rsp = oms_modbus::Response::Exception(3, oms_modbus::Exception::IllegalDataAddress);
    let err: oms_modbus::ModbusError = oms_modbus::ModbusError::from(rsp);
    assert!(matches!(err, oms_modbus::ModbusError::Exception { .. }));
    assert_eq!(err.label(), "MODBUS EXCEPTION");
}

#[test]
fn non_exception_response_converts_to_protocol_error() {
    // Non-exception responses in an error context now produce Protocol errors
    // instead of NoError (P1 fix: NoError should never appear in Err path).
    let rsp = oms_modbus::Response::ReadHoldingRegisters(vec![1, 2, 3]);
    let err = oms_modbus::ModbusError::from(rsp);
    assert!(matches!(err, oms_modbus::ModbusError::Protocol(_)));
    assert!(err.detail().contains("unexpected success response"));
}

// ── I/O Error WouldBlock mapping ───────────────────────────────────────

#[test]
fn io_error_would_block_maps_to_timeout() {
    let io_err = std::io::Error::new(std::io::ErrorKind::WouldBlock, "would block");
    let err = oms_modbus::ModbusError::from(io_err);
    assert!(matches!(err, oms_modbus::ModbusError::Timeout(_)));
}

// ── is_retryable ───────────────────────────────────────────────────────

#[test]
fn is_retryable_transport_errors() {
    use oms_modbus::reconnect::is_retryable;
    // Only hard physical-layer failures are retryable (connection, serial).
    // Timeout is NOT retryable — it's ambiguous (slow device, wrong slave ID, etc.)
    assert!(is_retryable(&oms_modbus::ModbusError::connection("c")));
    assert!(is_retryable(&oms_modbus::ModbusError::serial("s")));
}

#[test]
fn is_retryable_timeout_not_retried() {
    use oms_modbus::reconnect::is_retryable;
    // Timeout should NOT trigger reconnect — it's often a protocol-level
    // issue (wrong slave, silent reject) rather than a transport failure.
    assert!(!is_retryable(&oms_modbus::ModbusError::timeout("t")));
}

#[test]
fn is_retryable_non_transport_errors() {
    use oms_modbus::reconnect::is_retryable;
    assert!(!is_retryable(&oms_modbus::ModbusError::no_error()));
    assert!(!is_retryable(&oms_modbus::ModbusError::protocol("p")));
    assert!(!is_retryable(&oms_modbus::ModbusError::exception(3, 2)));
    assert!(!is_retryable(&oms_modbus::ModbusError::other("o")));
}
// ── P2-6: Error boundary values ──────────────────────────────────────────

#[test]
fn modbus_error_other_empty_string_no_panic() {
    let err = oms_modbus::ModbusError::Other(String::new());
    // Display on empty message → empty string (by design: Display = msg itself)
    let s = format!("{err}");
    assert_eq!(s, "", "empty Other Display = empty message");
    // label() must return a valid label
    let label = err.label();
    assert!(!label.is_empty());
    // detail() must return empty for an empty Other error
    let detail = err.detail();
    assert_eq!(detail, "", "empty Other → empty detail");

    // Also test single-char and special chars
    for msg in &["x", "\n", "错误"] {
        let err = oms_modbus::ModbusError::Other((*msg).into());
        let _ = format!("{err}");
        let _ = err.detail();
    }
}
