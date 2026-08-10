// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for PDU encoding/decoding — roundtrip, exceptions, diagnostics.

use bytes::Bytes;
use oms_modbus::{Exception, Request, Response};
use std::borrow::Cow;
use std::convert::TryFrom;

#[test]
fn roundtrip_read_holding() {
    let req = Request::ReadHoldingRegisters(100, 3);
    let bytes = Bytes::try_from(req).unwrap();
    let req2 = Request::try_from(bytes).unwrap();
    assert_eq!(req2, Request::ReadHoldingRegisters(100, 3));
}

#[test]
fn roundtrip_write_single() {
    let req = Request::WriteSingleRegister(200, 12345);
    let bytes = Bytes::try_from(req).unwrap();
    let req2 = Request::try_from(bytes).unwrap();
    assert_eq!(req2, Request::WriteSingleRegister(200, 12345));
}

#[test]
fn response_read_holding() {
    let rsp = Response::ReadHoldingRegisters(vec![1, 2, 3]);
    let bytes = Bytes::from(rsp);
    let rsp2 = Response::try_from(bytes).unwrap();
    assert_eq!(rsp2, Response::ReadHoldingRegisters(vec![1, 2, 3]));
}

#[test]
fn exception_response() {
    let rsp = Response::Exception(3, Exception::IllegalDataAddress);
    let bytes = Bytes::from(rsp);
    let rsp2 = Response::try_from(bytes).unwrap();
    assert!(matches!(
        rsp2,
        Response::Exception(3, Exception::IllegalDataAddress)
    ));
}

#[test]
fn truncated_pdu_rejected() {
    let result = Request::try_from(Bytes::from_static(&[0x03]));
    assert!(result.is_err());

    let result = Request::try_from(Bytes::from_static(&[0x03, 0x00, 0x00]));
    assert!(result.is_err());
}

#[test]
fn truncated_response_rejected() {
    let result = Response::try_from(Bytes::from_static(&[0x03, 0x02]));
    assert!(result.is_err());
}

#[test]
fn diagnostic_return_query_data_roundtrip() {
    let req = Request::Diagnostic(0x0000, 0xABCD);
    let bytes = Bytes::try_from(req).unwrap();
    assert_eq!(&bytes[..], &[0x08, 0x00, 0x00, 0xAB, 0xCD]);
    let req2 = Request::try_from(bytes).unwrap();
    assert_eq!(req2, Request::Diagnostic(0x0000, 0xABCD));
}

#[test]
fn diagnostic_response_roundtrip() {
    let rsp = Response::Diagnostic(0x0000, 0xABCD);
    let bytes = Bytes::from(rsp);
    let rsp2 = Response::try_from(bytes).unwrap();
    assert!(matches!(rsp2, Response::Diagnostic(0x0000, 0xABCD)));
}

#[test]
fn diagnostic_pdu_format_matches_spec() {
    let req = Request::Diagnostic(0x000B, 0x0000);
    let bytes = Bytes::try_from(req).unwrap();
    assert_eq!(bytes.len(), 5);
    assert_eq!(bytes[0], 0x08);
    assert_eq!(u16::from_be_bytes([bytes[1], bytes[2]]), 0x000B);
}

#[test]
fn all_exception_codes_match_modbus_spec() {
    // Verify exception code round-trip: Exception ↔ u8 for all 10 standard codes.
    // Static value assertions are in spec_compliance.rs.
    for code in [1u8, 2, 3, 4, 5, 6, 7, 8, 10, 11] {
        let exc = Exception::from(code);
        assert_eq!(
            u8::from(exc),
            code,
            "Exception code {code} round-trip failed"
        );
    }
}

// ── FunctionCode newtype ───────────────────────────────────────────────

#[test]
fn function_code_new_and_value() {
    use oms_modbus::FunctionCode;
    for fc in [1u8, 2, 3, 4, 5, 6, 15, 16, 22, 23] {
        let f = FunctionCode::new(fc);
        assert_eq!(f.value(), fc);
    }
}

#[test]
fn function_code_from_request() {
    use oms_modbus::FunctionCode;
    use oms_modbus::Request;
    assert_eq!(
        Request::ReadCoils(0, 1).function_code(),
        FunctionCode::new(1)
    );
    assert_eq!(
        Request::ReadHoldingRegisters(0, 1).function_code(),
        FunctionCode::new(3)
    );
    assert_eq!(
        Request::WriteSingleCoil(0, true).function_code(),
        FunctionCode::new(5)
    );
    assert_eq!(
        Request::WriteMultipleRegisters(0, std::borrow::Cow::Owned(vec![])).function_code(),
        FunctionCode::new(16)
    );
    assert_eq!(
        Request::MaskWriteRegister(0, 0, 0).function_code(),
        FunctionCode::new(22)
    );
    assert_eq!(
        Request::ReadWriteMultipleRegisters(0, 1, 0, std::borrow::Cow::Owned(vec![]))
            .function_code(),
        FunctionCode::new(23)
    );
    assert_eq!(
        Request::Diagnostic(0, 0).function_code(),
        FunctionCode::new(8)
    );
}

#[test]
fn function_code_from_response() {
    use oms_modbus::FunctionCode;
    use oms_modbus::Response;
    assert_eq!(
        Response::ReadCoils(vec![]).function_code(),
        FunctionCode::new(1)
    );
    assert_eq!(
        Response::ReadHoldingRegisters(vec![]).function_code(),
        FunctionCode::new(3)
    );
    assert_eq!(
        Response::WriteSingleCoil(0, true).function_code(),
        FunctionCode::new(5)
    );
    assert_eq!(
        Response::Diagnostic(0, 0).function_code(),
        FunctionCode::new(8)
    );
    assert_eq!(
        Response::Exception(4, oms_modbus::Exception::IllegalFunction).function_code(),
        FunctionCode::new(4 | 0x80)
    );
}

// ── Request::into_owned ────────────────────────────────────────────────

#[test]
fn request_into_owned_preserves_data() {
    use oms_modbus::Request;
    use std::borrow::Cow;

    // Test with Cow::Borrowed data
    let data = vec![true, false, true];
    let req = Request::WriteMultipleCoils(0, Cow::Borrowed(&data));
    let owned: Request<'static> = req.into_owned();
    assert_eq!(owned, Request::WriteMultipleCoils(0, Cow::Owned(data)));

    // Test with simple variant (no Cow involved — still works)
    let req = Request::ReadHoldingRegisters(100, 5);
    let owned = req.into_owned();
    assert_eq!(owned, Request::ReadHoldingRegisters(100, 5));
}

// ── Response::Exception function_code has MSB set ──────────────────────

#[test]
fn response_exception_function_code_has_msb_set() {
    use oms_modbus::{Exception, Response};
    for fc in 1u8..=11u8 {
        let rsp = Response::Exception(fc, Exception::IllegalFunction);
        assert_eq!(rsp.function_code().value(), fc | 0x80);
    }
}

// ── ExceptionResponse → ModbusError conversion ─────────────────────────

#[test]
fn exception_response_to_modbus_error() {
    use oms_modbus::{Exception, ExceptionResponse, FunctionCode, ModbusError};
    let er = ExceptionResponse {
        function: FunctionCode::new(3),
        exception: Exception::ServerDeviceFailure,
    };
    let err = ModbusError::from(er);
    assert!(matches!(err, ModbusError::Exception { .. }));
    assert_eq!(err.label(), "MODBUS EXCEPTION");
}

// ── Response variant roundtrips ────────────────────────────────────────

#[test]
fn response_read_coils_roundtrip() {
    let rsp = Response::ReadCoils(vec![true, false, true]);
    let bytes = Bytes::from(rsp);
    let decoded = Response::try_from(bytes).unwrap();
    assert!(matches!(decoded, Response::ReadCoils(_)));
}

#[test]
fn response_read_discrete_inputs_roundtrip() {
    // Use byte-aligned quantity to avoid padding on decode
    let rsp =
        Response::ReadDiscreteInputs(vec![false, true, false, true, false, false, true, true]);
    let bytes = Bytes::from(rsp.clone());
    let decoded = Response::try_from(bytes).unwrap();
    assert_eq!(decoded, rsp);
}

#[test]
fn response_read_input_registers_roundtrip() {
    let rsp = Response::ReadInputRegisters(vec![100, 200]);
    let bytes = Bytes::from(rsp.clone());
    let decoded = Response::try_from(bytes).unwrap();
    assert_eq!(decoded, rsp);
}

#[test]
fn response_write_multiple_coils_roundtrip() {
    let rsp = Response::WriteMultipleCoils(10, 8);
    let bytes = Bytes::from(rsp.clone());
    let decoded = Response::try_from(bytes).unwrap();
    assert_eq!(decoded, rsp);
}

#[test]
fn response_write_multiple_registers_roundtrip() {
    let rsp = Response::WriteMultipleRegisters(100, 3);
    let bytes = Bytes::from(rsp.clone());
    let decoded = Response::try_from(bytes).unwrap();
    assert_eq!(decoded, rsp);
}

#[test]
fn response_mask_write_register_roundtrip() {
    let rsp = Response::MaskWriteRegister(5, 0x00FF, 0xFF00);
    let bytes = Bytes::from(rsp.clone());
    let decoded = Response::try_from(bytes).unwrap();
    assert_eq!(decoded, rsp);
}

#[test]
fn response_read_write_multiple_registers_roundtrip() {
    let rsp = Response::ReadWriteMultipleRegisters(vec![1, 2, 3]);
    let bytes = Bytes::from(rsp.clone());
    let decoded = Response::try_from(bytes).unwrap();
    assert_eq!(decoded, rsp);
}

// ── P1-4: PDU size & value boundary tests ──────────────────────────────

#[test]
fn read_zero_registers_roundtrip() {
    // qty=0 is invalid per Modbus spec — decode must reject it cleanly.
    let req = Request::ReadHoldingRegisters(0, 0);
    let bytes = Bytes::try_from(req.clone()).unwrap();
    // PDU: FC(1) + addr(2) + qty(2) = 5 bytes
    assert_eq!(bytes.len(), 5);
    // Decode rejects qty=0 — no panic, just a clean error.
    let err = Request::try_from(bytes).unwrap_err();
    assert!(
        err.to_string().contains("not in 1..="),
        "expected qty-range error, got: {err}"
    );
}

#[test]
fn read_coils_zero_quantity_roundtrip() {
    let req = Request::ReadCoils(10, 0);
    let bytes = Bytes::try_from(req.clone()).unwrap();
    assert_eq!(bytes.len(), 5);
    let err = Request::try_from(bytes).unwrap_err();
    assert!(
        err.to_string().contains("not in 1..="),
        "expected qty-range error, got: {err}"
    );
}

#[test]
fn write_multiple_registers_empty_vec() {
    // qty=0 is invalid per Modbus spec — decode must reject it cleanly.
    let req = Request::WriteMultipleRegisters(0, Cow::Owned(vec![]));
    let bytes = Bytes::try_from(req.clone()).unwrap();
    // PDU: FC(1) + addr(2) + qty(2) + byte_count(1) = 6 bytes
    assert_eq!(bytes.len(), 6);
    assert_eq!(bytes[5], 0, "byte_count must be 0 for empty write");
    let err = Request::try_from(bytes).unwrap_err();
    assert!(
        err.to_string().contains("not in 1..="),
        "expected qty-range error, got: {err}"
    );
}

#[test]
fn write_multiple_coils_empty_vec() {
    let req = Request::WriteMultipleCoils(0, Cow::Owned(vec![]));
    let bytes = Bytes::try_from(req.clone()).unwrap();
    assert_eq!(bytes.len(), 6);
    assert_eq!(bytes[5], 0, "byte_count must be 0 for empty write");
    let err = Request::try_from(bytes).unwrap_err();
    assert!(
        err.to_string().contains("not in 1..="),
        "expected qty-range error, got: {err}"
    );
}

#[test]
fn write_single_coil_only_ff00_is_on() {
    // Per Modbus spec §6.5: ON=0xFF00, OFF=0x0000.
    // Test encoding: ON produces 0xFF00, OFF produces 0x0000.
    let on_req = Request::WriteSingleCoil(0, true);
    let on_bytes = Bytes::try_from(on_req).unwrap();
    assert_eq!(&on_bytes[3..5], &[0xFF, 0x00]);

    let off_req = Request::WriteSingleCoil(0, false);
    let off_bytes = Bytes::try_from(off_req).unwrap();
    assert_eq!(&off_bytes[3..5], &[0x00, 0x00]);

    // Non-standard coil values are rejected per the Modbus spec
    let nonstandard = Bytes::from_static(&[0x05, 0x00, 0x00, 0x00, 0x01]);
    assert!(
        Request::try_from(nonstandard).is_err(),
        "non-standard coil values must be rejected"
    );

    // 0xFF00 → ON
    let standard_on = Bytes::from_static(&[0x05, 0x00, 0x00, 0xFF, 0x00]);
    let decoded_on = Request::try_from(standard_on).unwrap();
    assert_eq!(decoded_on, Request::WriteSingleCoil(0, true));

    // 0x0000 → OFF
    let standard_off = Bytes::from_static(&[0x05, 0x00, 0x00, 0x00, 0x00]);
    let decoded_off = Request::try_from(standard_off).unwrap();
    assert_eq!(decoded_off, Request::WriteSingleCoil(0, false));
}

#[test]
fn max_pdu_response_fits_in_253_bytes() {
    // 125 registers × 2 bytes = 250 bytes + FC + byte_count = 252 bytes.
    // Within MAX_PDU_SIZE (253). Must encode successfully.
    let regs: Vec<u16> = vec![0xABCD; 125];
    let rsp = Response::ReadHoldingRegisters(regs);
    let bytes = Bytes::from(rsp);
    assert_eq!(bytes.len(), 252, "125-reg response: 2 + 250 = 252 bytes");
    assert!(bytes.len() <= 253, "must fit in MAX_PDU_SIZE=253");
    let decoded = Response::try_from(bytes).unwrap();
    assert!(matches!(decoded, Response::ReadHoldingRegisters(_)));
}

#[test]
fn write_single_register_value_boundaries() {
    // Test that 0x0000 and 0xFFFF round-trip correctly through encode/decode.
    for &val in &[0x0000u16, 0x0001, 0x7FFF, 0x8000, 0xFFFE, 0xFFFF] {
        let req = Request::WriteSingleRegister(0, val);
        let bytes = Bytes::try_from(req.clone()).unwrap();
        let decoded = Request::try_from(bytes).unwrap();
        assert_eq!(decoded, req, "value {val:#06X} round-trip failed");
    }
}

#[test]
fn max_coil_write_fits_in_pdu() {
    // 1968 coils → ceil(1968/8) = 246 bytes.
    // PDU: FC(1) + addr(2) + qty(2) + byte_count(1) + 246 = 252 ≤ MAX_PDU_SIZE(253)
    let coils: Vec<bool> = vec![true; 1968];
    let req = Request::WriteMultipleCoils(0, Cow::Owned(coils));
    let bytes = Bytes::try_from(req.clone()).unwrap();
    assert_eq!(bytes.len(), 252, "1968-coil write: 1+2+2+1+246 = 252");
    let decoded = Request::try_from(bytes).unwrap();
    assert_eq!(decoded, req);
}

#[test]
fn pdu_exceeding_max_size_is_rejected() {
    // 2000 coils → ceil(2000/8) = 250 bytes.
    // PDU = 1 + 2 + 2 + 1 + 250 = 256 > MAX_PDU_SIZE(253) → must fail.
    let coils: Vec<bool> = vec![true; 2000];
    let req = Request::WriteMultipleCoils(0, Cow::Owned(coils));
    let result = Bytes::try_from(req);
    assert!(result.is_err(), "PDU > 253 bytes must be rejected");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("exceeds"),
        "error should mention exceeds: {err}"
    );
}

#[test]
fn max_registers_write_fits_in_pdu() {
    // 123 registers → byte_count = 246 bytes.
    // PDU: FC(1) + addr(2) + qty(2) + byte_count(1) + 246 = 252 ≤ 253
    let regs: Vec<u16> = vec![0xABCD; 123];
    let req = Request::WriteMultipleRegisters(0, Cow::Owned(regs));
    let bytes = Bytes::try_from(req.clone()).unwrap();
    assert_eq!(bytes.len(), 252, "123-reg write: 1+2+2+1+246 = 252");
    let decoded = Request::try_from(bytes).unwrap();
    assert_eq!(decoded, req);
}

#[test]
fn empty_response_zero_registers() {
    // Response with 0 registers → byte_count=0, PDU = [FC, 0x00]
    let rsp = Response::ReadHoldingRegisters(vec![]);
    let bytes = Bytes::from(rsp);
    assert_eq!(bytes.len(), 2, "empty register response: FC + byte_count=0");
    assert_eq!(&bytes[..], &[0x03, 0x00]);
    let decoded = Response::try_from(bytes).unwrap();
    assert!(matches!(decoded, Response::ReadHoldingRegisters(ref v) if v.is_empty()));
}

#[test]
fn empty_coil_response() {
    let rsp = Response::ReadCoils(vec![]);
    let bytes = Bytes::from(rsp);
    assert_eq!(bytes.len(), 2, "empty coil response: FC + byte_count=0");
    assert_eq!(&bytes[..], &[0x01, 0x00]);
    let decoded = Response::try_from(bytes).unwrap();
    assert!(matches!(decoded, Response::ReadCoils(ref v) if v.is_empty()));
}
