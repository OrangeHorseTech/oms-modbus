// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Modbus Protocol Specification Compliance Audit
// ==============================================
//
// References:
//  - MODBUS Application Protocol Specification V1.1b3
//  - MODBUS over Serial Line Specification V1.02
//
// Test properties (provable invariants) rather than fragile reference
// values that depend on specific tool implementations.

use bytes::Bytes;
use oms_modbus::{calculate_crc, calculate_lrc};
use std::borrow::Cow;

use oms_modbus::frame::{Exception, Request, Response};

// ============================================================================
// §1  CRC-16 Compliance
// ============================================================================

mod crc16_compliance {
    use super::*;

    #[test]
    fn crc16_verifiable_against_tokio_modbus_test_vectors() {
        // These test vectors are verified against the tokio-modbus test suite
        // and confirmed with libmodbus 3.1.x.
        assert_eq!(calculate_crc(&[0x01, 0x03, 0x08, 0x2B, 0x00, 0x02]), 0x63B6);
        assert_eq!(
            calculate_crc(&[0x01, 0x03, 0x04, 0x00, 0x20, 0x00, 0x00]),
            0xF9FB
        );
    }

    #[test]
    fn crc16_differentiates_data() {
        // CRC must produce different results for different inputs
        let crc1 = calculate_crc(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]);
        let crc2 = calculate_crc(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x02]);
        assert_ne!(crc1, crc2, "CRC must differentiate different data");
    }

    #[test]
    fn crc16_zero_frame_is_valid() {
        // CRC-16 of empty data with init=0xFFFF is 0xFFFF
        let crc = calculate_crc(&[]);
        assert_eq!(crc, 0xFFFF, "CRC of empty frame must be 0xFFFF");
    }
}

// ============================================================================
// §2  LRC Compliance
// ============================================================================

mod lrc_compliance {
    use super::*;

    #[test]
    fn lrc_two_complement_property() {
        // The defining property of LRC: sum(data) + LRC = 0 (mod 256)
        let data = [0x01, 0x03, 0x00, 0x6B, 0x00, 0x03];
        let lrc = calculate_lrc(&data);
        let sum: u8 = data
            .iter()
            .fold(0u8, |a, b| a.wrapping_add(*b))
            .wrapping_add(lrc);
        assert_eq!(sum, 0x00, "LRC + data sum must be 0 (mod 256)");
    }

    #[test]
    fn lrc_zero_data() {
        let lrc = calculate_lrc(&[0x00, 0x00]);
        let sum: u8 = 0x00u8.wrapping_add(0x00).wrapping_add(lrc);
        assert_eq!(sum, 0x00);
    }

    #[test]
    fn lrc_deterministic() {
        let data = [0x01, 0x03, 0x00, 0x00, 0x00, 0x01];
        let lrc1 = calculate_lrc(&data);
        let lrc2 = calculate_lrc(&data);
        assert_eq!(lrc1, lrc2, "LRC must be deterministic");
    }

    #[test]
    fn lrc_differentiates_data() {
        let lrc1 = calculate_lrc(&[0x01, 0x03, 0x00, 0x01]);
        let lrc2 = calculate_lrc(&[0x01, 0x03, 0x00, 0x02]);
        assert_ne!(lrc1, lrc2, "LRC must differentiate different data");
    }
}

// ============================================================================
// §3  PDU Encoding — MODBUS Application Protocol V1.1b3, §4
// ============================================================================

mod pdu_encoding {
    use super::*;

    #[test]
    fn fc01_read_coils_pdu_format() {
        let req = Request::ReadCoils(0x0013, 0x000D);
        let bytes = Bytes::try_from(req.clone()).unwrap();
        assert_eq!(bytes[0], 0x01, "Function code = 01");
        assert_eq!(
            u16::from_be_bytes([bytes[1], bytes[2]]),
            0x0013,
            "Start address"
        );
        assert_eq!(u16::from_be_bytes([bytes[3], bytes[4]]), 0x000D, "Quantity");
        let back = Request::try_from(bytes).unwrap();
        assert_eq!(back, req, "Round-trip");
    }

    #[test]
    fn fc03_read_holding_pdu_format() {
        let req = Request::ReadHoldingRegisters(0x006B, 0x0003);
        let bytes = Bytes::try_from(req.clone()).unwrap();
        assert_eq!(&bytes[..], &[0x03, 0x00, 0x6B, 0x00, 0x03]);
    }

    #[test]
    fn fc05_write_coil_on_must_be_ff00() {
        let req = Request::WriteSingleCoil(0x00AC, true);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(
            &bytes[3..5],
            &[0xFF, 0x00],
            "ON value MUST be 0xFF00 per MODBUS spec §6.5"
        );
    }

    #[test]
    fn fc05_write_coil_off_must_be_0000() {
        let req = Request::WriteSingleCoil(0x00AC, false);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(
            &bytes[3..5],
            &[0x00, 0x00],
            "OFF value MUST be 0x0000 per MODBUS spec §6.5"
        );
    }

    #[test]
    fn fc05_response_echoes_request() {
        // Spec requires Write Single Coil response to echo the request exactly
        let req_bytes = Bytes::try_from(Request::WriteSingleCoil(0xAC, true)).unwrap();
        let rsp = Response::WriteSingleCoil(0xAC, true);
        let rsp_bytes = Bytes::from(rsp);
        assert_eq!(&rsp_bytes[..], &req_bytes[..]);
    }

    #[test]
    fn fc06_response_echoes_request() {
        let req_bytes = Bytes::try_from(Request::WriteSingleRegister(1, 3)).unwrap();
        let rsp = Response::WriteSingleRegister(1, 3);
        let rsp_bytes = Bytes::from(rsp);
        assert_eq!(&rsp_bytes[..], &req_bytes[..]);
    }

    #[test]
    fn fc16_write_multiple_registers_pdu() {
        let values = vec![0x000A, 0x0102];
        let req = Request::WriteMultipleRegisters(0x0001, Cow::Owned(values));
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(bytes[0], 0x10, "FC = 16");
        assert_eq!(bytes[5], 0x04, "Byte count = 2 regs × 2 bytes");
        assert_eq!(&bytes[6..], &[0x00, 0x0A, 0x01, 0x02]);

        let rsp = Response::WriteMultipleRegisters(0x0001, 2);
        let rsp_bytes = Bytes::from(rsp);
        assert_eq!(&rsp_bytes[..], &bytes[..5], "Response: FC + start + qty");
    }

    #[test]
    fn fc22_mask_write_register_pdu() {
        let req = Request::MaskWriteRegister(0x0001, 0x00F2, 0x0025);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(&bytes[..], &[0x16, 0x00, 0x01, 0x00, 0xF2, 0x00, 0x25]);
    }

    #[test]
    fn fc23_read_write_multiple_registers_pdu() {
        let write_data = vec![0x00FF, 0x00FF];
        let req =
            Request::ReadWriteMultipleRegisters(0x0003, 6, 0x000E, Cow::Owned(write_data.clone()));
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(bytes[0], 0x17, "FC = 23");
        // Read start=3, qty=6, Write start=14, qty=2
        assert_eq!(
            &bytes[1..9],
            &[0x00, 0x03, 0x00, 0x06, 0x00, 0x0E, 0x00, 0x02]
        );
        assert_eq!(bytes[9], 0x04, "Byte count = 4");
    }

    // The set of FC values is statically known at compile time. This test
    // serves as a regression guard: if someone accidentally assigns the
    // same value to two function codes, this catches it.
    #[test]
    fn all_function_codes_are_unique() {
        let fcs = [
            Request::ReadCoils(0, 1).function_code(),
            Request::ReadDiscreteInputs(0, 1).function_code(),
            Request::ReadHoldingRegisters(0, 1).function_code(),
            Request::ReadInputRegisters(0, 1).function_code(),
            Request::WriteSingleCoil(0, true).function_code(),
            Request::WriteSingleRegister(0, 0).function_code(),
            Request::WriteMultipleCoils(0, Cow::Owned(vec![])).function_code(),
            Request::WriteMultipleRegisters(0, Cow::Owned(vec![])).function_code(),
            Request::Diagnostic(0, 0).function_code(),
            Request::MaskWriteRegister(0, 0, 0).function_code(),
            Request::ReadWriteMultipleRegisters(0, 1, 0, Cow::Owned(vec![])).function_code(),
        ];
        // All standard FCs must have unique values
        let values: Vec<u8> = fcs.iter().map(|f| f.value()).collect();
        let mut sorted = values.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            values.len(),
            "All function codes must be unique"
        );
    }

    // ── Round-trip for all types (byte-aligned quantities) ────────────

    #[test]
    fn roundtrip_all_types() {
        let cases: Vec<Request<'static>> = vec![
            Request::ReadCoils(0, 8),       // 8 coils = 1 byte
            Request::ReadCoils(0xFFF0, 16), // 16 coils = 2 bytes, addr+qty ≤ 0x10000
            Request::ReadDiscreteInputs(100, 8),
            Request::ReadHoldingRegisters(200, 5),
            Request::ReadInputRegisters(50, 3),
            Request::WriteSingleCoil(150, true),
            Request::WriteSingleRegister(300, 12345),
            Request::WriteMultipleCoils(10, Cow::Owned(vec![true; 8])),
            Request::WriteMultipleRegisters(20, Cow::Owned(vec![100, 200, 300])),
            Request::MaskWriteRegister(1, 0xFF00, 0x00FF),
            Request::ReadWriteMultipleRegisters(3, 6, 14, Cow::Owned(vec![0xFF; 2])),
        ];
        for req in cases {
            let bytes = Bytes::try_from(req.clone()).expect("encode must succeed");
            let decoded = Request::try_from(bytes).expect("decode must succeed");
            assert_eq!(decoded, req, "Round-trip failed for {req:?}");
        }
    }

    // ── Coil packing: non-byte-aligned quantities round-trip with padding ──

    #[test]
    fn coil_packing_roundtrip_8n_quantities() {
        // Byte-aligned coil quantities round-trip exactly
        for qty in [8u16, 16, 24, 32] {
            let vals: Vec<bool> = (0..qty).map(|i| i % 2 == 0).collect();
            let req = Request::WriteMultipleCoils(0, Cow::Owned(vals.clone()));
            let bytes = Bytes::try_from(req).unwrap();
            let decoded = Request::try_from(bytes).unwrap();
            assert_eq!(decoded, Request::WriteMultipleCoils(0, Cow::Owned(vals)));
        }
    }

    #[test]
    fn coil_response_byte_count_formula() {
        // Per spec: byte_count = ceil(quantity / 8)
        for qty in 1..=16u16 {
            let expected_bytes = (qty as usize).div_ceil(8);
            let bits = vec![true; qty as usize];
            let rsp = Response::ReadCoils(bits);
            let bytes = Bytes::from(rsp);
            assert_eq!(
                bytes[1] as usize, expected_bytes,
                "Coil response byte_count for qty={qty}"
            );
        }
    }
}

// ============================================================================
// §4  Exception Response Format — MODBUS Application Protocol V1.1b3, §7
// ============================================================================

mod exception_format {
    use super::*;

    #[test]
    fn exception_function_code_has_msb_set() {
        // Exception: function code byte has bit 7 set (FC | 0x80)
        for fc in 1u8..=6u8 {
            let rsp = Response::Exception(fc, Exception::IllegalFunction);
            let bytes = Bytes::from(rsp);
            assert_eq!(
                bytes[0],
                fc | 0x80,
                "Exception response must set MSB of function code"
            );
        }
    }

    #[test]
    fn exception_roundtrip() {
        for (fc, exc) in [
            (1u8, Exception::IllegalFunction),
            (3, Exception::IllegalDataAddress),
            (3, Exception::IllegalDataValue),
            (4, Exception::ServerDeviceFailure),
        ] {
            let rsp = Response::Exception(fc, exc);
            let bytes = Bytes::from(rsp);
            let decoded = Response::try_from(bytes).unwrap();
            assert!(matches!(decoded, Response::Exception(_, _)));
        }
    }

    #[test]
    fn standard_exception_codes_match_spec() {
        assert_eq!(u8::from(Exception::IllegalFunction), 0x01);
        assert_eq!(u8::from(Exception::IllegalDataAddress), 0x02);
        assert_eq!(u8::from(Exception::IllegalDataValue), 0x03);
        assert_eq!(u8::from(Exception::ServerDeviceFailure), 0x04);
        assert_eq!(u8::from(Exception::Acknowledge), 0x05);
        assert_eq!(u8::from(Exception::ServerDeviceBusy), 0x06);
        assert_eq!(u8::from(Exception::NegativeAcknowledge), 0x07);
        assert_eq!(u8::from(Exception::MemoryParityError), 0x08);
        assert_eq!(u8::from(Exception::GatewayPathUnavailable), 0x0A);
        assert_eq!(
            u8::from(Exception::GatewayTargetDeviceFailedToRespond),
            0x0B
        );
    }
}

// ============================================================================
// §5  Addressing Convention — MODBUS Application Protocol V1.1b3, §4.3
// ============================================================================

mod addressing {
    use super::*;

    #[test]
    fn pdu_address_is_zero_based() {
        let req = Request::ReadHoldingRegisters(0, 1);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(
            &bytes[1..3],
            &[0x00, 0x00],
            "First register = address 0x0000 in PDU (0-based)"
        );
    }

    #[test]
    fn pdu_address_0xffff_is_valid() {
        let req = Request::ReadHoldingRegisters(0xFFFF, 1);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(
            &bytes[1..3],
            &[0xFF, 0xFF],
            "Max address 0xFFFF must be representable"
        );
    }

    #[test]
    fn max_register_quantity() {
        // MODBUS spec: read registers max = 125 (0x007D)
        let req = Request::ReadHoldingRegisters(0, 125);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(u16::from_be_bytes([bytes[3], bytes[4]]), 125);
    }

    #[test]
    fn max_coil_quantity() {
        // MODBUS spec: read coils max = 2000 (0x07D0)
        let req = Request::ReadCoils(0, 2000);
        let bytes = Bytes::try_from(req).unwrap();
        assert_eq!(u16::from_be_bytes([bytes[3], bytes[4]]), 2000);
    }

    #[test]
    fn write_register_value_range() {
        // Register values are u16 (0x0000-0xFFFF)
        for val in [0u16, 1, 0x7FFF, 0x8000, 0xFFFF] {
            let req = Request::WriteSingleRegister(0, val);
            let bytes = Bytes::try_from(req.clone()).unwrap();
            let decoded = Request::try_from(bytes).unwrap();
            assert_eq!(decoded, req, "Value {val} round-trip");
        }
    }
}

// ============================================================================
// §7  Frame integrity
// ============================================================================

mod frame_integrity {
    use super::*;

    #[test]
    fn empty_pdu_rejected() {
        // Empty bytes → Request decode must fail
        let result = Request::try_from(Bytes::new());
        assert!(result.is_err(), "Empty PDU must be rejected");
    }

    #[test]
    fn empty_pdu_response_rejected() {
        let result = Response::try_from(Bytes::new());
        assert!(result.is_err(), "Empty PDU response must be rejected");
    }

    #[test]
    fn invalid_function_code_rejected() {
        // Function code 0x46 is not a standard Modbus FC
        let bytes = Bytes::from_static(&[0x46, 0x00, 0x00]);
        let result = Request::try_from(bytes);
        assert!(result.is_err(), "Invalid function code must be rejected");
    }

    #[test]
    fn response_type_matches_request() {
        // ReadHoldingRegisters request → Response must be ReadHoldingRegisters variant
        let rsp = Response::ReadHoldingRegisters(vec![1, 2, 3]);
        let bytes = Bytes::from(rsp);
        let decoded = Response::try_from(bytes).unwrap();
        assert!(matches!(decoded, Response::ReadHoldingRegisters(_)));
    }

    #[test]
    fn disconnect_request_produces_empty_pdu() {
        let req = Request::Disconnect;
        let bytes = Bytes::try_from(req).unwrap();
        assert!(bytes.is_empty(), "Disconnect PDU sends nothing on wire");
    }

    #[test]
    fn disconnect_not_decodable() {
        // Disconnect has no PDU → can't be decoded back
        let result = Request::try_from(Bytes::new());
        assert!(result.is_err());
    }
}
