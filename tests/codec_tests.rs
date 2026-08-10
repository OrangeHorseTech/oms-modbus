// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for RTU and ASCII codecs — CRC, LRC, roundtrips.

use oms_modbus::{calculate_crc, calculate_lrc};

// ── CRC ──────────────────────────────────────────────────────────────────

#[test]
fn crc_calculation() {
    // CRC-16/MODBUS examples verified against libmodbus 3.1.x
    // (spec_compliance has additional test vectors with sources)
    assert_eq!(calculate_crc(&[]), 0xFFFF, "CRC of empty data = 0xFFFF");
    assert_eq!(calculate_crc(&[0x02, 0x07]), 0x1241);
}

// ── LRC ──────────────────────────────────────────────────────────────────

#[test]
fn lrc_calculation() {
    assert_eq!(calculate_lrc(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]), 0xFB);
}

#[test]
fn lrc_sum_128_no_panic() {
    let data = &[0x80u8]; // single byte, sum = 128
    let lrc = calculate_lrc(data);
    assert_eq!(lrc, 0x80u8);
}
