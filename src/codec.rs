// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Modbus codecs — RTU (CRC-16) and ASCII (LRC) framing.
//!
//! Frame parsing is handled inline by each transport's `send_recv` method.
//! This module provides only CRC, LRC, and frame encoding helpers.

use bytes::BufMut;

// ── RTU — CRC-16 ────────────────────────────────────────────────────────

/// Pre-computed CRC-16 lookup table (polynomial 0xA001, reflected).
/// Generated at compile time — ~8× faster than bit-by-bit computation.
const CRC_TABLE: [u16; 256] = {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u16;
        let mut j = 0;
        while j < 8 {
            if crc & 0x0001 != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Calculate Modbus RTU CRC-16 for a byte slice.
///
/// Returns the raw CRC value. Use `put_u16_le` when writing to wire
/// to get the correct Modbus RTU byte order (low byte first).
/// Polynomial 0xA001, reflected, using a pre-computed 256-entry lookup table.
#[inline]
pub fn calculate_crc(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &byte in data {
        let idx = ((crc as u8) ^ byte) as usize;
        crc = (crc >> 8) ^ CRC_TABLE[idx];
    }
    crc
}

/// Encode `[slave_id, pdu...]` into an RTU frame `[slave_id, pdu..., crc_lo, crc_hi]`.
#[inline]
pub fn encode_rtu_frame(data: &[u8], buf: &mut bytes::BytesMut) {
    let crc = calculate_crc(data);
    buf.reserve(data.len() + 2);
    buf.put_slice(data);
    buf.put_u16_le(crc);
}

// ── ASCII — LRC ─────────────────────────────────────────────────────────

const ASCII_START: u8 = b':';
const CR: u8 = b'\r';
const LF: u8 = b'\n';

/// Calculate Modbus ASCII LRC (Longitudinal Redundancy Check).
///
/// The LRC is the two's complement of the sum of all bytes in the frame.
/// `sum(data) + LRC ≡ 0 (mod 256)`.
#[inline]
pub fn calculate_lrc(data: &[u8]) -> u8 {
    let sum: u8 = data.iter().fold(0u8, |acc, &b| acc.wrapping_add(b));
    sum.wrapping_neg()
}

// ── Hex encoding ────────────────────────────────────────────────────────

#[inline]
fn hex_encode_byte(byte: u8) -> [u8; 2] {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    [HEX[(byte >> 4) as usize], HEX[(byte & 0x0F) as usize]]
}

/// Encode raw Modbus PDU bytes directly into a buffer — zero extra allocation.
///
/// Writes the complete ASCII frame (`:hexdataLRC\r\n`) into `buf`.
pub fn encode_ascii_frame_into(data: &[u8], buf: &mut impl BufMut) {
    let lrc = calculate_lrc(data);
    buf.put_u8(ASCII_START);
    for &byte in data {
        buf.put_slice(&hex_encode_byte(byte));
    }
    buf.put_slice(&hex_encode_byte(lrc));
    buf.put_u8(CR);
    buf.put_u8(LF);
}

/// Encode raw Modbus PDU bytes into an ASCII frame.
/// For zero-allocation encoding, use [`encode_ascii_frame_into`] instead.
pub fn encode_ascii_frame(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + (data.len() + 1) * 2 + 2);
    encode_ascii_frame_into(data, &mut out);
    out
}
