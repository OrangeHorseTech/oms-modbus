// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Transport implementations — TCP, RTU, ASCII clients and servers.

// ── Modbus protocol frame size limits ─────────────────────────────────
//
// These are per the Modbus Application Protocol Specification V1.1b3:
// - Maximum ADU size: 256 bytes (RS-485 / RTU framing)
// - Maximum PDU size: 253 bytes
// - Maximum TCP ADU size: 260 bytes (7-byte MBAP header + 253-byte PDU)

/// Maximum Modbus ADU (Application Data Unit): slave_id(1) + PDU(≤253) + CRC(2).
/// The largest legal RTU frame is exactly 256 bytes.
pub(crate) const MAX_ADU_SIZE: usize = 256;

/// Maximum Modbus PDU (Protocol Data Unit) per spec §4.1.
pub(crate) const MAX_PDU_SIZE: usize = 253;

/// CRC-16 field size in bytes (RTU framing).
pub(crate) const CRC_SIZE: usize = 2;

/// Minimum valid RTU frame: slave_id(1) + exception_fc(1) + code(1) + crc(2).
/// Anything shorter is truncated noise.
pub(crate) const MIN_RTU_FRAME: usize = 5;

/// Size of the MBAP (Modbus Application Protocol) header in bytes.
pub(crate) const MBAP_HEADER_SIZE: usize = 7;

/// MBAP bytes before the Length field: TID(2) + PID(2) + Length(2).
pub(crate) const MBAP_PREFIX_SIZE: usize = 6;

/// Maximum TCP ADU: MBAP header(7) + max PDU(253).
pub(crate) const MAX_TCP_ADU_SIZE: usize = MBAP_HEADER_SIZE + MAX_PDU_SIZE; // 260

pub mod ascii;
pub mod rtu;
pub(crate) mod send_recv;
pub mod sniff_io;
pub mod tcp;
