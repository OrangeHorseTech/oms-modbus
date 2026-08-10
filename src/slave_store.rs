// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! In-memory Modbus slave register store.
//!
//! Holding registers and input registers use `RwLock<Vec<u16>>` for O(1)
//! indexed access. Reading 125 consecutive registers is a single lock
//! acquisition + memcpy — orders of magnitude faster than per-register
//! hash lookups.
//!
//! Coils and discrete inputs use a sparse `Vec<u8>` (bit-packed) since
//! their address space is typically sparse and 65536 bits = 8KB is trivial.

use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::frame::{Exception, Request, Response};
use crate::server::Service;

/// Default size for register vectors (grow on demand).
const DEFAULT_REG_COUNT: usize = 1024;
/// In-memory register store. Holding/input registers use contiguous
/// `Vec<u16>` with `RwLock` for fast indexed access. Coils and discrete
/// inputs use bit-packed `Vec<u8>` for memory efficiency.
pub struct SlaveStore {
    coils: RwLock<Vec<u8>>,
    discrete_inputs: RwLock<Vec<u8>>,
    holding_registers: RwLock<Vec<u16>>,
    input_registers: RwLock<Vec<u16>>,
}

impl Default for SlaveStore {
    fn default() -> Self {
        Self {
            coils: RwLock::new(vec![0u8; DEFAULT_REG_COUNT / 8]),
            discrete_inputs: RwLock::new(vec![0u8; DEFAULT_REG_COUNT / 8]),
            holding_registers: RwLock::new(vec![0u16; DEFAULT_REG_COUNT]),
            input_registers: RwLock::new(vec![0u16; DEFAULT_REG_COUNT]),
        }
    }
}

impl SlaveStore {
    /// Create an empty store with default capacity (1024 registers).
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate holding registers from `(address, value)` pairs.
    /// Automatically grows the backing store if addresses exceed the default size.
    pub fn with_holding_registers(regs: &[(u16, u16)]) -> Self {
        let store = Self::new();
        let max_addr = regs.iter().map(|&(a, _)| a as usize).max().unwrap_or(0);
        {
            let mut hr = store
                .holding_registers
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if hr.len() <= max_addr {
                hr.resize(max_addr + 1, 0);
            }
            for &(addr, val) in regs {
                hr[addr as usize] = val;
            }
        }
        store
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Read a single bit from a bit-packed `Vec<u8>`.
    fn read_bit(lock: &RwLock<Vec<u8>>, addr: u16) -> bool {
        let bytes = lock.read().unwrap_or_else(|e| e.into_inner());
        let byte_idx = addr as usize / 8;
        let bit_idx = addr as usize % 8;
        bytes
            .get(byte_idx)
            .map(|b| (b >> bit_idx) & 1 != 0)
            .unwrap_or(false)
    }

    /// Write a single bit to a bit-packed `Vec<u8>`, growing if needed.
    fn write_bit(lock: &RwLock<Vec<u8>>, addr: u16, value: bool) {
        let mut bytes = lock.write().unwrap_or_else(|e| e.into_inner());
        let byte_idx = addr as usize / 8;
        if byte_idx >= bytes.len() {
            bytes.resize(byte_idx + 1, 0);
        }
        if value {
            bytes[byte_idx] |= 1 << (addr as usize % 8);
        } else {
            bytes[byte_idx] &= !(1 << (addr as usize % 8));
        }
    }

    /// Batch-read consecutive bits from a bit-packed `Vec<u8>`.
    /// Returns `false` for addresses beyond the store (sparse allocation).
    fn read_bits(lock: &RwLock<Vec<u8>>, addr: u16, count: u16) -> Vec<bool> {
        let bytes = lock.read().unwrap_or_else(|e| e.into_inner());
        let mut result = Vec::with_capacity(count as usize);
        for i in 0..count {
            let a = addr as usize + i as usize;
            let byte_idx = a / 8;
            let bit_idx = a % 8;
            result.push(
                bytes
                    .get(byte_idx)
                    .map(|b| (b >> bit_idx) & 1 != 0)
                    .unwrap_or(false),
            );
        }
        result
    }

    /// Batch-read consecutive registers from a `Vec<u16>`.
    fn read_registers(lock: &RwLock<Vec<u16>>, addr: u16, count: u16) -> Vec<u16> {
        let regs = lock.read().unwrap_or_else(|e| e.into_inner());
        let start = addr as usize;
        if start >= regs.len() {
            return vec![0u16; count as usize];
        }
        let end = (start + count as usize).min(regs.len());
        let mut result = vec![0u16; count as usize];
        result[..end - start].copy_from_slice(&regs[start..end]);
        result
    }

    /// Write a single register to a `Vec<u16>`, growing if needed.
    fn write_register(lock: &RwLock<Vec<u16>>, addr: u16, value: u16) {
        let mut regs = lock.write().unwrap_or_else(|e| e.into_inner());
        if addr as usize >= regs.len() {
            regs.resize(addr as usize + 1, 0);
        }
        regs[addr as usize] = value;
    }

    // ── Coils ────────────────────────────────────────────────────────

    /// Read a single coil. Returns `false` for uninitialized addresses.
    pub fn read_coil(&self, addr: u16) -> bool {
        Self::read_bit(&self.coils, addr)
    }

    /// Write a single coil.
    pub fn write_coil(&self, addr: u16, value: bool) {
        Self::write_bit(&self.coils, addr, value);
    }

    /// Batch-read consecutive coils (single lock acquisition).
    pub fn read_coils_range(&self, addr: u16, count: u16) -> Vec<bool> {
        Self::read_bits(&self.coils, addr, count)
    }

    // ── Discrete inputs ──────────────────────────────────────────────

    /// Read a single discrete input. Returns `false` for uninitialized addresses.
    pub fn read_discrete_input(&self, addr: u16) -> bool {
        Self::read_bit(&self.discrete_inputs, addr)
    }

    /// Write a discrete input value. This violates the Modbus spec (discrete
    /// inputs are read-only on real devices) but is provided for simulation
    /// and testing scenarios.
    pub fn write_discrete_input(&self, addr: u16, value: bool) {
        Self::write_bit(&self.discrete_inputs, addr, value);
    }

    /// Batch-read consecutive discrete inputs (single lock acquisition).
    pub fn read_discrete_inputs_range(&self, addr: u16, count: u16) -> Vec<bool> {
        Self::read_bits(&self.discrete_inputs, addr, count)
    }

    // ── Holding registers ────────────────────────────────────────────

    /// Read a single holding register. Returns `0` for uninitialized addresses.
    pub fn read_holding_register(&self, addr: u16) -> u16 {
        let hr = self
            .holding_registers
            .read()
            .unwrap_or_else(|e| e.into_inner());
        hr.get(addr as usize).copied().unwrap_or(0)
    }

    /// Batch-read consecutive holding registers (single lock + memcpy).
    pub fn read_holding_range(&self, addr: u16, count: u16) -> Vec<u16> {
        Self::read_registers(&self.holding_registers, addr, count)
    }

    /// Write a single holding register. Grows the backing store if needed.
    pub fn write_holding_register(&self, addr: u16, value: u16) {
        Self::write_register(&self.holding_registers, addr, value);
    }

    // ── Input registers ──────────────────────────────────────────────

    /// Read a single input register. Returns `0` for uninitialized addresses.
    pub fn read_input_register(&self, addr: u16) -> u16 {
        let ir = self
            .input_registers
            .read()
            .unwrap_or_else(|e| e.into_inner());
        ir.get(addr as usize).copied().unwrap_or(0)
    }

    /// Batch-read consecutive input registers (single lock + memcpy).
    pub fn read_input_range(&self, addr: u16, count: u16) -> Vec<u16> {
        Self::read_registers(&self.input_registers, addr, count)
    }

    /// Write an input register value. This violates the Modbus spec (input
    /// registers are read-only on real devices) but is provided for simulation
    /// and testing scenarios.
    pub fn write_input_register(&self, addr: u16, value: u16) {
        Self::write_register(&self.input_registers, addr, value);
    }
}

#[async_trait]
impl Service for Arc<SlaveStore> {
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
        let store = self.as_ref();
        Ok(match request {
            Request::ReadCoils(addr, cnt) => Response::ReadCoils(store.read_coils_range(addr, cnt)),
            Request::ReadDiscreteInputs(addr, cnt) => {
                Response::ReadDiscreteInputs(store.read_discrete_inputs_range(addr, cnt))
            }
            Request::ReadHoldingRegisters(addr, cnt) => {
                Response::ReadHoldingRegisters(store.read_holding_range(addr, cnt))
            }
            Request::ReadInputRegisters(addr, cnt) => {
                Response::ReadInputRegisters(store.read_input_range(addr, cnt))
            }
            Request::WriteSingleCoil(addr, value) => {
                store.write_coil(addr, value);
                Response::WriteSingleCoil(addr, value)
            }
            Request::WriteSingleRegister(addr, value) => {
                store.write_holding_register(addr, value);
                Response::WriteSingleRegister(addr, value)
            }
            Request::WriteMultipleCoils(addr, values) => {
                // Inline bit writes (not write_coil) — single lock for the batch
                let mut coils = store.coils.write().unwrap_or_else(|e| e.into_inner());
                for (i, &v) in values.iter().enumerate() {
                    let a = addr as usize + i;
                    let byte_idx = a / 8;
                    let bit_idx = a % 8;
                    if byte_idx >= coils.len() {
                        coils.resize(byte_idx + 1, 0);
                    }
                    if v {
                        coils[byte_idx] |= 1 << bit_idx;
                    } else {
                        coils[byte_idx] &= !(1 << bit_idx);
                    }
                }
                drop(coils);
                Response::WriteMultipleCoils(addr, values.len() as u16)
            }
            Request::WriteMultipleRegisters(addr, values) => {
                // Inline register writes (not write_holding_register) — single lock
                let mut hr = store
                    .holding_registers
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                for (i, &v) in values.iter().enumerate() {
                    let a = addr as usize + i;
                    if a >= hr.len() {
                        hr.resize(a + 1, 0);
                    }
                    hr[a] = v;
                }
                drop(hr);
                Response::WriteMultipleRegisters(addr, values.len() as u16)
            }
            Request::MaskWriteRegister(addr, and_mask, or_mask) => {
                // Atomic RMW: single write lock for the entire operation
                let mut hr = store
                    .holding_registers
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                let a = addr as usize;
                if a >= hr.len() {
                    hr.resize(a + 1, 0);
                }
                let current = hr[a];
                hr[a] = (current & and_mask) | (or_mask & !and_mask);
                Response::MaskWriteRegister(addr, and_mask, or_mask)
            }
            Request::ReadWriteMultipleRegisters(read_addr, read_qty, write_addr, values) => {
                // FC=23 is NOT atomic: the read lock is released before the
                // write lock is acquired. A concurrent writer between the two
                // can modify the register range, so the returned read values
                // may be stale. This is acceptable per the Modbus spec (which
                // does not require atomicity for function code 23) and matches
                // the behavior of common industrial devices.
                let regs = store.read_holding_range(read_addr, read_qty);
                // Inline writes — single lock (not write_holding_register loop)
                let mut hr = store
                    .holding_registers
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                for (i, &v) in values.iter().enumerate() {
                    let a = write_addr as usize + i;
                    if a >= hr.len() {
                        hr.resize(a + 1, 0);
                    }
                    hr[a] = v;
                }
                drop(hr);
                Response::ReadWriteMultipleRegisters(regs)
            }
            Request::Diagnostic(sf, data) => {
                match sf {
                    // 0x0000: Return Query Data — echo back
                    0x0000 => Response::Diagnostic(sf, data),
                    // 0x000A: Clear Counters and Diagnostic Register
                    0x000A => Response::Diagnostic(sf, 0x0000),
                    // 0x000B-0x000E: Return counters (not tracked → return 0)
                    0x000B..=0x000E => Response::Diagnostic(sf, 0x0000),
                    _ => return Err(Exception::IllegalDataValue),
                }
            }
            _ => return Err(Exception::IllegalFunction),
        })
    }
}
