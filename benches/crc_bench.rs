// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! CRC-16 throughput benchmarks — our impl vs `crc` crate.

use crc::{Crc, CRC_16_MODBUS};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oms_modbus::codec::calculate_crc;

/// The `crc` crate's CRC-16/MODBUS algorithm (pre-built, not in the loop).
static CRC_CRATE: Crc<u16> = Crc::<u16>::new(&CRC_16_MODBUS);

fn bench_crc_vs_crate(c: &mut Criterion) {
    let sizes = [
        ("6b_typical_rtu", vec![0x01u8, 0x03, 0x00, 0x00, 0x00, 0x01]),
        ("64b_medium_frame", vec![0x55u8; 64]),
        ("256b_rtu_max", vec![0x55u8; 256]),
    ];

    for (label, data) in &sizes {
        // Our impl
        c.bench_function(&format!("crc_our_{label}"), |b| {
            b.iter(|| {
                let crc = calculate_crc(black_box(data));
                black_box(crc)
            })
        });

        // crc crate
        c.bench_function(&format!("crc_crate_{label}"), |b| {
            b.iter(|| {
                let crc = CRC_CRATE.checksum(black_box(data));
                black_box(crc)
            })
        });
    }
}

criterion_group!(benches, bench_crc_vs_crate);
criterion_main!(benches);
