// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! PDU encode/decode and transport hot-path benchmarks.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oms_modbus::frame::{Request, Response};
use std::convert::TryFrom;

fn bench_pdu_encode_decode(c: &mut Criterion) {
    let req = Request::ReadHoldingRegisters(0, 125);
    let encoded = Bytes::try_from(req.clone()).unwrap();

    c.bench_function("pdu_encode_read_holding_125regs", |b| {
        b.iter(|| {
            let bytes = Bytes::try_from(black_box(req.clone())).unwrap();
            black_box(bytes)
        })
    });

    c.bench_function("pdu_decode_read_holding_125regs", |b| {
        b.iter(|| {
            let req = Request::try_from(black_box(encoded.clone())).unwrap();
            black_box(req)
        })
    });

    // Round-trip: encode → Bytes → decode → Request
    c.bench_function("pdu_roundtrip_read_holding_125regs", |b| {
        b.iter(|| {
            let bytes = Bytes::try_from(black_box(req.clone())).unwrap();
            let decoded = Request::try_from(bytes).unwrap();
            black_box(decoded)
        })
    });

    // WriteMultipleRegisters (large payload)
    let values: Vec<u16> = (0..123).collect();
    let write_req = Request::WriteMultipleRegisters(0, std::borrow::Cow::Owned(values));
    c.bench_function("pdu_encode_write_multiple_123regs", |b| {
        b.iter(|| {
            let bytes = Bytes::try_from(black_box(write_req.clone())).unwrap();
            black_box(bytes)
        })
    });

    // Response encode
    let values: Vec<u16> = (0..125).collect();
    let rsp = Response::ReadHoldingRegisters(values);
    c.bench_function("pdu_encode_response_125regs", |b| {
        b.iter(|| {
            let bytes = Bytes::from(black_box(rsp.clone()));
            black_box(bytes)
        })
    });
}

criterion_group!(benches, bench_pdu_encode_decode);
criterion_main!(benches);
