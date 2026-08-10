// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Transport hot-path benchmarks — frame encoding and round-trip throughput.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use oms_modbus::client::ModbusClient;
use oms_modbus::codec::{calculate_crc, calculate_lrc, encode_rtu_frame};
use oms_modbus::frame::{encode_request_into, Request};
use oms_modbus::{rtu, SlaveStore};

/// Benchmark: encode a Modbus request into wire bytes (PDU + slave_id + CRC).
/// This is the inner loop of `send_frame`, minus actual socket write.
fn bench_frame_encode(c: &mut Criterion) {
    let request = Request::ReadHoldingRegisters(0, 125);
    // Pre-encoded PDU bytes (what send_frame produces as input to transport framing)
    let mut write_buf = BytesMut::with_capacity(256);

    c.bench_function("rtu_encode_frame_from_pdu", |b| {
        b.iter(|| {
            write_buf.clear();
            write_buf.put_u8(0x01); // slave_id
            encode_request_into(black_box(&request), &mut write_buf).unwrap();
            let pdu = write_buf.split();
            encode_rtu_frame(&pdu, &mut write_buf);
            let _frame = write_buf.split().freeze();
            black_box(())
        })
    });

    // CRC-only: calculating CRC for a 7-byte RTU frame (slave + PDU).
    // This is the dominant cost of RTU encoding.
    let typical_rtu_frame: [u8; 8] = [0x01, 0x03, 0xFA, 0x00, 0x00, 0x00, 0x7D, 0x00];

    c.bench_function("rtu_crc_8byte_verify", |b| {
        b.iter(|| {
            let crc = calculate_crc(black_box(&typical_rtu_frame));
            black_box(crc)
        })
    });

    c.bench_function("ascii_lrc_8byte", |b| {
        b.iter(|| {
            let lrc = calculate_lrc(black_box(&typical_rtu_frame));
            black_box(lrc)
        })
    });
}

/// Benchmark: full RTU request-response round-trip over DuplexStream.
/// Measures end-to-end latency including framing, I/O, and decoding.
fn bench_rtu_roundtrip(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("rtu_roundtrip_read_holding_1reg", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (client_stream, server_stream) = tokio::io::duplex(1024);

                let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
                let server = rtu::RtuServer::new(server_stream);
                tokio::spawn(async move {
                    server.serve_forever(store).await.ok();
                });

                let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(1));
                let regs = client.read_holding_registers(1, 0, 1).await.unwrap();
                black_box(regs)
            })
        })
    });

    c.bench_function("rtu_roundtrip_read_holding_125regs", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (client_stream, server_stream) = tokio::io::duplex(1024);

                let store = SlaveStore::new();
                for i in 0..125u16 {
                    store.write_holding_register(i, i);
                }
                let store = Arc::new(store);
                let server = rtu::RtuServer::new(server_stream);
                tokio::spawn(async move {
                    server.serve_forever(store).await.ok();
                });

                let client = rtu::RtuClient::with_timeout(client_stream, Duration::from_secs(1));
                let regs = client.read_holding_registers(1, 0, 125).await.unwrap();
                black_box(regs)
            })
        })
    });
}

criterion_group!(benches, bench_frame_encode, bench_rtu_roundtrip);
criterion_main!(benches);
