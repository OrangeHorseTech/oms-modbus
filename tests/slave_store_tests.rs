// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for SlaveStore — read/write coils, registers, Service trait.

use oms_modbus::*;
use std::borrow::Cow;
use std::sync::Arc;

#[tokio::test]
async fn read_write_holding() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 100), (1, 200)]));
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![100, 200]));

    Service::call(&store, Request::WriteSingleRegister(0, 999))
        .await
        .unwrap();
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![999]));
}

#[tokio::test]
async fn read_write_coils() {
    let store = Arc::new(SlaveStore::new());
    store.write_coil(0, true);
    store.write_coil(1, false);
    store.write_coil(2, true);

    let rsp = Service::call(&store, Request::ReadCoils(0, 4))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadCoils(vec![true, false, true, false]));

    Service::call(&store, Request::WriteSingleCoil(1, true))
        .await
        .unwrap();
    let rsp = Service::call(&store, Request::ReadCoils(1, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadCoils(vec![true]));

    Service::call(
        &store,
        Request::WriteMultipleCoils(0, Cow::Owned(vec![false, false, true, true])),
    )
    .await
    .unwrap();
    let rsp = Service::call(&store, Request::ReadCoils(0, 4))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadCoils(vec![false, false, true, true]));
}

#[tokio::test]
async fn read_discrete_inputs() {
    let store = Arc::new(SlaveStore::new());
    let rsp = Service::call(&store, Request::ReadDiscreteInputs(0, 4))
        .await
        .unwrap();
    assert_eq!(
        rsp,
        Response::ReadDiscreteInputs(vec![false, false, false, false])
    );
}

#[tokio::test]
async fn read_input_registers() {
    let store = Arc::new(SlaveStore::new());
    let rsp = Service::call(&store, Request::ReadInputRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadInputRegisters(vec![0, 0]));
}

#[tokio::test]
async fn write_multiple_registers() {
    let store = Arc::new(SlaveStore::new());
    Service::call(
        &store,
        Request::WriteMultipleRegisters(0, Cow::Owned(vec![10, 20, 30])),
    )
    .await
    .unwrap();
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(0, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![10, 20, 30]));
}

#[tokio::test]
async fn mask_write_register() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(10, 0x1234)]));
    Service::call(&store, Request::MaskWriteRegister(10, 0x00FF, 0xFF00))
        .await
        .unwrap();
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(10, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![0xFF34]));
}

#[tokio::test]
async fn read_write_multiple_registers() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (20, 1),
        (21, 2),
        (22, 3),
    ]));
    let rsp = Service::call(
        &store,
        Request::ReadWriteMultipleRegisters(20, 3, 20, Cow::Owned(vec![100, 200])),
    )
    .await
    .unwrap();
    assert_eq!(rsp, Response::ReadWriteMultipleRegisters(vec![1, 2, 3]));
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(20, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![100, 200, 3]));
}

#[tokio::test]
async fn unsupported_fc_returns_illegal_function() {
    let store = Arc::new(SlaveStore::new());
    let result = Service::call(&store, Request::Disconnect).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Exception::IllegalFunction);
}

#[tokio::test]
async fn large_consecutive_read() {
    let store = Arc::new(SlaveStore::with_holding_registers(
        &(0..125).map(|i| (i, i * 10)).collect::<Vec<_>>(),
    ));
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(0, 125))
        .await
        .unwrap();
    let expected: Vec<u16> = (0..125).map(|i| i * 10).collect();
    assert_eq!(rsp, Response::ReadHoldingRegisters(expected));
}

#[tokio::test]
async fn read_beyond_default_capacity_returns_zero() {
    let store = Arc::new(SlaveStore::new());
    // Address beyond default 1024 — should return 0, not panic
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(5000, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![0]));
}

#[tokio::test]
async fn write_multiple_registers_batch_atomic() {
    let store = Arc::new(SlaveStore::new());
    let values: Vec<u16> = (0..125).map(|i| i * 10).collect();
    Service::call(
        &store,
        Request::WriteMultipleRegisters(0, Cow::Owned(values.clone())),
    )
    .await
    .unwrap();
    let rsp = Service::call(&store, Request::ReadHoldingRegisters(0, 125))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(values));
}

#[tokio::test]
async fn write_multiple_coils_batch_atomic() {
    let store = Arc::new(SlaveStore::new());
    let bits = vec![true; 2000];
    Service::call(
        &store,
        Request::WriteMultipleCoils(0, Cow::Owned(bits.clone())),
    )
    .await
    .unwrap();
    let rsp = Service::call(&store, Request::ReadCoils(0, 2000))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadCoils(bits));
}

#[tokio::test]
async fn address_wrapping_u16_boundary() {
    let store = Arc::new(SlaveStore::new());
    Service::call(
        &store,
        Request::WriteMultipleRegisters(u16::MAX - 1, Cow::Owned(vec![42, 99])),
    )
    .await
    .unwrap();
    let rsp1 = Service::call(&store, Request::ReadHoldingRegisters(u16::MAX - 1, 1))
        .await
        .unwrap();
    assert_eq!(rsp1, Response::ReadHoldingRegisters(vec![42]));
    let rsp2 = Service::call(&store, Request::ReadHoldingRegisters(u16::MAX, 1))
        .await
        .unwrap();
    assert_eq!(rsp2, Response::ReadHoldingRegisters(vec![99]));
}

#[tokio::test]
async fn write_input_register() {
    let store = Arc::new(SlaveStore::new());
    store.write_input_register(0, 42);
    store.write_input_register(1, 99);
    let rsp = Service::call(&store, Request::ReadInputRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadInputRegisters(vec![42, 99]));
}

#[tokio::test]
async fn diagnostic_return_query_data() {
    let store = Arc::new(SlaveStore::new());
    let rsp = Service::call(&store, Request::Diagnostic(0x0000, 0xABCD))
        .await
        .unwrap();
    assert_eq!(rsp, Response::Diagnostic(0x0000, 0xABCD));
}

#[tokio::test]
async fn diagnostic_counters_return_zero() {
    let store = Arc::new(SlaveStore::new());
    for sf in [0x000Bu16, 0x000C, 0x000D, 0x000E] {
        let rsp = Service::call(&store, Request::Diagnostic(sf, 0x0000))
            .await
            .unwrap();
        assert_eq!(rsp, Response::Diagnostic(sf, 0x0000));
    }
}

#[tokio::test]
async fn diagnostic_clear_counters() {
    let store = Arc::new(SlaveStore::new());
    let rsp = Service::call(&store, Request::Diagnostic(0x000A, 0x0000))
        .await
        .unwrap();
    assert_eq!(rsp, Response::Diagnostic(0x000A, 0x0000));
}

#[tokio::test]
async fn diagnostic_unknown_sub_function_returns_exception() {
    let store = Arc::new(SlaveStore::new());
    let result = Service::call(&store, Request::Diagnostic(0x00FF, 0x0000)).await;
    assert_eq!(result.unwrap_err(), Exception::IllegalDataValue);
}

#[tokio::test]
async fn write_discrete_input() {
    let store = Arc::new(SlaveStore::new());
    store.write_discrete_input(0, true);
    store.write_discrete_input(1, false);
    store.write_discrete_input(2, true);
    let rsp = Service::call(&store, Request::ReadDiscreteInputs(0, 4))
        .await
        .unwrap();
    assert_eq!(
        rsp,
        Response::ReadDiscreteInputs(vec![true, false, true, false])
    );
}
