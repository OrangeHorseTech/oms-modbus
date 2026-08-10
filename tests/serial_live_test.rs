// SPDX-License-Identifier: MIT OR Apache-2.0
/// Live serial tests: COM20 ↔ COM21 null-modem pair.
///
/// Each test spawns its own server on COM20, runs the client on COM21,
/// then tears down the server. All tests are `#[ignore]` — requires
/// hardware. Run with:
///
/// ```bash
/// cargo test --test serial_live_test -- --test-threads=1 --nocapture --ignored
/// ```
#[cfg(test)]
mod serial_live_tests {
    use oms_modbus::*;
    use std::sync::Arc;
    use std::time::Duration;

    // ── Helpers ──────────────────────────────────────────────────────────

    fn serial_cfg(name: &str) -> tokio_serial::SerialPortBuilder {
        tokio_serial::new(name, 9600)
            .data_bits(tokio_serial::DataBits::Eight)
            .parity(tokio_serial::Parity::None)
            .stop_bits(tokio_serial::StopBits::One)
            .flow_control(tokio_serial::FlowControl::None)
    }

    fn open(name: &str) -> tokio_serial::SerialStream {
        tokio_serial::SerialStream::open(&serial_cfg(name))
            .unwrap_or_else(|e| panic!("cannot open {name}: {e}"))
    }

    fn test_store() -> Arc<SlaveStore> {
        let store = Arc::new(SlaveStore::with_holding_registers(&[
            (0, 1234),
            (1, 5678),
            (2, 100),
            (3, 42),
            (10, 500),
            (11, 600),
            (20, 0),
        ]));
        store.write_input_register(0, 2400);
        store.write_input_register(1, 150);
        store.write_coil(0, true);
        store.write_coil(1, false);
        store.write_coil(2, true);
        store.write_discrete_input(0, true);
        store.write_discrete_input(1, false);
        store
    }

    struct ServerGuard {
        abort: tokio::task::AbortHandle,
    }

    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.abort.abort();
        }
    }

    fn spawn_rtu() -> ServerGuard {
        let store = test_store();
        let server = rtu::RtuServer::new(open("COM20"));
        let handle = tokio::spawn(async move {
            server.serve_forever(store).await.ok();
        });
        ServerGuard {
            abort: handle.abort_handle(),
        }
    }

    fn spawn_ascii() -> ServerGuard {
        let store = test_store();
        let server = ascii::AsciiServer::new(open("COM20"));
        let handle = tokio::spawn(async move {
            server.serve_forever(store).await.ok();
        });
        ServerGuard {
            abort: handle.abort_handle(),
        }
    }

    fn client_rtu() -> rtu::RtuClient<tokio_serial::SerialStream> {
        rtu::RtuClient::with_timeout(open("COM21"), Duration::from_secs(3))
    }

    fn client_ascii() -> ascii::AsciiClient<tokio_serial::SerialStream> {
        ascii::AsciiClient::with_timeout(open("COM21"), Duration::from_secs(3))
    }

    // ══════════════════════════════════════════════════════════════════════
    // RTU Tests
    // ══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_read_holding_registers() {
        let _srv = spawn_rtu();
        let regs = client_rtu().read_holding_registers(1, 0, 4).await.unwrap();
        assert_eq!(regs, vec![1234, 5678, 100, 42]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_read_input_registers() {
        let _srv = spawn_rtu();
        let regs = client_rtu().read_input_registers(1, 0, 2).await.unwrap();
        assert_eq!(regs, vec![2400, 150]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_read_coils() {
        let _srv = spawn_rtu();
        let coils = client_rtu().read_coils(1, 0, 3).await.unwrap();
        assert!(coils[0], "coil 0");
        assert!(!coils[1], "coil 1");
        assert!(coils[2], "coil 2");
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_read_discrete_inputs() {
        let _srv = spawn_rtu();
        let inputs = client_rtu().read_discrete_inputs(1, 0, 2).await.unwrap();
        assert!(inputs[0]);
        assert!(!inputs[1]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_write_single_register() {
        let _srv = spawn_rtu();
        let c = client_rtu();
        c.write_single_register(1, 20, 999).await.unwrap();
        let regs = c.read_holding_registers(1, 20, 1).await.unwrap();
        assert_eq!(regs, vec![999]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_write_single_coil() {
        let _srv = spawn_rtu();
        let c = client_rtu();
        c.write_single_coil(1, 1, true).await.unwrap();
        let coils = c.read_coils(1, 1, 1).await.unwrap();
        assert!(coils[0], "coil 1 should be ON after write");
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_write_multiple_registers() {
        let _srv = spawn_rtu();
        let c = client_rtu();
        c.write_multiple_registers(1, 20, &[111, 222, 333])
            .await
            .unwrap();
        let regs = c.read_holding_registers(1, 20, 3).await.unwrap();
        assert_eq!(regs, vec![111, 222, 333]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_diagnostic() {
        let _srv = spawn_rtu();
        let (_sub_fn, data) = client_rtu().diagnostic(1, 0x0000, 0xABCD).await.unwrap();
        assert_eq!(data, 0xABCD, "Return Query Data must echo");
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_with_bus_capture() {
        let _srv = spawn_rtu();
        let cap = Arc::new(BusCapture::unbounded());
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_tap(cap.clone());
        let c = rtu::with_options(open("COM21"), opts);
        let regs = c.read_holding_registers(1, 0, 2).await.unwrap();
        assert_eq!(regs, vec![1234, 5678]);
        assert!(!cap.is_empty(), "should capture RTU traffic");
        assert!(cap.count_requests() >= 1);
        assert!(cap.count_responses() >= 1);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_mask_write_register() {
        let _srv = spawn_rtu();
        let c = client_rtu();
        c.write_single_register(1, 10, 0x1234).await.unwrap();
        c.mask_write_register(1, 10, 0x00FF, 0xFF00).await.unwrap();
        let regs = c.read_holding_registers(1, 10, 1).await.unwrap();
        assert_eq!(regs, vec![0xFF34]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn rtu_read_write_multiple_registers() {
        let _srv = spawn_rtu();
        let c = client_rtu();
        let old = c
            .read_write_multiple_registers(1, 0, 2, 10, &[777, 888])
            .await
            .unwrap();
        assert_eq!(old, vec![1234, 5678]);
        let regs = c.read_holding_registers(1, 10, 2).await.unwrap();
        assert_eq!(regs, vec![777, 888]);
    }

    // ══════════════════════════════════════════════════════════════════════
    // ASCII Tests
    // ══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_read_holding_registers() {
        let _srv = spawn_ascii();
        let regs = client_ascii()
            .read_holding_registers(1, 0, 4)
            .await
            .unwrap();
        assert_eq!(regs, vec![1234, 5678, 100, 42]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_read_input_registers() {
        let _srv = spawn_ascii();
        let regs = client_ascii().read_input_registers(1, 0, 2).await.unwrap();
        assert_eq!(regs, vec![2400, 150]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_read_coils_and_discrete_inputs() {
        let _srv = spawn_ascii();
        let c = client_ascii();
        let coils = c.read_coils(1, 0, 3).await.unwrap();
        assert!(coils[0] && !coils[1] && coils[2]);
        let di = c.read_discrete_inputs(1, 0, 2).await.unwrap();
        assert!(di[0] && !di[1]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_write_single_register() {
        let _srv = spawn_ascii();
        let c = client_ascii();
        c.write_single_register(1, 20, 777).await.unwrap();
        let regs = c.read_holding_registers(1, 20, 1).await.unwrap();
        assert_eq!(regs, vec![777]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_write_multiple_registers() {
        let _srv = spawn_ascii();
        let c = client_ascii();
        c.write_multiple_registers(1, 20, &[100, 200, 300])
            .await
            .unwrap();
        let regs = c.read_holding_registers(1, 20, 3).await.unwrap();
        assert_eq!(regs, vec![100, 200, 300]);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_write_single_coil() {
        let _srv = spawn_ascii();
        let c = client_ascii();
        c.write_single_coil(1, 1, true).await.unwrap();
        let coils = c.read_coils(1, 1, 1).await.unwrap();
        assert!(coils[0], "coil should be ON");
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_diagnostic() {
        let _srv = spawn_ascii();
        let (_sf, data) = client_ascii().diagnostic(1, 0x0000, 0xBEEF).await.unwrap();
        assert_eq!(data, 0xBEEF);
    }

    #[tokio::test]
    #[ignore = "requires COM20↔COM21 null-modem"]
    async fn ascii_with_bus_capture() {
        let _srv = spawn_ascii();
        let cap = Arc::new(BusCapture::unbounded());
        let opts = ClientOptions::default()
            .with_timeout(Duration::from_secs(3))
            .with_tap(cap.clone());
        let c = ascii::with_options(open("COM21"), opts);
        let regs = c.read_holding_registers(1, 0, 2).await.unwrap();
        assert_eq!(regs, vec![1234, 5678]);
        assert!(!cap.is_empty(), "should capture ASCII traffic");
        assert!(cap.count_requests() >= 1);
    }
}
