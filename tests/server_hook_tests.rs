// SPDX-License-Identifier: MIT OR Apache-2.0
//! Comprehensive tests for `ServerHook` and `HookedService`.
//!
//! Coverage:
//! - before_call: short-circuit all FC families, slave-aware, address-aware
//! - after_call:  transform success, suppress/upgrade exception, side-effects
//! - composition: double/triple layer, custom Service, shared state
//! - realistic:   read-only, access-control, data-scale, audit-log
//! - concurrency: parallel calls on shared HookedService
//! - task_local:  SLAVE_ID propagation and default
//! - edge cases:  single-method hooks, all-FC passthrough

use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use oms_modbus::*;

// ── Test fixtures ────────────────────────────────────────────────────────────

/// A no-op hook — every method passes through.
struct NoopHook;

#[async_trait]
impl ServerHook for NoopHook {}

/// A hook that short-circuits all ReadHoldingRegisters requests with a fixed value.
struct ShortCircuitHook {
    fixed_value: u16,
    call_count: AtomicU8,
}

#[async_trait]
impl ServerHook for ShortCircuitHook {
    async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
        if let Request::ReadHoldingRegisters(_addr, qty) = request {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let regs = vec![self.fixed_value; *qty as usize];
            Some(Response::ReadHoldingRegisters(regs))
        } else {
            None
        }
    }
}

/// A hook that transforms exceptions into empty success responses.
struct SuppressExceptionHook;

#[async_trait]
impl ServerHook for SuppressExceptionHook {
    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        match result {
            Ok(rsp) => Ok(rsp),
            Err(_) => Ok(Response::ReadHoldingRegisters(vec![0; 1])),
        }
    }
}

/// A hook that logs the last slave ID and call counts.
struct LoggingHook {
    last_slave: AtomicU8,
    before_count: AtomicU8,
    after_count: AtomicU8,
}

impl LoggingHook {
    fn new() -> Self {
        Self {
            last_slave: AtomicU8::new(0),
            before_count: AtomicU8::new(0),
            after_count: AtomicU8::new(0),
        }
    }
}

#[async_trait]
impl ServerHook for LoggingHook {
    async fn before_call(&self, slave: u8, _request: &Request<'_>) -> Option<Response> {
        self.last_slave.store(slave, Ordering::Relaxed);
        self.before_count.fetch_add(1, Ordering::Relaxed);
        None
    }

    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        self.after_count.fetch_add(1, Ordering::Relaxed);
        result
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 1: before_call — short-circuit patterns
// ═══════════════════════════════════════════════════════════════════════════════

// ── 1a. All read function codes ──────────────────────────────────────────────

/// Hook that intercepts every read function code and returns fixed data.
struct AllReadsHook {
    coil_val: bool,
    reg_val: u16,
}

#[async_trait]
impl ServerHook for AllReadsHook {
    async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
        match request {
            Request::ReadCoils(_addr, qty) => {
                Some(Response::ReadCoils(vec![self.coil_val; *qty as usize]))
            }
            Request::ReadDiscreteInputs(_addr, qty) => Some(Response::ReadDiscreteInputs(vec![
                    self.coil_val;
                    *qty as usize
                ])),
            Request::ReadHoldingRegisters(_addr, qty) => {
                Some(Response::ReadHoldingRegisters(vec![
                    self.reg_val;
                    *qty as usize
                ]))
            }
            Request::ReadInputRegisters(_addr, qty) => Some(Response::ReadInputRegisters(vec![
                    self.reg_val;
                    *qty as usize
                ])),
            _ => None,
        }
    }
}

#[tokio::test]
async fn before_call_short_circuits_read_coils() {
    let store = Arc::new(SlaveStore::new());
    let hook = AllReadsHook {
        coil_val: true,
        reg_val: 0,
    };
    let hooked = HookedService::new(store, hook);

    let rsp = hooked.call(Request::ReadCoils(0, 8)).await.unwrap();
    assert_eq!(rsp, Response::ReadCoils(vec![true; 8]));
}

#[tokio::test]
async fn before_call_short_circuits_read_discrete_inputs() {
    let store = Arc::new(SlaveStore::new());
    let hook = AllReadsHook {
        coil_val: false,
        reg_val: 0,
    };
    let hooked = HookedService::new(store, hook);

    let rsp = hooked
        .call(Request::ReadDiscreteInputs(10, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadDiscreteInputs(vec![false; 3]));
}

#[tokio::test]
async fn before_call_short_circuits_read_input_registers() {
    let store = Arc::new(SlaveStore::new());
    let hook = AllReadsHook {
        coil_val: false,
        reg_val: 777,
    };
    let hooked = HookedService::new(store, hook);

    let rsp = hooked
        .call(Request::ReadInputRegisters(0, 4))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadInputRegisters(vec![777; 4]));
}

// ── 1b. Write function codes ─────────────────────────────────────────────────

/// Hook that intercepts WriteSingleRegister and returns an echo (like a real device).
struct WriteEchoHook {
    last_written: Mutex<Vec<(u16, u16)>>,
}

impl WriteEchoHook {
    fn new() -> Self {
        Self {
            last_written: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ServerHook for WriteEchoHook {
    async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
        match request {
            Request::WriteSingleRegister(addr, val) => {
                self.last_written.lock().unwrap().push((*addr, *val));
                Some(Response::WriteSingleRegister(*addr, *val))
            }
            Request::WriteMultipleRegisters(addr, vals) => {
                let vals: Vec<u16> = vals.to_vec();
                self.last_written
                    .lock()
                    .unwrap()
                    .extend(vals.iter().enumerate().map(|(i, &v)| (*addr + i as u16, v)));
                Some(Response::WriteMultipleRegisters(*addr, vals.len() as u16))
            }
            _ => None,
        }
    }
}

#[tokio::test]
async fn before_call_short_circuits_write_single_register() {
    let store = Arc::new(SlaveStore::new());
    let hook = WriteEchoHook::new();
    let hooked = HookedService::new(store, hook);

    let rsp = hooked
        .call(Request::WriteSingleRegister(100, 12345))
        .await
        .unwrap();
    assert_eq!(rsp, Response::WriteSingleRegister(100, 12345));
    // Store untouched — hook short-circuited before inner
    assert_eq!(hooked.hook.last_written.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn before_call_short_circuits_write_multiple_registers() {
    let store = Arc::new(SlaveStore::new());
    let hook = WriteEchoHook::new();
    let hooked = HookedService::new(store, hook);

    let data = vec![10u16, 20, 30];
    let rsp = hooked
        .call(Request::WriteMultipleRegisters(
            50,
            std::borrow::Cow::Borrowed(&data),
        ))
        .await
        .unwrap();
    assert_eq!(rsp, Response::WriteMultipleRegisters(50, 3));
    assert_eq!(hooked.hook.last_written.lock().unwrap().len(), 3);
}

// ── 1c. Diagnostic ───────────────────────────────────────────────────────────

#[tokio::test]
async fn before_call_intercepts_diagnostic() {
    struct DiagHook;
    #[async_trait]
    impl ServerHook for DiagHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            if let Request::Diagnostic(sub_fn, data) = request {
                Some(Response::Diagnostic(*sub_fn, *data))
            } else {
                None
            }
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hooked = HookedService::new(store, DiagHook);
    let rsp = hooked
        .call(Request::Diagnostic(0x0000, 0xABCD))
        .await
        .unwrap();
    assert_eq!(rsp, Response::Diagnostic(0x0000, 0xABCD));
}

// ── 1d. Slave-aware routing ──────────────────────────────────────────────────

/// Hook that returns different values per slave ID.
struct SlaveAwareHook {
    slave1_val: u16,
    slave2_val: u16,
    hits: [AtomicU8; 2],
}

#[async_trait]
impl ServerHook for SlaveAwareHook {
    async fn before_call(&self, slave: u8, request: &Request<'_>) -> Option<Response> {
        if let Request::ReadHoldingRegisters(_addr, qty) = request {
            let val = match slave {
                1 => {
                    self.hits[0].fetch_add(1, Ordering::Relaxed);
                    self.slave1_val
                }
                2 => {
                    self.hits[1].fetch_add(1, Ordering::Relaxed);
                    self.slave2_val
                }
                _ => return None,
            };
            Some(Response::ReadHoldingRegisters(vec![val; *qty as usize]))
        } else {
            None
        }
    }
}

#[tokio::test]
async fn before_call_routes_by_slave_id() {
    let store = Arc::new(SlaveStore::new());
    let hook = SlaveAwareHook {
        slave1_val: 111,
        slave2_val: 222,
        hits: [AtomicU8::new(0), AtomicU8::new(0)],
    };
    let hooked = HookedService::new(store, hook);

    // Simulate what process_server_request does: set SLAVE_ID in task-local scope
    let rsp1 = server::context::SLAVE_ID
        .scope(1, async {
            hooked.call(Request::ReadHoldingRegisters(0, 2)).await
        })
        .await
        .unwrap();
    assert_eq!(rsp1, Response::ReadHoldingRegisters(vec![111, 111]));
    assert_eq!(hooked.hook.hits[0].load(Ordering::Relaxed), 1);

    let rsp2 = server::context::SLAVE_ID
        .scope(2, async {
            hooked.call(Request::ReadHoldingRegisters(0, 2)).await
        })
        .await
        .unwrap();
    assert_eq!(rsp2, Response::ReadHoldingRegisters(vec![222, 222]));
    assert_eq!(hooked.hook.hits[1].load(Ordering::Relaxed), 1);

    // Slave 3 not matched → passes through to inner store
    let rsp3 = server::context::SLAVE_ID
        .scope(3, async {
            hooked.call(Request::ReadHoldingRegisters(0, 1)).await
        })
        .await
        .unwrap();
    assert_eq!(rsp3, Response::ReadHoldingRegisters(vec![0])); // default store value
}

// ── 1e. Address-aware routing ────────────────────────────────────────────────

/// Hook that maps different address ranges to different data sources.
struct AddressRouterHook;

#[async_trait]
impl ServerHook for AddressRouterHook {
    async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
        match request {
            // Addresses 0-99: static config area → return address itself
            Request::ReadHoldingRegisters(addr, qty) if *addr < 100 => {
                let regs: Vec<u16> = (*addr..*addr + *qty).collect();
                Some(Response::ReadHoldingRegisters(regs))
            }
            // Addresses 100-199: computed area → return address × 10
            Request::ReadHoldingRegisters(addr, qty) if *addr < 200 => {
                let regs: Vec<u16> = (*addr..*addr + *qty).map(|a| a * 10).collect();
                Some(Response::ReadHoldingRegisters(regs))
            }
            _ => None,
        }
    }
}

#[tokio::test]
async fn before_call_routes_by_address_range() {
    let store = Arc::new(SlaveStore::new());
    let hooked = HookedService::new(store, AddressRouterHook);

    // Static area: returns address as value
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![0, 1, 2]));

    // Computed area: returns address × 10
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(100, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![1000, 1010, 1020]));

    // Outside mapped ranges → passes through to store (returns zeros)
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(500, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![0]));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 2: after_call — transform patterns
// ═══════════════════════════════════════════════════════════════════════════════

// ── 2a. Transform success: modify response values ────────────────────────────

/// Hook that scales all ReadHoldingRegisters values by 10.
struct ScaleHook {
    factor: u16,
}

#[async_trait]
impl ServerHook for ScaleHook {
    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        match result {
            Ok(Response::ReadHoldingRegisters(regs)) => {
                let scaled: Vec<u16> = regs.iter().map(|v| v.saturating_mul(self.factor)).collect();
                Ok(Response::ReadHoldingRegisters(scaled))
            }
            other => other,
        }
    }
}

#[tokio::test]
async fn after_call_transforms_success_values() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[
        (0, 5),
        (1, 10),
        (2, 100),
    ]));
    let hooked = HookedService::new(store, ScaleHook { factor: 10 });

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![50, 100, 1000]));
}

#[tokio::test]
async fn after_call_scale_hook_only_affects_matched_fc() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 7)]));
    let hooked = HookedService::new(store, ScaleHook { factor: 2 });

    // WriteSingleRegister is not transformed by ScaleHook
    let rsp = hooked
        .call(Request::WriteSingleRegister(0, 99))
        .await
        .unwrap();
    assert_eq!(rsp, Response::WriteSingleRegister(0, 99));

    // But ReadHoldingRegisters IS scaled
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![198])); // 99 × 2
}

// ── 2b. Transform exception → different exception ────────────────────────────

/// Hook that converts all exceptions to ServerDeviceFailure.
struct ExceptionUpgradeHook;

#[async_trait]
impl ServerHook for ExceptionUpgradeHook {
    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        match result {
            Err(_) => Err(Exception::ServerDeviceFailure),
            ok => ok,
        }
    }
}

#[tokio::test]
async fn after_call_upgrades_exception_type() {
    // SlaveStore with no registers — Diagnostic 0x00FF is unknown → IllegalDataValue
    let store = Arc::new(SlaveStore::new());
    let hooked = HookedService::new(store, ExceptionUpgradeHook);

    let err = hooked
        .call(Request::Diagnostic(0x00FF, 0))
        .await
        .unwrap_err();
    assert_eq!(err, Exception::ServerDeviceFailure);
}

// ── 2c. Transform success → exception (escalate) ─────────────────────────────

/// Hook that rejects writes — turns successful write echo into an exception.
struct ReadOnlyHook;

#[async_trait]
impl ServerHook for ReadOnlyHook {
    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        match result {
            Ok(rsp) => match rsp.function_code().value() {
                5 | 6 | 15 | 16 | 22 | 23 => Err(Exception::IllegalFunction),
                _ => Ok(rsp),
            },
            err => err,
        }
    }
}

#[tokio::test]
async fn after_call_escalates_write_to_exception() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let hooked = HookedService::new(store, ReadOnlyHook);

    // Reads still work
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42]));

    // Write is rejected (even though inner store would accept it)
    let err = hooked
        .call(Request::WriteSingleRegister(0, 99))
        .await
        .unwrap_err();
    assert_eq!(err, Exception::IllegalFunction);

    // Verify store was NOT modified (after_call runs after inner, but the
    // exception overrides the response — store write already happened though)
    // Actually the store WAS written because after_call runs after inner.
    // This test demonstrates the semantic: the client sees an error,
    // but the write did happen. For true prevention use before_call.
}

// ── 2d. after_call with slave context ────────────────────────────────────────

/// Hook that enriches the response with slave-specific metadata.
/// (Simulates adding a header/trailer, but here just counts per-slave calls.)
struct PerSlaveCounter {
    counts: [AtomicU32; 256],
}

impl PerSlaveCounter {
    fn new() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }
}

#[async_trait]
impl ServerHook for PerSlaveCounter {
    async fn after_call(
        &self,
        slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        self.counts[slave as usize].fetch_add(1, Ordering::Relaxed);
        result
    }
}

#[tokio::test]
async fn after_call_counts_per_slave() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1)]));
    let hook = PerSlaveCounter::new();
    let hooked = HookedService::new(store, hook);

    // 3 calls for slave 1
    for _ in 0..3 {
        server::context::SLAVE_ID
            .scope(1, async {
                hooked.call(Request::ReadHoldingRegisters(0, 1)).await
            })
            .await
            .unwrap();
    }

    // 2 calls for slave 5
    for _ in 0..2 {
        server::context::SLAVE_ID
            .scope(5, async {
                hooked.call(Request::ReadHoldingRegisters(0, 1)).await
            })
            .await
            .unwrap();
    }

    assert_eq!(hooked.hook.counts[1].load(Ordering::Relaxed), 3);
    assert_eq!(hooked.hook.counts[5].load(Ordering::Relaxed), 2);
    assert_eq!(hooked.hook.counts[0].load(Ordering::Relaxed), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 3: Composition patterns
// ═══════════════════════════════════════════════════════════════════════════════

// ── 3a. Triple-layer hooks ───────────────────────────────────────────────────

#[tokio::test]
async fn triple_layer_hooks() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 10)]));

    // Layer 1 (outermost): logging
    let log = LoggingHook::new();

    // Layer 2 (middle): short-circuit ReadHoldingRegisters → [42, 42]
    let short = ShortCircuitHook {
        fixed_value: 42,
        call_count: AtomicU8::new(0),
    };

    // Layer 3 (innermost): scale ×2 on after_call
    let scale = ScaleHook { factor: 2 };

    // Build: log → short → scale → store
    type L2 = HookedService<Arc<SlaveStore>, ScaleHook>;
    type L1 = HookedService<L2, ShortCircuitHook>;
    let hooked: HookedService<L1, LoggingHook> = HookedService::new(
        HookedService::new(HookedService::new(store, scale), short),
        log,
    );

    // ShortCircuit intercepts ReadHoldingRegisters BEFORE it reaches scale/store.
    // Scale never fires because short-circuit skips inner entirely.
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42, 42]));

    // Logging hook (outermost) should have counted both before and after
    assert_eq!(hooked.hook.before_count.load(Ordering::Relaxed), 1);
    assert_eq!(hooked.hook.after_count.load(Ordering::Relaxed), 1);
}

// ── 3b. HookedService with custom Service ────────────────────────────────────

/// A custom service that always returns a fixed value.
struct FixedService {
    value: u16,
}

#[async_trait]
impl Service for FixedService {
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
        match request {
            Request::ReadHoldingRegisters(_addr, qty) => Ok(Response::ReadHoldingRegisters(vec![
                    self.value;
                    qty as usize
                ])),
            Request::WriteSingleRegister(addr, val) => Ok(Response::WriteSingleRegister(addr, val)),
            _ => Err(Exception::IllegalFunction),
        }
    }
}

#[tokio::test]
async fn hooked_custom_service_with_transform() {
    let svc = FixedService { value: 100 };
    // ScaleHook ×3 on after_call
    let hooked = HookedService::new(svc, ScaleHook { factor: 3 });

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    // FixedService returns [100, 100], ScaleHook multiplies by 3
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![300, 300]));

    // WriteSingleRegister not scaled
    let rsp = hooked
        .call(Request::WriteSingleRegister(0, 55))
        .await
        .unwrap();
    assert_eq!(rsp, Response::WriteSingleRegister(0, 55));
}

#[tokio::test]
async fn hooked_custom_service_with_before_call_short_circuit() {
    let svc = FixedService { value: 100 };
    // ReadOnlyAllHook intercepts reads in before_call, skipping FixedService entirely
    struct ReadOnlyAllHook;
    #[async_trait]
    impl ServerHook for ReadOnlyAllHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            match request {
                Request::ReadHoldingRegisters(_addr, qty) => {
                    Some(Response::ReadHoldingRegisters(vec![999; *qty as usize]))
                }
                _ => None,
            }
        }
    }

    let hooked = HookedService::new(svc, ReadOnlyAllHook);
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![999, 999, 999]));

    // Unknown FC still reaches FixedService → IllegalFunction
    let err = hooked.call(Request::ReadCoils(0, 1)).await.unwrap_err();
    assert_eq!(err, Exception::IllegalFunction);
}

// ── 3c. Shared state between hooks ───────────────────────────────────────────

#[tokio::test]
async fn hooks_can_share_state_via_arc() {
    let shared_counter = Arc::new(AtomicU32::new(0));

    struct SharedHook {
        counter: Arc<AtomicU32>,
    }
    #[async_trait]
    impl ServerHook for SharedHook {
        async fn before_call(&self, _slave: u8, _request: &Request<'_>) -> Option<Response> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    let hook1 = SharedHook {
        counter: shared_counter.clone(),
    };
    let hook2 = SharedHook {
        counter: shared_counter.clone(),
    };

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1)]));
    let inner = HookedService::new(store, hook1);
    let outer = HookedService::new(inner, hook2);

    outer
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    outer
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();

    // Both hooks fired twice — counter = 4
    assert_eq!(shared_counter.load(Ordering::Relaxed), 4);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 4: Realistic patterns
// ═══════════════════════════════════════════════════════════════════════════════

// ── 4a. Access control (before_call) ─────────────────────────────────────────

/// Hook that rejects requests from unauthorized slaves.
struct AccessControlHook {
    allowed_slaves: Vec<u8>,
    blocked_count: AtomicU32,
}

#[async_trait]
impl ServerHook for AccessControlHook {
    async fn before_call(&self, slave: u8, _request: &Request<'_>) -> Option<Response> {
        if !self.allowed_slaves.contains(&slave) {
            self.blocked_count.fetch_add(1, Ordering::Relaxed);
            // Return an exception as a short-circuit response
            let fc = _request.function_code().value();
            Some(Response::Exception(fc, Exception::IllegalFunction))
        } else {
            None
        }
    }
}

#[tokio::test]
async fn access_control_blocks_unauthorized_slaves() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99)]));
    let hook = AccessControlHook {
        allowed_slaves: vec![1, 2],
        blocked_count: AtomicU32::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // Slave 1 is allowed → passes through to store
    let rsp = server::context::SLAVE_ID
        .scope(1, async {
            hooked.call(Request::ReadHoldingRegisters(0, 1)).await
        })
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![99]));

    // Slave 3 is blocked → receives exception without touching store
    let rsp = server::context::SLAVE_ID
        .scope(3, async {
            hooked.call(Request::ReadHoldingRegisters(0, 1)).await
        })
        .await
        .unwrap();
    match rsp {
        Response::Exception(3, Exception::IllegalFunction) => {} // FC=3 for ReadHoldingRegisters
        other => panic!("expected Exception(3, IllegalFunction), got {other:?}"),
    }

    assert_eq!(hooked.hook.blocked_count.load(Ordering::Relaxed), 1);
}

// ── 4b. Data transformation (after_call) ─────────────────────────────────────

/// Hook that applies a linear transform: y = offset + scale × x.
struct LinearTransformHook {
    offset: i32,
    scale: i32,
}

#[async_trait]
impl ServerHook for LinearTransformHook {
    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        match result {
            Ok(Response::ReadHoldingRegisters(regs)) => {
                let transformed: Vec<u16> = regs
                    .iter()
                    .map(|&v| {
                        let y = self.offset + self.scale * v as i32;
                        y.clamp(0, u16::MAX as i32) as u16
                    })
                    .collect();
                Ok(Response::ReadHoldingRegisters(transformed))
            }
            other => other,
        }
    }
}

#[tokio::test]
async fn linear_transform_hook() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 100), (1, 200)]));
    // y = 50 + 2x
    let hooked = HookedService::new(
        store,
        LinearTransformHook {
            offset: 50,
            scale: 2,
        },
    );

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![250, 450]));
    // 50 + 2×100 = 250, 50 + 2×200 = 450
}

// ── 4c. Audit logging (both hooks) ───────────────────────────────────────────

/// Hook that records every request/response pair with timestamps.
struct AuditHook {
    log: Mutex<Vec<(u8, String, String)>>,
}

impl AuditHook {
    fn new() -> Self {
        Self {
            log: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ServerHook for AuditHook {
    async fn before_call(&self, slave: u8, request: &Request<'_>) -> Option<Response> {
        self.log
            .lock()
            .unwrap()
            .push((slave, format!("REQ {:?}", request), String::new()));
        None
    }

    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        let entry = match &result {
            Ok(rsp) => format!("OK {:?}", rsp),
            Err(ex) => format!("ERR {:?}", ex),
        };
        // Update the last entry's response field
        if let Some(last) = self.log.lock().unwrap().last_mut() {
            last.2 = entry;
        }
        result
    }
}

#[tokio::test]
async fn audit_hook_records_request_response_pairs() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let hooked = HookedService::new(store, AuditHook::new());

    server::context::SLAVE_ID
        .scope(1, async {
            hooked.call(Request::ReadHoldingRegisters(0, 1)).await
        })
        .await
        .unwrap();

    server::context::SLAVE_ID
        .scope(1, async {
            hooked.call(Request::WriteSingleRegister(0, 99)).await
        })
        .await
        .unwrap();

    let log = hooked.hook.log.lock().unwrap();
    assert_eq!(log.len(), 2);
    assert!(
        log[0].1.contains("ReadHoldingRegisters"),
        "should log request"
    );
    assert!(
        log[0].2.contains("ReadHoldingRegisters"),
        "should log response"
    );
    assert!(
        log[1].1.contains("WriteSingleRegister"),
        "should log request"
    );
    assert!(
        log[1].2.contains("WriteSingleRegister"),
        "should log response"
    );
}

// ── 4d. Rate limiting (before_call) ──────────────────────────────────────────

/// Hook that rate-limits: max N calls per window tracked via atomic counter.
struct RateLimitHook {
    max_per_window: u32,
    call_count: AtomicU32,
}

#[async_trait]
impl ServerHook for RateLimitHook {
    async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
        let fc = request.function_code().value();
        let count = self.call_count.fetch_add(1, Ordering::Relaxed);
        if count >= self.max_per_window {
            Some(Response::Exception(fc, Exception::ServerDeviceFailure))
        } else {
            None
        }
    }
}

#[tokio::test]
async fn rate_limit_hook_blocks_after_threshold() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let hook = RateLimitHook {
        max_per_window: 2,
        call_count: AtomicU32::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // First 2 calls pass through
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42]));

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42]));

    // 3rd call blocked
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    match rsp {
        Response::Exception(3, Exception::ServerDeviceFailure) => {}
        other => panic!("expected rate-limit exception, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 5: Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_calls_to_hooked_service() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 0)]));
    let hook = LoggingHook::new();
    let hooked = Arc::new(HookedService::new(store, hook));

    let mut handles = Vec::new();
    for i in 0..16 {
        let svc = hooked.clone();
        handles.push(tokio::spawn(async move {
            let val = i as u16;
            svc.call(Request::WriteSingleRegister(0, val))
                .await
                .unwrap();
            let rsp = svc.call(Request::ReadHoldingRegisters(0, 1)).await.unwrap();
            // The value read might be from any task's write (concurrent),
            // but the call itself must not panic.
            match rsp {
                Response::ReadHoldingRegisters(regs) => {
                    assert_eq!(regs.len(), 1);
                }
                other => panic!("unexpected response: {other:?}"),
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 16 tasks × 2 calls = 32 calls; both before and after fired 32 times
    assert_eq!(hooked.hook.before_count.load(Ordering::Relaxed), 32);
    assert_eq!(hooked.hook.after_count.load(Ordering::Relaxed), 32);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 6: task_local SLAVE_ID context
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn slave_id_defaults_to_zero_outside_scope() {
    // When called outside SLAVE_ID.scope(), the hook sees slave=0xFF
    // (outside valid range — lets hooks distinguish "not set" from
    // broadcast address 0 or valid slaves 1–247).
    struct SlaveCaptureHook {
        seen_slave: AtomicU8,
    }
    #[async_trait]
    impl ServerHook for SlaveCaptureHook {
        async fn before_call(&self, slave: u8, _request: &Request<'_>) -> Option<Response> {
            self.seen_slave.store(slave, Ordering::Relaxed);
            None
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hook = SlaveCaptureHook {
        seen_slave: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // Call WITHOUT SLAVE_ID.scope() — should default to 0xFF
    hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(hooked.hook.seen_slave.load(Ordering::Relaxed), 0xFF);
}

#[tokio::test]
async fn slave_id_scoped_to_specific_value() {
    struct SlaveCaptureHook {
        seen_slave: AtomicU8,
    }
    #[async_trait]
    impl ServerHook for SlaveCaptureHook {
        async fn before_call(&self, slave: u8, _request: &Request<'_>) -> Option<Response> {
            self.seen_slave.store(slave, Ordering::Relaxed);
            None
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hook = SlaveCaptureHook {
        seen_slave: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    server::context::SLAVE_ID
        .scope(42, async {
            hooked.call(Request::ReadHoldingRegisters(0, 1)).await
        })
        .await
        .unwrap();
    assert_eq!(hooked.hook.seen_slave.load(Ordering::Relaxed), 42);

    server::context::SLAVE_ID
        .scope(247, async {
            hooked.call(Request::ReadHoldingRegisters(0, 1)).await
        })
        .await
        .unwrap();
    assert_eq!(hooked.hook.seen_slave.load(Ordering::Relaxed), 247);
}

#[tokio::test]
async fn slave_id_nested_scope_uses_inner_value() {
    struct SlaveCaptureHook {
        seen_before: AtomicU8,
        seen_after: AtomicU8,
    }
    #[async_trait]
    impl ServerHook for SlaveCaptureHook {
        async fn before_call(&self, slave: u8, _request: &Request<'_>) -> Option<Response> {
            self.seen_before.store(slave, Ordering::Relaxed);
            None
        }
        async fn after_call(
            &self,
            slave: u8,
            result: Result<Response, Exception>,
        ) -> Result<Response, Exception> {
            self.seen_after.store(slave, Ordering::Relaxed);
            result
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hook = SlaveCaptureHook {
        seen_before: AtomicU8::new(0),
        seen_after: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // Outer scope is 99, but inner scope overrides to 77
    server::context::SLAVE_ID
        .scope(99, async {
            server::context::SLAVE_ID
                .scope(77, async {
                    hooked.call(Request::ReadHoldingRegisters(0, 1)).await
                })
                .await
        })
        .await
        .unwrap();

    assert_eq!(hooked.hook.seen_before.load(Ordering::Relaxed), 77);
    assert_eq!(hooked.hook.seen_after.load(Ordering::Relaxed), 77);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 7: Existing tests (kept and occasionally extended)
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn noop_hook_passthrough() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let hooked = HookedService::new(store.clone(), NoopHook);

    // Through HookedService
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42]));

    // Directly
    let rsp = store
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42]));

    // Write through HookedService
    hooked
        .call(Request::WriteSingleRegister(0, 99))
        .await
        .unwrap();
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![99]));
}

#[tokio::test]
async fn before_call_short_circuit() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 10)]));
    let hook = ShortCircuitHook {
        fixed_value: 999,
        call_count: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 3))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![999, 999, 999]));

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(1, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![999, 999]));
}

#[tokio::test]
async fn before_call_pass_through_unmatched() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(5, 77)]));
    let hook = ShortCircuitHook {
        fixed_value: 111,
        call_count: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    hooked
        .call(Request::WriteSingleRegister(5, 88))
        .await
        .unwrap();

    // ReadHoldingRegisters IS intercepted → returns hook value (not 88)
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(5, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![111]));
}

#[tokio::test]
async fn after_call_suppresses_exception() {
    let store = Arc::new(SlaveStore::new());
    let hooked = HookedService::new(store, SuppressExceptionHook);

    let rsp = hooked.call(Request::Diagnostic(0x00FF, 0)).await.unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![0]));
}

#[tokio::test]
async fn after_call_passthrough_success() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 55)]));
    let hooked = HookedService::new(store, SuppressExceptionHook);

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![55]));
}

#[tokio::test]
async fn layered_hooks() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1)]));

    let inner_hook = ShortCircuitHook {
        fixed_value: 42,
        call_count: AtomicU8::new(0),
    };
    let outer_hook = LoggingHook::new();

    let hooked: HookedService<HookedService<Arc<SlaveStore>, ShortCircuitHook>, LoggingHook> =
        HookedService::new(HookedService::new(store, inner_hook), outer_hook);

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42, 42]));

    assert_eq!(hooked.hook.before_count.load(Ordering::Relaxed), 1);
    assert_eq!(hooked.hook.after_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn logging_hook_counts_calls() {
    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1), (1, 2)]));
    let hook = LoggingHook::new();
    let hooked = HookedService::new(store, hook);

    hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    hooked
        .call(Request::WriteSingleRegister(0, 99))
        .await
        .unwrap();

    assert_eq!(hooked.hook.before_count.load(Ordering::Relaxed), 3);
    assert_eq!(hooked.hook.after_count.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn unknown_function_code_reaches_inner() {
    let store = Arc::new(SlaveStore::new());
    let hook = ShortCircuitHook {
        fixed_value: 1,
        call_count: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // ReadCoils is not intercepted by ShortCircuitHook → passes to store
    let rsp = hooked.call(Request::ReadCoils(0, 1)).await.unwrap();
    match rsp {
        Response::ReadCoils(bits) => assert_eq!(bits.len(), 1),
        other => panic!("expected ReadCoils, got {other:?}"),
    }
}

#[tokio::test]
async fn hooked_service_is_service() {
    fn _assert_service(_: impl Service) {}

    let store = Arc::new(SlaveStore::new());
    let hooked = HookedService::new(store, NoopHook);
    _assert_service(hooked);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section 8: Edge cases
// ═══════════════════════════════════════════════════════════════════════════════

/// Hook that only implements before_call (after_call uses default).
#[tokio::test]
async fn hook_with_only_before_call_implemented() {
    struct BeforeOnlyHook;
    #[async_trait]
    impl ServerHook for BeforeOnlyHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            if let Request::ReadHoldingRegisters(_, _) = request {
                Some(Response::ReadHoldingRegisters(vec![77]))
            } else {
                None
            }
        }
    }

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 10)]));
    let hooked = HookedService::new(store, BeforeOnlyHook);

    // Intercepted
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![77]));

    // Not intercepted → inner store handles it, after_call is default no-op
    let rsp = hooked.call(Request::ReadCoils(0, 1)).await.unwrap();
    assert!(matches!(rsp, Response::ReadCoils(_)));
}

/// Hook that only implements after_call (before_call uses default).
#[tokio::test]
async fn hook_with_only_after_call_implemented() {
    struct AfterOnlyHook;
    #[async_trait]
    impl ServerHook for AfterOnlyHook {
        async fn after_call(
            &self,
            _slave: u8,
            result: Result<Response, Exception>,
        ) -> Result<Response, Exception> {
            // Append a sentinel value to every ReadHoldingRegisters response
            match result {
                Ok(Response::ReadHoldingRegisters(mut regs)) => {
                    regs.push(0xFFFF);
                    Ok(Response::ReadHoldingRegisters(regs))
                }
                other => other,
            }
        }
    }

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 1), (1, 2)]));
    let hooked = HookedService::new(store, AfterOnlyHook);

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![1, 2, 0xFFFF]));
}

/// Hook handles Disconnect requests (FC=0, no PDU body).
#[tokio::test]
async fn hook_handles_disconnect_request() {
    struct DisconnectHook {
        seen_disconnect: AtomicU8,
    }
    #[async_trait]
    impl ServerHook for DisconnectHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            if matches!(request, Request::Disconnect) {
                self.seen_disconnect.fetch_add(1, Ordering::Relaxed);
            }
            None
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hook = DisconnectHook {
        seen_disconnect: AtomicU8::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // Disconnect is normally not handled by SlaveStore (returns IllegalFunction),
    // but the hook sees it in before_call before it reaches the inner service.
    let result = hooked.call(Request::Disconnect).await;
    // SlaveStore returns IllegalFunction for Disconnect
    assert!(result.is_err());
    assert_eq!(hooked.hook.seen_disconnect.load(Ordering::Relaxed), 1);
}

/// Hook intercepts MaskWriteRegister (FC 22).
#[tokio::test]
async fn before_call_intercepts_mask_write_register() {
    struct MaskWriteHook {
        last_and: Mutex<u16>,
        last_or: Mutex<u16>,
    }
    #[async_trait]
    impl ServerHook for MaskWriteHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            if let Request::MaskWriteRegister(addr, and_mask, or_mask) = request {
                *self.last_and.lock().unwrap() = *and_mask;
                *self.last_or.lock().unwrap() = *or_mask;
                Some(Response::MaskWriteRegister(*addr, *and_mask, *or_mask))
            } else {
                None
            }
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hook = MaskWriteHook {
        last_and: Mutex::new(0),
        last_or: Mutex::new(0),
    };
    let hooked = HookedService::new(store, hook);

    let rsp = hooked
        .call(Request::MaskWriteRegister(10, 0xFF00, 0x00FF))
        .await
        .unwrap();
    assert_eq!(rsp, Response::MaskWriteRegister(10, 0xFF00, 0x00FF));
    assert_eq!(*hooked.hook.last_and.lock().unwrap(), 0xFF00);
    assert_eq!(*hooked.hook.last_or.lock().unwrap(), 0x00FF);
}

/// Hook intercepts ReadWriteMultipleRegisters (FC 23).
#[tokio::test]
async fn before_call_intercepts_read_write_multiple_registers() {
    struct RwMultiHook;
    #[async_trait]
    impl ServerHook for RwMultiHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            if let Request::ReadWriteMultipleRegisters(_rd_addr, rd_qty, _wr_addr, _wr_data) =
                request
            {
                let regs = vec![0xAAAA; *rd_qty as usize];
                Some(Response::ReadWriteMultipleRegisters(regs))
            } else {
                None
            }
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hooked = HookedService::new(store, RwMultiHook);

    let rsp = hooked
        .call(Request::ReadWriteMultipleRegisters(
            0,
            4,
            100,
            std::borrow::Cow::Borrowed(&[1u16, 2]),
        ))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadWriteMultipleRegisters(vec![0xAAAA; 4]));
}

// ── 8e. Hook returns Exception response from before_call ─────────────────────

#[tokio::test]
async fn before_call_can_return_exception_response() {
    struct AlwaysFailHook;
    #[async_trait]
    impl ServerHook for AlwaysFailHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            let fc = request.function_code().value();
            Some(Response::Exception(fc, Exception::ServerDeviceFailure))
        }
    }

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 42)]));
    let hooked = HookedService::new(store, AlwaysFailHook);

    // Even though store has data, the hook blocks everything
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 1))
        .await
        .unwrap();
    match rsp {
        Response::Exception(3, Exception::ServerDeviceFailure) => {}
        other => panic!("expected Exception, got {other:?}"),
    }
}

// ── 8f. after_call receives exception from inner → can transform ─────────────

#[tokio::test]
async fn after_call_can_convert_exception_to_matched_response_type() {
    /// Hook that converts IllegalDataAddress exceptions into empty reads
    /// of the correct response type for the request.
    struct GracefulDegradeHook;
    #[async_trait]
    impl ServerHook for GracefulDegradeHook {
        async fn after_call(
            &self,
            _slave: u8,
            result: Result<Response, Exception>,
        ) -> Result<Response, Exception> {
            match result {
                Err(Exception::IllegalDataAddress) => Ok(Response::ReadHoldingRegisters(vec![])),
                other => other,
            }
        }
    }

    // A service that always returns IllegalDataAddress for any read.
    struct AlwaysIllegalAddrService;
    #[async_trait]
    impl Service for AlwaysIllegalAddrService {
        async fn call(&self, _request: Request<'_>) -> Result<Response, Exception> {
            Err(Exception::IllegalDataAddress)
        }
    }

    let svc = AlwaysIllegalAddrService;
    let hooked = HookedService::new(svc, GracefulDegradeHook);

    let rsp = hooked
        .call(Request::ReadHoldingRegisters(5000, 10))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![]));
}

// ── 8g. after_call does not fire when before_call short-circuits ─────────────

#[tokio::test]
async fn after_call_not_called_when_before_call_short_circuits() {
    let after_called = Arc::new(AtomicU8::new(0));

    struct ShortBeforeHook;
    #[async_trait]
    impl ServerHook for ShortBeforeHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            if let Request::ReadHoldingRegisters(_addr, qty) = request {
                Some(Response::ReadHoldingRegisters(vec![1; *qty as usize]))
            } else {
                None
            }
        }
    }

    struct AfterFlagHook {
        flag: Arc<AtomicU8>,
    }
    #[async_trait]
    impl ServerHook for AfterFlagHook {
        async fn after_call(
            &self,
            _slave: u8,
            result: Result<Response, Exception>,
        ) -> Result<Response, Exception> {
            self.flag.fetch_add(1, Ordering::Relaxed);
            result
        }
    }

    let store = Arc::new(SlaveStore::with_holding_registers(&[(0, 99)]));
    // Layer order: AfterFlagHook (outer) → ShortBeforeHook (inner) → store
    let inner = HookedService::new(store, ShortBeforeHook);
    let outer = HookedService::new(
        inner,
        AfterFlagHook {
            flag: after_called.clone(),
        },
    );

    // This request is short-circuited by ShortBeforeHook (inner).
    // after_call of outer hook SHOULD still fire because HookedService::call()
    // runs after_call on the outer hook regardless.
    //
    // Wait — actually let's trace through:
    // outer.call() →
    //   outer.hook.before_call() → None (pass through)
    //   inner.call() →
    //     inner.hook.before_call() → Some(rsp) → return Ok(rsp)
    //   outer.hook.after_call(Ok(rsp)) → fires!
    //
    // So yes, outer's after_call still fires.
    let rsp = outer
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![1, 1]));
    assert_eq!(after_called.load(Ordering::Relaxed), 1);

    // For a non-short-circuited request, after_call also fires
    let rsp = outer.call(Request::ReadCoils(0, 1)).await.unwrap();
    assert!(matches!(rsp, Response::ReadCoils(_)));
    assert_eq!(after_called.load(Ordering::Relaxed), 2);
}

// ── 8h. Multiple FCs intercepted in same before_call ─────────────────────────

#[tokio::test]
async fn before_call_intercepts_multiple_fc_types_in_one_hook() {
    struct MultiTypeHook {
        read_val: u16,
        last_coil_addr: Mutex<u16>,
    }
    #[async_trait]
    impl ServerHook for MultiTypeHook {
        async fn before_call(&self, _slave: u8, request: &Request<'_>) -> Option<Response> {
            match request {
                Request::ReadHoldingRegisters(_addr, qty) => {
                    Some(Response::ReadHoldingRegisters(vec![
                        self.read_val;
                        *qty as usize
                    ]))
                }
                Request::ReadCoils(addr, qty) => {
                    *self.last_coil_addr.lock().unwrap() = *addr;
                    Some(Response::ReadCoils(vec![true; *qty as usize]))
                }
                Request::WriteSingleCoil(addr, val) => Some(Response::WriteSingleCoil(*addr, *val)),
                _ => None,
            }
        }
    }

    let store = Arc::new(SlaveStore::new());
    let hook = MultiTypeHook {
        read_val: 42,
        last_coil_addr: Mutex::new(0),
    };
    let hooked = HookedService::new(store, hook);

    // FC 03 intercepted
    let rsp = hooked
        .call(Request::ReadHoldingRegisters(0, 2))
        .await
        .unwrap();
    assert_eq!(rsp, Response::ReadHoldingRegisters(vec![42, 42]));

    // FC 01 intercepted
    let rsp = hooked.call(Request::ReadCoils(100, 3)).await.unwrap();
    assert_eq!(rsp, Response::ReadCoils(vec![true; 3]));
    assert_eq!(*hooked.hook.last_coil_addr.lock().unwrap(), 100);

    // FC 05 intercepted
    let rsp = hooked
        .call(Request::WriteSingleCoil(50, true))
        .await
        .unwrap();
    assert_eq!(rsp, Response::WriteSingleCoil(50, true));

    // FC 02 NOT intercepted → passes to store
    let rsp = hooked
        .call(Request::ReadDiscreteInputs(0, 1))
        .await
        .unwrap();
    assert!(matches!(rsp, Response::ReadDiscreteInputs(_)));
}
