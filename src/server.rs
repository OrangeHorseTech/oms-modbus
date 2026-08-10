// SPDX-License-Identifier: MIT OR Apache-2.0
//! Modbus server (slave) service trait.

use async_trait::async_trait;

use crate::frame::{Exception, Request, Response};

/// A Modbus server service — handles incoming requests and produces responses.
///
/// Implement this trait to create a custom Modbus server. The transport loops
/// (TCP, RTU, ASCII) call [`call`](Service::call) for each decoded request.
///
/// For hooking into the request/response lifecycle without reimplementing the
/// entire trait, see [`ServerHook`] and [`HookedService`].
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use oms_modbus::frame::{Request, Response, Exception};
/// use oms_modbus::server::Service;
///
/// /// A fixed-value service — always returns the same register value.
/// struct FixedService { value: u16 }
///
/// #[async_trait]
/// impl Service for FixedService {
///     async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
///         match request {
///             Request::ReadHoldingRegisters(_addr, qty) =>
///                 Ok(Response::ReadHoldingRegisters(vec![self.value; qty as usize])),
///             _ => Err(Exception::IllegalFunction),
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Service: Send + Sync + 'static {
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception>;
}

// ── Server hook support ─────────────────────────────────────────────────────

/// Thread-local request context — set by `process_server_request` before
/// calling [`Service::call`]. Hooks use this to see which slave the request
/// is addressed to without changing the `Service` trait signature.
#[doc(hidden)]
pub mod context {
    tokio::task_local! {
        pub static SLAVE_ID: u8;
    }
}

/// A hook that intercepts server requests — compose with [`HookedService`].
///
/// All methods have default no-op implementations. Implement only the hooks
/// you need.
///
/// # Examples
///
/// ```no_run
/// use oms_modbus::*;
/// use async_trait::async_trait;
///
/// // Log every request/response pair
/// struct LogHook;
/// #[async_trait]
/// impl ServerHook for LogHook {
///     async fn after_call(
///         &self, slave: u8,
///         result: Result<Response, Exception>,
///     ) -> Result<Response, Exception> {
///         println!("[slave={slave}] {:?}", result);
///         result
///     }
/// }
/// ```
#[async_trait]
pub trait ServerHook: Send + Sync + 'static {
    /// Called before the inner service. Return `Some(response)` to skip the
    /// inner service entirely (short-circuit). Return `None` to proceed.
    async fn before_call(&self, _slave: u8, _request: &Request<'_>) -> Option<Response> {
        None
    }

    /// Called after the inner service produces a result. Transform or replace
    /// the response before it is sent to the client.
    async fn after_call(
        &self,
        _slave: u8,
        result: Result<Response, Exception>,
    ) -> Result<Response, Exception> {
        result
    }
}

/// A [`Service`] wrapper that applies a [`ServerHook`] around another service.
///
/// # Examples
///
/// ```no_run
/// use oms_modbus::*;
/// use std::sync::Arc;
///
/// # async fn example() {
/// let store = Arc::new(SlaveStore::new());
/// # struct DummyHook; #[async_trait::async_trait] impl ServerHook for DummyHook {}
/// let hook = DummyHook;
/// let hooked = HookedService::new(store, hook);
/// // Pass `hooked` to any serve_forever() — it implements Service.
/// # }
/// ```
#[derive(Clone)]
pub struct HookedService<S, H> {
    pub inner: S,
    pub hook: H,
}

impl<S, H> HookedService<S, H> {
    /// Wrap a service with a hook.
    pub fn new(inner: S, hook: H) -> Self {
        Self { inner, hook }
    }
}

#[async_trait]
impl<S, H> Service for HookedService<S, H>
where
    S: Service,
    H: ServerHook,
{
    async fn call(&self, request: Request<'_>) -> Result<Response, Exception> {
        // Fallback to 0xFF (outside valid 0–247 range) when SLAVE_ID
        // task-local isn't set — lets hooks distinguish "not set" from
        // the broadcast address (0) or a real slave (1–247).
        let slave = context::SLAVE_ID.try_with(|&id| id).unwrap_or(0xFF);

        // 1. before_call — may short-circuit the inner service
        if let Some(rsp) = self.hook.before_call(slave, &request).await {
            return Ok(rsp);
        }

        // 2. Call the inner service
        let result = self.inner.call(request).await;

        // 3. after_call — transform or pass through
        self.hook.after_call(slave, result).await
    }
}
