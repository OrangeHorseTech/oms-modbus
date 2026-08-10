# Changelog

## [0.2.0] — 2026-08-10

### Preview Release

This is the first public preview of OMS Modbus. The API may change based on
user feedback.

### Added

- **TCP Client & Server** — MBAP framing, configurable `TcpConfig` (TID mode,
  unit ID placement, length mode), gateway mode preset
- **RTU Client & Server** — CRC-16 validation, generic transport
  (`AsyncRead + AsyncWrite`), bus timing
- **ASCII Client & Server** — LRC validation, generic transport, hex framing
- **BusCapture** — unified `WireTap` implementation with recording ring buffer
  and traffic statistics
- **SniffIo<T>** — transparent I/O proxy for WireTap attachment, zero-overhead
  when disabled (`tap = None`)
- **BusTiming** — spec-compliant RS-485 frame spacing (3.5T silence, 1.75 ms
  floor above 19200 baud)
- **Auto-Reconnect** — fixed-interval reconnect for all transports
  (TCP: address-based; RTU/ASCII: factory closure for USB serial resilience)
- **11 Function Codes** — FC01–06, FC08, FC15–16, FC22–23 with diagnostic
  sub-functions
- **ServerHook** — middleware for custom server behavior
- **Service trait** — implement custom backends (hardware bridge, access
  control, value transformation)
- **ChannelRecorder + RecordSink** — async dispatch for custom recording
  backends
- **FileRecorder** — non-blocking file recording with ISO 8601 timestamps
- **SlaveStore** — in-memory register/coil store with O(1) indexed access
- **ModbusError** — structured error handling with `Display`, `detail()`,
  and machine-readable `label()`

### Design

- Zero `.unwrap()` in production code — poison-safe mutexes, checked arithmetic
- All three protocols share the same client/server API patterns
- No feature gates — TCP, RTU, ASCII, capture, reconnect always compile
- Compiler-verified README code blocks via `tests/readme_examples.rs`
