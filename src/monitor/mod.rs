// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Recording backends — in-memory, file, channel, and user-extensible.
//!
//! Recording modes:
//! 1. RingBufferCapture — in-memory ring buffer (bounded or unbounded)
//! 2. ChannelRecorder    — push to a dedicated async task via mpsc
//! 3. RecordSink         — user-implementable trait for custom backends
//!
//! Built-in sinks:
//! - FileRecorder — writes hex-dump logs to file (implements RecordSink)

pub mod channel;
pub mod file_recorder;
pub mod ring_buffer;

pub use channel::{ChannelRecorder, RecordSink, RecorderHandle};
pub use file_recorder::FileRecorder;
pub use ring_buffer::RingBufferCapture;

// Re-export so tests referencing oms_modbus::monitor::BusCapture still compile.
pub use crate::capture::BusCapture;
