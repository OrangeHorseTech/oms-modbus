// SPDX-License-Identifier: MIT OR Apache-2.0
//!
//! Modbus error types and message constants.

use std::fmt;

// ── ModbusError label strings (used by label(), Display, detail()) ────

pub const ERR_OK: &str = "OK";
pub const ERR_TIMEOUT: &str = "TIMEOUT";
pub const ERR_CONNECTION: &str = "CONNECTION ERROR";
pub const ERR_PROTOCOL: &str = "PROTOCOL ERROR";
pub const ERR_EXCEPTION: &str = "MODBUS EXCEPTION";
pub const ERR_SERIAL: &str = "PORT ERROR";
pub const ERR_OTHER: &str = "ERROR";

// ── Diagnostic prefixes (used by detail()) ──────────────────────────────

pub const TIMEOUT_PREFIX: &str = "TIMEOUT:";
pub const CONNECTION_PREFIX: &str = "CONNECTION ERROR:";
pub const PROTOCOL_PREFIX: &str = "PROTOCOL ERROR:";
pub const PORT_PREFIX: &str = "PORT ERROR:";

// ── Exception names ─────────────────────────────────────────────────────

pub const EXN_ILLEGAL_FUNCTION: &str = "Illegal Function";
pub const EXN_ILLEGAL_DATA_ADDRESS: &str = "Illegal Data Address";
pub const EXN_ILLEGAL_DATA_VALUE: &str = "Illegal Data Value";
pub const EXN_SERVER_DEVICE_FAILURE: &str = "Server Device Failure";
pub const EXN_ACKNOWLEDGE: &str = "Acknowledge";
pub const EXN_SERVER_DEVICE_BUSY: &str = "Server Device Busy";
pub const EXN_NEGATIVE_ACKNOWLEDGE: &str = "Negative Acknowledge";
pub const EXN_MEMORY_PARITY_ERROR: &str = "Memory Parity Error";
pub const EXN_GATEWAY_PATH_UNAVAILABLE: &str = "Gateway Path Unavailable";
pub const EXN_GATEWAY_TARGET_FAILED: &str = "Gateway Target Device Failed to Respond";
pub const EXN_UNKNOWN: &str = "Unknown Exception";

// ── Transport messages (used by TransportOps) ───────────────────────────

/// Generic send timeout — used by shared `send_frame` across all transports.
pub const SEND_TIMEOUT: &str = "send timed out";
/// Generic receive timeout — used by shared `read_at_least` across all transports.
pub const RECV_TIMEOUT: &str = "recv timed out";

pub const TCP_SEND_TIMEOUT: &str = "TCP send timed out";
pub const TCP_RECV_TIMEOUT: &str = "TCP recv timed out";
pub const TCP_SEND_ERROR: &str = "TCP send:";
pub const TCP_RECV_ERROR: &str = "TCP recv:";
pub const CONN_CLOSED: &str = "connection closed";
pub const TCP_EMPTY_RESP: &str = "empty TCP response PDU";

pub const RTU_SEND_TIMEOUT: &str = "RTU send timed out";
pub const RTU_RECV_TIMEOUT: &str = "RTU recv timed out";
pub const RTU_SEND_ERROR: &str = "RTU send:";
pub const RTU_RECV_ERROR: &str = "RTU recv:";
pub const RTU_EMPTY_RESP: &str = "empty RTU response";

pub const ASCII_SEND_TIMEOUT: &str = "ASCII send timed out";
pub const ASCII_RECV_TIMEOUT: &str = "ASCII recv timed out";
pub const ASCII_SEND_ERROR: &str = "ASCII send:";
pub const ASCII_RECV_ERROR: &str = "ASCII recv:";
pub const ASCII_EMPTY_RESP: &str = "empty ASCII response";

pub const PDU_ENCODE_ERROR: &str = "PDU encode:";
pub const PDU_DECODE_ERROR: &str = "PDU decode:";
pub const SLAVE_ID_MISMATCH: &str = "slave ID mismatch: expected";

/// Structured Modbus / transport error — no string parsing needed.
///
/// Use `Display` for short UI messages and
/// [`detail`](ModbusError::detail) for full diagnostic logs.
///
/// # Short label for UI
///
/// Use [`label`](ModbusError::label) for a short human-readable tag
/// suitable for display in a status bar or error column.
///
/// # Example
///
/// ```
/// use oms_modbus::ModbusError;
///
/// // Match on specific error kinds
/// let result: Result<Vec<u16>, ModbusError> = Err(ModbusError::timeout("RTU recv timed out"));
///
/// match &result {
///     Err(ModbusError::Timeout(_)) => println!("Retry or re-connect"),
///     Err(ModbusError::Exception { function: _, code: _ }) => println!("Modbus exception"),
///     Err(e) => eprintln!("{} — {}", e.label(), e.detail()),
///     Ok(_) => {}
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ModbusError {
    /// No error — normal/expected response.
    NoError,
    Timeout(String),
    Connection(String),
    Protocol(String),
    Exception {
        function: u8,
        code: u8,
    },
    Serial(String),
    Other(String),
}

/// Human-readable description for a Modbus exception code.
fn exception_name(code: u8) -> &'static str {
    match code {
        1 => EXN_ILLEGAL_FUNCTION,
        2 => EXN_ILLEGAL_DATA_ADDRESS,
        3 => EXN_ILLEGAL_DATA_VALUE,
        4 => EXN_SERVER_DEVICE_FAILURE,
        5 => EXN_ACKNOWLEDGE,
        6 => EXN_SERVER_DEVICE_BUSY,
        7 => EXN_NEGATIVE_ACKNOWLEDGE,
        8 => EXN_MEMORY_PARITY_ERROR,
        10 => EXN_GATEWAY_PATH_UNAVAILABLE,
        11 => EXN_GATEWAY_TARGET_FAILED,
        _ => EXN_UNKNOWN,
    }
}

impl fmt::Display for ModbusError {
    /// Short summary for UI display. Delegates to [`label`](ModbusError::label)
    /// for simple variants; adds context for `Exception` and `Other`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModbusError::Exception { function: _, code } => {
                write!(f, "{} (code={})", exception_name(*code), code)
            }
            ModbusError::Other(m) => write!(f, "{m}"),
            _ => write!(f, "{}", self.label()),
        }
    }
}

impl ModbusError {
    /// Full diagnostic message using centralized message prefixes.
    /// Use this for log output; `Display` gives a short summary for UI.
    pub fn detail(&self) -> String {
        match self {
            ModbusError::NoError => "OK".to_string(),
            ModbusError::Timeout(m) => format!("{TIMEOUT_PREFIX} {m}"),
            ModbusError::Connection(m) => format!("{CONNECTION_PREFIX} {m}"),
            ModbusError::Protocol(m) => format!("{PROTOCOL_PREFIX} {m}"),
            ModbusError::Exception { function, code } => {
                let ex_name = exception_name(*code);
                format!("{ex_name} (FC={function}, code={code})")
            }
            ModbusError::Serial(m) => format!("{PORT_PREFIX} {m}"),
            ModbusError::Other(m) => m.clone(),
        }
    }

    /// Short label for UI display (status bar, error column).
    pub fn label(&self) -> &'static str {
        match self {
            ModbusError::NoError => ERR_OK,
            ModbusError::Timeout(_) => ERR_TIMEOUT,
            ModbusError::Connection(_) => ERR_CONNECTION,
            ModbusError::Protocol(_) => ERR_PROTOCOL,
            ModbusError::Exception { .. } => ERR_EXCEPTION,
            ModbusError::Serial(_) => ERR_SERIAL,
            ModbusError::Other(_) => ERR_OTHER,
        }
    }

    /// Create a successful (no-error) result. `label()` returns `"OK"`.
    pub const fn no_error() -> Self {
        ModbusError::NoError
    }
    /// Create a timeout error. Covers send and recv timeouts.
    pub fn timeout(msg: impl Into<String>) -> Self {
        ModbusError::Timeout(msg.into())
    }
    /// Create a connection error. Covers TCP connect/read/write failures.
    pub fn connection(msg: impl Into<String>) -> Self {
        ModbusError::Connection(msg.into())
    }
    /// Create a protocol error. Covers CRC mismatch, slave ID mismatch, invalid PDU.
    pub fn protocol(msg: impl Into<String>) -> Self {
        ModbusError::Protocol(msg.into())
    }
    /// Create a Modbus exception error from a server response.
    pub fn exception(function: u8, code: u8) -> Self {
        ModbusError::Exception { function, code }
    }
    /// Create a serial port error. Covers port open/read/write failures.
    pub fn serial(msg: impl Into<String>) -> Self {
        ModbusError::Serial(msg.into())
    }
    /// Create a miscellaneous error. Used for uncategorized failures.
    pub fn other(msg: impl Into<String>) -> Self {
        ModbusError::Other(msg.into())
    }
}

impl From<String> for ModbusError {
    fn from(s: String) -> Self {
        ModbusError::Other(s)
    }
}

impl From<std::io::Error> for ModbusError {
    fn from(e: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match e.kind() {
            ErrorKind::TimedOut | ErrorKind::WouldBlock => ModbusError::timeout(e.to_string()),
            ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected => ModbusError::connection(e.to_string()),
            _ => ModbusError::Other(e.to_string()),
        }
    }
}

impl std::error::Error for ModbusError {}
