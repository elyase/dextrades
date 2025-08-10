use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyErr;

/// Unified error type for the Dextrades library
#[derive(Debug, thiserror::Error)]
pub enum DextradesError {
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("RPC error: {0}")]
    Rpc(#[from] eyre::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Parsing error: {0}")]
    Parsing(String),

    #[error("Stream error: {0}")]
    Stream(String),
}

impl From<DextradesError> for PyErr {
    fn from(err: DextradesError) -> Self {
        match err {
            DextradesError::InvalidAddress(msg) => PyValueError::new_err(msg),
            DextradesError::Config(msg) => PyValueError::new_err(msg),
            DextradesError::Parsing(msg) => PyValueError::new_err(msg),
            DextradesError::Rpc(e) => PyRuntimeError::new_err(e.to_string()),
            DextradesError::Protocol(msg) => PyRuntimeError::new_err(msg),
            DextradesError::Stream(msg) => PyRuntimeError::new_err(msg),
        }
    }
}

// Enhanced error conversion with custom exception classes
impl DextradesError {
    pub fn to_py_err(self) -> PyErr {
        match self {
            DextradesError::InvalidAddress(msg) => crate::InvalidAddressError::new_err(msg),
            DextradesError::Config(msg) => crate::ConfigError::new_err(msg),
            DextradesError::Parsing(msg) => crate::ParsingError::new_err(msg),
            DextradesError::Rpc(e) => crate::RpcError::new_err(e.to_string()),
            DextradesError::Protocol(msg) => crate::ProtocolError::new_err(msg),
            DextradesError::Stream(msg) => crate::StreamError::new_err(msg),
        }
    }
}

/// Result type alias for convenience
pub type DextradesResult<T> = Result<T, DextradesError>;

/// Helper function to parse addresses with consistent error handling
pub fn parse_address(address: &str) -> DextradesResult<alloy::primitives::Address> {
    use std::str::FromStr;
    alloy::primitives::Address::from_str(address)
        .map_err(|_| DextradesError::InvalidAddress(format!("Invalid address format: {}", address)))
}
