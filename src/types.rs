use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

// TokenMetadata only contains information about the token itself
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenMetadata {
    // Temporarily comment out the serde attribute until we fix the alloy::serde issue
    // #[serde(with = "alloy::serde::address")]
    pub address: Address,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
}

impl Default for TokenMetadata {
    fn default() -> Self {
        Self {
            address: Address::ZERO,
            name: None,
            symbol: None,
            decimals: None,
        }
    }
}

// PoolMetadata contains information about a pool including its tokens
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PoolMetadata {
    // Temporarily comment out the serde attributes until we fix the alloy::serde issue
    // #[serde(with = "alloy::serde::address")]
    pub address: Address,
    // #[serde(with = "alloy::serde::address")]
    pub token0: Address,
    // #[serde(with = "alloy::serde::address")]
    pub token1: Address,
}

impl Default for PoolMetadata {
    fn default() -> Self {
        Self {
            address: Address::ZERO,
            token0: Address::ZERO,
            token1: Address::ZERO,
        }
    }
}