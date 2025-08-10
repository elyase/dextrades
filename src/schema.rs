use arrow::datatypes::{DataType, Field, Schema};
use serde_json;
use std::collections::HashMap;
use alloy::primitives::Address;

/// Core swap event data structure
/// Contains only the minimal fields extracted directly from logs
/// All other data is added via enrichments
#[derive(Debug, Clone, Default)]
pub struct SwapEvent {
    // CORE FIELDS - extracted directly from logs
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u64,
    pub dex_protocol: String,
    pub pool_address: String,

    // PROTOCOL-SPECIFIC RAW DATA - varies by protocol
    pub raw_data: HashMap<String, serde_json::Value>,

    // ENRICHED FIELDS - added by enrichers
    pub enriched_fields: HashMap<String, serde_json::Value>,

    // NORMALIZED FIELDS - protocol-agnostic fields for common use cases
    pub token_in: Option<Address>,
    pub token_out: Option<Address>,
    pub amount_in: Option<String>,        // Raw amount as string
    pub amount_out: Option<String>,       // Raw amount as string
    pub amount_in_decimal: Option<f64>,   // Decimal amount
    pub amount_out_decimal: Option<f64>,  // Decimal amount

    // LEGACY FIELDS - for backward compatibility during migration
    // TODO: Remove these once migration is complete
    pub block_timestamp: Option<i64>,
    pub taker: Option<String>,
    pub recipient: Option<String>,
    pub token0_address: Option<String>,
    pub token1_address: Option<String>,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_decimals: Option<u8>,
    pub token1_decimals: Option<u8>,
    pub token_bought_address: Option<String>,
    pub token_sold_address: Option<String>,
    pub token_bought_symbol: Option<String>,
    pub token_sold_symbol: Option<String>,
    pub token_bought_amount_raw: Option<String>,
    pub token_sold_amount_raw: Option<String>,
    pub token_bought_amount: Option<f64>,
    pub token_sold_amount: Option<f64>,
    pub tx_from: Option<String>,
    pub tx_to: Option<String>,
    pub gas_used: Option<u64>,
    pub price_weth_per_token: Option<f64>,
    // USD pricing enrichment
    pub value_usd: Option<f64>,
    pub value_usd_method: Option<String>,
}

impl SwapEvent {
    /// Create a new SwapEvent with only core fields
    pub fn new(
        block_number: u64,
        tx_hash: String,
        log_index: u64,
        dex_protocol: String,
        pool_address: String,
    ) -> Self {
        Self {
            block_number,
            tx_hash,
            log_index,
            dex_protocol,
            pool_address,
            raw_data: HashMap::new(),
            enriched_fields: HashMap::new(),
            ..Default::default()
        }
    }

    /// Add raw protocol-specific data
    pub fn add_raw_data(&mut self, key: String, value: serde_json::Value) {
        self.raw_data.insert(key, value);
    }

    /// Add enriched field
    pub fn add_enriched_field(&mut self, key: String, value: serde_json::Value) {
        self.enriched_fields.insert(key, value);
    }

    /// Get enriched field as string
    pub fn get_enriched_string(&self, key: &str) -> Option<String> {
        self.enriched_fields
            .get(key)?
            .as_str()
            .map(|s| s.to_string())
    }

    /// Get enriched field as u64
    pub fn get_enriched_u64(&self, key: &str) -> Option<u64> {
        self.enriched_fields.get(key)?.as_u64()
    }

    /// Get enriched field as f64
    pub fn get_enriched_f64(&self, key: &str) -> Option<f64> {
        self.enriched_fields.get(key)?.as_f64()
    }

    /// Get enriched field as i64
    pub fn get_enriched_i64(&self, key: &str) -> Option<i64> {
        self.enriched_fields.get(key)?.as_i64()
    }

    /// Get enriched field as u8
    pub fn get_enriched_u8(&self, key: &str) -> Option<u8> {
        self.enriched_fields.get(key)?.as_u64().map(|v| v as u8)
    }

    /// Helper methods for normalized fields
    pub fn set_token_in(&mut self, token: Address) {
        self.token_in = Some(token);
    }

    pub fn set_token_out(&mut self, token: Address) {
        self.token_out = Some(token);
    }

    pub fn set_amount_in(&mut self, amount: String) {
        self.amount_in = Some(amount);
    }

    pub fn set_amount_out(&mut self, amount: String) {
        self.amount_out = Some(amount);
    }

    pub fn set_amount_in_decimal(&mut self, amount: f64) {
        self.amount_in_decimal = Some(amount);
    }

    pub fn set_amount_out_decimal(&mut self, amount: f64) {
        self.amount_out_decimal = Some(amount);
    }
}

/// Create the Arrow schema for swap events
/// This includes both core fields and common enriched fields for backward compatibility
pub fn swap_event_schema() -> Schema {
    Schema::new(vec![
        // Core fields
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::Int64, true),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("dex_protocol", DataType::Utf8, false),
        Field::new("pool_address", DataType::Utf8, false),
        // Participants
        Field::new("taker", DataType::Utf8, true),
        Field::new("recipient", DataType::Utf8, true),
        // Token metadata
        Field::new("token0_address", DataType::Utf8, true),
        Field::new("token1_address", DataType::Utf8, true),
        Field::new("token0_symbol", DataType::Utf8, true),
        Field::new("token1_symbol", DataType::Utf8, true),
        Field::new("token0_decimals", DataType::UInt8, true),
        Field::new("token1_decimals", DataType::UInt8, true),
        // Trade direction
        Field::new("token_bought_address", DataType::Utf8, true),
        Field::new("token_sold_address", DataType::Utf8, true),
        Field::new("token_bought_symbol", DataType::Utf8, true),
        Field::new("token_sold_symbol", DataType::Utf8, true),
        Field::new("token_bought_amount_raw", DataType::Utf8, true),
        Field::new("token_sold_amount_raw", DataType::Utf8, true),
        Field::new("token_bought_amount", DataType::Float64, true),
        Field::new("token_sold_amount", DataType::Float64, true),
        // Transaction context
        Field::new("tx_from", DataType::Utf8, true),
        Field::new("tx_to", DataType::Utf8, true),
        Field::new("gas_used", DataType::UInt64, true),
        // Price analysis
        Field::new("price_weth_per_token", DataType::Float64, true),
        // USD analysis
        Field::new("value_usd", DataType::Float64, true),
        Field::new("value_usd_method", DataType::Utf8, true),
    ])
}

/// Legacy schema function for backward compatibility
#[allow(dead_code)]
pub fn dextrades_schema() -> Schema {
    swap_event_schema()
}
