use crate::enrichment::SwapEnricher;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;
use serde_json;

/// Enricher that determines trade direction and calculates decimal amounts
pub struct TradeDirectionEnricher;

#[async_trait]
impl SwapEnricher for TradeDirectionEnricher {
    fn name(&self) -> &'static str {
        "trade_direction"
    }

    fn required_fields(&self) -> Vec<&'static str> {
        vec![
            "token0_address",
            "token1_address",
            "token0_symbol",
            "token1_symbol",
            "token0_decimals",
            "token1_decimals",
        ]
    }

    fn provided_fields(&self) -> Vec<&'static str> {
        vec![
            "token_bought_address",
            "token_sold_address",
            "token_bought_symbol",
            "token_sold_symbol",
            "token_bought_amount_raw",
            "token_sold_amount_raw",
            "token_bought_amount",
            "token_sold_amount",
        ]
    }

    async fn enrich(&self, events: &mut [SwapEvent], _service: &DextradesService) -> Result<()> {
        for event in events {
            // Apply protocol-specific trade direction logic
            match event.dex_protocol.as_str() {
                "uniswap_v2" => {
                    self.enrich_v2_trade_direction(event)?;
                }
                "uniswap_v3" => {
                    self.enrich_v3_trade_direction(event)?;
                }
                _ => {
                    log::warn!(
                        "Unknown protocol for trade direction: {}",
                        event.dex_protocol
                    );
                }
            }
        }

        Ok(())
    }
}

impl TradeDirectionEnricher {
    /// Calculate V2 trade direction from original swap amounts
    /// This fixes the issue where raw data is lost during enrichment
    fn calculate_v2_trade_direction(&self, event: &SwapEvent) -> bool {
        // Get the original swap amounts from raw data
        let amount0_in = event.raw_data.get("amount0In")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u128>().ok())
            .unwrap_or(0);
        
        let _amount1_in = event.raw_data.get("amount1In")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u128>().ok())
            .unwrap_or(0);
        
        // V2 trade direction logic: if amount0In > 0, then token0 was sold
        // This matches the original logic in uniswap_v2.rs line 82-97
        amount0_in > 0
    }
    /// Enrich V2 trade direction using the existing V2 logic
    fn enrich_v2_trade_direction(&self, event: &mut SwapEvent) -> Result<()> {
        // CRITICAL FIX: Instead of relying on lost raw data, recalculate trade direction
        // from the original swap amounts stored in raw data
        let v2_token0_sold = self.calculate_v2_trade_direction(event);
        
        // Manually set the trade direction instead of using the protocol function
        // that depends on raw data that gets lost during enrichment
        if let (Some(token0_addr), Some(token1_addr)) = (
            event.get_enriched_string("token0_address"),
            event.get_enriched_string("token1_address"),
        ) {
            if v2_token0_sold {
                // Token0 was sold, token1 was bought
                event.add_enriched_field(
                    "token_sold_address".to_string(),
                    serde_json::Value::String(token0_addr.clone()),
                );
                event.add_enriched_field(
                    "token_bought_address".to_string(),
                    serde_json::Value::String(token1_addr.clone()),
                );
                event.token_sold_address = Some(token0_addr.clone());
                event.token_bought_address = Some(token1_addr.clone());

                // Set symbols if available
                if let Some(symbol) = event.get_enriched_string("token0_symbol") {
                        log::info!(
                            "V2 trade direction for tx {}: sold token0 symbol: {}",
                            event.tx_hash,
                            symbol
                        );                    event.add_enriched_field(
                        "token_sold_symbol".to_string(),
                        serde_json::Value::String(symbol.clone()),
                    );
                    event.token_sold_symbol = Some(symbol);
                }
                if let Some(symbol) = event.get_enriched_string("token1_symbol") {
                    event.add_enriched_field(
                        "token_bought_symbol".to_string(),
                        serde_json::Value::String(symbol.clone()),
                    );
                    event.token_bought_symbol = Some(symbol);
                }
            } else {
                // Token1 was sold, token0 was bought
                event.add_enriched_field(
                    "token_sold_address".to_string(),
                    serde_json::Value::String(token1_addr.clone()),
                );
                event.add_enriched_field(
                    "token_bought_address".to_string(),
                    serde_json::Value::String(token0_addr.clone()),
                );
                event.token_sold_address = Some(token1_addr.clone());
                event.token_bought_address = Some(token0_addr.clone());

                // Set symbols if available
                if let Some(symbol) = event.get_enriched_string("token1_symbol") {
                    log::info!(
                        "V2 trade direction for tx {}: sold token1 symbol: {}",
                        event.tx_hash,
                        symbol
                    );
                    event.add_enriched_field(
                        "token_sold_symbol".to_string(),
                        serde_json::Value::String(symbol.clone()),
                    );
                    event.token_sold_symbol = Some(symbol);
                }
                if let Some(symbol) = event.get_enriched_string("token0_symbol") {
                    event.add_enriched_field(
                        "token_bought_symbol".to_string(),
                        serde_json::Value::String(symbol.clone()),
                    );
                    event.token_bought_symbol = Some(symbol);
                }
            }
        }

        // Now calculate decimal amounts with the correct trade direction
        let mut temp_events = vec![event.clone()];
        crate::protocols::uniswap_v2::calculate_decimal_amounts(&mut temp_events);

        // Copy results back to the original event
        if let Some(enriched_event) = temp_events.into_iter().next() {
            // Copy trade direction fields
            if let Some(addr) = &enriched_event.token_bought_address {
                event.add_enriched_field(
                    "token_bought_address".to_string(),
                    serde_json::Value::String(addr.clone()),
                );
                event.token_bought_address = Some(addr.clone());
            }

            if let Some(addr) = &enriched_event.token_sold_address {
                event.add_enriched_field(
                    "token_sold_address".to_string(),
                    serde_json::Value::String(addr.clone()),
                );
                event.token_sold_address = Some(addr.clone());
            }

            if let Some(symbol) = &enriched_event.token_bought_symbol {
                event.add_enriched_field(
                    "token_bought_symbol".to_string(),
                    serde_json::Value::String(symbol.clone()),
                );
                event.token_bought_symbol = Some(symbol.clone());
            }

            if let Some(symbol) = &enriched_event.token_sold_symbol {
                event.add_enriched_field(
                    "token_sold_symbol".to_string(),
                    serde_json::Value::String(symbol.clone()),
                );
                event.token_sold_symbol = Some(symbol.clone());
            }

            if let Some(amount_raw) = &enriched_event.token_bought_amount_raw {
                event.add_enriched_field(
                    "token_bought_amount_raw".to_string(),
                    serde_json::Value::String(amount_raw.clone()),
                );
                event.token_bought_amount_raw = Some(amount_raw.clone());
            }

            if let Some(amount_raw) = &enriched_event.token_sold_amount_raw {
                event.add_enriched_field(
                    "token_sold_amount_raw".to_string(),
                    serde_json::Value::String(amount_raw.clone()),
                );
                event.token_sold_amount_raw = Some(amount_raw.clone());
            }

            if let Some(amount) = enriched_event.token_bought_amount {
                event.add_enriched_field(
                    "token_bought_amount".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(amount).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                event.token_bought_amount = Some(amount);
            }

            if let Some(amount) = enriched_event.token_sold_amount {
                event.add_enriched_field(
                    "token_sold_amount".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(amount).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                event.token_sold_amount = Some(amount);
            }
        }

        Ok(())
    }

    /// Enrich V3 trade direction using the existing V3 logic
    fn enrich_v3_trade_direction(&self, event: &mut SwapEvent) -> Result<()> {
        // For V3, we need to build token metadata for the enricher
        let mut token_metadata = std::collections::HashMap::new();

        // Build metadata from enriched fields
        if let (Some(token0_addr_str), Some(token1_addr_str)) = (
            event.get_enriched_string("token0_address"),
            event.get_enriched_string("token1_address"),
        ) {
            if let Ok(addr0) = crate::error::parse_address(&token0_addr_str) {
                if let (Some(symbol), Some(decimals)) = (
                    event.get_enriched_string("token0_symbol"),
                    event.get_enriched_u8("token0_decimals"),
                ) {
                    token_metadata.insert(
                        addr0,
                        crate::types::TokenMetadata {
                            address: addr0,
                            name: None, // We don't have name in enriched fields yet
                            symbol: Some(symbol),
                            decimals: Some(decimals),
                        },
                    );
                }
            }

            if let Ok(addr1) = crate::error::parse_address(&token1_addr_str) {
                if let (Some(symbol), Some(decimals)) = (
                    event.get_enriched_string("token1_symbol"),
                    event.get_enriched_u8("token1_decimals"),
                ) {
                    token_metadata.insert(
                        addr1,
                        crate::types::TokenMetadata {
                            address: addr1,
                            name: None, // We don't have name in enriched fields yet
                            symbol: Some(symbol),
                            decimals: Some(decimals),
                        },
                    );
                }
            }
        }

        // Create a temporary vector with just this event for the V3 enricher
        let mut temp_events = vec![event.clone()];

        // Apply V3 enrichment logic
        crate::protocols::uniswap_v3::enrich_trade_direction(&mut temp_events, &token_metadata);
        crate::protocols::uniswap_v3::calculate_decimal_amounts(&mut temp_events);

        // Copy results back to the original event (same logic as V2)
        if let Some(enriched_event) = temp_events.into_iter().next() {
            // Copy trade direction fields (same as V2 logic above)
            if let Some(addr) = &enriched_event.token_bought_address {
                event.add_enriched_field(
                    "token_bought_address".to_string(),
                    serde_json::Value::String(addr.clone()),
                );
                event.token_bought_address = Some(addr.clone());
            }

            if let Some(addr) = &enriched_event.token_sold_address {
                event.add_enriched_field(
                    "token_sold_address".to_string(),
                    serde_json::Value::String(addr.clone()),
                );
                event.token_sold_address = Some(addr.clone());
            }

            if let Some(symbol) = &enriched_event.token_bought_symbol {
                event.add_enriched_field(
                    "token_bought_symbol".to_string(),
                    serde_json::Value::String(symbol.clone()),
                );
                event.token_bought_symbol = Some(symbol.clone());
            }

            if let Some(symbol) = &enriched_event.token_sold_symbol {
                event.add_enriched_field(
                    "token_sold_symbol".to_string(),
                    serde_json::Value::String(symbol.clone()),
                );
                event.token_sold_symbol = Some(symbol.clone());
            }

            if let Some(amount_raw) = &enriched_event.token_bought_amount_raw {
                event.add_enriched_field(
                    "token_bought_amount_raw".to_string(),
                    serde_json::Value::String(amount_raw.clone()),
                );
                event.token_bought_amount_raw = Some(amount_raw.clone());
            }

            if let Some(amount_raw) = &enriched_event.token_sold_amount_raw {
                event.add_enriched_field(
                    "token_sold_amount_raw".to_string(),
                    serde_json::Value::String(amount_raw.clone()),
                );
                event.token_sold_amount_raw = Some(amount_raw.clone());
            }

            if let Some(amount) = enriched_event.token_bought_amount {
                event.add_enriched_field(
                    "token_bought_amount".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(amount).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                event.token_bought_amount = Some(amount);
            }

            if let Some(amount) = enriched_event.token_sold_amount {
                event.add_enriched_field(
                    "token_sold_amount".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(amount).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                event.token_sold_amount = Some(amount);
            }
        }

        Ok(())
    }
}
