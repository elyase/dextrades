use crate::schema::SwapEvent;
use alloy::primitives::{Address, B256};
use alloy::rpc::types::Log;
use alloy::sol;
use alloy::sol_types::SolEvent;
use eyre::Result;
use serde_json;

// Define Uniswap V2 Pair interface
sol! {
    #[sol(rpc)]
    interface UniswapV2Pair {
        event Swap(
            address indexed sender,
            uint256 amount0In,
            uint256 amount1In,
            uint256 amount0Out,
            uint256 amount1Out,
            address indexed to
        );

        function token0() external view returns (address);
        function token1() external view returns (address);
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    }
}

/// Extract Uniswap V2 swap events and transform to standardized schema
pub fn extract_swaps(logs: &[Log]) -> Vec<SwapEvent> {
    logs.iter()
        .filter_map(|log| {
            // Convert RPC log to primitives log for event decoding
            let primitive_log = alloy::primitives::Log {
                address: log.address(),
                data: log.data().clone(),
            };

            // Try to decode as Uniswap V2 Swap event
            if let Ok(decoded_log) = UniswapV2Pair::Swap::decode_log(&primitive_log) {
                let swap_event = decoded_log.data;

                // Create new SwapEvent with core fields only
                let mut event = SwapEvent::new(
                    log.block_number.unwrap_or_default(),
                    log.transaction_hash.unwrap_or_default().to_string(),
                    log.log_index.unwrap_or_default() as u64,
                    "uniswap_v2".to_string(),
                    log.address().to_string(),
                );

                // Add protocol-specific raw data
                event.add_raw_data(
                    "amount0In".to_string(),
                    serde_json::Value::String(swap_event.amount0In.to_string()),
                );
                event.add_raw_data(
                    "amount1In".to_string(),
                    serde_json::Value::String(swap_event.amount1In.to_string()),
                );
                event.add_raw_data(
                    "amount0Out".to_string(),
                    serde_json::Value::String(swap_event.amount0Out.to_string()),
                );
                event.add_raw_data(
                    "amount1Out".to_string(),
                    serde_json::Value::String(swap_event.amount1Out.to_string()),
                );
                event.add_raw_data(
                    "sender".to_string(),
                    serde_json::Value::String(swap_event.sender.to_string()),
                );
                event.add_raw_data(
                    "to".to_string(),
                    serde_json::Value::String(swap_event.to.to_string()),
                );

                // Set legacy fields for backward compatibility during migration
                event.taker = Some(swap_event.sender.to_string());
                event.recipient = Some(swap_event.to.to_string());

                // Determine trade direction and amounts using proper big number comparisons
                let (token_bought_amount_u256, token_sold_amount_u256, v2_token0_sold) =
                    if !swap_event.amount0In.is_zero() {
                        // Token0 was sent in, token1 was received out
                        (
                            swap_event.amount1Out,
                            swap_event.amount0In,
                            true,
                        )
                    } else {
                        // Token1 was sent in, token0 was received out
                        (
                            swap_event.amount0Out,
                            swap_event.amount1In,
                            false,
                        )
                    };

                // Set normalized fields with U256 values (convert to string only when needed)
                event.set_amount_in(token_sold_amount_u256.to_string());
                event.set_amount_out(token_bought_amount_u256.to_string());

                // Store raw amounts and direction info for enrichment
                event.add_raw_data(
                    "token_bought_amount_raw".to_string(),
                    serde_json::Value::String(token_bought_amount_u256.to_string()),
                );
                event.add_raw_data(
                    "token_sold_amount_raw".to_string(),
                    serde_json::Value::String(token_sold_amount_u256.to_string()),
                );
                event.add_raw_data(
                    "v2_token0_sold".to_string(),
                    serde_json::Value::Bool(v2_token0_sold),
                );

                // Set legacy fields for backward compatibility
                event.token_bought_amount_raw = Some(token_bought_amount_u256.to_string());
                event.token_sold_amount_raw = Some(token_sold_amount_u256.to_string());

                Some(event)
            } else {
                None
            }
        })
        .collect()
}

/// Enrich V2 events with trade direction information
pub fn enrich_trade_direction(events: &mut [SwapEvent]) {
    for event in events {
        // Get the trade direction from raw data
        let v2_token0_sold = event
            .raw_data
            .get("v2_token0_sold")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if let (Some(token0_addr), Some(token1_addr)) =
            (&event.token0_address, &event.token1_address)
        {
            if v2_token0_sold {
                // Token0 was sold, token1 was bought
                event.token_sold_address = Some(token0_addr.clone());
                event.token_bought_address = Some(token1_addr.clone());

                // Set symbols if available
                if let Some(symbol) = &event.token0_symbol {
                    event.token_sold_symbol = Some(symbol.clone());
                }
                if let Some(symbol) = &event.token1_symbol {
                    event.token_bought_symbol = Some(symbol.clone());
                }
            } else {
                // Token1 was sold, token0 was bought
                event.token_sold_address = Some(token1_addr.clone());
                event.token_bought_address = Some(token0_addr.clone());

                // Set symbols if available
                if let Some(symbol) = &event.token1_symbol {
                    event.token_sold_symbol = Some(symbol.clone());
                }
                if let Some(symbol) = &event.token0_symbol {
                    event.token_bought_symbol = Some(symbol.clone());
                }
            }
        }
    }
}

/// Calculate decimal amounts from raw amounts using token decimals
/// Uses Alloy's format_units for precise big number handling
pub fn calculate_decimal_amounts(events: &mut [SwapEvent]) {
    use alloy::primitives::{utils::format_units, U256};

    for event in events {
        // Calculate bought amount using the correct decimals
        if let (Some(raw_amount), Some(bought_addr)) =
            (&event.token_bought_amount_raw, &event.token_bought_address)
        {
            // Determine which decimals to use based on the bought token address
            let decimals = if Some(bought_addr) == event.token0_address.as_ref() {
                event.token0_decimals
            } else {
                event.token1_decimals
            };

            if let Some(decimals) = decimals {
                if let Ok(raw_u256) = U256::from_str_radix(raw_amount, 10) {
                    // Use Alloy's format_units for precise decimal conversion
                    if let Ok(formatted) = format_units(raw_u256, decimals) {
                        if let Ok(decimal_amount) = formatted.parse::<f64>() {
                            event.token_bought_amount = Some(decimal_amount);
                        }
                    }
                }
            }
        }

        // Calculate sold amount using the correct decimals
        if let (Some(raw_amount), Some(sold_addr)) =
            (&event.token_sold_amount_raw, &event.token_sold_address)
        {
            // Determine which decimals to use based on the sold token address
            let decimals = if Some(sold_addr) == event.token0_address.as_ref() {
                event.token0_decimals
            } else {
                event.token1_decimals
            };

            if let Some(decimals) = decimals {
                if let Ok(raw_u256) = U256::from_str_radix(raw_amount, 10) {
                    // Use Alloy's format_units for precise decimal conversion
                    if let Ok(formatted) = format_units(raw_u256, decimals) {
                        if let Ok(decimal_amount) = formatted.parse::<f64>() {
                            event.token_sold_amount = Some(decimal_amount);
                        }
                    }
                }
            }
        }
    }
}

/// Get token0 and token1 addresses from a Uniswap V2 pool (now with enhanced retry in RpcOrchestrator)
pub async fn get_pool_tokens(
    rpc_service: &crate::rpc_orchestrator::RpcOrchestrator,
    pool_address: Address,
) -> Result<(Address, Address)> {
    use alloy::sol_types::SolCall;

    // Create encoded calls for both functions
    let token0_data = UniswapV2Pair::token0Call::new(()).abi_encode();
    let token1_data = UniswapV2Pair::token1Call::new(()).abi_encode();

    // Execute both calls in parallel using enhanced RPC service (now with retry logic)
    let (token0_result, token1_result) = tokio::join!(
        rpc_service.call(pool_address, token0_data),
        rpc_service.call(pool_address, token1_data)
    );

    // Decode results
    let token0_bytes = token0_result.map_err(|e| eyre::eyre!("Failed to call token0: {}", e))?;
    let token1_bytes = token1_result.map_err(|e| eyre::eyre!("Failed to call token1: {}", e))?;

    let token0 = UniswapV2Pair::token0Call::abi_decode_returns(&token0_bytes)
        .map_err(|e| eyre::eyre!("Failed to decode token0: {}", e))?;
    let token1 = UniswapV2Pair::token1Call::abi_decode_returns(&token1_bytes)
        .map_err(|e| eyre::eyre!("Failed to decode token1: {}", e))?;

    Ok((token0, token1))
}

/// Get the Uniswap V2 Swap event signature hash
pub fn get_swap_event_signature() -> B256 {
    UniswapV2Pair::Swap::SIGNATURE_HASH
}

// (No trait-based protocol registry; using free functions only.)
