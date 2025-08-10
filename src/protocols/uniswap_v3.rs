use crate::schema::SwapEvent;
use alloy::primitives::{Address, B256, I256};
use alloy::rpc::types::Log;
use alloy::sol;
use alloy::sol_types::SolEvent;
use eyre::Result;
use serde_json;

// Define Uniswap V3 Pool interface
sol! {
    #[sol(rpc)]
    interface UniswapV3Pool {
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256 amount0,
            int256 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick
        );

        function token0() external view returns (address);
        function token1() external view returns (address);
        function fee() external view returns (uint24);
    }
}

/// Extract Uniswap V3 swap events and transform to standardized schema
pub fn extract_swaps(logs: &[Log]) -> Vec<SwapEvent> {
    logs.iter()
        .filter_map(|log| {
            // Convert RPC log to primitives log for event decoding
            let primitive_log = alloy::primitives::Log {
                address: log.address(),
                data: log.data().clone(),
            };

            // Try to decode as Uniswap V3 Swap event
            if let Ok(decoded_log) = UniswapV3Pool::Swap::decode_log(&primitive_log) {
                let swap_event = decoded_log.data;

                // Create new SwapEvent with core fields only
                let mut event = SwapEvent::new(
                    log.block_number.unwrap_or_default(),
                    log.transaction_hash.unwrap_or_default().to_string(),
                    log.log_index.unwrap_or_default() as u64,
                    "uniswap_v3".to_string(),
                    log.address().to_string(),
                );

                // Add protocol-specific raw data
                event.add_raw_data(
                    "sender".to_string(),
                    serde_json::Value::String(swap_event.sender.to_string()),
                );
                event.add_raw_data(
                    "recipient".to_string(),
                    serde_json::Value::String(swap_event.recipient.to_string()),
                );
                event.add_raw_data(
                    "amount0".to_string(),
                    serde_json::Value::String(swap_event.amount0.to_string()),
                );
                event.add_raw_data(
                    "amount1".to_string(),
                    serde_json::Value::String(swap_event.amount1.to_string()),
                );
                event.add_raw_data(
                    "sqrtPriceX96".to_string(),
                    serde_json::Value::String(swap_event.sqrtPriceX96.to_string()),
                );
                event.add_raw_data(
                    "liquidity".to_string(),
                    serde_json::Value::String(swap_event.liquidity.to_string()),
                );
                event.add_raw_data(
                    "tick".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(swap_event.tick.as_i32())),
                );

                // Set legacy fields for backward compatibility during migration
                event.taker = Some(swap_event.sender.to_string());
                event.recipient = Some(swap_event.recipient.to_string());

                // Store raw amounts for later enrichment
                // V3 logic from USER perspective: negative pool amount = user received, positive pool amount = user sent
                // Actual log data: amount0=-18750 (pool sent eXRD = user bought eXRD), amount1=+482... (pool received WETH = user sold WETH)
                // So: user sold WETH (token1), user bought eXRD (token0)
                let (token_bought_amount, token_sold_amount) =
                    if swap_event.amount0.is_negative() {
                        // amount0 negative = pool sent token0 = user bought token0
                        // amount1 positive = pool received token1 = user sold token1
                        (
                            -swap_event.amount0, // user bought token0 (make positive)
                            swap_event.amount1,  // user sold token1
                        )
                    } else {
                        // amount0 positive = pool received token0 = user sold token0
                        // amount1 negative = pool sent token1 = user bought token1
                        (
                            -swap_event.amount1, // user bought token1 (make positive)
                            swap_event.amount0,  // user sold token0
                        )
                    };

                // Set normalized fields with I256 values (convert to string only when needed)
                event.set_amount_in(token_sold_amount.to_string());
                event.set_amount_out(token_bought_amount.to_string());

                // Store raw amounts for enrichment
                event.add_raw_data(
                    "token_bought_amount_raw".to_string(),
                    serde_json::Value::String(token_bought_amount.to_string()),
                );
                event.add_raw_data(
                    "token_sold_amount_raw".to_string(),
                    serde_json::Value::String(token_sold_amount.to_string()),
                );

                // Set legacy fields for backward compatibility
                event.token_bought_amount_raw = Some(token_bought_amount.to_string());
                event.token_sold_amount_raw = Some(token_sold_amount.to_string());

                Some(event)
            } else {
                None
            }
        })
        .collect()
}

/// Enrich V3 events with trade direction information
pub fn enrich_trade_direction(
    events: &mut [SwapEvent],
    token_metadata: &std::collections::HashMap<Address, crate::types::TokenMetadata>,
) {
    for event in events {
        // For V3, determine trade direction based on the original amount0/amount1 signs
        // V3 uses signed integers: negative = outgoing, positive = incoming

        if let (Some(token0_addr_str), Some(token1_addr_str)) =
            (&event.token0_address, &event.token1_address)
        {
            // Get the original amount0 and amount1 from raw data to determine direction
            if let (Some(amount0_str), Some(amount1_str)) = (
                event.raw_data.get("amount0").and_then(|v| v.as_str()),
                event.raw_data.get("amount1").and_then(|v| v.as_str()),
            ) {
                // Use I256 for proper big number comparison
                if let (Ok(amount0), Ok(_amount1)) =
                    (amount0_str.parse::<I256>(), amount1_str.parse::<I256>())
                {
                    // Determine trade direction based on signs (USER perspective)
                    // Match the extraction logic: check amount0 first
                    let (bought_addr, sold_addr, _bought_decimals, _sold_decimals) =
                        if amount0.is_negative() {
                            // amount0 negative = pool sent token0 = user bought token0
                            // amount1 positive = pool received token1 = user sold token1
                            (
                                token0_addr_str,
                                token1_addr_str,
                                event.token0_decimals,
                                event.token1_decimals,
                            )
                        } else {
                            // amount0 positive = pool received token0 = user sold token0
                            // amount1 negative = pool sent token1 = user bought token1
                            (
                                token1_addr_str,
                                token0_addr_str,
                                event.token1_decimals,
                                event.token0_decimals,
                            )
                        };

                    // Set the correct addresses
                    event.token_bought_address = Some(bought_addr.clone());
                    event.token_sold_address = Some(sold_addr.clone());

                    // Set symbols from metadata
                    if let Ok(bought_addr_parsed) = crate::error::parse_address(bought_addr) {
                        if let Some(metadata) = token_metadata.get(&bought_addr_parsed) {
                            if let Some(symbol) = &metadata.symbol {
                                event.token_bought_symbol = Some(symbol.clone());
                            }
                        }
                    }

                    if let Ok(sold_addr_parsed) = crate::error::parse_address(sold_addr) {
                        if let Some(metadata) = token_metadata.get(&sold_addr_parsed) {
                            if let Some(symbol) = &metadata.symbol {
                                event.token_sold_symbol = Some(symbol.clone());
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Calculate decimal amounts for V3 swaps using Alloy's format_units
/// Uses proper big number handling to avoid floating point precision issues
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

/// Get token0 and token1 addresses from a Uniswap V3 pool
pub async fn get_pool_tokens(
    rpc_service: &crate::rpc_orchestrator::RpcOrchestrator,
    pool_address: Address,
) -> Result<(Address, Address)> {
    use alloy::sol_types::SolCall;

    // Create encoded calls for both functions
    let token0_data = UniswapV3Pool::token0Call::new(()).abi_encode();
    let token1_data = UniswapV3Pool::token1Call::new(()).abi_encode();

    // Execute both calls in parallel using enhanced RPC service (now with retry logic)
    let (token0_result, token1_result) = tokio::join!(
        rpc_service.call(pool_address, token0_data),
        rpc_service.call(pool_address, token1_data)
    );

    // Decode results
    let token0_bytes = token0_result.map_err(|e| eyre::eyre!("Failed to call token0: {}", e))?;
    let token1_bytes = token1_result.map_err(|e| eyre::eyre!("Failed to call token1: {}", e))?;

    let token0 = UniswapV3Pool::token0Call::abi_decode_returns(&token0_bytes)
        .map_err(|e| eyre::eyre!("Failed to decode token0: {}", e))?;
    let token1 = UniswapV3Pool::token1Call::abi_decode_returns(&token1_bytes)
        .map_err(|e| eyre::eyre!("Failed to decode token1: {}", e))?;

    Ok((token0, token1))
}

/// Get the Uniswap V3 Swap event signature hash
pub fn get_swap_event_signature() -> B256 {
    UniswapV3Pool::Swap::SIGNATURE_HASH
}

// (No trait-based protocol registry; using free functions only.)
