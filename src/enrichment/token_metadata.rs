use crate::enrichment::SwapEnricher;
use crate::error::parse_address;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;
use serde_json;

/// Enricher that adds token metadata (addresses, symbols, decimals)
pub struct TokenMetadataEnricher;

#[async_trait]
impl SwapEnricher for TokenMetadataEnricher {
    fn name(&self) -> &'static str {
        "token_metadata"
    }

    fn required_fields(&self) -> Vec<&'static str> {
        vec!["pool_address", "dex_protocol"]
    }

    fn provided_fields(&self) -> Vec<&'static str> {
        vec![
            "token0_address",
            "token1_address",
            "token0_symbol",
            "token1_symbol",
            "token0_decimals",
            "token1_decimals",
        ]
    }

    async fn enrich(&self, events: &mut [SwapEvent], service: &DextradesService) -> Result<()> {
        use log::{debug, info};
        use std::collections::{HashMap, HashSet};
        use alloy::primitives::Address;
        
        if events.is_empty() {
            return Ok(());
        }

        info!("🚀 [TokenMetadataEnricher] Starting enrichment for {} events", events.len());
        let start_time = std::time::Instant::now();

        // First pass: collect all unique pool addresses and get their tokens in parallel
        let mut pool_addresses_to_fetch: Vec<(String, String)> = Vec::new();
        
        for event in events.iter() {
            pool_addresses_to_fetch.push((event.pool_address.clone(), event.dex_protocol.clone()));
        }
        
        // Remove duplicates
        pool_addresses_to_fetch.sort_unstable();
        pool_addresses_to_fetch.dedup();
        
        info!("📋 [TokenMetadataEnricher] Found {} unique pools to process", pool_addresses_to_fetch.len());

        // Process pools in very small batches to avoid overwhelming RPC providers (global parallel chunks)
        // Using 1 here materially reduces cross-chunk RPC fan-out while maintaining correctness
        let max_concurrent_pools = 1;
        let mut pool_tokens_map: HashMap<String, Option<(Address, Address)>> = HashMap::new();
        
        for pool_batch in pool_addresses_to_fetch.chunks(max_concurrent_pools) {
            info!("🔄 [TokenMetadataEnricher] Processing batch of {} pools (max concurrent: {})", pool_batch.len(), max_concurrent_pools);
            
            // Fetch pool tokens in parallel for this batch
            let pool_token_futures: Vec<_> = pool_batch
                .iter()
                .map(|(pool_address, dex_protocol)| {
                    let service = service.clone();
                    let pool_address = pool_address.clone();
                    let dex_protocol = dex_protocol.clone();
                    async move {
                        let pool_tokens_result = match dex_protocol.as_str() {
                            "uniswap_v2" => match parse_address(&pool_address) {
                                Ok(pool_addr) => {
                                    match crate::protocols::uniswap_v2::get_pool_tokens(
                                        service.rpc_service(),
                                        pool_addr,
                                    ).await {
                                        Ok(tokens) => Some(tokens),
                                        Err(e) => {
                                            debug!("Failed to get V2 pool tokens for {}: {}", pool_addr, e);
                                            None
                                        }
                                    }
                                },
                                Err(e) => {
                                    debug!("Invalid pool address for V2: {}", e);
                                    None
                                }
                            },
                            "uniswap_v3" => match parse_address(&pool_address) {
                                Ok(pool_addr) => {
                                    match crate::protocols::uniswap_v3::get_pool_tokens(
                                        service.rpc_service(),
                                        pool_addr,
                                    ).await {
                                        Ok(tokens) => Some(tokens),
                                        Err(e) => {
                                            debug!("Failed to get V3 pool tokens for {}: {}", pool_addr, e);
                                            None
                                        }
                                    }
                                },
                                Err(e) => {
                                    debug!("Invalid pool address for V3: {}", e);
                                    None
                                }
                            },
                            _ => {
                                log::warn!("Unknown protocol for token metadata: {}", dex_protocol);
                                None
                            }
                        };
                        (pool_address, pool_tokens_result)
                    }
                })
                .collect();

            // Wait for this batch of pool token results
            let pool_token_results = futures::future::join_all(pool_token_futures).await;
            
            // Add results to the main map
            for (pool_address, tokens) in pool_token_results {
                pool_tokens_map.insert(pool_address, tokens);
            }
            
            // Small delay between batches to avoid overwhelming RPC providers
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Second pass: collect all unique token addresses
        let mut unique_token_addresses = HashSet::new();
        let mut successful_pools = 0;
        let mut failed_pools = 0;
        
        for event in events.iter() {
            if let Some(Some((token0, token1))) = pool_tokens_map.get(&event.pool_address) {
                unique_token_addresses.insert(*token0);
                unique_token_addresses.insert(*token1);
                successful_pools += 1;
            } else {
                failed_pools += 1;
                debug!("❌ [TokenMetadataEnricher] No tokens found for pool: {}", event.pool_address);
            }
        }
        
        info!("🪙 [TokenMetadataEnricher] Pool token resolution: {} successful, {} failed", successful_pools, failed_pools);
        info!("🪙 [TokenMetadataEnricher] Found {} unique token addresses to enrich", unique_token_addresses.len());

        // Fetch all token metadata in bulk using the enhanced service
        let token_addresses: Vec<Address> = unique_token_addresses.into_iter().collect();
        info!("⚡ [TokenMetadataEnricher] Starting bulk token metadata fetch for {} tokens", token_addresses.len());
        
        let token_metadata_results = if !token_addresses.is_empty() {
            let fetch_start = std::time::Instant::now();
            
            // ULTRA-CONSERVATIVE STRATEGY: Prevent RPC overwhelm at all costs for 100% reliability
            // Based on extensive debugging, RPC providers cannot handle concurrent token requests reliably
            let (batch_size, delay_ms) = if token_addresses.len() <= 3 {
                (1, 200) // Even small sets: batch_size=1, 200ms delay for maximum reliability
            } else {
                (1, 800) // Larger sets: batch_size=1, 800ms delay to prevent ANY rate limiting
            };
            
            info!("🎯 [TokenMetadataEnricher] ULTRA-CONSERVATIVE strategy: batch_size={}, delay={}ms for {} tokens", 
                  batch_size, delay_ms, token_addresses.len());
            
            let mut all_results = Vec::new();
            let mut successful_batches = 0;
            let mut failed_batches = 0;
            
            for (batch_num, token_batch) in token_addresses.chunks(batch_size).enumerate() {
                debug!("🔄 [TokenMetadataEnricher] Processing batch {} of {} tokens", batch_num + 1, token_batch.len());
                
                let batch_start = std::time::Instant::now();
                
                // Process this batch of tokens in parallel
                let fetch_futures: Vec<_> = token_batch
                    .iter()
                    .map(|&addr| {
                        let service = service.clone();
                        async move {
                            let addr_str = addr.to_string();
                            let result = service.get_token_metadata(&addr_str).await;
                            (addr, result)
                        }
                    })
                    .collect();
                
                let batch_results = futures::future::join_all(fetch_futures).await;
                let batch_duration = batch_start.elapsed();
                
                // Track batch success rate for adaptive optimization
                let batch_success_count = batch_results.iter().filter(|(_, result)| result.is_ok()).count();
                if batch_success_count == token_batch.len() {
                    successful_batches += 1;
                } else {
                    failed_batches += 1;
                    debug!("⚠️ [TokenMetadataEnricher] Batch {} partial success: {}/{} in {:?}", 
                           batch_num + 1, batch_success_count, token_batch.len(), batch_duration);
                }
                
                all_results.extend(batch_results);
                
                // Adaptive delay: longer delay after failures to prevent cascading issues
                if token_batch.len() == batch_size && batch_num < (token_addresses.len() + batch_size - 1) / batch_size - 1 {
                    let adaptive_delay = if batch_success_count < token_batch.len() {
                        delay_ms * 2 // Double delay after failures
                    } else {
                        delay_ms // Normal delay after success
                    };
                    tokio::time::sleep(tokio::time::Duration::from_millis(adaptive_delay)).await;
                }
            }
            
            let fetch_duration = fetch_start.elapsed();
            let total_batches = successful_batches + failed_batches;
            let batch_success_rate = if total_batches > 0 { successful_batches as f64 / total_batches as f64 } else { 0.0 };
            
            info!("⚡ [TokenMetadataEnricher] Adaptive fetch completed in {:?}: {}/{} batches successful ({:.1}%)", 
                  fetch_duration, successful_batches, total_batches, batch_success_rate * 100.0);
                  
            all_results
        } else {
            Vec::new()
        };

        // Build token metadata map
        let mut token_metadata_map = HashMap::new();
        let mut successful_fetches = 0;
        let mut failed_fetches = 0;
        
        for (addr, result) in token_metadata_results {
            match result {
                Ok(Some(token_info)) => {
                    token_metadata_map.insert(addr, token_info);
                    successful_fetches += 1;
                }
                Ok(None) => {
                    debug!("❌ [TokenMetadataEnricher] No metadata found for token: {}", addr);
                    failed_fetches += 1;
                }
                Err(e) => {
                    debug!("❌ [TokenMetadataEnricher] Error fetching metadata for token {}: {}", addr, e);
                    failed_fetches += 1;
                }
            }
        }
        
        info!("📊 [TokenMetadataEnricher] Token metadata fetch results: {} successful, {} failed", successful_fetches, failed_fetches);

        // Third pass: enrich all events with the collected data
        let mut enriched_events = 0;
        for event in events.iter_mut() {
            if let Some(Some((token0, token1))) = pool_tokens_map.get(&event.pool_address) {
                // Add token addresses
                let token0_str = token0.to_string();
                let token1_str = token1.to_string();

                event.add_enriched_field(
                    "token0_address".to_string(),
                    serde_json::Value::String(token0_str.clone()),
                );
                event.add_enriched_field(
                    "token1_address".to_string(),
                    serde_json::Value::String(token1_str.clone()),
                );

                // Also populate legacy fields for backward compatibility
                event.token0_address = Some(token0_str.clone());
                event.token1_address = Some(token1_str.clone());

                // Add token0 metadata if available
                if let Some(token0_info) = token_metadata_map.get(token0) {
                    let (_, symbol, decimals) = token0_info.as_tuple();

                    event.add_enriched_field(
                        "token0_symbol".to_string(),
                        serde_json::Value::String(symbol.clone()),
                    );
                    event.add_enriched_field(
                        "token0_decimals".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(decimals)),
                    );

                    // Also populate legacy fields
                    event.token0_symbol = Some(symbol.clone());
                    log::debug!("[TokenMetadata] Set token0_symbol to {} for pool {} in block {}", symbol, event.pool_address, event.block_number);
                    event.token0_decimals = Some(decimals);
                }

                // Add token1 metadata if available
                if let Some(token1_info) = token_metadata_map.get(token1) {
                    let (_, symbol, decimals) = token1_info.as_tuple();

                    event.add_enriched_field(
                        "token1_symbol".to_string(),
                        serde_json::Value::String(symbol.clone()),
                    );
                    event.add_enriched_field(
                        "token1_decimals".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(decimals)),
                    );

                    // Also populate legacy fields
                    event.token1_symbol = Some(symbol.clone());
                    log::debug!("[TokenMetadata] Set token1_symbol to {} for pool {} in block {}", symbol, event.pool_address, event.block_number);
                    event.token1_decimals = Some(decimals);
                }
                
                enriched_events += 1;
            }
        }

        let duration = start_time.elapsed();
        info!("✅ [TokenMetadataEnricher] Completed enrichment for {}/{} events in {:?}", enriched_events, events.len(), duration);
        info!("📊 [TokenMetadataEnricher] Final stats - Events: {}, Pools resolved: {}, Tokens found: {}, Tokens enriched: {}/{}", 
              events.len(), successful_pools, token_addresses.len(), successful_fetches, token_addresses.len());

        Ok(())
    }
}
