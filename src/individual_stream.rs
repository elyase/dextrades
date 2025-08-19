/// Individual event streaming implementation for true real-time processing
/// 
/// This module provides functionality for streaming individual swap events
/// as they are processed and enriched, rather than batching them by block.
/// 
/// Key differences from block-based streaming:
/// - Events are processed and streamed individually
/// - No waiting for entire blocks to complete
/// - Immediate enrichment and delivery
/// - True real-time streaming behavior

use arrow::record_batch::RecordBatch;
use eyre::Report;
use log::{debug, info, warn};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::config::DextradesConfig;
use crate::error::parse_address;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use crate::stream::{DextradesArrowStream, create_batch_from_swap_events, apply_swap_filters};

/// Stream individual DEX swap events in real-time
/// 
/// This function processes events individually as they are discovered,
/// enriches them immediately, and streams them without waiting for
/// block completion.
#[pyfunction]
pub fn stream_individual_swaps(
    _py: Python,
    protocols: Vec<String>,
    from_block: u64,
    to_block: u64,
    address: Option<String>,
    rpc_urls: Vec<String>,
    enrich_timestamps: Option<bool>,
    enrich_usd: Option<bool>,
    routers: Option<Vec<String>>,
) -> PyResult<DextradesArrowStream> {
    if rpc_urls.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "At least one RPC URL must be provided",
        ));
    }

    if protocols.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "At least one protocol must be provided",
        ));
    }

    let rt = pyo3_async_runtimes::tokio::get_runtime();
    let (tx, rx) = mpsc::channel::<Result<RecordBatch, Report>>(100); // Larger buffer for individual events

    info!("[Individual Stream] Using real-time individual event streaming");

    // Clone values for async task
    let protocols_clone = protocols.clone();
    let address_clone = address.clone();
    let enrich_timestamps = enrich_timestamps.unwrap_or(false);
    let enrich_usd = enrich_usd.unwrap_or(false);

    // Start async task for individual event processing
    let future = async move {
        // Create service with immediate streaming configuration
        let mut config = DextradesConfig::immediate_streaming();
        config.default_rpc_urls = rpc_urls.clone();
        if let Some(r) = routers { config.router_whitelist = Some(r); }
        
        info!("🚀 [Individual Stream] Using immediate streaming mode");
        
        let service = match DextradesService::new(config).await {
            Ok(service) => service,
            Err(e) => {
                let err = format!("Failed to create service: {}", e);
                info!("[Individual Stream] {}", err);
                let _ = tx.send(Err(eyre::eyre!(err))).await;
                return;
            }
        };
        
        // Warmup session
        info!("🔥 [Individual Stream] Performing global session warmup");
        match service.warmup_streaming_session().await {
            Ok(_) => info!("✅ [Individual Stream] Warmup completed successfully"),
            Err(e) => info!("⚠️ [Individual Stream] Warmup failed (non-fatal): {}", e),
        }
        
        let service = Arc::new(service);

        // Create enrichment pipeline
        let enrichment_pipeline = {
            let base_result = if enrich_timestamps {
                crate::enrichment::EnrichmentPresets::standard()
            } else {
                crate::enrichment::EnrichmentPresets::minimal()
            };
            let mut base = match base_result {
                Ok(p) => p,
                Err(e) => {
                    let err = format!("Failed to create enrichment pipeline: {}", e);
                    info!("[Individual Stream] {}", err);
                    let _ = tx.send(Err(eyre::eyre!(err))).await;
                    return;
                }
            };
            if enrich_usd {
                base = base.add_enricher(Box::new(crate::enrichment::price_usd::PriceUsdEnricher::default()));
            }
            Ok::<_, eyre::Error>(base)
        };

        let pipeline = match enrichment_pipeline {
            Ok(pipeline) => pipeline,
            Err(e) => {
                let err = format!("Failed to create enrichment pipeline: {}", e);
                info!("[Individual Stream] {}", err);
                let _ = tx.send(Err(eyre::eyre!(err))).await;
                return;
            }
        };
        let pipeline = Arc::new(pipeline);

        // Parse the pool address if provided
        let pool_filter = if let Some(ref addr_str) = address_clone {
            match parse_address(addr_str) {
                Ok(addr) => Some(addr),
                Err(e) => {
                    let err = format!("Invalid pool address: {}", e);
                    info!("[Individual Stream] {}", err);
                    let _ = tx.send(Err(eyre::eyre!(err))).await;
                    return;
                }
            }
        } else {
            None
        };

        info!(
            "🚀 [Individual Stream] Starting individual event stream for protocols {:?}",
            protocols_clone
        );
        info!(
            "📆 [Individual Stream] Blocks: {} to {}",
            from_block, to_block
        );

        // Process blocks sequentially, but stream events individually
        for block_num in from_block..=to_block {
            info!("📦 [Individual Stream] Processing block {}", block_num);
            
            // Process each protocol for this block
            for protocol in &protocols_clone {
                // Get event signature and extract function for this protocol
                let (event_signature, extract_fn): (alloy::primitives::B256, fn(&[alloy::rpc::types::Log]) -> Vec<SwapEvent>) = match protocol.as_str() {
                    "uniswap_v2" => (crate::protocols::uniswap_v2::get_swap_event_signature(), crate::protocols::uniswap_v2::extract_swaps),
                    "uniswap_v3" => (crate::protocols::uniswap_v3::get_swap_event_signature(), crate::protocols::uniswap_v3::extract_swaps),
                    _ => {
                        eprintln!("[Individual Stream] Unknown protocol: {}", protocol);
                        continue;
                    }
                };

                // Fetch logs for this block and protocol
                let mut filter = alloy::rpc::types::Filter::new()
                    .from_block(block_num)
                    .to_block(block_num)
                    .event_signature(event_signature);

                // Add pool filter if specified
                if let Some(pool_addr) = pool_filter {
                    filter = filter.address(pool_addr);
                }

                let logs = match service.rpc_service().get_logs(&filter).await {
                    Ok(raw_logs) => {
                        if !raw_logs.is_empty() {
                            info!("✅ [{}] Got {} raw logs for block {}", protocol, raw_logs.len(), block_num);
                        }
                        // Extract swaps using protocol-specific function
                        extract_fn(&raw_logs)
                    }
                    Err(e) => {
                        let error_str = e.to_string();
                        if error_str.contains("32701") || error_str.contains("Please specify an address") {
                            debug!("⚠️ [{}] Provider restriction - using fallback (expected behavior)", protocol);
                        } else {
                            warn!("❌ [{}] RPC error: {}", protocol, e);
                        }
                        continue;
                    }
                };

                // Process each event individually
                for mut event in logs {
                    info!("⚡ [Individual Stream] Processing individual event from tx {}", event.tx_hash);
                    
                    // Enrich this single event immediately
                    let mut single_event_vec = vec![event.clone()];
                    
                    let enrichment_start = std::time::Instant::now();
                    match pipeline.enrich_all(&mut single_event_vec, &service).await {
                        Ok(()) => {
                            let enrichment_duration = enrichment_start.elapsed();
                            debug!(
                                "✨ [Individual Stream] Enriched single event in {:?}",
                                enrichment_duration
                            );
                            event = single_event_vec.into_iter().next().unwrap();
                        }
                        Err(e) => {
                            warn!("[Individual Stream] Error during enrichment: {}", e);
                            // Continue with non-enriched event
                        }
                    }

                    // Apply filtering
                    let filtered_events = apply_swap_filters(&[event], &service.config());
                    
                    if !filtered_events.is_empty() {
                        let enriched_event = filtered_events.into_iter().next().unwrap();
                        
                        // Create a single-event batch and send immediately
                        match create_batch_from_swap_events(&[enriched_event]) {
                            Ok(batch) => {
                                info!("📤 [Individual Stream] Streaming individual event");
                                if tx.send(Ok(batch)).await.is_err() {
                                    info!("[Individual Stream] Receiver dropped, stopping stream");
                                    return;
                                }
                            }
                            Err(e) => {
                                eprintln!("[Individual Stream] Error creating batch from single event: {}", e);
                            }
                        }
                    }
                }
            }
        }

        info!("[Individual Stream] Individual event streaming complete");
    };

    // Spawn the streaming task
    rt.spawn(future);

    Ok(DextradesArrowStream::new(rx, None, None))
}
