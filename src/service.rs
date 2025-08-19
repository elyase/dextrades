use crate::config::DextradesConfig;
use crate::error::{parse_address, DextradesResult};
use crate::protocols::uniswap_v2;
use crate::rpc_orchestrator::RpcOrchestrator;
use crate::token_metadata_service::TokenMetadataService;
use std::time::Duration;
use log::{info, error, debug};
use std::sync::atomic::{AtomicU64, Ordering};

/// Unified service for all Dextrades operations
#[derive(Clone)]
pub struct DextradesService {
    rpc: RpcOrchestrator,
    config: DextradesConfig,
    token_metadata_service: TokenMetadataService,
    stats: std::sync::Arc<GlobalStats>,
}

impl DextradesService {
    /// Create a new service with the given configuration
    pub async fn new(config: DextradesConfig) -> DextradesResult<Self> {
        let rpc = RpcOrchestrator::with_config(config.clone()).await?;

        let token_metadata_service = TokenMetadataService::new(
            rpc.clone(),
            config.cache_size,
            config.max_concurrent_requests,
            Duration::from_secs(60), // 1 minute failure cache (reduced from 5 minutes)
        );

        Ok(Self { 
            rpc, 
            config,
            token_metadata_service,
            stats: std::sync::Arc::new(GlobalStats::default()),
        })
    }

    /// Create a new service with custom RPC URLs
    pub async fn with_rpc_urls(rpc_urls: Vec<String>) -> DextradesResult<Self> {
        let config = DextradesConfig::with_rpc_urls(rpc_urls);
        Self::new(config).await
    }


    /// Global session warmup to prevent cold start failures
    /// CRITICAL FIX: Call this ONCE per streaming session to pre-populate connection pools
    pub async fn warmup_streaming_session(&self) -> DextradesResult<()> {
        info!("🔥 [DextradesService] Starting global streaming session warmup");
        let warmup_start = std::time::Instant::now();
        
        // Determine warmup tokens based on network and overrides
        let chain_id = self.get_chain_id().await.unwrap_or(0);
        let warmup_tokens: Vec<String> = if let Some(ref ov) = self.config.network_overrides {
            if !ov.warmup_tokens.is_empty() { ov.warmup_tokens.clone() } else { Vec::new() }
        } else if chain_id == 1 {
            vec![
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(), // WETH
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(), // USDC
                "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_string(), // USDT
                "0x6B175474E89094C44Da98b954EedeAC495271d0F".to_string(), // DAI
            ]
        } else {
            Vec::new()
        };

        if warmup_tokens.is_empty() {
            info!("🌡️ [DextradesService] No warmup tokens configured for chain {} - skipping token warmup", chain_id);
            // Still do a lightweight call to warm connections
            let _ = self.get_block_number().await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            info!("✅ [DextradesService] Global warmup completed in {:?}", warmup_start.elapsed());
            return Ok(());
        }

        info!("🌡️ [DextradesService] Warming up with {} tokens on chain {}", warmup_tokens.len(), chain_id);
        
        // Parallel warmup to populate connection pool quickly
        let warmup_tasks: Vec<_> = warmup_tokens.into_iter()
            .map(|addr| {
                let service = self.clone();
                async move {
                    let result = service.get_token_metadata_with_chunk_id(&addr, "WARMUP").await;
                    match result {
                        Ok(Some(token_info)) => {
                            debug!("✅ [Warmup] {} -> {}", addr, token_info.symbol);
                        },
                        _ => {
                            debug!("⚠️ [Warmup] {} -> failed (expected during warmup)", addr);
                        }
                    }
                }
            })
            .collect();
        
        // Wait for all warmup requests (ignore individual results)
        futures::future::join_all(warmup_tasks).await;
        
        // Progressive delay to allow connection pool settling
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        
        let warmup_duration = warmup_start.elapsed();
        info!("✅ [DextradesService] Global warmup completed in {:?}", warmup_duration);
        
        Ok(())
    }

    /// Get token metadata for a single address with enhanced retry and caching
    pub async fn get_token_metadata(&self, address: &str) -> DextradesResult<Option<TokenInfo>> {
        self.get_token_metadata_with_chunk_id(address, "DIRECT_API").await
    }

    /// Get token metadata with chunk tracking for debugging
    pub async fn get_token_metadata_with_chunk_id(&self, address: &str, chunk_id: &str) -> DextradesResult<Option<TokenInfo>> {
        let addr = parse_address(address)?;
        
        // Call token metadata service and trace the result
        let result = self.token_metadata_service.get_token_metadata(addr).await;
        
        // Process and trace the result  
        let processed_result = match result {
            Ok(metadata) => {
                let token_info = TokenInfo {
                    name: metadata.name.unwrap_or_else(|| "Unknown".to_string()),
                    symbol: metadata.symbol.unwrap_or_else(|| "???".to_string()),
                    decimals: metadata.decimals.unwrap_or(18),
                };
                
                let success_result = Ok(Some(token_info));
                
                success_result
            }
            Err(e) => {
                // Reduce severity for warmup failures to avoid noisy startup logs
                if chunk_id == "WARMUP" {
                    debug!("[Warmup] Token metadata failed for {} (chunk {}): {}", address, chunk_id, e);
                } else {
                    error!("🚨 Token metadata failed for {} (chunk {}): {}", address, chunk_id, e);
                }
                Err(e.into())
            }
        };
        
        processed_result
    }

    /// Get pool tokens for a single pool address
    pub async fn get_pool_tokens(
        &self,
        address: &str,
    ) -> DextradesResult<Option<(String, String)>> {
        let addr = parse_address(address)?;

        // Use the protocol-specific function to get pool tokens
        match uniswap_v2::get_pool_tokens(&self.rpc, addr).await {
            Ok((token0, token1)) => Ok(Some((token0.to_string(), token1.to_string()))),
            Err(_) => Ok(None), // Pool doesn't exist or isn't a valid Uniswap V2 pool
        }
    }

    /// Get chain ID
    pub async fn get_chain_id(&self) -> DextradesResult<u64> {
        Ok(self.rpc.get_chain_id().await?)
    }

    /// Get current block number
    pub async fn get_block_number(&self) -> DextradesResult<u64> {
        Ok(self.rpc.get_block_number().await?)
    }

    /// Get logs for a given filter
    pub async fn get_logs(
        &self,
        from_block: u64,
        to_block: u64,
        address: Option<&str>,
    ) -> DextradesResult<Vec<(String, u64, String)>> {
        let mut filter = alloy::rpc::types::Filter::new()
            .from_block(from_block)
            .to_block(to_block);

        if let Some(addr_str) = address {
            let addr = parse_address(addr_str)?;
            filter = filter.address(addr);
        }

        let logs = self.rpc.get_logs(&filter).await?;

        let log_data = logs
            .into_iter()
            .map(|log| {
                (
                    log.transaction_hash.unwrap_or_default().to_string(),
                    log.block_number.unwrap_or_default(),
                    format!("{:?}", log.data()),
                )
            })
            .collect();

        Ok(log_data)
    }

    /// Get the underlying RPC service (for streaming operations)
    pub fn rpc_service(&self) -> &RpcOrchestrator {
        &self.rpc
    }

    /// Get block timestamp for a given block number
    pub async fn get_block_timestamp(&self, block_number: u64) -> DextradesResult<Option<i64>> {
        match self.rpc.get_block_by_number(block_number).await {
            Ok(Some(block)) => {
                // Convert timestamp to i64 - the timestamp is a u64 in seconds since epoch
                Ok(Some(block.header.timestamp as i64))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get transaction details for a given transaction hash
    pub async fn get_tx_details(&self, tx_hash: String) -> DextradesResult<TxDetails> {
        let tx_hash_parsed = tx_hash
            .parse()
            .map_err(|_| eyre::eyre!("Invalid transaction hash: {}", tx_hash))?;

        let tx = self.rpc.get_transaction_by_hash(tx_hash_parsed).await?;

        if let Some(transaction) = tx {
            // Extract from address using the signer method
            let tx_from = Some(transaction.inner.signer().to_string());

            // Extract to address using the transaction's to field
            // The transaction has a `to` field that returns Option<TxKind>
            let tx_to = transaction.inner.to().map(|addr| addr.to_string());

            Ok(TxDetails {
                tx_from,
                tx_to,
                gas_used: None, // TODO: Get from transaction receipt
            })
        } else {
            Ok(TxDetails {
                tx_from: None,
                tx_to: None,
                gas_used: None,
            })
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &DextradesConfig {
        &self.config
    }

    /// Get a snapshot of current RPC load metrics
    pub async fn get_rpc_metrics(&self) -> crate::rpc_rate_limiter::RpcLoadMetrics {
        self.rpc.get_rpc_load_metrics().await
    }

    /// Record stage counters (best-effort)
    pub fn record_extracted(&self, count: u64) { self.stats.events_extracted.fetch_add(count, Ordering::Relaxed); }
    pub fn record_enriched(&self, count: u64) { self.stats.events_enriched.fetch_add(count, Ordering::Relaxed); }
    pub fn record_batch_emitted(&self, rows: u64) { self.stats.batches_emitted.fetch_add(1, Ordering::Relaxed); self.stats.rows_emitted.fetch_add(rows, Ordering::Relaxed); }
    pub fn record_enrichment_error(&self) { self.stats.enrichment_errors.fetch_add(1, Ordering::Relaxed); }

    /// Get pipeline/service statistics snapshot
    pub fn get_stats_snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            events_extracted: self.stats.events_extracted.load(Ordering::Relaxed),
            events_enriched: self.stats.events_enriched.load(Ordering::Relaxed),
            batches_emitted: self.stats.batches_emitted.load(Ordering::Relaxed),
            rows_emitted: self.stats.rows_emitted.load(Ordering::Relaxed),
            enrichment_errors: self.stats.enrichment_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct GlobalStats {
    events_extracted: AtomicU64,
    events_enriched: AtomicU64,
    batches_emitted: AtomicU64,
    rows_emitted: AtomicU64,
    enrichment_errors: AtomicU64,
}

#[derive(Clone)]
pub struct StatsSnapshot {
    pub events_extracted: u64,
    pub events_enriched: u64,
    pub batches_emitted: u64,
    pub rows_emitted: u64,
    pub enrichment_errors: u64,
}

/// Simple token information struct
#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

impl TokenInfo {
    pub fn as_tuple(&self) -> (String, String, u8) {
        (self.name.clone(), self.symbol.clone(), self.decimals)
    }
}

/// Transaction details struct
#[derive(Debug, Clone)]
pub struct TxDetails {
    pub tx_from: Option<String>,
    pub tx_to: Option<String>,
    pub gas_used: Option<u64>,
}
use alloy::consensus::Transaction;
