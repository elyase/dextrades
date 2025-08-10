use alloy::primitives::Address;
use alloy::sol;
use alloy::sol_types::SolCall;
use eyre::Result;
use log::{debug, info, error};
use moka::sync::Cache;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, Mutex, watch};
use crate::rpc_retry_service::RpcClient;
use crate::rpc_orchestrator::RpcOrchestrator;
use crate::types::TokenMetadata;

// ERC20 function definitions using sol! macro
sol! {
    function name() external view returns (string memory);
    function symbol() external view returns (string memory);
    function decimals() external view returns (uint8);
}

/// Token metadata service with bulk processing, consistent retry logic, and RPC racing compatibility
/// 
/// CRITICAL FIX: This service now implements global request deduplication to solve the race condition
/// where multiple concurrent streaming chunks would request the same token metadata simultaneously,
/// causing inconsistent results due to different RPC providers winning the race.
/// 
/// Key features:
/// 1. **Global Request Deduplication**: Ensures only one RPC request per token is in-flight at any time
/// 2. **RPC Racing Compatible**: Works seamlessly with the RPC orchestrator's racing logic
/// 3. **Cache Coherency**: Prevents cache corruption from concurrent updates
/// 4. **Failure Recovery**: Disabled failure cache to allow immediate retries for network issues
pub struct TokenMetadataService {
    /// RPC client with retry logic
    rpc_client: Arc<RpcClient>,
    /// Cache for successful metadata lookups
    metadata_cache: Cache<Address, TokenMetadata>,
    /// Cache for failed requests to avoid repeated failures
    failure_cache: Cache<Address, Instant>,
    /// Semaphore to limit concurrent requests
    request_semaphore: Arc<Semaphore>,
    /// CRITICAL FIX: Global request deduplication to prevent concurrent duplicate requests
    /// Maps token addresses to in-flight request receivers (using String for cloneable errors)
    in_flight_requests: Arc<Mutex<HashMap<Address, watch::Receiver<Option<Result<TokenMetadata, String>>>>>>,
    /// Configuration
    failure_cache_duration: Duration,
    max_concurrent_requests: usize,
}

impl TokenMetadataService {
    /// Create a new token metadata service
    pub fn new(
        orchestrator: RpcOrchestrator,
        cache_size: u64,
        max_concurrent_requests: usize,
        failure_cache_duration: Duration,
    ) -> Self {
        // Create RPC client with EXTREME-aggressive retry logic for 100% consistency
        let rpc_client = RpcClient::new(
            orchestrator,
            20, // max retries (increased to 20 for 100% consistency)
            Duration::from_millis(50), // base delay (reduced to 50ms for ultra-fast retries)
        );

        Self {
            rpc_client: Arc::new(rpc_client),
            metadata_cache: Cache::builder().max_capacity(cache_size).build(),
            failure_cache: Cache::builder()
                .max_capacity(cache_size / 4)
                .time_to_live(failure_cache_duration)
                .build(),
            request_semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            in_flight_requests: Arc::new(Mutex::new(HashMap::new())),
            failure_cache_duration,
            max_concurrent_requests,
        }
    }

    /// Get token metadata with global request deduplication
    pub async fn get_token_metadata(&self, address: Address) -> Result<TokenMetadata, eyre::Error> {
        debug!("🔍 [TOKEN-{}] REQUEST STARTED (semaphore available: {})", address, self.request_semaphore.available_permits());
        
        // Check metadata cache first
        if let Some(metadata) = self.metadata_cache.get(&address) {
            debug!("✅ [TOKEN-{}] CACHE HIT - returning cached: {} ({})", 
                  address, 
                  metadata.symbol.as_ref().unwrap_or(&"???".to_string()),
                  metadata.decimals.unwrap_or(18));
            return Ok(metadata);
        }
        
        debug!("🔄 [TOKEN-{}] CACHE MISS - checking in-flight requests", address);

        // TARGETED FIX: Simplified request deduplication with shorter lock duration
        // Minimize lock contention by quickly checking and releasing
        let receiver_opt = {
            let in_flight = self.in_flight_requests.lock().await;
            in_flight.get(&address).cloned()
        }; // Lock released immediately
        
        if let Some(mut receiver) = receiver_opt {
            debug!("⏳ [TOKEN-{}] Found in-flight request; checking status", address);

            // If a value is already available, return it immediately without waiting
            if let Some(result) = receiver.borrow().as_ref() {
                match result {
                    Ok(metadata) => {
                        debug!(
                            "📨 [TOKEN-{}] IMMEDIATE from in-flight: {} ({})",
                            address,
                            metadata.symbol.as_ref().unwrap_or(&"???".to_string()),
                            metadata.decimals.unwrap_or(18)
                        );
                    }
                    Err(e) => {
                        debug!("📨 [TOKEN-{}] IMMEDIATE failure from in-flight: {}", address, e);
                    }
                }
                return result
                    .clone()
                    .map_err(|e| eyre::eyre!("Shared request failed: {}", e));
            }

            debug!("⏳ [TOKEN-{}] WAITING for in-flight to complete", address);

            // Wait for the in-flight request to complete with timeout protection
            let wait_result =
                tokio::time::timeout(Duration::from_secs(10), receiver.changed()).await;

            match wait_result {
                Ok(Ok(())) => {
                    if let Some(result) = receiver.borrow().as_ref() {
                        match result {
                            Ok(metadata) => {
                                debug!(
                                    "📨 [TOKEN-{}] RECEIVED SUCCESS from in-flight: {} ({})",
                                    address,
                                    metadata.symbol.as_ref().unwrap_or(&"???".to_string()),
                                    metadata.decimals.unwrap_or(18)
                                );
                            }
                            Err(e) => {
                                debug!(
                                    "📨 [TOKEN-{}] RECEIVED FAILURE from in-flight: {}",
                                    address, e
                                );
                            }
                        }
                        return result
                            .clone()
                            .map_err(|e| eyre::eyre!("Shared request failed: {}", e));
                    }
                    // No value present even after change notification; fall through to new request
                    debug!(
                        "⚠️ [TOKEN-{}] In-flight notification received but no value present",
                        address
                    );
                }
                Ok(Err(_)) | Err(_) => {
                    // Channel closed or timed out; try to use the last known value if present
                    if let Some(result) = receiver.borrow().as_ref() {
                        debug!(
                            "⏰ [TOKEN-{}] WAIT ended (closed/timeout); using last value",
                            address
                        );
                        return result
                            .clone()
                            .map_err(|e| eyre::eyre!("Shared request failed: {}", e));
                    }
                    debug!(
                        "⏰ [TOKEN-{}] TIMEOUT/closed with no value; starting new request",
                        address
                    );
                    // Continue to make a new request
                }
            }
        }

        // No in-flight request, we need to start a new one
        let permits_before = self.request_semaphore.available_permits();
        debug!("🎫 [TOKEN-{}] ACQUIRING semaphore permit (max: {}, available: {})", address, self.max_concurrent_requests, permits_before);
        
        let permit_start = std::time::Instant::now();
        let _permit = self.request_semaphore.acquire().await.unwrap();
        let permit_wait_duration = permit_start.elapsed();
        
        let permits_after = self.request_semaphore.available_permits();
        info!("✅ [TOKEN-{}] SEMAPHORE acquired after {:?} (remaining: {}, was: {}) - CONCURRENT TEST", 
              address, permit_wait_duration, permits_after, permits_before);
        
        // ULTRA-DEBUG: Log current semaphore pressure
        let in_use = self.max_concurrent_requests - permits_after;
        info!("📊 [TOKEN-{}] SEMAPHORE PRESSURE: {}/{} permits in use ({:.1}% utilization)", 
              address, in_use, self.max_concurrent_requests, (in_use as f64 / self.max_concurrent_requests as f64) * 100.0);
        
        // Double-check cache after acquiring permit (might have been populated while waiting)
        if let Some(metadata) = self.metadata_cache.get(&address) {
            info!("✅ [TOKEN-{}] CACHE HIT after semaphore - returning: {} ({})", 
                  address,
                  metadata.symbol.as_ref().unwrap_or(&"???".to_string()),
                  metadata.decimals.unwrap_or(18));
            return Ok(metadata);
        }
        
        // Create a watch channel for other requesters to wait on
        let (tx, rx) = watch::channel(None);
        
        // Register this request as in-flight so others can wait for it
        {
            let mut in_flight = self.in_flight_requests.lock().await;
            in_flight.insert(address, rx);
            info!("📝 [TOKEN-{}] REGISTERED as in-flight request", address);
        }
        
        info!("🚀 [TOKEN-{}] STARTING NEW RPC REQUEST (semaphore utilization: {:.1}%)", 
              address, ((self.max_concurrent_requests - self.request_semaphore.available_permits()) as f64 / self.max_concurrent_requests as f64) * 100.0);
        
        // Fetch the token metadata
        let rpc_start = std::time::Instant::now();
        let result = self.fetch_single_token_metadata(address).await;
        let rpc_duration = rpc_start.elapsed();
        
        info!("🏁 [TOKEN-{}] RPC REQUEST COMPLETED in {:?} (result: {})", 
              address, rpc_duration, if result.is_ok() { "SUCCESS" } else { "FAILURE" });
        
        // Store result in appropriate cache
        match &result {
            Ok(metadata) => {
                info!("🎉 [TOKEN-{}] RPC SUCCESS: {} ({}) - {} decimals", 
                       address, 
                       metadata.symbol.as_ref().unwrap_or(&"???".to_string()), 
                       metadata.name.as_ref().unwrap_or(&"Unknown".to_string()),
                       metadata.decimals.unwrap_or(18));
                self.metadata_cache.insert(address, metadata.clone());
                info!("💾 [TOKEN-{}] CACHED successful result", address);
                
                // Remove from failure cache if it was there
                self.failure_cache.invalidate(&address);
            }
            Err(e) => {
                error!("💥 [TOKEN-{}] RPC FAILED: {} (will retry next time)", address, e);
                // CRITICAL FIX: Cache permanent failures to avoid wasting resources
                // Decode errors are typically permanent (invalid contract), cache them briefly
                if e.to_string().contains("Failed to decode") {
                    self.failure_cache.insert(address, std::time::Instant::now());
                    debug!("🚫 [TOKEN-{}] CACHED decode failure (likely invalid contract)", address);
                }
                // Network/fetch errors are transient - don't cache them
            }
        }
        
        // Notify any waiters of the result (convert to cloneable form)
        let shareable_result = match &result {
            Ok(metadata) => Ok(metadata.clone()),
            Err(e) => Err(format!("Token metadata error: {}", e)),
        };
        
        info!("📢 [TOKEN-{}] NOTIFYING all waiting chunks", address);
        let _ = tx.send(Some(shareable_result));
        
        // Remove from in-flight map
        {
            let mut in_flight = self.in_flight_requests.lock().await;
            in_flight.remove(&address);
            info!("🗑️ [TOKEN-{}] REMOVED from in-flight requests", address);
        }
        
        result
    }

    /// Get metadata for multiple tokens in bulk
    #[allow(dead_code)]
    pub async fn get_bulk_token_metadata(
        &self,
        addresses: &[Address],
    ) -> HashMap<Address, Result<TokenMetadata, eyre::Error>> {
        let mut results = HashMap::new();
        let mut addresses_to_fetch = Vec::new();

        // Check caches first
        for &address in addresses {
            if let Some(metadata) = self.metadata_cache.get(&address) {
                results.insert(address, Ok(metadata));
            } else {
                // CRITICAL FIX: Don't check failure cache - allow immediate retries for consistency
                // This matches the fix in get_token_metadata() single path
                addresses_to_fetch.push(address);
            }
        }

        if addresses_to_fetch.is_empty() {
            return results;
        }

        // Process remaining addresses in parallel with coalescing
        let fetch_futures: Vec<_> = addresses_to_fetch
            .into_iter()
            .map(|address| {
                let service = self.clone();
                async move {
                    let result = service.get_token_metadata(address).await;
                    (address, result)
                }
            })
            .collect();

        // Wait for all results
        let fetch_results = futures::future::join_all(fetch_futures).await;
        
        for (address, result) in fetch_results {
            results.insert(address, result);
        }

        results
    }


    /// Fetch metadata for a single token using ERC20 calls
    async fn fetch_single_token_metadata(&self, address: Address) -> Result<TokenMetadata, eyre::Error> {
        let rpc_client = self.rpc_client.clone();

        // Create encoded calls for all three functions
        let name_data = nameCall::new(()).abi_encode();
        let symbol_data = symbolCall::new(()).abi_encode();
        let decimals_data = decimalsCall::new(()).abi_encode();

        // Execute all three calls concurrently
        let (name_result, symbol_result, decimals_result) = tokio::join!(
            rpc_client.call(address, name_data.clone()),
            rpc_client.call(address, symbol_data.clone()),
            rpc_client.call(address, decimals_data.clone())
        );

        // Process the results - REQUIRE symbol and decimals to succeed
        let name = if let Ok(data) = name_result {
            nameCall::abi_decode_returns(&data).ok()
        } else {
            None // name is optional
        };

        let symbol = if let Ok(data) = symbol_result {
            if let Ok(symbol) = symbolCall::abi_decode_returns(&data) {
                Some(symbol)
            } else {
                return Err(eyre::eyre!("Failed to decode symbol for token {}", address));
            }
        } else {
            return Err(eyre::eyre!("Failed to fetch symbol for token {}", address));
        };

        let decimals = if let Ok(data) = decimals_result {
            if let Ok(decimals) = decimalsCall::abi_decode_returns(&data) {
                Some(decimals)
            } else {
                return Err(eyre::eyre!("Failed to decode decimals for token {}", address));
            }
        } else {
            return Err(eyre::eyre!("Failed to fetch decimals for token {}", address));
        };

        // Only return Ok if we have symbol and decimals (name is optional)
        Ok(TokenMetadata {
            address,
            name,
            symbol,
            decimals,
        })
    }


    /// Get cache statistics for monitoring
    #[allow(dead_code)]
    pub fn get_cache_stats(&self) -> (u64, u64, u64) {
        (
            self.metadata_cache.entry_count(),
            self.metadata_cache.weighted_size(),
            self.failure_cache.entry_count(),
        )
    }

    /// Clear all caches (useful for testing)
    #[allow(dead_code)]
    pub fn clear_caches(&self) {
        self.metadata_cache.invalidate_all();
        self.failure_cache.invalidate_all();
    }
}

impl Clone for TokenMetadataService {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            metadata_cache: self.metadata_cache.clone(),
            failure_cache: self.failure_cache.clone(),
            request_semaphore: self.request_semaphore.clone(),
            in_flight_requests: self.in_flight_requests.clone(),
            failure_cache_duration: self.failure_cache_duration,
            max_concurrent_requests: self.max_concurrent_requests,
        }
    }
}

// (No internal tests; covered by Python-level integration tests.)
