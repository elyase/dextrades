use alloy::primitives::{Address, Bytes};
use alloy::primitives::B256;
use alloy::providers::{layers::CallBatchLayer, Provider, ProviderBuilder};
use alloy::rpc::types::{Filter, Log, TransactionRequest};
use alloy::sol;
use alloy::sol_types::SolCall;
use alloy::transports::http::reqwest::Url;
use eyre::Result;
use log::{debug, info, warn};
use moka::sync::Cache;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
// (semaphore removed for minimal orchestrator)
use tokio::time::timeout;
use futures::StreamExt;

use crate::circuit_breaker::CircuitBreaker;
use crate::config::DextradesConfig;
use crate::rpc_rate_limiter::{GlobalRpcMetrics, RpcProviderLimits, RpcLoadMetrics};
use crate::types::TokenMetadata;

// ERC20 function definitions using sol! macro
sol! {
    function name() external view returns (string memory);
    function symbol() external view returns (string memory);
    function decimals() external view returns (uint8);
}

/// A managed RPC provider with health scoring and circuit breaker
pub struct ManagedProvider {
    provider: Arc<dyn Provider + Send + Sync>,
    url: String,
    circuit_breaker: CircuitBreaker,
    health_score: AtomicUsize,
    requires_address_for_logs: AtomicBool,
}

impl std::fmt::Debug for ManagedProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedProvider")
            .field("url", &self.url)
            .field("provider", &"<dyn Provider>")
            .field("circuit_breaker", &self.circuit_breaker)
            .field("health_score", &self.health_score.load(Ordering::Relaxed))
            .finish()
    }
}

/// RPC Orchestrator that provides true racing functionality
pub struct RpcOrchestrator {
    providers: Vec<Arc<ManagedProvider>>,
    cache: Cache<String, Vec<u8>>,
    config: DextradesConfig,
    global_rpc_metrics: Arc<GlobalRpcMetrics>,
}

impl Clone for RpcOrchestrator {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            cache: self.cache.clone(),
            config: self.config.clone(),
            global_rpc_metrics: self.global_rpc_metrics.clone(),
        }
    }
}

impl RpcOrchestrator {
    /// Create a new RpcOrchestrator using a full configuration
    pub async fn with_config(config: DextradesConfig) -> Result<Self, eyre::Error> {

        if config.default_rpc_urls.is_empty() {
            return Err(eyre::eyre!("At least one RPC URL must be provided"));
        }

        info!(
            "Creating RpcOrchestrator with {} RPC URL(s)",
            config.default_rpc_urls.len()
        );

        // Create providers for all RPC URLs
        let mut providers = Vec::new();
        let mut successful_connections = 0;

        for rpc_url in &config.default_rpc_urls {
            match Self::create_provider(rpc_url, &config).await {
                Ok(provider) => {
                    info!("Successfully connected to RPC at {}", rpc_url);
                    providers.push(Arc::new(provider));
                    successful_connections += 1;
                }
                Err(e) => {
                    warn!("Failed to connect to RPC at {}: {}", rpc_url, e);
                    // Continue trying other URLs instead of failing
                }
            }
        }

        if providers.is_empty() {
            return Err(eyre::eyre!(
                "Failed to connect to any of the provided RPC URLs"
            ));
        }

        info!(
            "Successfully connected to {}/{} RPC endpoints",
            successful_connections,
            config.default_rpc_urls.len()
        );

        let cache = Cache::builder().max_capacity(config.cache_size).build();

        let orchestrator = Self {
            providers,
            cache,
            config,
            global_rpc_metrics: Arc::new(GlobalRpcMetrics::new()),
        };

        // Test connection with the first available provider
        let chain_id = orchestrator.get_chain_id().await?;
        info!("Connected to chain ID: {}", chain_id);

        Ok(orchestrator)
    }

    /// Get a snapshot of current RPC load metrics
    pub async fn get_rpc_load_metrics(&self) -> RpcLoadMetrics {
        self.global_rpc_metrics.get_load_metrics().await
    }

    /// Backward-compatible constructor used by older call sites
    pub async fn new(
        rpc_urls: Vec<String>,
        max_concurrent_requests: usize,
        cache_size: u64,
    ) -> Result<Self, eyre::Error> {
        let config = DextradesConfig {
            default_rpc_urls: rpc_urls.clone(),
            max_concurrent_requests,
            cache_size,
            ..Default::default()
        };
        Self::with_config(config).await
    }

    /// Create an Alloy provider from an RPC URL
    async fn create_provider(
        rpc_url: &str,
        config: &DextradesConfig,
    ) -> Result<ManagedProvider, eyre::Error> {
        // Parse the URL
        let url = Url::parse(rpc_url)?;

        // Detect provider limits based on URL
        let provider_limits = RpcProviderLimits::from_url(rpc_url);
        info!("Configuring rate limiting for {}: {} CU/s", rpc_url, provider_limits.compute_units_per_second);

        // Create the provider with HTTP connection and call batching
        let provider_builder = ProviderBuilder::new();
        
        // Create provider with call batching layer
        let provider = provider_builder
            .layer(CallBatchLayer::new().wait(config.batch_delay))
            .connect_http(url);
            
        info!("Provider configured with limits: {} CU/s, {} retries", 
              provider_limits.compute_units_per_second, 
              provider_limits.max_retries);

        // Test the provider with a simple call and timeout
        match timeout(config.request_timeout, provider.get_chain_id()).await {
            Ok(result) => {
                result?; // Ensure the call succeeded
                
                // Create circuit breaker for this provider
                let circuit_breaker = CircuitBreaker::new(
                    config.circuit_breaker_failure_threshold,
                    config.circuit_breaker_recovery_timeout,
                    config.circuit_breaker_success_threshold,
                );
                
                Ok(ManagedProvider {
                    provider: Arc::new(provider),
                    url: rpc_url.to_string(),
                    circuit_breaker,
                    health_score: AtomicUsize::new(100), // Start with perfect health
                    requires_address_for_logs: AtomicBool::new(false),
                })
            }
            Err(_) => Err(eyre::eyre!("Timed out connecting to RPC at {}", rpc_url)),
        }
    }

    /// Get a specified number of healthy providers for racing
    fn get_healthy_providers(&self, count: usize) -> Vec<Arc<ManagedProvider>> {
        let mut healthy_providers = Vec::new();
        
        // Sort providers by health score (descending)
        let mut providers_with_scores: Vec<_> = self.providers.iter()
            .filter(|p| {
                // Only include providers that are not circuit broken and have decent health
                p.health_score.load(Ordering::Relaxed) > 20
            })
            .collect();
        
        providers_with_scores.sort_by_key(|p| std::cmp::Reverse(p.health_score.load(Ordering::Relaxed)));

        for provider in providers_with_scores.into_iter().take(count) {
            healthy_providers.push(provider.clone());
        }

        // If we don't have enough healthy providers, add remaining ones
        if healthy_providers.len() < count {
            for provider in &self.providers {
                if healthy_providers.len() >= count {
                    break;
                }
                if !healthy_providers.iter().any(|p| Arc::ptr_eq(p, provider)) {
                    healthy_providers.push(provider.clone());
                }
            }
        }

        healthy_providers
    }

    /// Execute operation with circuit breaker protection for a single provider
    async fn execute_with_circuit_breaker<F, T>(
        provider: &ManagedProvider, 
        operation: F, 
        operation_name: &str
    ) -> Result<T, eyre::Error>
    where
        F: std::future::Future<Output = Result<T, alloy::transports::RpcError<alloy::transports::TransportErrorKind>>>,
    {
        if !provider.circuit_breaker.should_allow_request().await {
            return Err(eyre::eyre!("Circuit breaker is open for {} ({})", provider.url, operation_name));
        }

        match operation.await {
            Ok(result) => {
                provider.circuit_breaker.record_success().await;
                provider.health_score.fetch_add(1, Ordering::Relaxed); // Improve health on success
                Ok(result)
            }
            Err(e) => {
                provider.circuit_breaker.record_failure().await;
                provider.health_score.fetch_sub(5, Ordering::Relaxed); // Decrease health on failure
                Err(eyre::eyre!("Failed to {} from {}: {}", operation_name, provider.url, e))
            }
        }
    }

    /// Race identical requests across multiple providers using select_ok with enhanced retry
    async fn race_request<T, F>(&self, operation_name: &str, request_factory: F) -> Result<T, eyre::Error>
    where
        T: Send + 'static,
        F: Fn(Arc<ManagedProvider>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, eyre::Error>> + Send>>,
    {
        use crate::rpc_retry_service::RpcRetryPolicy;
        use std::time::Duration;
        
        // Create retry policy for this operation
        let retry_policy = RpcRetryPolicy::new(3, Duration::from_millis(500));
        
        // Execute with retry logic
        retry_policy.execute_with_retry(|| {
            let request_factory = &request_factory;
            async move {
                // Get all healthy providers for true racing
                let race_count = self
                    .config
                    .providers_to_race
                    .max(1)
                    .min(self.providers.len());
                let providers_to_race = self.get_healthy_providers(race_count);

                if providers_to_race.is_empty() {
                    return Err(eyre::eyre!("No healthy providers available for racing"));
                }

                info!("🏁 Racing {} across {} providers simultaneously", operation_name, providers_to_race.len());
                let start_time = std::time::Instant::now();
                
                // Create concurrent racing tasks
                let racing_futures: Vec<_> = providers_to_race.into_iter()
                    .map(|provider| {
                        let request_future = request_factory(provider.clone());
                        tokio::spawn(async move {
                            request_future.await
                        })
                    })
                    .collect();
                
                // Race all requests - first successful response wins
                match futures::future::select_ok(racing_futures).await {
                    Ok((result, _remaining)) => {
                        let duration = start_time.elapsed();
                        info!("✅ Racing {} won in {:?}", operation_name, duration);
                        result
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        warn!("❌ All providers failed for {} after {:?}: {}", operation_name, duration, e);
                        Err(eyre::eyre!("All racing providers failed for {}: {}", operation_name, e))
                    }
                }
            }
        }).await
    }

    /// Try operation across multiple providers with failover
    #[allow(dead_code)]
    async fn try_with_fallback<T, F>(&self, operation_name: &str, operation: F) -> Result<T, eyre::Error>
    where
        F: Fn(Arc<ManagedProvider>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, eyre::Error>> + Send>>,
    {
        let providers_to_try = self.get_healthy_providers(self.providers.len());
        
        if providers_to_try.is_empty() {
            return Err(eyre::eyre!("No healthy providers available"));
        }

        let mut last_error = None;
        
        // Try each provider in order of health score
        for (i, provider) in providers_to_try.iter().enumerate() {
            let provider_start = std::time::Instant::now();
            info!("🚀 Trying {} with provider {} ({}/{})", operation_name, provider.url, i + 1, providers_to_try.len());
            
            match operation(provider.clone()).await {
                Ok(result) => {
                    let duration = provider_start.elapsed();
                    if i > 0 {
                        info!("✅ {} succeeded with fallback provider {} in {:?}", operation_name, provider.url, duration);
                    } else {
                        info!("✅ {} succeeded with primary provider {} in {:?}", operation_name, provider.url, duration);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    let duration = provider_start.elapsed();
                    warn!("❌ {} failed with {} after {:?}: {}", operation_name, provider.url, duration, e);
                    last_error = Some(e);
                    continue;
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| eyre::eyre!("All providers failed for {}", operation_name)))
    }

    /// Get current block number with racing
    pub async fn get_block_number(&self) -> Result<u64> {
        self.race_request("get_block_number", |provider| {
            Box::pin(async move {
                match provider.provider.get_block_number().await {
                    Ok(result) => {
                        provider.circuit_breaker.record_success().await;
                        provider.health_score.fetch_add(1, Ordering::Relaxed);
                        Ok(result)
                    }
                    Err(e) => {
                        provider.circuit_breaker.record_failure().await;
                        provider.health_score.fetch_sub(5, Ordering::Relaxed);
                        Err(eyre::eyre!("Failed to get block number from {}: {}", provider.url, e))
                    }
                }
            })
        }).await
    }

    /// Get chain ID with racing
    pub async fn get_chain_id(&self) -> Result<u64, eyre::Error> {
        self.race_request("get_chain_id", |provider| {
            Box::pin(async move {
                match provider.provider.get_chain_id().await {
                    Ok(result) => {
                        provider.circuit_breaker.record_success().await;
                        provider.health_score.fetch_add(1, Ordering::Relaxed);
                        Ok(result)
                    }
                    Err(e) => {
                        provider.circuit_breaker.record_failure().await;
                        provider.health_score.fetch_sub(5, Ordering::Relaxed);
                        Err(eyre::eyre!("Failed to get chain ID from {}: {}", provider.url, e))
                    }
                }
            })
        }).await
    }

    /// Get logs with fast-first-success semantics and provider capability filtering
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, eyre::Error> {
        // Record the request
        self.global_rpc_metrics.record_request("get_logs").await;

        // Build candidate providers
        let race_count = self
            .config
            .providers_to_race
            .max(1)
            .min(self.providers.len());
        let candidates = self.get_healthy_providers(race_count);

        if candidates.is_empty() {
            self.global_rpc_metrics.record_completion();
            return Err(eyre::eyre!("No providers available for get_logs"));
        }

        let filter_clone = filter.clone();
        let mut tasks = futures::stream::FuturesUnordered::new();
        for provider in candidates.into_iter() {
            let provider_cloned = provider.clone();
            let filter = filter_clone.clone();
            tasks.push(tokio::spawn(async move {
                // Execute request
                let res = provider_cloned.provider.get_logs(&filter).await;
                (provider_cloned, res)
            }));
        }

        // Wait for first success, track restrictions
        let mut last_err: Option<eyre::Error> = None;
        while let Some(joined) = tasks.next().await {
            match joined {
                Ok((provider, Ok(logs))) => {
                    provider.circuit_breaker.record_success().await;
                    provider.health_score.fetch_add(1, Ordering::Relaxed);
                    debug!("✅ get_logs: {} logs from {}", logs.len(), provider.url);
                    self.global_rpc_metrics.record_completion();
                    return Ok(logs);
                }
                Ok((provider, Err(e))) => {
                    provider.circuit_breaker.record_failure().await;
                    provider.health_score.fetch_sub(5, Ordering::Relaxed);
                    let es = e.to_string();
                    if es.contains("32701") || es.contains("Please specify an address") {
                        debug!("⚠️ Provider {} requires address for get_logs", provider.url);
                        provider
                            .requires_address_for_logs
                            .store(true, Ordering::Relaxed);
                    } else {
                        debug!("❌ get_logs failed from {}: {}", provider.url, es);
                    }
                    last_err = Some(eyre::eyre!("get_logs failed from {}: {}", provider.url, es));
                    continue;
                }
                Err(join_err) => {
                    last_err = Some(eyre::eyre!("get_logs join error: {}", join_err));
                    continue;
                }
            }
        }

        self.global_rpc_metrics.record_completion();
        Err(last_err.unwrap_or_else(|| eyre::eyre!("All providers failed for get_logs")))
    }

    /// Shard get_logs by splitting the block range into subranges and issuing parallel requests
    pub async fn get_logs_sharded(
        &self,
        event_signature: B256,
        address: Option<Address>,
        from_block: u64,
        to_block: u64,
        shards: usize,
    ) -> Result<Vec<Log>, eyre::Error> {
        let shards = shards.max(1);
        if from_block >= to_block || shards == 1 {
            let mut filter = Filter::new()
                .from_block(from_block)
                .to_block(to_block)
                .event_signature(event_signature);
            if let Some(addr) = address {
                filter = filter.address(addr);
            }
            return self.get_logs(&filter).await;
        }

        let total = to_block - from_block + 1;
        let chunk = (total as f64 / shards as f64).ceil() as u64;
        let mut tasks = futures::stream::FuturesUnordered::new();
        for i in 0..shards {
            let start = from_block + i as u64 * chunk;
            if start > to_block { break; }
            let end = std::cmp::min(to_block, start + chunk - 1);
            let mut filter = Filter::new()
                .from_block(start)
                .to_block(end)
                .event_signature(event_signature);
            if let Some(addr) = address {
                filter = filter.address(addr);
            }
            let filter_clone = filter.clone();
            let this = self.clone();
            tasks.push(tokio::spawn(async move { this.get_logs(&filter_clone).await }));
        }

        let mut all: Vec<Log> = Vec::new();
        while let Some(res) = tasks.next().await {
            match res {
                Ok(Ok(mut logs)) => all.append(&mut logs),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(eyre::eyre!("join error: {}", e)),
            }
        }
        Ok(all)
    }

    /// Get logs using provider-assigned shards strategy (each shard gets a dedicated provider)
    /// This provides near-linear scaling with the number of providers
    pub async fn get_logs_provider_assigned_shards(
        &self,
        event_signature: B256,
        address: Option<Address>,
        from_block: u64,
        to_block: u64,
        shards: usize,
    ) -> Result<Vec<Log>, eyre::Error> {
        let shards = shards.max(1).min(self.providers.len()); // Can't have more shards than providers
        
        if from_block >= to_block || shards == 1 {
            // Fall back to regular racing for single shard
            let mut filter = Filter::new()
                .from_block(from_block)
                .to_block(to_block)
                .event_signature(event_signature);
            if let Some(addr) = address {
                filter = filter.address(addr);
            }
            return self.get_logs(&filter).await;
        }

        let total = to_block - from_block + 1;
        let chunk = (total as f64 / shards as f64).ceil() as u64;
        let healthy_providers = self.get_healthy_providers(shards);
        
        info!(
            "🚀 Provider-assigned shards: {} shards across {} providers for {} blocks", 
            shards, healthy_providers.len(), total
        );

        let mut tasks = futures::stream::FuturesUnordered::new();
        
        for (i, provider) in healthy_providers.into_iter().enumerate() {
            if i >= shards { break; }
            
            let start = from_block + i as u64 * chunk;
            if start > to_block { break; }
            let end = std::cmp::min(to_block, start + chunk - 1);
            
            let mut filter = Filter::new()
                .from_block(start)
                .to_block(end)
                .event_signature(event_signature);
            if let Some(addr) = address {
                filter = filter.address(addr);
            }

            info!(
                "📦 Shard {}: blocks {}-{} assigned to provider {}",
                i, start, end, provider.url
            );

            // Execute directly on the assigned provider (no racing)
            let provider_clone = provider.clone();
            let filter_clone = filter.clone();
            tasks.push(tokio::spawn(async move {
                Self::execute_with_circuit_breaker(&provider_clone, 
                    provider_clone.provider.get_logs(&filter_clone), "get_logs_assigned").await
            }));
        }

        let mut all: Vec<Log> = Vec::new();
        while let Some(res) = tasks.next().await {
            match res {
                Ok(Ok(mut logs)) => {
                    debug!("📥 Shard completed with {} logs", logs.len());
                    all.append(&mut logs);
                }
                Ok(Err(e)) => {
                    warn!("⚠️ Shard failed: {}, continuing with other shards", e);
                    // Don't fail the entire request if one shard fails
                    // This provides graceful degradation
                }
                Err(e) => return Err(eyre::eyre!("join error: {}", e)),
            }
        }
        
        info!("✅ Provider-assigned shards completed: {} total logs", all.len());
        Ok(all)
    }

    /// Make a contract call with racing
    pub async fn call(&self, to: Address, data: Vec<u8>) -> Result<Vec<u8>, eyre::Error> {
        // Record the request
        self.global_rpc_metrics.record_request("contract_call").await;
        
        let data_clone = data.clone();
        let result = self.race_request("contract_call", |provider| {
            let data = data_clone.clone();
            Box::pin(async move {
                // Create a transaction request for the call
                let tx = TransactionRequest {
                    to: Some(alloy::primitives::TxKind::Call(to)),
                    input: Bytes::from(data).into(),
                    ..Default::default()
                };

                match provider.provider.call(tx).await {
                    Ok(result) => {
                        provider.circuit_breaker.record_success().await;
                        provider.health_score.fetch_add(1, Ordering::Relaxed);
                        debug!("✅ call {} from {}: {} bytes", to, provider.url, result.0.len());
                        Ok(result.0.to_vec())
                    }
                    Err(e) => {
                        provider.circuit_breaker.record_failure().await;
                        provider.health_score.fetch_sub(5, Ordering::Relaxed);
                        debug!("❌ call {} failed from {}: {}", to, provider.url, e);
                        Err(eyre::eyre!("Failed to call contract from {}: {}", provider.url, e))
                    }
                }
            })
        }).await;

        // Record completion
        self.global_rpc_metrics.record_completion();
        result
    }

    /// Get transaction by hash with racing
    pub async fn get_transaction_by_hash(
        &self,
        hash: alloy::primitives::TxHash,
    ) -> Result<Option<alloy::rpc::types::Transaction>, eyre::Error> {
        self.race_request("get_transaction_by_hash", |provider| {
            Box::pin(async move {
                match provider.provider.get_transaction_by_hash(hash).await {
                    Ok(result) => {
                        provider.circuit_breaker.record_success().await;
                        provider.health_score.fetch_add(1, Ordering::Relaxed);
                        Ok(result)
                    }
                    Err(e) => {
                        provider.circuit_breaker.record_failure().await;
                        provider.health_score.fetch_sub(5, Ordering::Relaxed);
                        Err(eyre::eyre!("Failed to get transaction by hash from {}: {}", provider.url, e))
                    }
                }
            })
        }).await
    }

    /// Get block by number with true racing
    pub async fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<Option<alloy::rpc::types::Block>, eyre::Error> {
        self.race_request("get_block_by_number", |provider| {
            Box::pin(async move {
                match provider.provider.get_block_by_number(block_number.into()).await {
                    Ok(result) => {
                        provider.circuit_breaker.record_success().await;
                        provider.health_score.fetch_add(1, Ordering::Relaxed);
                        debug!("✅ get_block_by_number {} from {}", block_number, provider.url);
                        Ok(result)
                    }
                    Err(e) => {
                        provider.circuit_breaker.record_failure().await;
                        provider.health_score.fetch_sub(5, Ordering::Relaxed);
                        debug!("❌ get_block_by_number {} failed from {}: {}", block_number, provider.url, e);
                        Err(eyre::eyre!("Failed to get block by number from {}: {}", provider.url, e))
                    }
                }
            })
        }).await
    }

    /// Get token metadata with racing and caching
    pub async fn get_token_metadata(
        &self,
        addresses: &[Address],
    ) -> Result<HashMap<Address, TokenMetadata>, eyre::Error> {
        let mut results_map: HashMap<Address, TokenMetadata> = HashMap::new();
        let mut addresses_to_fetch: Vec<Address> = Vec::new();

        // Check cache first
        for &addr in addresses {
            let cache_key = format!("token:{}", addr);
            if let Some(cached_bytes) = self.cache.get(&cache_key) {
                match serde_json::from_slice::<TokenMetadata>(&cached_bytes) {
                    Ok(metadata) => {
                        results_map.insert(addr, metadata);
                        continue;
                    }
                    Err(_) => {
                        // Cache data is corrupted, will fetch fresh
                    }
                }
            }
            addresses_to_fetch.push(addr);
        }

        if addresses_to_fetch.is_empty() {
            return Ok(results_map);
        }

        // Process each token with racing
        for &addr in &addresses_to_fetch {
            let mut name: Option<String> = None;
            let mut symbol: Option<String> = None;
            let mut decimals: Option<u8> = None;

            // Create the call data for each function
            let name_data = nameCall::new(()).abi_encode();
            let symbol_data = symbolCall::new(()).abi_encode();
            let decimals_data = decimalsCall::new(()).abi_encode();

            // Race the three calls concurrently
            let (name_result, symbol_result, decimals_result) = tokio::join!(
                self.call(addr, name_data),
                self.call(addr, symbol_data),
                self.call(addr, decimals_data)
            );

            // Process the results
            if let Ok(data) = name_result {
                if let Ok(decoded) = nameCall::abi_decode_returns(&data) {
                    name = Some(decoded);
                }
            }

            if let Ok(data) = symbol_result {
                if let Ok(decoded) = symbolCall::abi_decode_returns(&data) {
                    symbol = Some(decoded);
                }
            }

            if let Ok(data) = decimals_result {
                if let Ok(decoded) = decimalsCall::abi_decode_returns(&data) {
                    decimals = Some(decoded);
                }
            }

            // Create token metadata
            let metadata = TokenMetadata {
                address: addr,
                name,
                symbol,
                decimals,
            };

            // Update cache
            let cache_key = format!("token:{}", addr);
            if let Ok(bytes) = serde_json::to_vec(&metadata) {
                self.cache.insert(cache_key, bytes);
            }

            results_map.insert(addr, metadata);
        }

        Ok(results_map)
    }

    /// Get current RPC metrics for monitoring
    pub async fn get_rpc_metrics(&self) -> crate::rpc_rate_limiter::RpcLoadMetrics {
        self.global_rpc_metrics.get_load_metrics().await
    }

    /// Check if system is under high RPC load
    pub async fn is_under_high_load(&self) -> bool {
        self.global_rpc_metrics.is_under_high_load().await
    }

    /// Reset RPC metrics (useful for monitoring periods)
    pub async fn reset_rpc_metrics(&self) {
        self.global_rpc_metrics.reset_metrics().await
    }
}
