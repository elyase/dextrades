use std::time::Duration;

/// Streaming optimization modes for different use cases
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingMode {
    /// Immediate results: small batches, low concurrency for real-time streaming
    Immediate,
    /// Balanced: medium batches, medium concurrency for good balance
    Balanced,
    /// Throughput: large batches, high concurrency for maximum speed
    Throughput,
    /// Custom: use explicit batch_size and concurrency settings
    Custom,
}

/// Provider strategy for handling multiple RPC endpoints
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderStrategy {
    /// Race providers - all providers compete for each request (higher redundancy, more requests)
    Race,
    /// Shard assignment - assign specific providers to specific shards (better scaling, fewer requests)
    Shard,
}

/// Optional per-network overrides for pricing and warmup
#[derive(Debug, Clone, Default)]
pub struct NetworkOverrides {
    /// Wrapped native token address (e.g., WETH, wCAMP)
    pub native_wrapped: Option<String>,
    /// Addresses to treat as USD stables on this network
    pub stable_addresses: Vec<String>,
    /// Chainlink-style native/USD aggregator address for this network
    pub native_usd_aggregator: Option<String>,
    /// Addresses to warm up at session start (should exist on current chain)
    pub warmup_tokens: Vec<String>,
}

/// Configuration for the Dextrades library
#[derive(Debug, Clone)]
pub struct DextradesConfig {
    /// Default RPC URLs to use when none are provided
    pub default_rpc_urls: Vec<String>,
    /// Maximum number of concurrent RPC requests
    pub max_concurrent_requests: usize,
    /// Cache size for metadata
    pub cache_size: u64,
    /// Default batch size for streaming
    pub batch_size: u64,
    /// Request timeout duration
    pub request_timeout: Duration,
    /// Delay between batches to prevent rate limiting
    pub batch_delay: Duration,
    /// Maximum batch size for metadata fetching
    pub metadata_batch_size: usize,
    /// Maximum number of retries for transient errors before failover
    pub max_retries: usize,
    /// Base delay for exponential backoff retries
    pub retry_base_delay: Duration,
    /// Maximum delay for exponential backoff retries
    pub retry_max_delay: Duration,
    
    // Circuit breaker configuration
    /// Number of consecutive failures before circuit opens
    pub circuit_breaker_failure_threshold: usize,
    /// Duration to keep circuit open before attempting recovery
    pub circuit_breaker_recovery_timeout: Duration,
    /// Number of successful requests needed to close circuit
    pub circuit_breaker_success_threshold: usize,
    
    // Filtering configuration
    /// Minimum token volume to include in results (in USD)
    pub min_volume_filter: Option<f64>,
    /// Specific tokens to include (if None, include all)
    pub token_whitelist: Option<Vec<String>>,
    /// Specific tokens to exclude
    pub token_blacklist: Option<Vec<String>>,
    
    // Streaming optimization configuration
    /// Streaming optimization mode (immediate/balanced/throughput/custom)
    pub streaming_mode: StreamingMode,
    /// Number of blocks to process per batch (used when streaming_mode = Custom)
    pub streaming_batch_size: u64,
    /// Maximum number of concurrent batches in flight
    pub max_concurrent_batches: usize,
    /// Maximum number of providers to race per request
    pub providers_to_race: usize,
    /// Enable sharding of log requests by splitting block range into subranges
    pub shard_logs: bool,
    /// Provider strategy for handling multiple RPC endpoints
    pub provider_strategy: ProviderStrategy,
    /// Optional per-network overrides (pricing, warmup)
    pub network_overrides: Option<NetworkOverrides>,
    /// Optional whitelist of router addresses (tx.to must match one of these)
    pub router_whitelist: Option<Vec<String>>,
}

impl Default for DextradesConfig {
    fn default() -> Self {
        Self {
            // BENCHMARK-OPTIMIZED: Use only the 2 fastest RPC endpoints for optimal racing
            // Benchmark shows 2 providers (19.1 results/sec) > 3+ providers (12-15 results/sec)
            default_rpc_urls: vec![
                "https://eth-pokt.nodies.app".to_string(),           // 0.069s latency - fastest
                "https://ethereum-mainnet.gateway.tatum.io".to_string(), // 0.069s latency - fastest
            ],
            max_concurrent_requests: 10,
            cache_size: 1000,
            batch_size: 1,  // Process 1 block at a time for immediate results
            request_timeout: Duration::from_secs(2),  // Reduced from 10s to 2s for faster fallback
            batch_delay: Duration::from_millis(10),   // Reduced from 100ms to 10ms for faster batch processing
            metadata_batch_size: 5,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(5),
            
            // Circuit breaker defaults
            circuit_breaker_failure_threshold: 5,
            circuit_breaker_recovery_timeout: Duration::from_secs(30),
            circuit_breaker_success_threshold: 3,
            
            // Filtering defaults
            min_volume_filter: None,
            token_whitelist: None,
            token_blacklist: None,
            
            // TESTING: Moderate concurrency with targeted token cache fix
            streaming_mode: StreamingMode::Balanced,
            streaming_batch_size: 1,   // PROVEN OPTIMAL: batch size 1 beats all larger sizes
            max_concurrent_batches: 4, // TESTING: Back to proven sweet spot
            
            providers_to_race: 2, // Race top-2 providers by default
            shard_logs: false,
            provider_strategy: ProviderStrategy::Race, // Race by default for reliability
            network_overrides: None,
            router_whitelist: None,
        }
    }
}

/// Builder for fluent configuration
#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    config: DextradesConfig,
}

impl ConfigBuilder {
    /// Create a new ConfigBuilder with default values
    pub fn new() -> Self {
        Self {
            config: DextradesConfig::default(),
        }
    }
    
    /// Set RPC URLs
    pub fn rpc_urls(mut self, urls: Vec<String>) -> Self {
        self.config.default_rpc_urls = urls;
        self
    }
    
    /// Set maximum concurrent requests
    pub fn max_concurrent_requests(mut self, max: usize) -> Self {
        self.config.max_concurrent_requests = max;
        self
    }
    
    /// Set cache size
    pub fn cache_size(mut self, size: u64) -> Self {
        self.config.cache_size = size;
        self
    }
    
    /// Set batch size
    pub fn batch_size(mut self, size: u64) -> Self {
        self.config.batch_size = size;
        self
    }
    
    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }
    
    /// Set batch delay
    pub fn batch_delay(mut self, delay: Duration) -> Self {
        self.config.batch_delay = delay;
        self
    }
    
    /// Configure circuit breaker
    pub fn circuit_breaker(mut self, failure_threshold: usize, recovery_timeout: Duration, success_threshold: usize) -> Self {
        self.config.circuit_breaker_failure_threshold = failure_threshold;
        self.config.circuit_breaker_recovery_timeout = recovery_timeout;
        self.config.circuit_breaker_success_threshold = success_threshold;
        self
    }
    
    /// Set volume filter
    pub fn volume_filter(mut self, min_volume: f64) -> Self {
        self.config.min_volume_filter = Some(min_volume);
        self
    }
    
    /// Set token whitelist
    pub fn token_whitelist(mut self, whitelist: Vec<String>) -> Self {
        self.config.token_whitelist = Some(whitelist);
        self
    }
    
    /// Set token blacklist
    pub fn token_blacklist(mut self, blacklist: Vec<String>) -> Self {
        self.config.token_blacklist = Some(blacklist);
        self
    }
    
    /// Set the number of providers to race per request
    pub fn providers_to_race(mut self, count: usize) -> Self {
        self.config.providers_to_race = count.max(1);
        self
    }

    /// Enable or disable sharding of log requests (split block range into subranges)
    pub fn shard_logs(mut self, enable: bool) -> Self {
        self.config.shard_logs = enable;
        self
    }

    /// Set the provider strategy for handling multiple RPC endpoints
    pub fn provider_strategy(mut self, strategy: ProviderStrategy) -> Self {
        self.config.provider_strategy = strategy;
        self
    }

    /// Set per-network overrides (pricing, warmup)
    pub fn network_overrides(mut self, overrides: NetworkOverrides) -> Self {
        self.config.network_overrides = Some(overrides);
        self
    }

    /// Set a whitelist of router addresses to filter swaps by tx.to
    pub fn router_whitelist(mut self, routers: Vec<String>) -> Self {
        self.config.router_whitelist = Some(routers);
        self
    }
    
    /// Set streaming optimization mode
    pub fn streaming_mode(mut self, mode: StreamingMode) -> Self {
        self.config.streaming_mode = mode;
        self
    }
    
    /// Set custom streaming batch size (only used when streaming_mode = Custom)
    pub fn streaming_batch_size(mut self, size: u64) -> Self {
        self.config.streaming_batch_size = size;
        self.config.streaming_mode = StreamingMode::Custom;
        self
    }
    
    /// Set maximum concurrent batches in flight
    pub fn max_concurrent_batches(mut self, count: usize) -> Self {
        self.config.max_concurrent_batches = count;
        self
    }
    
    /// Build the final configuration
    pub fn build(self) -> DextradesConfig {
        self.config
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DextradesConfig {
    /// Create a new config builder
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }
    
    /// Create a new config with custom RPC URLs
    pub fn with_rpc_urls(rpc_urls: Vec<String>) -> Self {
        Self {
            default_rpc_urls: rpc_urls,
            ..Default::default()
        }
    }

    /// Create a new config with custom batch size
    pub fn with_batch_size(batch_size: u64) -> Self {
        Self {
            batch_size,
            ..Default::default()
        }
    }

    /// Create a new config with custom retry settings
    pub fn with_retry_config(
        max_retries: usize,
        base_delay: Duration,
        max_delay: Duration,
    ) -> Self {
        Self {
            max_retries,
            retry_base_delay: base_delay,
            retry_max_delay: max_delay,
            ..Default::default()
        }
    }

    /// Create a new config with circuit breaker settings
    pub fn with_circuit_breaker(
        failure_threshold: usize,
        recovery_timeout: Duration,
        success_threshold: usize,
    ) -> Self {
        Self {
            circuit_breaker_failure_threshold: failure_threshold,
            circuit_breaker_recovery_timeout: recovery_timeout,
            circuit_breaker_success_threshold: success_threshold,
            ..Default::default()
        }
    }

    // with_adaptive_batching removed for minimal core

    /// Create a new config with volume filtering
    pub fn with_volume_filter(min_volume: f64) -> Self {
        Self {
            min_volume_filter: Some(min_volume),
            ..Default::default()
        }
    }

    /// Create a new config with token filtering
    pub fn with_token_filter(
        whitelist: Option<Vec<String>>,
        blacklist: Option<Vec<String>>,
    ) -> Self {
        Self {
            token_whitelist: whitelist,
            token_blacklist: blacklist,
            ..Default::default()
        }
    }

    /// Create a new config optimized for immediate streaming results
    pub fn immediate_streaming() -> Self {
        Self {
            streaming_mode: StreamingMode::Immediate,
            streaming_batch_size: 1,    // Batch size 1 proven optimal
            max_concurrent_batches: 1,  // FIXED: Sequential processing to prevent state corruption
            ..Default::default()
        }
    }

    /// Create a new config optimized for balanced streaming performance
    pub fn balanced_streaming() -> Self {
        Self {
            streaming_mode: StreamingMode::Balanced,
            streaming_batch_size: 1,    // BENCHMARK PROVEN: batch size 1 optimal
            max_concurrent_batches: 4, // Use proven sweet spot concurrency
            ..Default::default()
        }
    }

    /// Create a new config optimized for maximum throughput
    pub fn throughput_streaming() -> Self {
        Self {
            streaming_mode: StreamingMode::Throughput,
            streaming_batch_size: 1,    // BENCHMARK PROVEN: even throughput mode uses batch size 1
            max_concurrent_batches: 1, // Sequential for guaranteed ordering
            ..Default::default()
        }
    }

    /// Get the effective batch size based on streaming mode
    pub fn effective_batch_size(&self) -> u64 {
        match self.streaming_mode {
            StreamingMode::Immediate => 1,
            StreamingMode::Balanced => 1,   // Optimal: small batches with RPC racing  
            StreamingMode::Throughput => 1, // Even throughput mode uses small batches now
            StreamingMode::Custom => self.streaming_batch_size,
        }
    }

    /// Get the effective max concurrent batches based on streaming mode
    pub fn effective_max_concurrent_batches(&self) -> usize {
        match self.streaming_mode {
            StreamingMode::Immediate => 12, // TESTING: High concurrency for immediate results
            StreamingMode::Balanced => self.max_concurrent_batches, // TESTING: Use configured value
            StreamingMode::Throughput => 1, // Keep sequential for guaranteed ordering
            StreamingMode::Custom => self.max_concurrent_batches,
        }
    }
}
