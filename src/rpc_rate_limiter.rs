/// Global RPC Rate Limiter Architecture
///
/// This module implements the hybrid approach recommended by Grok:
/// 1. Use Alloy's RetryBackoffLayer as the primary rate limiter
/// 2. Add lightweight application-level monitoring for enricher coordination
/// 3. Preserve decentralized enricher RPC batching strategies

use alloy::transports::layers::RetryBackoffLayer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Provider-specific rate limit configurations
/// Based on common RPC provider limits
#[derive(Debug, Clone)]
pub struct RpcProviderLimits {
    pub compute_units_per_second: u64,
    pub max_retries: u64,
    pub initial_backoff_ms: u64,
}

impl RpcProviderLimits {
    /// Infura configuration (3300 CU/s, use 80% = 2640)
    pub fn infura() -> Self {
        Self {
            compute_units_per_second: 2640, // 80% of 3300 CU/s
            max_retries: 5,
            initial_backoff_ms: 100,
        }
    }

    /// Alchemy configuration (similar to Infura)
    pub fn alchemy() -> Self {
        Self {
            compute_units_per_second: 2400, // Conservative estimate
            max_retries: 5,
            initial_backoff_ms: 100,
        }
    }

    /// Generic/Unknown provider - very conservative
    pub fn generic() -> Self {
        Self {
            compute_units_per_second: 1000, // Very conservative
            max_retries: 3,
            initial_backoff_ms: 200,
        }
    }

    /// Auto-detect provider from URL
    pub fn from_url(url: &str) -> Self {
        let url_lower = url.to_lowercase();
        if url_lower.contains("infura") {
            Self::infura()
        } else if url_lower.contains("alchemy") {
            Self::alchemy()
        } else {
            Self::generic()
        }
    }
}

/// Global RPC metrics for enricher coordination
/// This provides lightweight monitoring without centralized rate limiting
#[derive(Debug)]
pub struct GlobalRpcMetrics {
    /// Total RPC requests made across all enrichers
    total_requests: AtomicU64,
    
    /// Total rate limit hits detected
    rate_limit_hits: AtomicU64,
    
    /// Current active requests (approximate)
    active_requests: AtomicU64,
    
    /// Requests per enricher for coordination
    enricher_requests: Arc<RwLock<std::collections::HashMap<String, u64>>>,
    
    /// Last reset timestamp for rate calculations
    last_reset: Arc<RwLock<Instant>>,
}

impl GlobalRpcMetrics {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            rate_limit_hits: AtomicU64::new(0),
            active_requests: AtomicU64::new(0),
            enricher_requests: Arc::new(RwLock::new(std::collections::HashMap::new())),
            last_reset: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Record a new RPC request from an enricher
    pub async fn record_request(&self, enricher_name: &str) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.active_requests.fetch_add(1, Ordering::Relaxed);
        
        // Track per-enricher metrics
        let mut enricher_map = self.enricher_requests.write().await;
        *enricher_map.entry(enricher_name.to_string()).or_insert(0) += 1;
    }

    /// Record a completed RPC request
    pub fn record_completion(&self) {
        self.active_requests.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a rate limit hit
    #[allow(dead_code)]
    pub fn record_rate_limit_hit(&self) {
        self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current RPC load metrics
    pub async fn get_load_metrics(&self) -> RpcLoadMetrics {
        let total = self.total_requests.load(Ordering::Relaxed);
        let rate_limits = self.rate_limit_hits.load(Ordering::Relaxed);
        let active = self.active_requests.load(Ordering::Relaxed);
        
        let enricher_map = self.enricher_requests.read().await;
        let enricher_counts = enricher_map.clone();
        
        let last_reset = *self.last_reset.read().await;
        let duration = last_reset.elapsed();
        let requests_per_second = if duration.as_secs() > 0 {
            total as f64 / duration.as_secs() as f64
        } else {
            0.0
        };

        RpcLoadMetrics {
            total_requests: total,
            rate_limit_hits: rate_limits,
            active_requests: active,
            requests_per_second,
            enricher_breakdown: enricher_counts,
            measurement_duration: duration,
        }
    }

    /// Reset metrics (useful for periodic monitoring)
    pub async fn reset_metrics(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.rate_limit_hits.store(0, Ordering::Relaxed);
        // Don't reset active_requests as it tracks current state
        
        let mut enricher_map = self.enricher_requests.write().await;
        enricher_map.clear();
        
        let mut last_reset = self.last_reset.write().await;
        *last_reset = Instant::now();
    }

    /// Check if system is under high RPC load
    pub async fn is_under_high_load(&self) -> bool {
        let metrics = self.get_load_metrics().await;
        
        // High load indicators:
        // 1. Recent rate limit hits
        // 2. High requests per second
        // 3. Many active requests
        metrics.rate_limit_hits > 0 
            || metrics.requests_per_second > 50.0 
            || metrics.active_requests > 20
    }
}

/// Snapshot of current RPC load metrics
#[derive(Debug, Clone)]
pub struct RpcLoadMetrics {
    pub total_requests: u64,
    pub rate_limit_hits: u64,
    pub active_requests: u64,
    pub requests_per_second: f64,
    pub enricher_breakdown: std::collections::HashMap<String, u64>,
    pub measurement_duration: Duration,
}

/// Create Alloy RetryBackoffLayer with appropriate configuration
/// TODO: Fix ownership issue with with_avg_unit_cost
#[allow(dead_code)]
pub fn create_retry_layer(provider_limits: &RpcProviderLimits) -> RetryBackoffLayer {
    // For now, just create basic layer without avg_unit_cost configuration
    RetryBackoffLayer::new(
        provider_limits.max_retries as u32,
        provider_limits.initial_backoff_ms,
        provider_limits.compute_units_per_second,
    )
}

/// Enhanced error detection for rate limiting
#[allow(dead_code)]
pub fn is_rate_limit_error(error: &str) -> bool {
    let error_lower = error.to_lowercase();
    error_lower.contains("rate limit")
        || error_lower.contains("429")
        || error_lower.contains("too many requests")
        || error_lower.contains("quota exceeded")
        || error_lower.contains("rate exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_global_rpc_metrics() {
        let metrics = GlobalRpcMetrics::new();
        
        // Record some requests
        metrics.record_request("token_metadata").await;
        metrics.record_request("transaction_context").await;
        metrics.record_request("token_metadata").await;
        
        let load = metrics.get_load_metrics().await;
        assert_eq!(load.total_requests, 3);
        assert_eq!(load.active_requests, 3);
        assert_eq!(load.enricher_breakdown.get("token_metadata"), Some(&2));
        assert_eq!(load.enricher_breakdown.get("transaction_context"), Some(&1));
        
        // Complete some requests
        metrics.record_completion();
        metrics.record_completion();
        
        let load = metrics.get_load_metrics().await;
        assert_eq!(load.active_requests, 1);
    }

    #[test]
    fn test_provider_detection() {
        assert_eq!(
            RpcProviderLimits::from_url("https://mainnet.infura.io/v3/abc123").compute_units_per_second,
            2640
        );
        assert_eq!(
            RpcProviderLimits::from_url("https://eth-mainnet.alchemyapi.io/v2/xyz").compute_units_per_second,
            2400
        );
        assert_eq!(
            RpcProviderLimits::from_url("https://unknown-provider.com/rpc").compute_units_per_second,
            1000
        );
    }

    #[test]
    fn test_rate_limit_error_detection() {
        assert!(is_rate_limit_error("Error: rate limit exceeded"));
        assert!(is_rate_limit_error("HTTP 429 Too Many Requests"));
        assert!(is_rate_limit_error("Quota exceeded"));
        assert!(!is_rate_limit_error("Connection timeout"));
        assert!(!is_rate_limit_error("Invalid method"));
    }
}
