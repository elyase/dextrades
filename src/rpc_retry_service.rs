use alloy::primitives::Address;
use eyre::Result;
use log::{debug, warn};
use std::time::Duration;
use crate::rpc_orchestrator::RpcOrchestrator;

/// Classifies RPC errors for retry logic
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorClass {
    /// Transient errors that should be retried (rate limits, network timeouts)
    Transient,
    /// Permanent errors that should not be retried (invalid contract, invalid ABI)
    Permanent,
}

/// Simple retry policy for RPC requests
#[derive(Clone)]
pub struct RpcRetryPolicy {
    max_retries: usize,
    base_delay: Duration,
}

impl RpcRetryPolicy {
    pub fn new(max_retries: usize, base_delay: Duration) -> Self {
        Self {
            max_retries,
            base_delay,
        }
    }

    /// Classify an error to determine if it should be retried
    pub fn classify_error(error: &eyre::Error) -> ErrorClass {
        let error_str = error.to_string().to_lowercase();
        
        // Transient errors that should be retried
        if error_str.contains("timeout") ||
           error_str.contains("rate limit") ||
           error_str.contains("429") ||
           error_str.contains("503") ||
           error_str.contains("502") ||
           error_str.contains("connection") ||
           error_str.contains("network") ||
           error_str.contains("temporary") ||
           error_str.contains("circuit breaker") {
            return ErrorClass::Transient;
        }
        
        // Permanent errors that should not be retried
        if error_str.contains("invalid") ||
           error_str.contains("revert") ||
           error_str.contains("execution reverted") ||
           error_str.contains("400") ||
           error_str.contains("401") ||
           error_str.contains("403") ||
           error_str.contains("404") {
            return ErrorClass::Permanent;
        }
        
        // Default to transient for unknown errors to be safe
        ErrorClass::Transient
    }

    /// Execute a function with retry logic
    pub async fn execute_with_retry<F, T, Fut>(&self, operation: F) -> Result<T, eyre::Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, eyre::Error>>,
    {
        let mut attempts = 0;
        let mut delay = self.base_delay;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    match Self::classify_error(&error) {
                        ErrorClass::Permanent => {
                            debug!("Permanent error, not retrying: {}", error);
                            return Err(error);
                        }
                        ErrorClass::Transient => {
                            if attempts >= self.max_retries {
                                warn!("Max retries ({}) exceeded for transient error: {}", self.max_retries, error);
                                return Err(error);
                            }
                            
                            debug!("Transient error, retrying in {:?} (attempt {}/{}): {}", 
                                   delay, attempts + 1, self.max_retries, error);
                            
                            tokio::time::sleep(delay).await;
                            attempts += 1;
                            delay = std::cmp::min(delay * 2, Duration::from_secs(30)); // Exponential backoff with cap
                        }
                    }
                }
            }
        }
    }
}

/// Enhanced RPC client with retry logic
pub struct RpcClient {
    orchestrator: RpcOrchestrator,
    retry_policy: RpcRetryPolicy,
}

impl RpcClient {
    /// Create a new RPC client with retry capabilities
    pub fn new(orchestrator: RpcOrchestrator, max_retries: usize, base_delay: Duration) -> Self {
        let retry_policy = RpcRetryPolicy::new(max_retries, base_delay);
        Self { orchestrator, retry_policy }
    }

    /// Get logs with retry logic
    #[allow(dead_code)]
    pub async fn get_logs(&self, filter: &alloy::rpc::types::Filter) -> Result<Vec<alloy::rpc::types::Log>, eyre::Error> {
        let filter = filter.clone();
        let orchestrator = self.orchestrator.clone();
        
        self.retry_policy.execute_with_retry(|| {
            let filter = filter.clone();
            let orchestrator = orchestrator.clone();
            async move {
                orchestrator.get_logs(&filter).await
            }
        }).await
    }

    /// Make a contract call with retry logic
    pub async fn call(&self, to: Address, data: Vec<u8>) -> Result<Vec<u8>, eyre::Error> {
        let orchestrator = self.orchestrator.clone();
        
        self.retry_policy.execute_with_retry(|| {
            let data = data.clone();
            let orchestrator = orchestrator.clone();
            async move {
                orchestrator.call(to, data).await
            }
        }).await
    }

    /// Get block number with retry logic
    #[allow(dead_code)]
    pub async fn get_block_number(&self) -> Result<u64, eyre::Error> {
        let orchestrator = self.orchestrator.clone();
        
        self.retry_policy.execute_with_retry(|| {
            let orchestrator = orchestrator.clone();
            async move {
                orchestrator.get_block_number().await
            }
        }).await
    }

    /// Get chain ID with retry logic
    #[allow(dead_code)]
    pub async fn get_chain_id(&self) -> Result<u64, eyre::Error> {
        let orchestrator = self.orchestrator.clone();
        
        self.retry_policy.execute_with_retry(|| {
            let orchestrator = orchestrator.clone();
            async move {
                orchestrator.get_chain_id().await
            }
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        // Test transient errors
        let timeout_error = eyre::eyre!("Request timeout after 30s");
        assert_eq!(RpcRetryPolicy::classify_error(&timeout_error), ErrorClass::Transient);

        let rate_limit_error = eyre::eyre!("Rate limit exceeded: 429 Too Many Requests");
        assert_eq!(RpcRetryPolicy::classify_error(&rate_limit_error), ErrorClass::Transient);

        let network_error = eyre::eyre!("Network connection failed");
        assert_eq!(RpcRetryPolicy::classify_error(&network_error), ErrorClass::Transient);

        // Test permanent errors
        let invalid_error = eyre::eyre!("Invalid contract address");
        assert_eq!(RpcRetryPolicy::classify_error(&invalid_error), ErrorClass::Permanent);

        let revert_error = eyre::eyre!("Execution reverted: insufficient balance");
        assert_eq!(RpcRetryPolicy::classify_error(&revert_error), ErrorClass::Permanent);

        let auth_error = eyre::eyre!("HTTP 401 Unauthorized");
        assert_eq!(RpcRetryPolicy::classify_error(&auth_error), ErrorClass::Permanent);
    }
}
