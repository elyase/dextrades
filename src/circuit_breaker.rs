use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Circuit breaker state
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    /// Circuit is closed, requests flow normally
    Closed,
    /// Circuit is open, requests fail fast
    Open { opened_at: Instant },
    /// Circuit is half-open, testing if service has recovered
    HalfOpen,
}

/// Circuit breaker for RPC provider reliability
/// 
/// Implements the circuit breaker pattern to prevent cascading failures
/// when an RPC provider becomes unavailable or unreliable.
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Current state of the circuit
    state: Arc<RwLock<CircuitState>>,
    /// Number of consecutive failures
    failure_count: Arc<AtomicUsize>,
    /// Number of consecutive successes in half-open state
    success_count: Arc<AtomicUsize>,
    /// Failure threshold before opening circuit
    failure_threshold: usize,
    /// Recovery timeout before trying half-open
    recovery_timeout: Duration,
    /// Success threshold to close circuit from half-open
    success_threshold: usize,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(
        failure_threshold: usize,
        recovery_timeout: Duration,
        success_threshold: usize,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: Arc::new(AtomicUsize::new(0)),
            success_count: Arc::new(AtomicUsize::new(0)),
            failure_threshold,
            recovery_timeout,
            success_threshold,
        }
    }

    /// Check if request should be allowed through circuit
    pub async fn should_allow_request(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true,
            CircuitState::Open { opened_at } => {
                // Check if recovery timeout has elapsed
                if opened_at.elapsed() >= self.recovery_timeout {
                    drop(state);
                    self.transition_to_half_open().await;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful operation
    pub async fn record_success(&self) {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                let success_count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if success_count >= self.success_threshold {
                    drop(state);
                    self.transition_to_closed().await;
                }
            }
            CircuitState::Open { .. } => {
                // Ignore successes when circuit is open
            }
        }
    }

    /// Record a failed operation
    pub async fn record_failure(&self) {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => {
                let failure_count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if failure_count >= self.failure_threshold {
                    drop(state);
                    self.transition_to_open().await;
                }
            }
            CircuitState::HalfOpen => {
                // Immediately return to open on any failure in half-open state
                drop(state);
                self.transition_to_open().await;
            }
            CircuitState::Open { .. } => {
                // Already open, nothing to do
            }
        }
    }

    /// Get current circuit state
    #[allow(dead_code)]
    pub async fn get_state(&self) -> CircuitState {
        self.state.read().await.clone()
    }

    /// Get current failure count
    #[allow(dead_code)]
    pub fn get_failure_count(&self) -> usize {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Transition to open state
    async fn transition_to_open(&self) {
        let mut state = self.state.write().await;
        *state = CircuitState::Open {
            opened_at: Instant::now(),
        };
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
        log::warn!("Circuit breaker transitioned to OPEN state");
    }

    /// Transition to half-open state
    async fn transition_to_half_open(&self) {
        let mut state = self.state.write().await;
        *state = CircuitState::HalfOpen;
        self.success_count.store(0, Ordering::Relaxed);
        log::info!("Circuit breaker transitioned to HALF-OPEN state");
    }

    /// Transition to closed state
    async fn transition_to_closed(&self) {
        let mut state = self.state.write().await;
        *state = CircuitState::Closed;
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
        log::info!("Circuit breaker transitioned to CLOSED state");
    }
}

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            failure_count: self.failure_count.clone(),
            success_count: self.success_count.clone(),
            failure_threshold: self.failure_threshold,
            recovery_timeout: self.recovery_timeout,
            success_threshold: self.success_threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_circuit_breaker_basic() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(100), 1);

        // Initially closed
        assert!(cb.should_allow_request().await);
        assert_eq!(cb.get_state().await, CircuitState::Closed);

        // Record failures
        cb.record_failure().await;
        assert!(cb.should_allow_request().await);
        assert_eq!(cb.get_state().await, CircuitState::Closed);

        cb.record_failure().await;
        assert!(!cb.should_allow_request().await);
        assert!(matches!(cb.get_state().await, CircuitState::Open { .. }));

        // Wait for recovery timeout
        sleep(Duration::from_millis(101)).await;
        assert!(cb.should_allow_request().await);
        assert_eq!(cb.get_state().await, CircuitState::HalfOpen);

        // Success should close circuit
        cb.record_success().await;
        assert_eq!(cb.get_state().await, CircuitState::Closed);
    }
}
