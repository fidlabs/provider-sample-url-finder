use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

/// Circuit breaker states for external service calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - all requests allowed
    Closed,
    /// Service failing - requests rejected to allow recovery
    Open,
    /// Testing recovery - single probe request allowed
    HalfOpen,
}

/// Thread-safe circuit breaker for protecting external service calls.
///
/// When consecutive failures exceed the threshold, the circuit "opens" and
/// rejects all requests for a cooldown period. After cooldown, it enters
/// "half-open" state and allows a single probe request to test recovery.
pub struct CircuitBreaker {
    /// Service name for logging
    name: String,
    /// Current consecutive failure count
    failure_count: AtomicUsize,
    /// Timestamp when circuit was opened (None if closed)
    opened_at: Mutex<Option<DateTime<Utc>>>,
    /// Whether we're in half-open state (allowing one probe)
    in_half_open: AtomicUsize, // 0 = not half-open, 1 = half-open waiting for probe result
    /// Number of consecutive failures before opening circuit
    failure_threshold: usize,
    /// How long to wait before attempting probe request
    cooldown: Duration,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    ///
    /// - `name`: Service name for logging (e.g., "BMS")
    /// - `failure_threshold`: Open circuit after this many consecutive failures
    /// - `cooldown`: Wait this long before allowing probe request
    pub fn new(name: impl Into<String>, failure_threshold: usize, cooldown: Duration) -> Self {
        Self {
            name: name.into(),
            failure_count: AtomicUsize::new(0),
            opened_at: Mutex::new(None),
            in_half_open: AtomicUsize::new(0),
            failure_threshold,
            cooldown,
        }
    }

    /// Check if a request is allowed through the circuit breaker.
    ///
    /// Returns `Ok(())` if the request can proceed, or `Err(CircuitOpenError)`
    /// if the circuit is open and the request should be rejected.
    pub fn check_allowed(&self) -> Result<(), CircuitOpenError> {
        let state = self.get_state();

        match state {
            CircuitState::Closed => Ok(()),
            CircuitState::HalfOpen => {
                // In half-open, only one probe request is allowed
                // Use compare_exchange to atomically claim the probe slot
                match self.in_half_open.compare_exchange(
                    1, // Expected: half-open state
                    2, // New: probe in progress
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        debug!("{} circuit breaker allowing probe request", self.name);
                        Ok(())
                    }
                    Err(_) => {
                        // Another request already claimed the probe slot
                        debug!(
                            "{} circuit breaker rejecting request - probe already in progress",
                            self.name
                        );
                        Err(CircuitOpenError::ProbeInProgress)
                    }
                }
            }
            CircuitState::Open => {
                let opened = self.opened_at.lock().unwrap();
                let remaining = opened
                    .map(|t| {
                        let elapsed = Utc::now() - t;
                        self.cooldown
                            .checked_sub(elapsed.to_std().unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                Err(CircuitOpenError::Open {
                    failures: self.failure_count.load(Ordering::SeqCst),
                    remaining_cooldown: remaining,
                })
            }
        }
    }

    /// Record a successful request, resetting the failure count.
    pub fn record_success(&self) {
        let prev_failures = self.failure_count.swap(0, Ordering::SeqCst);
        let was_half_open = self.in_half_open.swap(0, Ordering::SeqCst) > 0;

        // Clear opened_at
        *self.opened_at.lock().unwrap() = None;

        if was_half_open {
            info!(
                "{} circuit breaker closed - probe succeeded after {} failures",
                self.name, prev_failures
            );
        } else if prev_failures > 0 {
            debug!(
                "{} circuit breaker: success resets {} consecutive failures",
                self.name, prev_failures
            );
        }
    }

    /// Record a failed request, potentially opening the circuit.
    pub fn record_failure(&self) {
        // Atomically check if we were the probe request (state 2 = probe in progress)
        // Only treat as probe failure if we successfully consume the probe-in-progress state
        if self
            .in_half_open
            .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // We were the probe request and it failed - reopen the circuit
            *self.opened_at.lock().unwrap() = Some(Utc::now());
            let current_failures = self.failure_count.load(Ordering::SeqCst);
            warn!(
                "{} circuit breaker reopened - probe failed (failures at threshold: {})",
                self.name, current_failures
            );
            return;
        }

        let new_count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;

        // Check if we've hit the threshold to open the circuit
        if new_count >= self.failure_threshold {
            let mut opened = self.opened_at.lock().unwrap();
            if opened.is_none() {
                *opened = Some(Utc::now());
                warn!(
                    "{} circuit breaker opened after {} consecutive failures (cooldown: {:?})",
                    self.name, new_count, self.cooldown
                );
            }
        } else {
            debug!(
                "{} circuit breaker: failure {} of {} threshold",
                self.name, new_count, self.failure_threshold
            );
        }
    }

    /// Get the current circuit state.
    pub fn get_state(&self) -> CircuitState {
        let opened_at = *self.opened_at.lock().unwrap();

        match opened_at {
            None => CircuitState::Closed,
            Some(opened) => {
                let elapsed = Utc::now() - opened;
                if elapsed.to_std().unwrap_or_default() >= self.cooldown {
                    // Cooldown expired, transition to half-open
                    // Use compare_exchange to ensure only one thread transitions
                    match self.in_half_open.compare_exchange(
                        0,
                        1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            debug!(
                                "{} circuit breaker entering half-open state after {:?} cooldown",
                                self.name, self.cooldown
                            );
                        }
                        Err(_) => {
                            // Already in half-open or probe in progress
                        }
                    }
                    CircuitState::HalfOpen
                } else {
                    CircuitState::Open
                }
            }
        }
    }

    /// Get current failure count (for testing/monitoring).
    pub fn failure_count(&self) -> usize {
        self.failure_count.load(Ordering::SeqCst)
    }
}

/// Error returned when circuit breaker rejects a request.
#[derive(Debug, Clone)]
pub enum CircuitOpenError {
    /// Circuit is open, request rejected
    Open {
        failures: usize,
        remaining_cooldown: Duration,
    },
    /// Circuit is half-open but probe already in progress
    ProbeInProgress,
}

impl std::fmt::Display for CircuitOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitOpenError::Open {
                failures,
                remaining_cooldown,
            } => write!(
                f,
                "circuit open after {} failures, {:.0}s remaining",
                failures,
                remaining_cooldown.as_secs_f64()
            ),
            CircuitOpenError::ProbeInProgress => write!(f, "probe request already in progress"),
        }
    }
}

impl std::error::Error for CircuitOpenError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_secs(60));
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.check_allowed().is_ok());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_secs(60));

        // Record failures up to threshold
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        cb.record_failure();

        // Now should be open
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(cb.check_allowed().is_err());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_secs(60));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);

        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_after_cooldown() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_millis(10));

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for cooldown
        std::thread::sleep(Duration::from_millis(20));

        // Should be half-open now
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
        assert!(cb.check_allowed().is_ok()); // First probe allowed
    }

    #[test]
    fn test_circuit_breaker_probe_success_closes() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_millis(10));

        // Open and wait for half-open
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));

        // Claim probe slot
        assert!(cb.check_allowed().is_ok());

        // Success closes the circuit
        cb.record_success();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert_eq!(cb.failure_count(), 0);
    }

    #[test]
    fn test_circuit_breaker_probe_failure_reopens() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_millis(10));

        // Open and wait for half-open
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 3);
        std::thread::sleep(Duration::from_millis(20));

        // Claim probe slot
        assert!(cb.check_allowed().is_ok());

        // Failure reopens the circuit but doesn't increment failure count
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert_eq!(cb.failure_count(), 3); // Still at threshold, not incremented
    }

    #[test]
    fn test_circuit_breaker_only_one_probe_allowed() {
        let cb = CircuitBreaker::new("test", 3, Duration::from_millis(10));

        // Open and wait for half-open
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));

        // First probe allowed
        assert!(cb.check_allowed().is_ok());

        // Second probe rejected
        let result = cb.check_allowed();
        assert!(matches!(result, Err(CircuitOpenError::ProbeInProgress)));
    }
}
