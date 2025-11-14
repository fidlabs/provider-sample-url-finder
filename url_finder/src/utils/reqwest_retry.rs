use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use retry_policies::Jitter;
use std::time::Duration;

pub fn build_reqwest_retry_client(
    min_retry_interval_ms: u64,
    max_retry_interval_ms: u64,
) -> ClientWithMiddleware {
    let retry_policy = ExponentialBackoff::builder()
        .jitter(Jitter::None)
        .base(2)
        .retry_bounds(
            Duration::from_millis(min_retry_interval_ms),
            Duration::from_millis(max_retry_interval_ms),
        )
        .build_with_max_retries(3);

    ClientBuilder::new(Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}
