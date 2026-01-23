use std::time::Duration;

use http::Extensions;
use reqwest::{Client, Request, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use retry_policies::Jitter;
use tracing::Instrument;

/// Add context to tracing spans for HTTP requests.
pub struct HttpRequestContextLogger;

#[async_trait::async_trait]
impl Middleware for HttpRequestContextLogger {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let url = req.url().to_string();
        let method = req.method().as_str();
        let service = req.url().host_str().unwrap_or("unknown");

        // TODO: At some point we should change WARN to INFO after we verify this is working as intended.
        let span = tracing::warn_span!(
            "http_retry_request",
            method = %method,
            url = %url,
            service = %service
        );

        async move { next.run(req, extensions).await }
            .instrument(span)
            .await
    }
}

pub fn build_reqwest_retry_client(
    min_retry_interval_ms: u64,
    max_retry_interval_ms: u64,
) -> ClientWithMiddleware {
    build_reqwest_retry_client_with_config(
        min_retry_interval_ms,
        max_retry_interval_ms,
        3,
        None,
        None,
    )
}

/// Build an HTTP client with configurable retry policy and timeouts.
///
/// - `max_retries`: Number of retry attempts (use 1 for services with long gateway timeouts)
/// - `connect_timeout_ms`: TCP connection timeout (None = no timeout)
/// - `request_timeout_ms`: Per-request timeout applied to each attempt (None = no timeout)
pub fn build_reqwest_retry_client_with_config(
    min_retry_interval_ms: u64,
    max_retry_interval_ms: u64,
    max_retries: u32,
    connect_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
) -> ClientWithMiddleware {
    let retry_policy = ExponentialBackoff::builder()
        .jitter(Jitter::None)
        .base(2)
        .retry_bounds(
            Duration::from_millis(min_retry_interval_ms),
            Duration::from_millis(max_retry_interval_ms),
        )
        .build_with_max_retries(max_retries);

    let mut client_builder = Client::builder();

    if let Some(connect_ms) = connect_timeout_ms {
        client_builder = client_builder.connect_timeout(Duration::from_millis(connect_ms));
    }

    if let Some(request_ms) = request_timeout_ms {
        client_builder = client_builder.timeout(Duration::from_millis(request_ms));
    }

    let client = client_builder.build().expect("Failed to build HTTP client");

    ClientBuilder::new(client)
        .with(HttpRequestContextLogger)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}
