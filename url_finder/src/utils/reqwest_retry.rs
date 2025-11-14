use std::time::Duration;

use http::Extensions;
use reqwest::{Client, Request, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
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
    let retry_policy = ExponentialBackoff::builder()
        .jitter(Jitter::None)
        .base(2)
        .retry_bounds(
            Duration::from_millis(min_retry_interval_ms),
            Duration::from_millis(max_retry_interval_ms),
        )
        .build_with_max_retries(3);

    ClientBuilder::new(Client::new())
        .with(HttpRequestContextLogger) // Add context before retry middleware
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}
