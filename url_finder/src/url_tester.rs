use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::Utc;
use futures::{StreamExt, stream};
use reqwest::Client;
use serde_json::json;
use tracing::{debug, info};

use crate::{config::Config, http_client::build_client, types::UrlValidationResult};

const FILTER_CONCURENCY_LIMIT: usize = 5;
const RETRI_CONCURENCY_LIMIT: usize = 20;

/// Minimum Content-Length for a URL to be considered "working" (100 MB)
pub const MIN_VALID_CONTENT_LENGTH: u64 = 100 * 1024 * 1024;
/// Below this threshold, retry to check for warm-up behavior (10 MB)
pub const SUSPICIOUS_SMALL_THRESHOLD: u64 = 10 * 1024 * 1024;
/// Number of retries when response is suspiciously small
pub const CONSISTENCY_CHECK_RETRIES: u32 = 2;
/// Delay between consistency check retries (ms)
pub const CONSISTENCY_CHECK_DELAY_MS: u64 = 500;

/// return first working url through head requests
/// let's keep both head and get versions for now
#[allow(dead_code)]
pub async fn filter_working_with_head(urls: Vec<String>) -> Option<String> {
    let client = Client::new();
    let counter = Arc::new(AtomicUsize::new(0));

    // stream of requests with concurency limit
    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                match client.head(&url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            Some(url)
                        } else {
                            debug!("URL::HEAD not working: {:?}", url);
                            None
                        }
                    }
                    Err(err) => {
                        debug!(
                            "Head request for working url failed for {:?}: {:?}",
                            url, err
                        );
                        None
                    }
                }
            }
        })
        .buffer_unordered(FILTER_CONCURENCY_LIMIT);

    while let Some(result) = stream.next().await {
        if let Some(url) = result {
            tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));
            return Some(url);
        }
    }

    tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));

    None
}

/// return retrivable percent of the urls
/// let's keep both head and get versions for now
#[allow(dead_code)]
pub async fn get_retrivability_with_head(
    config: &Config,
    urls: Vec<String>,
) -> (Option<String>, f64) {
    let client: Client = build_client(config).unwrap();
    let success_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));

    // stream of requests with concurency limit
    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let total_clone = Arc::clone(&total_counter);
            let success_clone = Arc::clone(&success_counter);
            async move {
                total_clone.fetch_add(1, Ordering::SeqCst);
                match client.head(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        debug!("url WORKING: {:?}", url);
                        success_clone.fetch_add(1, Ordering::SeqCst);
                        Some(url)
                    }
                    _ => {
                        debug!("url not working: {:?}", url);
                        None
                    }
                }
            }
        })
        .buffer_unordered(RETRI_CONCURENCY_LIMIT);

    let mut sample_url: Option<String> = None;

    while let Some(result) = stream.next().await {
        // process the stream

        // save a sample url that is working
        if sample_url.is_none() && result.is_some() {
            sample_url = result;
        }
    }

    let success = success_counter.load(Ordering::SeqCst);
    let total = total_counter.load(Ordering::SeqCst);

    let retri_percentage = if total > 0 {
        (success as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    info!(
        "Successfully retrieved URLs: {} out of {} ({:.2}%)",
        success, total, retri_percentage
    );

    (sample_url, round_to_two_decimals(retri_percentage))
}

pub async fn check_retrievability_with_get(
    config: &Config,
    urls: Vec<String>,
    with_stats: bool,
) -> (Option<String>, Option<f64>) {
    let client = build_client(config).unwrap();

    let success_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));

    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let total_clone = Arc::clone(&total_counter);
            let success_clone = Arc::clone(&success_counter);

            async move {
                total_clone.fetch_add(1, Ordering::SeqCst);

                debug!("Testing URL: {}", url);
                match client.get(&url).send().await {
                    Ok(resp) => {
                        let status = resp.status();
                        let content_type = resp
                            .headers()
                            .get("content-type")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        let etag = resp.headers().get("etag").is_some();

                        debug!(
                            "Response for {}: status={}, content_type={:?}, etag={:?}",
                            url, status, content_type, etag
                        );

                        // Drain body to allow connection reuse
                        drain_response_body(resp).await;

                        if status.is_success()
                            && matches!(
                                content_type.as_deref(),
                                Some("application/octet-stream") | Some("application/piece")
                            )
                            && etag
                        {
                            success_clone.fetch_add(1, Ordering::SeqCst);
                            Some(url)
                        } else {
                            debug!("GET not working or missing headers: {:?}", url);
                            None
                        }
                    }
                    Err(err) => {
                        debug!("GET request failed for {:?}: {:?}", url, err);
                        None
                    }
                }
            }
        })
        .buffer_unordered(RETRI_CONCURENCY_LIMIT);

    let mut sample_url: Option<String> = None;

    while let Some(result) = stream.next().await {
        // save a sample url that is working
        if sample_url.is_none() && result.is_some() {
            sample_url = result;
            if !with_stats {
                return (sample_url, None);
            }
        }
    }

    let success = success_counter.load(Ordering::SeqCst);
    let total = total_counter.load(Ordering::SeqCst);

    let retri_percentage = if total > 0 {
        (success as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    info!(
        "Successfully retrieved URLs: {} out of {} ({:.2}%)",
        success, total, retri_percentage
    );

    (sample_url, round_to_two_decimals(retri_percentage).into())
}

fn round_to_two_decimals(number: f64) -> f64 {
    (number * 100.0).round() / 100.0
}

/// Validates a working URL and collects metadata.
/// Uses GET request (not HEAD) for Curio compatibility.
/// If response is suspiciously small (< 10MB), retries to detect inconsistency.
pub async fn validate_url_with_metadata(config: &Config, url: &str) -> UrlValidationResult {
    let client = match build_client(config) {
        Ok(c) => c,
        Err(e) => {
            return UrlValidationResult::invalid(
                None,
                json!({
                    "validated_at": Utc::now(),
                    "error": format!("Failed to build HTTP client: {e}"),
                }),
            );
        }
    };

    // First GET request - extract Content-Length from headers
    let first_result = get_content_length(&client, url).await;

    let Some(first_length) = first_result.content_length else {
        return UrlValidationResult::invalid(
            None,
            json!({
                "validated_at": Utc::now(),
                "error": first_result.error.unwrap_or_else(|| "Missing Content-Length".to_string()),
            }),
        );
    };

    // If large enough, it's valid
    if first_length >= MIN_VALID_CONTENT_LENGTH {
        return UrlValidationResult::valid(
            first_length,
            json!({
                "validated_at": Utc::now(),
                "content_length": first_length,
                "content_type": first_result.content_type,
                "response_time_ms": first_result.response_time_ms,
            }),
        );
    }

    // If suspiciously small, check for inconsistency (warm-up behavior)
    if first_length < SUSPICIOUS_SMALL_THRESHOLD {
        return check_consistency(&client, url, first_length).await;
    }

    // Between suspicious and valid threshold - just invalid, not worth retrying
    UrlValidationResult::invalid(
        Some(first_length),
        json!({
            "validated_at": Utc::now(),
            "content_length": first_length,
            "failure_reason": "Content-Length below minimum threshold",
        }),
    )
}

struct GetResult {
    content_length: Option<u64>,
    content_type: Option<String>,
    response_time_ms: u128,
    error: Option<String>,
}

/// Maximum Content-Length we're willing to drain for connection reuse.
/// Error responses are typically small; large bodies aren't worth draining.
const MAX_DRAIN_CONTENT_LENGTH: u64 = 8192;

/// Drains the response body to allow HTTP connection reuse.
/// Only drains if Content-Length is present and small (typical for error responses).
/// Skips draining for chunked/unknown transfer encodings to avoid reading large streams.
async fn drain_response_body(resp: reqwest::Response) {
    let content_length = resp.content_length();

    // Check transfer-encoding header - skip draining for chunked responses
    let is_chunked = resp
        .headers()
        .get("transfer-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|te| te.eq_ignore_ascii_case("chunked"));

    // Only drain when we know the size is small (Content-Length present and <= threshold)
    // Skip draining for:
    // - Chunked responses (unknown size, potentially large)
    // - Missing Content-Length (unknown size)
    // - Large Content-Length (not worth the cost)
    if !is_chunked && content_length.is_some_and(|len| len <= MAX_DRAIN_CONTENT_LENGTH) {
        // Consume the body - ignore errors, we just want to drain it
        let _ = resp.bytes().await;
    }
    // For unknown/large bodies, dropping resp closes the connection
    // This is acceptable since large file responses are less frequent
}

async fn get_content_length(client: &Client, url: &str) -> GetResult {
    let start = std::time::Instant::now();

    match client.get(url).send().await {
        Ok(resp) => {
            let response_time_ms = start.elapsed().as_millis();
            let status = resp.status();

            debug!("Response status={}, headers={:?}", status, resp.headers());

            if !status.is_success() {
                // Drain body to allow connection reuse
                drain_response_body(resp).await;
                return GetResult {
                    content_length: None,
                    content_type: None,
                    response_time_ms,
                    error: Some(format!("HTTP status {status}")),
                };
            }

            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            // Read Content-Length from header directly
            // resp.content_length() may not work correctly with empty bodies
            let content_length = resp
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            debug!(
                "Parsed content_length={:?}, content_type={:?}",
                content_length, content_type
            );

            // Drain body to allow connection reuse
            drain_response_body(resp).await;

            GetResult {
                content_length,
                content_type,
                response_time_ms,
                error: None,
            }
        }
        Err(e) => GetResult {
            content_length: None,
            content_type: None,
            response_time_ms: start.elapsed().as_millis(),
            error: Some(format!("Request failed: {e}")),
        },
    }
}

async fn check_consistency(client: &Client, url: &str, first_length: u64) -> UrlValidationResult {
    let mut samples = vec![first_length];
    let mut max_length = first_length;
    let mut failed_attempts: u32 = 0;

    for _ in 0..CONSISTENCY_CHECK_RETRIES {
        tokio::time::sleep(Duration::from_millis(CONSISTENCY_CHECK_DELAY_MS)).await;

        let result = get_content_length(client, url).await;
        match result.content_length {
            Some(length) => {
                samples.push(length);
                max_length = max_length.max(length);
            }
            None => {
                failed_attempts += 1;
                debug!(
                    "Consistency check failed for {}: {}",
                    url,
                    result.error.as_deref().unwrap_or("unknown error")
                );
            }
        }
    }

    // Consistent only if all successful samples match AND no failures occurred
    let samples_all_equal = samples.iter().all(|&len| len == first_length);
    let is_consistent = samples_all_equal && failed_attempts == 0;
    let warmed_up = max_length >= MIN_VALID_CONTENT_LENGTH;

    let failure_reason = if failed_attempts > 0 {
        Some(format!(
            "Transient failures during consistency check: {failed_attempts} failed attempts"
        ))
    } else if !samples_all_equal {
        Some(format!("Content-Length variance: {:?}", samples))
    } else {
        None::<String>
    };

    let consistency_metadata = json!({
        "checked": true,
        "samples": samples,
        "retries": CONSISTENCY_CHECK_RETRIES,
        "failed_attempts": failed_attempts,
        "failure_reason": failure_reason,
    });

    let metadata = json!({
        "validated_at": Utc::now(),
        "content_length": max_length,
        "consistency": consistency_metadata,
    });

    match (is_consistent, warmed_up) {
        // All same, large enough, no failures = valid & consistent
        (true, true) => UrlValidationResult::valid(max_length, metadata),
        // All same, too small, no failures = invalid but consistent
        (true, false) => UrlValidationResult::invalid(Some(first_length), metadata),
        // Varied or had failures, eventually large = valid but INCONSISTENT
        (false, true) => UrlValidationResult::inconsistent(true, Some(max_length), metadata),
        // Varied or had failures, never large = invalid & inconsistent
        (false, false) => UrlValidationResult::inconsistent(false, Some(max_length), metadata),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    #[tokio::test]
    async fn test_drain_small_response_body() {
        let mock_server = MockServer::start().await;
        let small_body = vec![0u8; 100]; // Well under MAX_DRAIN_CONTENT_LENGTH

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(small_body, "text/plain"))
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let resp = client.get(&mock_server.uri()).send().await.unwrap();

        // Should complete without error - body is drained
        drain_response_body(resp).await;
    }

    #[tokio::test]
    async fn test_drain_large_response_skipped() {
        let mock_server = MockServer::start().await;
        // Body larger than MAX_DRAIN_CONTENT_LENGTH (8192)
        let large_body = vec![0u8; 50_000];

        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(large_body, "application/octet-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let resp = client.get(&mock_server.uri()).send().await.unwrap();

        let start = Instant::now();
        drain_response_body(resp).await;
        let elapsed = start.elapsed();

        // Should return quickly since we skip draining large bodies
        // Reading 50KB would take noticeable time; skipping is instant
        assert!(
            elapsed.as_millis() < 100,
            "Large body should be skipped, not drained. Took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_drain_empty_response_body() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(204)) // No content
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let resp = client.get(&mock_server.uri()).send().await.unwrap();

        // Should handle empty body gracefully
        drain_response_body(resp).await;
    }

    #[tokio::test]
    async fn test_drain_error_response() {
        let mock_server = MockServer::start().await;
        let error_body = b"Not Found".to_vec();

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_raw(error_body, "text/plain"))
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let resp = client.get(&mock_server.uri()).send().await.unwrap();

        // Error responses are typically small and should be drained
        drain_response_body(resp).await;
    }

    #[tokio::test]
    async fn test_get_content_length_extracts_headers_before_drain() {
        let mock_server = MockServer::start().await;
        let body = vec![0u8; 1000];

        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/octet-stream")
                    .set_body_raw(body.clone(), "application/octet-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let result = get_content_length(&client, &mock_server.uri()).await;

        assert!(result.error.is_none(), "Should succeed: {:?}", result.error);
        assert_eq!(result.content_length, Some(1000));
        assert_eq!(
            result.content_type,
            Some("application/octet-stream".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_content_length_captures_error_status() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let result = get_content_length(&client, &mock_server.uri()).await;

        assert!(result.content_length.is_none());
        assert!(result.error.is_some());
        assert!(
            result.error.as_ref().unwrap().contains("503"),
            "Error should contain status code: {:?}",
            result.error
        );
    }

    #[tokio::test]
    async fn test_drain_chunked_response_skipped() {
        let mock_server = MockServer::start().await;
        // Small body but with chunked transfer-encoding should NOT be drained
        let body = vec![0u8; 100];

        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("transfer-encoding", "chunked")
                    .set_body_raw(body, "text/plain"),
            )
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let resp = client.get(&mock_server.uri()).send().await.unwrap();

        let start = Instant::now();
        drain_response_body(resp).await;
        let elapsed = start.elapsed();

        // Should return quickly since chunked responses are skipped
        assert!(
            elapsed.as_millis() < 100,
            "Chunked response should be skipped, not drained. Took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_drain_missing_content_length_skipped() {
        let mock_server = MockServer::start().await;

        // Response without Content-Length header (wiremock may add it, but let's test the logic)
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let resp = client.get(&mock_server.uri()).send().await.unwrap();

        // Should complete without error - missing Content-Length means skip draining
        drain_response_body(resp).await;
    }

    #[tokio::test]
    async fn test_check_consistency_tracks_failures() {
        use wiremock::matchers::path;

        let mock_server = MockServer::start().await;

        // Mock returns 503 for all consistency check requests
        // (the first_length is passed to check_consistency, so we only mock the retry requests)
        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let url = format!("{}/test", mock_server.uri());
        let first_length = 1000u64; // Simulated first successful request value

        let result = check_consistency(&client, &url, first_length).await;

        // Should be marked as inconsistent due to failures
        assert!(
            !result.is_consistent,
            "Should be inconsistent due to failures"
        );

        // Verify metadata contains failure information
        let consistency = result
            .metadata
            .get("consistency")
            .expect("Missing consistency");
        let failed_attempts = consistency
            .get("failed_attempts")
            .expect("Missing failed_attempts")
            .as_u64()
            .unwrap();
        assert!(
            failed_attempts > 0,
            "Should have recorded failed attempts, got: {failed_attempts}"
        );

        // Verify failure_reason mentions transient failures
        let failure_reason = consistency
            .get("failure_reason")
            .expect("Missing failure_reason")
            .as_str()
            .unwrap();
        assert!(
            failure_reason.contains("Transient failures"),
            "failure_reason should mention transient failures: {failure_reason}"
        );

        // Samples should only contain the initial value (failures don't add samples)
        let samples = consistency.get("samples").expect("Missing samples");
        let samples_arr = samples.as_array().expect("samples should be array");
        assert_eq!(
            samples_arr.len(),
            1,
            "Only initial sample should be recorded"
        );
        assert_eq!(samples_arr[0].as_u64().unwrap(), first_length);
    }

    #[tokio::test]
    async fn test_check_consistency_all_successful_same_values() {
        use wiremock::matchers::path;

        let mock_server = MockServer::start().await;

        // All requests return same small content-length
        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "5000")
                    .set_body_raw(vec![0u8; 5000], "application/octet-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = Client::new();
        let url = format!("{}/test", mock_server.uri());
        let first_length = 5000u64;

        let result = check_consistency(&client, &url, first_length).await;

        // Should be consistent (all same, no failures) but invalid (too small)
        assert!(
            result.is_consistent,
            "Should be consistent - all same values"
        );
        assert!(!result.is_valid, "Should be invalid - content too small");

        // Verify no failures
        let consistency = result
            .metadata
            .get("consistency")
            .expect("Missing consistency");
        let failed_attempts = consistency
            .get("failed_attempts")
            .expect("Missing failed_attempts")
            .as_u64()
            .unwrap();
        assert_eq!(failed_attempts, 0, "Should have no failed attempts");
    }
}
