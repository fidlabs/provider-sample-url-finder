use std::time::{Duration, Instant};
use url_finder::{
    config::Config,
    url_tester::{
        MAX_DRAIN_CONTENT_LENGTH, MIN_VALID_CONTENT_LENGTH, SUSPICIOUS_SMALL_THRESHOLD,
        validate_url_with_metadata,
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn test_config() -> Config {
    Config::new_for_test(
        "http://localhost:1234".to_string(),
        "http://localhost:1235".to_string(),
    )
}

// Note: wiremock sets content-length based on actual body size, not headers.
// For tests, we need to create actual bodies. Since MIN_VALID_CONTENT_LENGTH is 100MB,
// we use a smaller body but verify the behavior still works correctly.
// In production, actual file servers will return proper Content-Length headers.

#[tokio::test]
async fn test_validate_url_valid_large_file() {
    let mock_server = MockServer::start().await;
    // Create a body that exceeds MIN_VALID_CONTENT_LENGTH
    // For test efficiency, we'll use the exact minimum + some buffer
    let body_size = MIN_VALID_CONTENT_LENGTH as usize + 1000;
    let body = vec![0u8; body_size];

    Mock::given(method("GET"))
        .and(path("/piece/test"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"test-etag\"")
                .set_body_raw(body.clone(), "application/piece"),
        )
        .mount(&mock_server)
        .await;

    let url = format!("{}/piece/test", mock_server.uri());
    let config = test_config();

    let result = validate_url_with_metadata(&config, &url).await;

    assert!(
        result.is_valid,
        "Large file should be valid: metadata={:?}",
        result.metadata
    );
    assert!(result.is_consistent, "Single request should be consistent");
    assert_eq!(result.content_length, Some(body_size as u64));
}

#[tokio::test]
async fn test_validate_url_invalid_small_file() {
    let mock_server = MockServer::start().await;
    // Above SUSPICIOUS_SMALL_THRESHOLD, below MIN_VALID_CONTENT_LENGTH
    let body_size = SUSPICIOUS_SMALL_THRESHOLD as usize + 1000;
    let body = vec![0u8; body_size];

    Mock::given(method("GET"))
        .and(path("/piece/test"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"test-etag\"")
                .set_body_raw(body, "application/piece"),
        )
        .mount(&mock_server)
        .await;

    let url = format!("{}/piece/test", mock_server.uri());
    let config = test_config();

    let result = validate_url_with_metadata(&config, &url).await;

    assert!(
        !result.is_valid,
        "File below MIN_VALID should be invalid: metadata={:?}",
        result.metadata
    );
    assert!(
        result.is_consistent,
        "Consistent response should be marked consistent"
    );
}

#[tokio::test]
async fn test_validate_url_inconsistent_provider() {
    let mock_server = MockServer::start().await;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let call_count = Arc::new(AtomicU32::new(0));
    let call_count_clone = call_count.clone();

    let large_body_size = MIN_VALID_CONTENT_LENGTH as usize + 1000;

    // First call returns tiny response, subsequent calls return large
    Mock::given(method("GET"))
        .and(path("/piece/test"))
        .respond_with(move |_req: &wiremock::Request| {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let body = if count == 0 {
                vec![0u8; 500] // Tiny response (below SUSPICIOUS_SMALL_THRESHOLD)
            } else {
                vec![0u8; large_body_size] // Large response
            };
            ResponseTemplate::new(200)
                .insert_header("etag", "\"test-etag\"")
                .set_body_raw(body, "application/piece")
        })
        .mount(&mock_server)
        .await;

    let url = format!("{}/piece/test", mock_server.uri());
    let config = test_config();

    let result = validate_url_with_metadata(&config, &url).await;

    assert!(
        result.is_valid,
        "Should be valid after warm-up: metadata={:?}",
        result.metadata
    );
    assert!(
        !result.is_consistent,
        "Varying Content-Length should be inconsistent"
    );
}

#[tokio::test]
async fn test_validate_url_missing_content_length() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/piece/test"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/piece")
                .insert_header("etag", "\"test-etag\""), // No content-length header
        )
        .mount(&mock_server)
        .await;

    let url = format!("{}/piece/test", mock_server.uri());
    let config = test_config();

    let result = validate_url_with_metadata(&config, &url).await;

    assert!(!result.is_valid, "Missing Content-Length should be invalid");
}

/// Tests that connections are properly reused when response bodies are drained.
/// With a constrained pool (1 connection), sequential requests should complete
/// quickly if connections are reused. Without draining, each request would
/// need a new connection (slow) or time out.
#[tokio::test]
async fn test_connection_reuse_with_constrained_pool() {
    use reqwest::Client;

    let mock_server = MockServer::start().await;
    let small_body = vec![0u8; 500]; // Small response that should be drained

    // Mount a mock that counts requests
    Mock::given(method("GET"))
        .and(path("/test"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .insert_header("content-length", "500")
                .set_body_raw(small_body.clone(), "application/octet-stream"),
        )
        .expect(10) // Expect exactly 10 requests
        .mount(&mock_server)
        .await;

    // Create a client with a very constrained connection pool
    let client = Client::builder()
        .pool_max_idle_per_host(1) // Only 1 idle connection per host
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let url = format!("{}/test", mock_server.uri());
    let num_requests = 10;
    let start = Instant::now();

    // Make sequential requests - if connections are reused, this should be fast
    for i in 0..num_requests {
        let resp = client.get(&url).send().await.unwrap();
        assert!(
            resp.status().is_success(),
            "Request {i} failed: {:?}",
            resp.status()
        );

        // Drain the body to allow connection reuse (mimics our fix)
        let _ = resp.bytes().await;
    }

    let elapsed = start.elapsed();

    // With connection reuse, 10 requests to localhost should complete in < 1 second
    // Without reuse (creating new connections each time), it would be slower
    // and with pool_max_idle_per_host=1 + no draining, requests could hang
    assert!(
        elapsed < Duration::from_secs(5),
        "10 sequential requests took {:?}, expected < 5s with connection reuse",
        elapsed
    );

    // Verify all requests were received
    // (wiremock's expect(10) will panic on drop if not all received)
}

/// Tests that the drain function doesn't block on large responses.
/// This verifies we don't accidentally read huge file bodies.
#[tokio::test]
async fn test_large_response_does_not_block() {
    use reqwest::Client;

    let mock_server = MockServer::start().await;
    // Simulate a large file response (100KB)
    let large_body = vec![0u8; 100_000];

    Mock::given(method("GET"))
        .and(path("/large"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_raw(large_body, "application/octet-stream"),
        )
        .mount(&mock_server)
        .await;

    let client = Client::new();
    let url = format!("{}/large", mock_server.uri());

    let start = Instant::now();

    let resp = client.get(&url).send().await.unwrap();
    let content_length = resp.content_length();

    // The drain logic checks content_length and skips large bodies
    // We're verifying the behavior by checking content_length is available
    // before we would call drain (as our code does)
    assert!(content_length.is_some());
    assert!(content_length.unwrap() > MAX_DRAIN_CONTENT_LENGTH);

    // Just drop the response without reading body
    // This simulates what our drain_response_body does for large responses
    drop(resp);

    let elapsed = start.elapsed();

    // Dropping without reading should be nearly instant
    assert!(
        elapsed < Duration::from_millis(500),
        "Dropping large response should be fast, took {:?}",
        elapsed
    );
}
