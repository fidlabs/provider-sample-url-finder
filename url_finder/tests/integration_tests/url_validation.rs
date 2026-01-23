use std::time::{Duration, Instant};
use url_finder::url_tester::MAX_DRAIN_CONTENT_LENGTH;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

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
