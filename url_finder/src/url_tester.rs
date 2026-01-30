use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures::{StreamExt, stream};
use reqwest::Client;
use tokio::sync::Semaphore;
use tracing::debug;

use crate::car_header::{CarHeaderParseResult, parse_car_header};
use crate::config::{
    Config, DOUBLE_TAP_DELAY_MS, MAX_CONCURRENT_URL_TESTS, MIN_VALID_CONTENT_LENGTH,
    RANGE_REQUEST_BYTES,
};
use crate::http_client::build_client;
use crate::types::{InconsistencyType, UrlTestError, UrlTestResult};

const FILTER_CONCURRENCY_LIMIT: usize = 5;
const RETRI_CONCURRENCY_LIMIT: usize = 20;

/// Response from a range request, containing the total file size from Content-Range header
#[derive(Debug)]
#[allow(dead_code)]
struct RangeResponse {
    content_length: Option<u64>,
    response_time_ms: u64,
    body_sample: Option<Vec<u8>>,
}

/// Classification of a single tap result for consistency checking.
#[derive(Debug, Clone)]
enum TapResult {
    /// HTTP success with Content-Length >= MIN_VALID_CONTENT_LENGTH (8GB)
    Valid {
        content_length: u64,
        response_time_ms: u64,
        car_header: Option<CarHeaderParseResult>,
    },
    /// HTTP success but Content-Length < MIN_VALID_CONTENT_LENGTH (likely error page)
    Small {
        content_length: u64,
        response_time_ms: u64,
        car_header: Option<CarHeaderParseResult>,
    },
    /// Request failed (timeout, connection error, HTTP error status)
    Failed { error: UrlTestError },
}

impl TapResult {
    /// Classify a range request result into Valid/Small/Failed
    fn from_range_result(result: Result<RangeResponse, UrlTestError>) -> Self {
        let response = match result {
            Ok(r) => r,
            Err(e) => return TapResult::Failed { error: e },
        };

        let content_length = response.content_length.unwrap_or(0);
        let response_time_ms = response.response_time_ms;

        // Parse CAR header from body sample
        let car_header = response
            .body_sample
            .as_ref()
            .map(|bytes| parse_car_header(bytes));

        if content_length >= MIN_VALID_CONTENT_LENGTH {
            return TapResult::Valid {
                content_length,
                response_time_ms,
                car_header,
            };
        }

        TapResult::Small {
            content_length,
            response_time_ms,
            car_header,
        }
    }

    fn is_valid(&self) -> bool {
        matches!(self, TapResult::Valid { .. })
    }

    fn content_length(&self) -> Option<u64> {
        match self {
            TapResult::Valid { content_length, .. } => Some(*content_length),
            TapResult::Small { content_length, .. } => Some(*content_length),
            TapResult::Failed { .. } => None,
        }
    }

    fn response_time_ms(&self) -> u64 {
        match self {
            TapResult::Valid {
                response_time_ms, ..
            } => *response_time_ms,
            TapResult::Small {
                response_time_ms, ..
            } => *response_time_ms,
            TapResult::Failed { .. } => 0,
        }
    }

    fn error(&self) -> Option<UrlTestError> {
        match self {
            TapResult::Failed { error } => Some(error.clone()),
            _ => None,
        }
    }

    fn car_header(&self) -> Option<&CarHeaderParseResult> {
        match self {
            TapResult::Valid { car_header, .. } => car_header.as_ref(),
            TapResult::Small { car_header, .. } => car_header.as_ref(),
            TapResult::Failed { .. } => None,
        }
    }

    #[allow(dead_code)]
    fn root_cid(&self) -> Option<String> {
        self.car_header()
            .filter(|h| h.is_valid)
            .and_then(|h| h.root_cid.clone())
    }
}

/// Determines if two tap results indicate a consistent provider.
/// Everything else (Small responses, failures, mismatched sizes) = inconsistent.
fn is_consistent_pair(tap1: &TapResult, tap2: &TapResult) -> bool {
    match (tap1, tap2) {
        (
            TapResult::Valid {
                content_length: a, ..
            },
            TapResult::Valid {
                content_length: b, ..
            },
        ) => a == b,
        // All other combinations are inconsistent:
        // - VALID + SMALL: real piece vs error page
        // - SMALL + VALID: error page vs real piece
        // - SMALL + SMALL: both returned garbage
        // - VALID + FAILED: gaming pattern (timeout then respond)
        // - FAILED + VALID: gaming pattern (timeout then respond)
        // - SMALL + FAILED: got garbage
        // - FAILED + SMALL: got garbage
        // - FAILED + FAILED: cannot verify, assume bad
        _ => false,
    }
}

/// Classifies WHY a pair of tap results is inconsistent.
fn classify_inconsistency(tap1: &TapResult, tap2: &TapResult) -> InconsistencyType {
    use TapResult::*;

    // Size mismatch: both valid but different Content-Length
    if let (
        Valid {
            content_length: a, ..
        },
        Valid {
            content_length: b, ..
        },
    ) = (tap1, tap2)
        && a != b
    {
        return InconsistencyType::SizeMismatch;
    }

    // WarmUp: tap2 returned valid data (tap1 was not valid)
    if matches!(tap2, Valid { .. }) && !matches!(tap1, Valid { .. }) {
        return InconsistencyType::WarmUp;
    }

    // Flaky: tap1 was valid but tap2 degraded
    if matches!(tap1, Valid { .. }) && !matches!(tap2, Valid { .. }) {
        return InconsistencyType::Flaky;
    }

    // Both failed
    if matches!((tap1, tap2), (Failed { .. }, Failed { .. })) {
        return InconsistencyType::BothFailed;
    }

    // Default: small/garbage responses
    InconsistencyType::SmallResponses
}

/// Makes a range request (bytes=0-4095) and extracts total file size from Content-Range header.
/// Used for double-tap consistency testing to verify Content-Length without downloading full file.
async fn range_request(client: &Client, url: &str) -> Result<RangeResponse, UrlTestError> {
    let start = std::time::Instant::now();

    let resp = client
        .get(url)
        .header("Range", format!("bytes=0-{}", RANGE_REQUEST_BYTES - 1))
        .send()
        .await
        .map_err(|e| classify_request_error(&e))?;

    let status = resp.status();
    if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(UrlTestError::HttpError(status.as_u16()));
    }

    let content_length = extract_total_length(&resp);
    let response_time_ms = start.elapsed().as_millis() as u64;

    // Capture body sample for CAR header parsing (limited to prevent full file downloads)
    let body_sample = read_limited_body(resp, RANGE_REQUEST_BYTES as usize).await;

    Ok(RangeResponse {
        content_length,
        response_time_ms,
        body_sample,
    })
}

/// Extracts total file size from Content-Range header (e.g., "bytes 0-4095/19327352832" -> 19327352832)
/// Falls back to Content-Length header if Content-Range is not present.
#[allow(dead_code)]
fn extract_total_length(resp: &reqwest::Response) -> Option<u64> {
    // Try Content-Range first: "bytes 0-4095/19327352832"
    if let Some(range) = resp.headers().get("content-range")
        && let Ok(s) = range.to_str()
        && let Some(total) = s.split('/').nth(1)
        && total != "*"
    {
        return total.parse().ok();
    }
    // Fall back to Content-Length
    resp.content_length()
}

/// Classifies a reqwest error into a more specific UrlTestError type
#[allow(dead_code)]
fn classify_request_error(e: &reqwest::Error) -> UrlTestError {
    if e.is_timeout() {
        UrlTestError::Timeout
    } else if e.is_connect() {
        if e.to_string().contains("dns") {
            UrlTestError::DnsFailure
        } else {
            UrlTestError::ConnectionRefused
        }
    } else if e.to_string().contains("reset") {
        UrlTestError::ConnectionReset
    } else if e.to_string().contains("tls") || e.to_string().contains("ssl") {
        UrlTestError::TlsError
    } else {
        UrlTestError::Other(e.to_string())
    }
}

/// Performs a double-tap URL test: two range requests with a delay between them.
///
/// STRICT CONSISTENCY RULES
/// - success: true if either request succeeded (HTTP 2xx with valid Content-Length)
/// - consistent: true ONLY if both requests return VALID responses (>= 8GB) with identical Content-Length
/// - Everything else is inconsistent: failures, small responses (error pages), mismatched sizes
pub async fn test_url_double_tap(client: &Client, url: &str) -> UrlTestResult {
    let r1 = range_request(client, url).await;
    tokio::time::sleep(Duration::from_millis(DOUBLE_TAP_DELAY_MS)).await;
    let r2 = range_request(client, url).await;

    let tap1 = TapResult::from_range_result(r1);
    let tap2 = TapResult::from_range_result(r2);

    let success = tap1.is_valid() || tap2.is_valid();
    let consistent = is_consistent_pair(&tap1, &tap2);

    let best_content_length = match (&tap1, &tap2) {
        (
            TapResult::Valid {
                content_length: a, ..
            },
            TapResult::Valid {
                content_length: b, ..
            },
        ) => Some(std::cmp::max(*a, *b)),
        (TapResult::Valid { content_length, .. }, _) => Some(*content_length),
        (_, TapResult::Valid { content_length, .. }) => Some(*content_length),
        _ => tap2.content_length().or(tap1.content_length()),
    };

    let best_response_time = match (&tap1, &tap2) {
        (
            _,
            TapResult::Valid {
                response_time_ms, ..
            },
        ) => *response_time_ms,
        (
            TapResult::Valid {
                response_time_ms, ..
            },
            _,
        ) => *response_time_ms,
        _ if tap2.content_length().is_some() => tap2.response_time_ms(),
        _ => tap1.response_time_ms(),
    };

    let error = tap2.error().or(tap1.error());

    let inconsistency_type = if consistent {
        None
    } else {
        Some(classify_inconsistency(&tap1, &tap2))
    };

    // CAR header info: prefer tap2, fall back to tap1
    let best_car = tap2.car_header().or(tap1.car_header());
    let is_valid_car = best_car.map(|h| h.is_valid).unwrap_or(false);
    let root_cid = best_car.and_then(|h| h.root_cid.clone());

    UrlTestResult {
        url: url.to_string(),
        success,
        consistent,
        inconsistency_type,
        content_length: best_content_length,
        response_time_ms: best_response_time,
        error,
        is_valid_car,
        root_cid,
    }
}

/// Tests multiple URLs in parallel using double-tap consistency checks.
/// Limits concurrency to MAX_CONCURRENT_URL_TESTS to avoid overwhelming targets.
pub async fn test_urls_double_tap(client: &Client, urls: Vec<String>) -> Vec<UrlTestResult> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_URL_TESTS));

    let futures: Vec<_> = urls
        .into_iter()
        .map(|url| {
            let client = client.clone();
            let permit = semaphore.clone();

            async move {
                let _permit = permit.acquire().await.unwrap();
                test_url_double_tap(&client, &url).await
            }
        })
        .collect();

    futures::future::join_all(futures).await
}

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
        .buffer_unordered(FILTER_CONCURRENCY_LIMIT);

    while let Some(result) = stream.next().await {
        if let Some(url) = result {
            tracing::debug!("number of requests: {:?}", counter.load(Ordering::SeqCst));
            return Some(url);
        }
    }

    tracing::debug!("number of requests: {:?}", counter.load(Ordering::SeqCst));

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
        .buffer_unordered(RETRI_CONCURRENCY_LIMIT);

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

    debug!(
        "Successfully retrieved URLs: {} out of {} ({:.2}%)",
        success, total, retri_percentage
    );

    (sample_url, round_to_two_decimals(retri_percentage))
}

#[allow(dead_code)]
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
        .buffer_unordered(RETRI_CONCURRENCY_LIMIT);

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

    debug!(
        "Successfully retrieved URLs: {} out of {} ({:.2}%)",
        success, total, retri_percentage
    );

    (sample_url, round_to_two_decimals(retri_percentage).into())
}

fn round_to_two_decimals(number: f64) -> f64 {
    (number * 100.0).round() / 100.0
}

/// Maximum Content-Length we're willing to drain for connection reuse.
/// Error responses are typically small; large bodies aren't worth draining.
pub const MAX_DRAIN_CONTENT_LENGTH: u64 = 8192;

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

/// Reads up to `limit` bytes from response body, then stops.
/// Prevents downloading entire files when servers ignore Range headers.
/// Returns partial data on network errors (CAR header is in first bytes).
async fn read_limited_body(resp: reqwest::Response, limit: usize) -> Option<Vec<u8>> {
    let mut stream = resp.bytes_stream();
    let mut buffer = Vec::with_capacity(limit);

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                let remaining = limit.saturating_sub(buffer.len());
                if remaining == 0 {
                    break;
                }
                let take = bytes.len().min(remaining);
                buffer.extend_from_slice(&bytes[..take]);
            }
            Err(_) => break,
        }
    }

    if buffer.is_empty() {
        None
    } else {
        Some(buffer)
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
        let resp = client.get(mock_server.uri()).send().await.unwrap();

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
        let resp = client.get(mock_server.uri()).send().await.unwrap();

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
        let resp = client.get(mock_server.uri()).send().await.unwrap();

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
        let resp = client.get(mock_server.uri()).send().await.unwrap();

        // Error responses are typically small and should be drained
        drain_response_body(resp).await;
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
        let resp = client.get(mock_server.uri()).send().await.unwrap();

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
        let resp = client.get(mock_server.uri()).send().await.unwrap();

        // Should complete without error - missing Content-Length means skip draining
        drain_response_body(resp).await;
    }

    #[tokio::test]
    async fn test_range_request_extracts_content_length_from_content_range() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/19327352832")
                    .insert_header("Content-Length", "4096"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = range_request(&client, &mock_server.uri()).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.content_length, Some(19327352832));
    }

    #[tokio::test]
    async fn test_double_tap_both_success_consistent() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .expect(2)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(result.success);
        assert!(result.consistent);
        assert_eq!(result.content_length, Some(16000000000));
    }

    /// SMALL + VALID = WarmUp pattern - THE KEY REAL-WORLD SCENARIO
    /// First request returns small stub, second returns real data after warm-up.
    /// This is exactly what double-tap was built to handle.
    #[tokio::test]
    async fn test_double_tap_warm_up_small_then_valid() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // First request returns small stub/placeholder
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206).insert_header("Content-Range", "bytes 0-252/252"),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second request returns valid large file - warm-up worked!
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(result.success); // tap2 was valid - we got real data!
        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::WarmUp)
        );
        assert_eq!(result.content_length, Some(16000000000)); // We captured the valid size
    }

    /// FAIL + VALID = WarmUp pattern - THIS IS WHY DOUBLE-TAP EXISTS
    /// First request fails/times out, second succeeds with valid data.
    /// We still get the data, provider just needs warm-up.
    #[tokio::test]
    async fn test_double_tap_warm_up_fail_then_valid() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // First request fails (simulates timeout/warm-up needed)
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second request succeeds with valid large file
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(result.success); // tap2 succeeded - we got the data!
        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::WarmUp)
        );
        assert_eq!(result.content_length, Some(16000000000));
    }

    /// VALID + FAIL = Flaky - provider served data once then stopped
    #[tokio::test]
    async fn test_double_tap_flaky_valid_then_fail() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // First request succeeds
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second request fails - provider degraded
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(result.success); // tap1 succeeded - we got the data
        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::Flaky)
        );
        assert_eq!(result.content_length, Some(16000000000));
    }

    /// SMALL + SMALL = NOT successful - neither returned valid (>= 8GB) data
    #[tokio::test]
    async fn test_double_tap_both_small_responses() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // Both requests return small responses - neither is valid
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206).insert_header("Content-Range", "bytes 0-500/500"),
            )
            .expect(2)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(!result.success); // Neither tap was valid - NOT successful!
        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::SmallResponses)
        );
    }

    /// FAIL + FAIL = inconsistent (cannot verify, assume bad)
    #[tokio::test]
    async fn test_double_tap_both_fail() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // Both requests fail
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(!result.success); // Both failed
        assert!(!result.consistent); // STRICT: FAIL + FAIL = inconsistent
    }

    #[tokio::test]
    async fn test_batch_url_testing() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let urls = vec![
            format!("{}/piece/a", mock_server.uri()),
            format!("{}/piece/b", mock_server.uri()),
            format!("{}/piece/c", mock_server.uri()),
        ];

        let results = test_urls_double_tap(&client, urls).await;

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.success));
        assert!(results.iter().all(|r| r.consistent));
    }

    #[tokio::test]
    async fn test_classify_flaky_valid_then_fail() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // First request succeeds with valid size
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second request fails - provider degraded
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(result.success); // tap1 was valid
        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::Flaky)
        );
    }

    #[tokio::test]
    async fn test_classify_both_failed() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::BothFailed)
        );
    }

    #[tokio::test]
    async fn test_classify_small_responses() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // Both return small responses - neither is valid
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206).insert_header("Content-Range", "bytes 0-500/500"),
            )
            .expect(2)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(!result.success); // Neither tap was valid
        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::SmallResponses)
        );
    }

    #[tokio::test]
    async fn test_classify_size_mismatch() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // First returns one size
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second returns different size
        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/20000000000"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(!result.consistent);
        assert_eq!(
            result.inconsistency_type,
            Some(crate::types::InconsistencyType::SizeMismatch)
        );
    }

    #[tokio::test]
    async fn test_consistent_has_no_inconsistency_type() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-4095/16000000000"),
            )
            .expect(2)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let result = test_url_double_tap(&client, &mock_server.uri()).await;

        assert!(result.consistent);
        assert_eq!(result.inconsistency_type, None);
    }

    #[tokio::test]
    async fn test_range_request_limits_body_download() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // Server ignores Range header: returns HTTP 200 (not 206) with 50KB body
        let large_body = vec![0xCAu8; 50_000];

        Mock::given(method("GET"))
            .and(header("Range", "bytes=0-4095"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", "50000")
                    .set_body_raw(large_body, "application/octet-stream"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let start = Instant::now();
        let result = range_request(&client, &mock_server.uri()).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Request should succeed");
        let response = result.unwrap();

        assert!(response.body_sample.is_some(), "Should have body sample");
        let body = response.body_sample.unwrap();
        assert_eq!(
            body.len(),
            RANGE_REQUEST_BYTES as usize,
            "Body should be limited to RANGE_REQUEST_BYTES (4096), got {}",
            body.len()
        );

        assert!(
            body.iter().all(|&b| b == 0xCA),
            "Should contain expected pattern"
        );

        assert!(
            elapsed.as_millis() < 1000,
            "Should not download full body, took {:?}",
            elapsed
        );
    }
}
