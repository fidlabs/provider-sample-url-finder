use crate::common::*;
use axum::http::StatusCode;
use serde_json::json;

// Must match burst_size in routes.rs GovernorConfigBuilder
const BURST_LIMIT: usize = 30;

#[tokio::test]
async fn test_rate_limit_429_after_burst_exceeded() {
    let ctx = TestContext::new().await;

    // burst limit - all should succeed
    for i in 1..=BURST_LIMIT {
        let response = ctx.app.get("/healthcheck").await;
        assert_eq!(
            response.status_code(),
            StatusCode::OK,
            "Request {} should succeed within burst limit",
            i
        );
    }

    // should be rate limited
    let response = ctx.app.get("/healthcheck").await;

    assert_json_response(
        response,
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error": "Rate limit exceeded"}),
    );
}
