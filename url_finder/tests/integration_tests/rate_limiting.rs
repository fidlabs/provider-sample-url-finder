use crate::common::*;
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn test_rate_limit_429_after_burst_exceeded() {
    let ctx = TestContext::new().await;

    // 300 requests should reliably trigger rate limiting
    let mut got_rate_limited = false;

    for _ in 0..300 {
        let response = ctx.app.get("/healthcheck").await;
        if response.status_code() == StatusCode::TOO_MANY_REQUESTS {
            assert_json_response(
                response,
                StatusCode::TOO_MANY_REQUESTS,
                json!({"error": "Rate limit exceeded"}),
            );
            got_rate_limited = true;
            break;
        }
    }

    assert!(
        got_rate_limited,
        "Expected to hit rate limit within 300 requests"
    );
}
