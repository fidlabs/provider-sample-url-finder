use crate::common::*;
use axum::http::StatusCode;
use serde_json::json;

const READ_BURST: usize = 500;
const DEAL_RUN_BURST: usize = 5;

async fn exhaust_read_bucket(ctx: &TestContext) -> usize {
    for sent in 1..=READ_BURST * 4 {
        let response = ctx.app.get("/healthcheck").await;
        if response.status_code() == StatusCode::TOO_MANY_REQUESTS {
            assert_json_response(
                response,
                StatusCode::TOO_MANY_REQUESTS,
                json!({"error": "Rate limit exceeded"}),
            );
            return sent - 1;
        }
        assert_eq!(response.status_code(), StatusCode::OK);
    }
    panic!(
        "Expected read rate limit within {} requests",
        READ_BURST * 4
    );
}

async fn exhaust_deal_run_bucket(ctx: &TestContext) {
    for _ in 0..DEAL_RUN_BURST {
        let response = ctx.app.post("/deals/123/runs").await;
        assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    }

    let response = ctx.app.post("/deals/123/runs").await;
    assert_json_response(
        response,
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error": "Rate limit exceeded"}),
    );
}

#[tokio::test]
async fn test_read_burst_allows_500_requests_then_429() {
    let ctx = TestContext::new().await;

    let allowed = exhaust_read_bucket(&ctx).await;

    assert!(
        allowed >= READ_BURST,
        "Expected at least {READ_BURST} reads before 429, got {allowed}"
    );
}

#[tokio::test]
async fn test_deal_run_429_after_burst() {
    let ctx = TestContext::new().await;

    exhaust_deal_run_bucket(&ctx).await;
}

#[tokio::test]
async fn test_exhausted_deal_run_bucket_does_not_limit_reads_or_upserts() {
    let ctx = TestContext::new().await;

    exhaust_deal_run_bucket(&ctx).await;

    let response = ctx.app.get("/healthcheck").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let response = ctx.app.put("/deals/123").await;
    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_exhausted_read_bucket_does_not_limit_upserts_or_runs() {
    let ctx = TestContext::new().await;

    exhaust_read_bucket(&ctx).await;

    // GET and PUT share the /deals/{deal_id} path but not the bucket.
    // Retry because the read bucket refills one token every 5 ms.
    let mut get_deal_limited = false;
    for _ in 0..10 {
        if ctx.app.get("/deals/123").await.status_code() == StatusCode::TOO_MANY_REQUESTS {
            get_deal_limited = true;
            break;
        }
    }
    assert!(
        get_deal_limited,
        "Expected GET /deals/{{deal_id}} to share the read bucket"
    );

    let response = ctx.app.put("/deals/123").await;
    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);

    let response = ctx.app.post("/deals/123/runs").await;
    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
}
