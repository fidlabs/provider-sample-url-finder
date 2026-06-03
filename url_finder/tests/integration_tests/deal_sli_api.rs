use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::common::*;

#[derive(sqlx::FromRow)]
struct StoredDealSliRun {
    state: String,
    measurement_state: String,
    result_code: Option<url_finder::ResultCode>,
    piece_count: i32,
    success_count: i32,
    failed_count: i32,
}

#[derive(sqlx::FromRow)]
struct StoredDealSliPieceResult {
    piece_index: i32,
    piece_cid: String,
    url_tested: String,
    success: bool,
    result_code: Option<url_finder::ResultCode>,
}

fn deal_request() -> Value {
    json!({
        "deal_version": "v2",
        "provider_id": "1234",
        "client": "5678",
        "manifest_hash": "bafy-manifest",
        "manifest_location": "https://example.com/manifest.car",
        "requirements": {
            "retrievability_bps": 9500,
            "bandwidth_mbps": 200,
            "latency_ms": 150
        },
        "pieces": [
            {
                "piece_cid": "baga6ea4seaq",
                "piece_size_bytes": "1024",
                "allocation_id": "44",
                "claim_id": "44"
            },
            {
                "piece_cid": "baga6ea4sear",
                "piece_size_bytes": "2048",
                "allocation_id": "55",
                "claim_id": "55"
            }
        ]
    })
}

#[tokio::test]
async fn test_put_deal_persists_and_get_deal_returns_stored_target() {
    let ctx = TestContext::new().await;

    let put_response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await;

    assert_eq!(put_response.status_code(), StatusCode::OK);

    let get_response = ctx.app.get("/deals/123").await;

    assert_eq!(get_response.status_code(), StatusCode::OK);
    let body: Value = get_response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "deal_version": "v2",
            "provider_id": "1234",
            "client": "5678",
            "manifest_hash": "bafy-manifest",
            "manifest_location": "https://example.com/manifest.car",
            "requirements": {
                "retrievability_bps": 9500,
                "bandwidth_mbps": 200,
                "latency_ms": 150
            },
            "pieces": [
                {
                    "piece_cid": "baga6ea4seaq",
                    "piece_size_bytes": "1024",
                    "allocation_id": "44",
                    "claim_id": "44"
                },
                {
                    "piece_cid": "baga6ea4sear",
                    "piece_size_bytes": "2048",
                    "allocation_id": "55",
                    "claim_id": "55"
                }
            ]
        })
    );
    assert!(body.get("created_at").and_then(Value::as_str).is_some());
    assert!(body.get("updated_at").and_then(Value::as_str).is_some());
}

#[tokio::test]
async fn test_put_deal_with_invalid_decimal_deal_id_returns_bad_request() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .put("/deals/not-a-decimal")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_empty_pieces_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request();
    request["pieces"] = json!([]);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_fractional_piece_size_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request();
    request["pieces"][0]["piece_size_bytes"] = json!("1024.5");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_overflowing_requirement_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request();
    request["requirements"]["bandwidth_mbps"] = json!(u32::MAX);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_invalid_retrievability_requirement_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request();
    request["requirements"]["retrievability_bps"] = json!(10_001);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_get_deal_returns_not_found_when_target_missing() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/deals/123").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "NOT_FOUND"
        })
    );
}

#[tokio::test]
async fn test_get_latest_returns_missing_state_with_target_piece_count_when_no_runs_exist() {
    let ctx = TestContext::new().await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await
        .assert_status_ok();

    let response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "missing",
            "piece_count": 2,
            "success_count": 0,
            "failed_count": 0,
            "result_code": null
        })
    );
}

#[tokio::test]
async fn test_get_latest_returns_not_found_when_target_missing() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "NOT_FOUND"
        })
    );
}

#[tokio::test]
async fn test_post_run_without_cached_endpoints_persists_failed_run_and_latest_uses_it() {
    let ctx = TestContext::new().await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await
        .assert_status_ok();

    seed_provider(&ctx.dbs.app_pool, "1234").await;

    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "failed",
            "piece_count": 2,
            "success_count": 0,
            "failed_count": 2,
            "result_code": "MissingHttpAddrFromCidContact"
        })
    );
    assert!(run_body.get("tested_at").and_then(Value::as_str).is_some());

    let row = sqlx::query_as::<_, StoredDealSliRun>(
        r#"SELECT
                state,
                measurement_state,
                result_code,
                piece_count,
                success_count,
                failed_count
           FROM
                deal_sli_runs
           WHERE
                deal_id = $1
        "#,
    )
    .bind("123")
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("stored run should exist");

    assert_eq!(row.state, "completed");
    assert_eq!(row.measurement_state, "failed");
    assert_eq!(
        row.result_code,
        Some(url_finder::ResultCode::MissingHttpAddrFromCidContact)
    );
    assert_eq!(row.piece_count, 2);
    assert_eq!(row.success_count, 0);
    assert_eq!(row.failed_count, 2);

    let latest_response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(latest_response.status_code(), StatusCode::OK);
    let latest_body: Value = latest_response.json();
    assert_json_include!(
        actual: latest_body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "failed",
            "piece_count": 2,
            "success_count": 0,
            "failed_count": 2,
            "result_code": "MissingHttpAddrFromCidContact"
        })
    );
}

#[tokio::test]
async fn test_post_run_with_cached_endpoint_tests_target_pieces_and_stores_piece_results() {
    let ctx = TestContext::new().await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await
        .assert_status_ok();

    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4seaq", true)
        .await;
    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4sear", false)
        .await;
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "fresh",
            "retrievability_percent": 50.0,
            "large_files_percent": 50.0,
            "piece_count": 2,
            "success_count": 1,
            "failed_count": 1,
            "result_code": "FailedToGetWorkingUrl"
        })
    );
    assert!(
        run_body
            .get("working_url")
            .and_then(Value::as_str)
            .is_some_and(|url| url.ends_with("/piece/baga6ea4seaq"))
    );

    let piece_rows = sqlx::query_as::<_, StoredDealSliPieceResult>(
        r#"SELECT
                piece_index,
                piece_cid,
                url_tested,
                success,
                result_code
           FROM
                deal_sli_piece_results
           WHERE
                deal_id = $1
           ORDER BY
                piece_index ASC
        "#,
    )
    .bind("123")
    .fetch_all(&ctx.dbs.app_pool)
    .await
    .expect("piece result rows should load");

    assert_eq!(piece_rows.len(), 2);
    assert_eq!(piece_rows[0].piece_index, 0);
    assert_eq!(piece_rows[0].piece_cid, "baga6ea4seaq");
    assert!(piece_rows[0].success);
    assert_eq!(
        piece_rows[0].result_code,
        Some(url_finder::ResultCode::Success)
    );
    assert!(piece_rows[0].url_tested.ends_with("/piece/baga6ea4seaq"));
    assert_eq!(piece_rows[1].piece_index, 1);
    assert_eq!(piece_rows[1].piece_cid, "baga6ea4sear");
    assert!(!piece_rows[1].success);
    assert_eq!(
        piece_rows[1].result_code,
        Some(url_finder::ResultCode::FailedToGetWorkingUrl)
    );
}

#[tokio::test]
async fn test_post_run_rejects_oversized_synchronous_fanout() {
    let ctx = TestContext::new().await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await
        .assert_status_ok();

    let endpoints = (0..257)
        .map(|index| format!("http://127.0.0.1:{}/", 10_000 + index))
        .collect::<Vec<_>>();
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &endpoints).await;

    let response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_rejects_piece_identity_change_after_no_endpoint_run_exists() {
    let ctx = TestContext::new().await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await
        .assert_status_ok();

    seed_provider(&ctx.dbs.app_pool, "1234").await;
    ctx.app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await
        .assert_status_ok();

    let mut changed_request = deal_request();
    changed_request["pieces"][0]["piece_cid"] = json!("baga6ea4seaz");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&changed_request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_rejects_piece_identity_change_after_piece_results_exist() {
    let ctx = TestContext::new().await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await
        .assert_status_ok();

    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4seaq", true)
        .await;
    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4sear", false)
        .await;
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;
    ctx.app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await
        .assert_status_ok();

    let mut changed_request = deal_request();
    changed_request["pieces"][0]["piece_cid"] = json!("baga6ea4seaz");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&changed_request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}
