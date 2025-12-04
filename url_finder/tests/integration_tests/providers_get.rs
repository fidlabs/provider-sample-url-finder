use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_get_provider_success() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        85.5,
        "Success",
    )
    .await;
    seed_bms_bandwidth_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        TEST_WORKING_URL,
        "completed",
        Some(10.5),
        Some(25.0),
        Some(50.0),
        Some(100.0),
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "provider_id": TEST_PROVIDER_1_API,
            "working_url": TEST_WORKING_URL,
            "retrievability_percent": 85.5,
            "performance": {
                "bandwidth": {
                    "status": "completed",
                    "ping_avg_ms": 10.5,
                    "head_avg_ms": 25.0,
                    "ttfb_ms": 50.0,
                    "download_speed_mbps": 100.0
                }
            }
        })
    );
    assert!(body["tested_at"].is_string());
}

#[tokio::test]
async fn test_get_provider_without_bms() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some(TEST_WORKING_URL),
        75.0,
        "Success",
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_2_API}"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "provider_id": TEST_PROVIDER_2_API,
            "working_url": TEST_WORKING_URL,
            "retrievability_percent": 75.0
        })
    );
    assert!(body["performance"]["bandwidth"].is_null());
}

#[tokio::test]
async fn test_get_provider_invalid_address() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/providers/invalid").await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "INVALID_ADDRESS"}));
}

#[tokio::test]
async fn test_get_provider_not_found() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/providers/f099999999").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "NOT_FOUND"}));
}
