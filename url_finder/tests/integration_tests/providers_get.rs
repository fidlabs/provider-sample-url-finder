use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use chrono::Utc;
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
        Some(85.5),
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
        Some(75.0),
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
async fn test_get_provider_with_multi_level_retri() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_with_metadata(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(90.0), // lenient retri (stored in DB column)
        Some(60.0), // car_retrievability
        Some(80.0), // full_piece_retrievability
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
        Some(json!({
            "counts": {
                "sample_count": 10,
                "http_responded_count": 9,
                "success_count": 8,
                "valid_car_count": 6,
                "small_car_count": 0,
                "timeout_count": 0,
                "failed_count": 1
            },
            "inconsistency_breakdown": {
                "total": 1,
                "warm_up": 1,
                "flaky": 0,
                "small_responses": 0,
                "size_mismatch": 0
            },
            "sector_utilization": {
                "sample_count": 8,
                "min_percent": 85.0,
                "max_percent": 98.0
            }
        })),
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
            "retrievability_percent": 90.0,
            "large_files_percent": 80.0,
            "car_files_percent": 60.0
        })
    );
}

#[tokio::test]
async fn test_get_provider_without_metadata_omits_new_fields() {
    let ctx = TestContext::new().await;

    // Seed without url_metadata -- simulates old records
    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(75.0),
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
            "retrievability_percent": 75.0
        })
    );

    // large_files_percent should be absent (skip_serializing_if = None)
    assert!(
        body.get("large_files_percent").is_none() || body["large_files_percent"].is_null(),
        "large_files_percent should be absent or null without metadata"
    );

    assert!(
        body.get("car_files_percent").is_none() || body["car_files_percent"].is_null(),
        "car_files_percent should be absent or null without metadata"
    );
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
