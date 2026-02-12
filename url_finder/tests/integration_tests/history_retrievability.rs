use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use chrono::{Duration, Utc};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_history_returns_latest_per_day() {
    let ctx = TestContext::new().await;
    let now = Utc::now();
    let yesterday = now - Duration::days(1);

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    // Two results on same day - should return only latest (85.5)
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(75.0),
        "Success",
        yesterday - Duration::hours(2),
        Some(true),
        Some(true),
    )
    .await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(85.5),
        "Success",
        yesterday,
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["retrievability_percent"], 85.5);
}

#[tokio::test]
async fn test_history_date_range_filtering() {
    let ctx = TestContext::new().await;
    let now = Utc::now();

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    // Result 5 days ago
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(80.0),
        "Success",
        now - Duration::days(5),
        Some(true),
        Some(true),
    )
    .await;

    // Result 2 days ago
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(90.0),
        "Success",
        now - Duration::days(2),
        Some(true),
        Some(true),
    )
    .await;

    let from = (now - Duration::days(3)).format("%Y-%m-%d");
    let to = (now - Duration::days(1)).format("%Y-%m-%d");

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability?from={from}&to={to}"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Should only include the 2-day-ago result
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["retrievability_percent"], 90.0);
}

#[tokio::test]
async fn test_history_extended_fields() {
    let ctx = TestContext::new().await;
    let now = Utc::now();

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(85.5),
        "Success",
        now - Duration::days(1),
        Some(true),
        Some(false),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability?extended=true"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: &body["data"][0],
        expected: json!({
            "retrievability_percent": 85.5,
            "is_consistent": true,
            "is_reliable": false,
            "working_url": TEST_WORKING_URL,
            "result_code": "Success"
        })
    );
    assert!(body["data"][0]["tested_at"].is_string());
}

#[tokio::test]
async fn test_history_minimal_omits_extended() {
    let ctx = TestContext::new().await;
    let now = Utc::now();

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(85.5),
        "Success",
        now - Duration::days(1),
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Minimal: test-result fields present as null (always serialized)
    assert!(body["data"][0]["is_consistent"].is_null());
    assert!(body["data"][0]["working_url"].is_null());

    // Extended-only fields still absent (skip_serializing_if)
    assert!(body["data"][0].get("tested_at").is_none());

    // Should have minimal fields
    assert!(body["data"][0]["date"].is_string());
    assert_eq!(body["data"][0]["retrievability_percent"], 85.5);
}

#[tokio::test]
async fn test_history_empty_range_returns_empty_array() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    // No url_results seeded

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_history_invalid_date_range() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability?from=2026-01-10&to=2026-01-01"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "INVALID_DATE_RANGE"}));
}

#[tokio::test]
async fn test_history_exceeds_max_days() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/retrievability?from=2025-01-01&to=2026-01-09"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "DATE_RANGE_EXCEEDED"}));
}

#[tokio::test]
async fn test_history_invalid_provider() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .get("/providers/invalid/history/retrievability")
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "INVALID_ADDRESS"}));
}

#[tokio::test]
async fn test_history_provider_client() {
    let ctx = TestContext::new().await;
    let now = Utc::now();

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some(TEST_CLIENT_ID_DB),
        Some(TEST_WORKING_URL),
        Some(92.0),
        "Success",
        now - Duration::days(1),
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/clients/{TEST_CLIENT_ID_API}/history/retrievability"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["provider_id"], TEST_PROVIDER_1_API);
    assert_eq!(body["client_id"], TEST_CLIENT_ID_API);
    assert_eq!(body["data"][0]["retrievability_percent"], 92.0);
}
