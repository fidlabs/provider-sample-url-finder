use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_bulk_providers_mixed() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        Some(90.0),
        "Success",
    )
    .await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some(TEST_WORKING_URL_2),
        Some(85.0),
        "Success",
    )
    .await;

    let response = ctx
        .app
        .post("/providers/bulk")
        .json(&json!({
            "provider_ids": [TEST_PROVIDER_1_API, TEST_PROVIDER_2_API, "f099999999"]
        }))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["providers"].as_array().unwrap().len(), 2);
    assert_json_include!(
        actual: body,
        expected: json!({
            "not_found": ["f099999999"]
        })
    );
}

#[tokio::test]
async fn test_bulk_providers_all_not_found() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .post("/providers/bulk")
        .json(&json!({
            "provider_ids": ["f099999991", "f099999992"]
        }))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "providers": []
        })
    );
    assert_eq!(body["not_found"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_bulk_providers_exceeds_limit() {
    let ctx = TestContext::new().await;

    let provider_ids: Vec<String> = (0..101).map(|i| format!("f0{i}")).collect();

    let response = ctx
        .app
        .post("/providers/bulk")
        .json(&json!({ "provider_ids": provider_ids }))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert!(body["error"].as_str().unwrap().contains("exceeds maximum"));
}
