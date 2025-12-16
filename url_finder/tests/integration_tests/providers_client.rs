use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_get_provider_client_success() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some(TEST_CLIENT_ID_DB),
        Some(TEST_WORKING_URL),
        92.5,
        "Success",
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{}/clients/{}",
            TEST_PROVIDER_1_API, TEST_CLIENT_ID_API
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "provider_id": TEST_PROVIDER_1_API,
            "client_id": TEST_CLIENT_ID_API,
            "working_url": TEST_WORKING_URL,
            "retrievability_percent": 92.5
        })
    );
}

#[tokio::test]
async fn test_get_provider_client_not_found() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        80.0,
        "Success",
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{}/clients/{}",
            TEST_PROVIDER_1_API, TEST_CLIENT_ID_API
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_provider_client_invalid_provider() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/invalid/clients/{}",
            TEST_CLIENT_ID_API
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "INVALID_ADDRESS"}));
}

#[tokio::test]
async fn test_get_provider_client_invalid_client() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{}/clients/invalid",
            TEST_PROVIDER_1_API
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "INVALID_ADDRESS"}));
}
