use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_get_client_providers_success() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some(TEST_CLIENT_ID_DB),
        Some(TEST_WORKING_URL),
        Some(90.0),
        "Success",
    )
    .await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        Some(TEST_CLIENT_ID_DB),
        Some(TEST_WORKING_URL_2),
        Some(85.0),
        "Success",
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/clients/{}/providers", TEST_CLIENT_ID_API))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "client_id": TEST_CLIENT_ID_API,
            "total": 2
        })
    );
    assert_eq!(body["providers"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_get_client_providers_not_found() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/clients/f099999999/providers").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "NOT_FOUND"}));
}

#[tokio::test]
async fn test_get_client_providers_invalid_address() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/clients/invalid/providers").await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: json!({"error_code": "INVALID_ADDRESS"}));
}
