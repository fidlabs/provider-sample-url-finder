use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_list_providers_success() {
    let ctx = TestContext::new().await;

    for (db_id, url) in [
        (TEST_PROVIDER_1_DB, TEST_WORKING_URL),
        (TEST_PROVIDER_2_DB, TEST_WORKING_URL_2),
        (TEST_PROVIDER_3_DB, TEST_WORKING_URL),
    ] {
        seed_provider(&ctx.dbs.app_pool, db_id).await;
        seed_url_result(&ctx.dbs.app_pool, db_id, None, Some(url), 80.0, "Success").await;
    }

    let response = ctx.app.get("/providers?limit=10&offset=0").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "total": 3,
            "limit": 10,
            "offset": 0
        })
    );
    assert_eq!(body["providers"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_list_providers_pagination() {
    let ctx = TestContext::new().await;

    for db_id in [TEST_PROVIDER_1_DB, TEST_PROVIDER_2_DB, TEST_PROVIDER_3_DB] {
        seed_provider(&ctx.dbs.app_pool, db_id).await;
        seed_url_result(
            &ctx.dbs.app_pool,
            db_id,
            None,
            Some(TEST_WORKING_URL),
            80.0,
            "Success",
        )
        .await;
    }

    let response = ctx.app.get("/providers?limit=2&offset=1").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "total": 3,
            "limit": 2,
            "offset": 1
        })
    );
    assert_eq!(body["providers"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_list_providers_empty() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/providers").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "total": 0,
            "providers": []
        })
    );
}
