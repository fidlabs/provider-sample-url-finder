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
        seed_url_result(
            &ctx.dbs.app_pool,
            db_id,
            None,
            Some(url),
            Some(80.0),
            "Success",
        )
        .await;
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
            Some(80.0),
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

#[tokio::test]
async fn test_list_providers_filter_has_working_url_true() {
    let ctx = TestContext::new().await;

    // Provider 1: has working URL, consistent
    seed_provider_with_url_status(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some("http://example.com/piece/123"),
        Some(true),
    )
    .await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some("http://example.com/piece/123"),
        Some(80.0),
        "Success",
    )
    .await;

    // Provider 2: no working URL
    seed_provider_with_url_status(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB, None, Some(true)).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        None,
        None,
        "NoDealsFound",
    )
    .await;

    // Filter for providers WITH working URL
    let response = ctx.app.get("/providers?has_working_url=true").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["total"].as_i64().unwrap(), 1);
    assert_eq!(body["providers"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_list_providers_filter_has_working_url_false() {
    let ctx = TestContext::new().await;

    // Provider 1: has working URL
    seed_provider_with_url_status(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some("http://example.com/piece/123"),
        Some(true),
    )
    .await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some("http://example.com/piece/123"),
        Some(80.0),
        "Success",
    )
    .await;

    // Provider 2: no working URL
    seed_provider_with_url_status(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB, None, Some(true)).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        None,
        None,
        "NoDealsFound",
    )
    .await;

    // Filter for providers WITHOUT working URL
    let response = ctx.app.get("/providers?has_working_url=false").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["total"].as_i64().unwrap(), 1);
    assert_eq!(body["providers"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_list_providers_filter_is_consistent() {
    let ctx = TestContext::new().await;

    // Provider 1: has working URL, consistent
    seed_provider_with_url_status(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some("http://example.com/piece/123"),
        Some(true),
    )
    .await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some("http://example.com/piece/123"),
        Some(80.0),
        "Success",
    )
    .await;

    // Provider 2: has working URL, NOT consistent
    seed_provider_with_url_status(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        Some("http://example.com/piece/456"),
        Some(false),
    )
    .await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some("http://example.com/piece/456"),
        Some(60.0),
        "Success",
    )
    .await;

    // Filter for consistent providers only
    let response = ctx.app.get("/providers?is_consistent=true").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["total"].as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_list_providers_filter_combined() {
    let ctx = TestContext::new().await;

    // Provider 1: has URL, consistent (BMS ready)
    seed_provider_with_url_status(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        Some("http://example.com/piece/123"),
        Some(true),
    )
    .await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some("http://example.com/piece/123"),
        Some(80.0),
        "Success",
    )
    .await;

    // Provider 2: has URL, NOT consistent
    seed_provider_with_url_status(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        Some("http://example.com/piece/456"),
        Some(false),
    )
    .await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some("http://example.com/piece/456"),
        Some(60.0),
        "Success",
    )
    .await;

    // Provider 3: no URL
    seed_provider_with_url_status(&ctx.dbs.app_pool, TEST_PROVIDER_3_DB, None, Some(true)).await;
    seed_url_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_3_DB,
        None,
        None,
        None,
        "NoDealsFound",
    )
    .await;

    // Filter for BMS-ready providers (has_working_url=true AND is_consistent=true)
    let response = ctx
        .app
        .get("/providers?has_working_url=true&is_consistent=true")
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    assert_eq!(body["total"].as_i64().unwrap(), 1);
    assert_eq!(body["providers"].as_array().unwrap().len(), 1);
}
