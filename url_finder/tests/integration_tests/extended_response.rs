//! Integration tests for extended provider response functionality.
//!
//! Tests verify that:
//! - Standard responses include is_consistent/is_reliable but NOT diagnostics/scheduling
//! - Extended responses (extended=true) include diagnostics and scheduling sections
//! - List and bulk endpoints respect the extended flag

use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use chrono::Utc;
use serde_json::json;

use crate::common::*;

// =============================================================================
// GET /providers/{id} tests
// =============================================================================

#[tokio::test]
async fn test_get_provider_standard_response_has_quality_metrics() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        85.0,
        "Success",
        Utc::now(),
        Some(true), // is_consistent
        Some(true), // is_reliable
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Standard fields present
    assert_json_include!(
        actual: body,
        expected: json!({
            "provider_id": TEST_PROVIDER_1_API,
            "working_url": TEST_WORKING_URL,
            "retrievability_percent": 85.0,
            "is_consistent": true,
            "is_reliable": true
        })
    );

    // Extended fields ABSENT in standard response
    assert!(
        body.get("diagnostics").is_none(),
        "diagnostics should not be present in standard response"
    );
    assert!(
        body.get("scheduling").is_none(),
        "scheduling should not be present in standard response"
    );
}

#[tokio::test]
async fn test_get_provider_standard_response_with_null_quality_metrics() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        75.0,
        "Success",
        Utc::now(),
        None, // is_consistent not set
        None, // is_reliable not set
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Fields should be absent (null -> omitted via skip_serializing_if)
    assert!(
        body.get("is_consistent").is_none(),
        "is_consistent should not be present when null"
    );
    assert!(
        body.get("is_reliable").is_none(),
        "is_reliable should not be present when null"
    );

    // Extended fields still absent
    assert!(body.get("diagnostics").is_none());
    assert!(body.get("scheduling").is_none());
}

#[tokio::test]
async fn test_get_provider_extended_response_has_diagnostics() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        90.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}?extended=true"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Standard fields still present
    assert_json_include!(
        actual: body,
        expected: json!({
            "provider_id": TEST_PROVIDER_1_API,
            "is_consistent": true,
            "is_reliable": true
        })
    );

    // Diagnostics present in extended response
    let diagnostics = body
        .get("diagnostics")
        .expect("diagnostics should be present in extended response");
    assert_json_include!(
        actual: diagnostics,
        expected: json!({
            "result_code": "Success"
        })
    );

    // scheduling is present when there's a storage_provider record
    // (it may be null if no SP record exists - depends on service implementation)
}

#[tokio::test]
async fn test_get_provider_extended_response_has_scheduling() {
    let ctx = TestContext::new().await;

    // Seed both provider and url_result
    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        80.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(false),
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}?extended=true"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Diagnostics always present in extended mode
    assert!(
        body.get("diagnostics").is_some(),
        "diagnostics should be present in extended response"
    );

    // scheduling should be present when SP record exists
    let scheduling = body
        .get("scheduling")
        .expect("scheduling should be present in extended response");

    // Verify scheduling structure
    assert!(
        scheduling.get("url_discovery").is_some(),
        "scheduling.url_discovery should exist"
    );
    assert!(
        scheduling.get("bms_test").is_some(),
        "scheduling.bms_test should exist"
    );
}

#[tokio::test]
async fn test_get_provider_extended_false_explicit() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        85.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    // Explicitly set extended=false
    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}?extended=false"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Quality metrics present
    assert!(body.get("is_consistent").is_some());
    assert!(body.get("is_reliable").is_some());

    // Extended fields absent
    assert!(body.get("diagnostics").is_none());
    assert!(body.get("scheduling").is_none());
}

// =============================================================================
// GET /providers (list) tests
// =============================================================================

#[tokio::test]
async fn test_list_providers_standard_response() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        80.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(false),
    )
    .await;

    let response = ctx.app.get("/providers").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let providers = body["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 1);

    let first = &providers[0];

    // Quality metrics present
    assert!(
        first.get("is_consistent").is_some(),
        "is_consistent should be present in list response"
    );

    // Extended fields absent
    assert!(
        first.get("diagnostics").is_none(),
        "diagnostics should not be present in standard list response"
    );
    assert!(
        first.get("scheduling").is_none(),
        "scheduling should not be present in standard list response"
    );
}

#[tokio::test]
async fn test_list_providers_extended_response() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        85.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx.app.get("/providers?extended=true").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let providers = body["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 1);

    let first = &providers[0];

    // Quality metrics present
    assert_json_include!(
        actual: first,
        expected: json!({
            "is_consistent": true,
            "is_reliable": true
        })
    );

    // Diagnostics present in extended mode
    assert!(
        first.get("diagnostics").is_some(),
        "diagnostics should be present in extended list response"
    );

    // NOTE: scheduling is intentionally NOT included in list responses
    // to avoid N+1 queries. This is by design.
    assert!(
        first.get("scheduling").is_none(),
        "scheduling should NOT be present in list response (N+1 avoidance)"
    );
}

#[tokio::test]
async fn test_list_providers_extended_multiple() {
    let ctx = TestContext::new().await;

    // Provider 1: consistent and reliable
    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        90.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    // Provider 2: not consistent, not reliable
    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some(TEST_WORKING_URL_2),
        50.0,
        "Success",
        Utc::now(),
        Some(false),
        Some(false),
    )
    .await;

    let response = ctx.app.get("/providers?extended=true&limit=10").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let providers = body["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 2);

    // All providers should have diagnostics
    for provider in providers {
        assert!(
            provider.get("diagnostics").is_some(),
            "each provider should have diagnostics in extended mode"
        );
    }
}

// =============================================================================
// POST /providers/bulk tests
// =============================================================================

#[tokio::test]
async fn test_bulk_providers_standard_response() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        85.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .post("/providers/bulk")
        .json(&json!({
            "provider_ids": [TEST_PROVIDER_1_API]
        }))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let providers = body["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 1);

    let first = &providers[0];

    // Quality metrics present
    assert!(first.get("is_consistent").is_some());
    assert!(first.get("is_reliable").is_some());

    // Extended fields absent
    assert!(
        first.get("diagnostics").is_none(),
        "diagnostics should not be present in standard bulk response"
    );
}

#[tokio::test]
async fn test_bulk_providers_extended_response() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        90.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(false),
    )
    .await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_2_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_2_DB,
        None,
        Some(TEST_WORKING_URL_2),
        75.0,
        "Success",
        Utc::now(),
        Some(false),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .post("/providers/bulk?extended=true")
        .json(&json!({
            "provider_ids": [TEST_PROVIDER_1_API, TEST_PROVIDER_2_API]
        }))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let providers = body["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 2);

    // All providers should have diagnostics in extended mode
    for provider in providers {
        assert!(
            provider.get("diagnostics").is_some(),
            "diagnostics should be present in extended bulk response"
        );

        let diagnostics = provider.get("diagnostics").unwrap();
        assert!(
            diagnostics.get("result_code").is_some(),
            "diagnostics should have result_code"
        );
    }
}

#[tokio::test]
async fn test_bulk_providers_extended_with_not_found() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        80.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    let response = ctx
        .app
        .post("/providers/bulk?extended=true")
        .json(&json!({
            "provider_ids": [TEST_PROVIDER_1_API, "f099999999"]
        }))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // One found with diagnostics
    let providers = body["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 1);
    assert!(providers[0].get("diagnostics").is_some());

    // One not found
    let not_found = body["not_found"].as_array().expect("not_found array");
    assert_eq!(not_found.len(), 1);
    assert_eq!(not_found[0].as_str().unwrap(), "f099999999");
}

// =============================================================================
// BMS extended fields tests
// =============================================================================

#[tokio::test]
async fn test_get_provider_bms_extended_fields() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        Some(TEST_WORKING_URL),
        85.0,
        "Success",
        Utc::now(),
        Some(true),
        Some(true),
    )
    .await;

    // Seed BMS result with extended fields
    seed_bms_bandwidth_result(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        TEST_WORKING_URL,
        "completed",
        Some(10.0),  // ping_avg_ms
        Some(25.0),  // head_avg_ms
        Some(50.0),  // ttfb_ms
        Some(100.0), // download_speed_mbps
    )
    .await;

    // Standard response - BMS extended fields should be absent
    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let bandwidth = &body["performance"]["bandwidth"];
    assert!(bandwidth["status"].is_string());
    assert!(bandwidth["ping_avg_ms"].is_number());

    // Extended BMS fields should be absent in standard response
    assert!(
        bandwidth.get("worker_count").is_none() || bandwidth["worker_count"].is_null(),
        "worker_count should not be in standard response"
    );
    assert!(
        bandwidth.get("routing_key").is_none() || bandwidth["routing_key"].is_null(),
        "routing_key should not be in standard response"
    );
    assert!(
        bandwidth.get("url_tested").is_none() || bandwidth["url_tested"].is_null(),
        "url_tested should not be in standard response"
    );

    // Extended response - BMS extended fields should be present
    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}?extended=true"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    let bandwidth = &body["performance"]["bandwidth"];

    // Extended BMS fields should be present
    assert!(
        bandwidth.get("worker_count").is_some() && !bandwidth["worker_count"].is_null(),
        "worker_count should be present in extended response"
    );
    assert!(
        bandwidth.get("routing_key").is_some() && !bandwidth["routing_key"].is_null(),
        "routing_key should be present in extended response"
    );
    assert!(
        bandwidth.get("url_tested").is_some() && !bandwidth["url_tested"].is_null(),
        "url_tested should be present in extended response"
    );
}

// =============================================================================
// Error code in diagnostics tests
// =============================================================================

#[tokio::test]
async fn test_get_provider_extended_with_error_code() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;
    // Seed with a failure result code
    seed_url_result_at(
        &ctx.dbs.app_pool,
        TEST_PROVIDER_1_DB,
        None,
        None, // No working URL
        0.0,
        "NoDealsFound",
        Utc::now(),
        None,
        None,
    )
    .await;

    let response = ctx
        .app
        .get(&format!("/providers/{TEST_PROVIDER_1_API}?extended=true"))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();

    // Diagnostics present
    let diagnostics = body
        .get("diagnostics")
        .expect("diagnostics should be present");

    assert_json_include!(
        actual: diagnostics,
        expected: json!({
            "result_code": "NoDealsFound"
        })
    );
}
