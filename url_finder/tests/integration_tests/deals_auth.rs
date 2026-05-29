use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::common::*;

fn deal_request() -> Value {
    json!({
        "deal_version": "v2",
        "provider_id": "f01234",
        "client": "f05678",
        "manifest_hash": "bafy-manifest",
        "manifest_location": "https://example.com/manifest.car",
        "requested_size_bytes": "2048",
        "requirements": {
            "retrievability_bps": 9500,
            "bandwidth_mbps": 200,
            "latency_ms": 150
        },
        "pieces": [{
            "piece_cid": "baga6ea4seaq",
            "piece_size_bytes": "1024",
            "allocation_id": "44",
            "claim_id": "55"
        }]
    })
}

#[tokio::test]
async fn test_put_deal_without_auth_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx.app.put("/deals/123").json(&deal_request()).await;

    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    let body: Value = response.json();
    assert_eq!(body, json!({ "error": "Unauthorized" }));
}

#[tokio::test]
async fn test_put_deal_with_wrong_auth_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("wrong-token")
        .json(&deal_request())
        .await;

    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    let body: Value = response.json();
    assert_eq!(body, json!({ "error": "Unauthorized" }));
}

#[tokio::test]
async fn test_put_deal_without_auth_rejects_before_json_parsing() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .put("/deals/123")
        .content_type("application/json")
        .text("{")
        .await;

    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    let body: Value = response.json();
    assert_eq!(body, json!({ "error": "Unauthorized" }));
}

#[tokio::test]
async fn test_put_deal_with_wrong_auth_rejects_before_json_parsing() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("wrong-token")
        .content_type("application/json")
        .text("{")
        .await;

    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    let body: Value = response.json();
    assert_eq!(body, json!({ "error": "Unauthorized" }));
}

#[tokio::test]
async fn test_put_deal_with_correct_auth_returns_shell_response() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&deal_request())
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();

    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "deal_version": "v2",
            "provider_id": "f01234",
            "manifest_hash": "bafy-manifest",
            "pieces": [{
                "piece_cid": "baga6ea4seaq",
                "claim_id": "55"
            }]
        })
    );
}

#[tokio::test]
async fn test_get_deal_remains_public() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/deals/123").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "deal_version": "v2"
        })
    );
}

#[tokio::test]
async fn test_get_latest_deal_measurement_remains_public() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "missing",
            "piece_count": 0,
            "success_count": 0,
            "failed_count": 0
        })
    );
}
