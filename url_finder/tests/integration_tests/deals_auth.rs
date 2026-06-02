use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::common::*;

fn deal_request() -> Value {
    json!({
        "deal_version": "v2",
        "provider_id": "1234",
        "client": "5678",
        "pieces": [{
            "piece_cid": "baga6ea4seaq",
            "piece_size_bytes": "1024",
            "allocation_id": "44",
            "claim_id": "44"
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
async fn test_post_run_without_auth_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx.app.post("/deals/123/runs").await;

    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    let body: Value = response.json();
    assert_eq!(body, json!({ "error": "Unauthorized" }));
}

#[tokio::test]
async fn test_post_run_with_wrong_auth_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("wrong-token")
        .await;

    assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED);
    let body: Value = response.json();
    assert_eq!(body, json!({ "error": "Unauthorized" }));
}
