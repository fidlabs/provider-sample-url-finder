use crate::common::*;
use serde_json::json;

#[tokio::test]
async fn test_find_client_invalid_client() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(&ctx.app, "/url/client/invalid", "Invalid client address").await;
}

#[tokio::test]
async fn test_find_client_no_providers() {
    let ctx = TestContext::new().await;
    let client_id = "9998000";

    let response = ctx.app.get(&format!("/url/client/f0{client_id}")).await;

    assert_json_response_ok(
        response,
        json!({
            "result": "Error",
            "client": format!("f0{client_id}"),
            "providers": []
        }),
    );
}

#[tokio::test]
async fn test_find_client_providers_found() {
    let ctx = TestContext::new().await;
    let client_id = "9999000";

    let fixture = ctx
        .setup_provider_with_deals("99998000", Some(client_id), multiaddrs_http_8080())
        .await;

    let response = ctx.app.get(&format!("/url/client/f0{client_id}")).await;

    assert_json_response_ok(
        response,
        json!({
            "result": "Success",
            "client": format!("f0{client_id}"),
            "providers": [{
                "provider": fixture.provider_address,
                "result": "Success",
                "retrievability_percent": 0.0
            }]
        }),
    );
}
