use crate::common::*;
use serde_json::json;

#[tokio::test]
async fn test_find_retri_sp_invalid_provider() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(
        &ctx.app,
        "/url/retrievability/invalid",
        "Invalid provider address",
    )
    .await;
}

#[tokio::test]
async fn test_find_retri_sp_no_deals() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99994000", multiaddrs_http_80())
        .await;

    let response = ctx
        .app
        .get(&format!("/url/retrievability/{}", fixture.provider_address))
        .await;

    assert_json_response_ok(
        response,
        json!({
            "result": "NoDealsFound",
            "retrievability_percent": 0.0
        }),
    );
}

#[tokio::test]
async fn test_find_retri_sp_endpoints_discovered() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals("99995000", None, multiaddrs_http_8080())
        .await;

    let response = ctx
        .app
        .get(&format!("/url/retrievability/{}", fixture.provider_address))
        .await;

    assert_json_response_ok(
        response,
        json!({
            "result": "Success",
            "retrievability_percent": 0.0
        }),
    );
}
