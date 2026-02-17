use crate::common::test_constants::TEST_PIECE_CID;
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
async fn test_find_retri_sp_not_indexed() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99994000", multiaddrs_http_80())
        .await;

    let response = ctx
        .app
        .get(&format!("/url/retrievability/{}", fixture.provider_address))
        .await;

    let body = assert_json_response_ok(
        response,
        json!({
            "result": "Error",
            "retrievability_percent": null
        }),
    );
    assert_message_contains(&body, "not been indexed");
}

#[tokio::test]
async fn test_find_retri_sp_no_deals() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99994001", multiaddrs_http_80())
        .await;

    ctx.run_discovery_for_provider(&fixture, None).await;

    let response = ctx
        .app
        .get(&format!("/url/retrievability/{}", fixture.provider_address))
        .await;

    assert_json_response_ok(
        response,
        json!({
            "result": "NoDealsFound",
            "retrievability_percent": null
        }),
    );
}

#[tokio::test]
async fn test_find_retri_sp_endpoints_discovered() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server("99995000", None, vec![TEST_PIECE_CID], 1.0)
        .await;

    // Run discovery to populate url_results
    ctx.run_discovery_for_provider(&fixture, None).await;

    let response = ctx
        .app
        .get(&format!("/url/retrievability/{}", fixture.provider_address))
        .await;

    assert_json_response_ok(
        response,
        json!({
            "result": "Success",
            "retrievability_percent": 100.0
        }),
    );
}
