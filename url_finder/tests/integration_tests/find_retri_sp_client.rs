use crate::common::test_constants::TEST_PIECE_CID;
use crate::common::*;
use serde_json::json;

#[tokio::test]
async fn test_find_retri_sp_client_invalid_provider() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(
        &ctx.app,
        &format!("/url/retrievability/invalid/{TEST_CLIENT_ID_API}"),
        "Invalid provider address",
    )
    .await;
}

#[tokio::test]
async fn test_find_retri_sp_client_invalid_client() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(
        &ctx.app,
        "/url/retrievability/f01234/invalid",
        "Invalid client address",
    )
    .await;
}

#[tokio::test]
async fn test_find_retri_sp_client_not_indexed() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99996000", multiaddrs_http_80())
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/retrievability/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
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
async fn test_find_retri_sp_client_no_deals() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99996001", multiaddrs_http_80())
        .await;

    ctx.run_discovery_for_provider(&fixture, Some(test_client_address()))
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/retrievability/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
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
async fn test_find_retri_sp_client_endpoints_discovered() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            "99997000",
            Some(TEST_CLIENT_ID_DB),
            vec![TEST_PIECE_CID],
            1.0,
        )
        .await;

    ctx.run_discovery_for_provider(&fixture, Some(test_client_address()))
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/retrievability/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
        .await;

    assert_json_response_ok(
        response,
        json!({
            "result": "Success",
            "retrievability_percent": 100.0
        }),
    );
}
