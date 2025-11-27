use crate::common::*;
use serde_json::json;

#[tokio::test]
async fn test_find_url_sp_client_invalid_provider() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(
        &ctx.app,
        &format!("/url/find/invalid/{TEST_CLIENT_ID_API}"),
        "Invalid provider address",
    )
    .await;
}

#[tokio::test]
async fn test_find_url_sp_client_invalid_client() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(
        &ctx.app,
        "/url/find/f01234/invalid",
        "Invalid client address",
    )
    .await;
}

#[tokio::test]
async fn test_find_url_sp_client_not_indexed() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99992000", multiaddrs_http_80())
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/find/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "Error"}));
    assert_no_url(&body);
    assert_message_contains(&body, "not been indexed");
}

#[tokio::test]
async fn test_find_url_sp_client_no_deals() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99992001", multiaddrs_http_80())
        .await;

    // Run discovery to populate url_results
    ctx.run_discovery_for_provider(&fixture, Some(test_client_address()))
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/find/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "NoDealsFound"}));
    assert_no_url(&body);
}

#[tokio::test]
async fn test_find_url_sp_client_unreachable_endpoints() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals("99993000", Some(TEST_CLIENT_ID_DB), multiaddrs_http_8080())
        .await;

    ctx.run_discovery_for_provider(&fixture, Some(test_client_address()))
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/find/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "FailedToGetWorkingUrl"}));
    assert_no_url(&body);
}

#[tokio::test]
async fn test_find_url_sp_client_success() {
    let ctx = TestContext::new().await;

    let piece_cid = TEST_PIECE_CID;
    ctx.mocks.setup_piece_retrieval_mock(piece_cid, true).await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            "88883000",
            Some(TEST_CLIENT_ID_DB),
            vec![piece_cid],
            1.0,
        )
        .await;

    ctx.run_discovery_for_provider(&fixture, Some(test_client_address()))
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/find/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "Success"}));

    let url = body["url"].as_str().expect("URL should be present");
    assert!(
        url.contains(&ctx.mocks.piece_server_url()),
        "URL should contain mock server address"
    );
    assert!(url.contains(piece_cid), "URL should contain piece CID");
}

#[tokio::test]
async fn test_find_url_sp_client_partial_retrievability() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            "88884000",
            Some(TEST_CLIENT_ID_DB),
            vec![TEST_PIECE_CID, TEST_PIECE_CID_2],
            0.5,
        )
        .await;

    ctx.run_discovery_for_provider(&fixture, Some(test_client_address()))
        .await;

    let response = ctx
        .app
        .get(&format!(
            "/url/find/{}/{}",
            fixture.provider_address, TEST_CLIENT_ID_API
        ))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "Success"}));

    let url = body["url"]
        .as_str()
        .expect("URL should be present even with partial retrievability");
    assert!(
        url.contains(&ctx.mocks.piece_server_url()),
        "URL should contain mock server address"
    );
}
