use crate::common::*;
use serde_json::json;

#[tokio::test]
async fn test_find_url_sp_invalid_provider() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(&ctx.app, "/url/find/invalid", "Invalid provider address").await;
}

#[tokio::test]
async fn test_find_url_sp_not_indexed() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99991234", multiaddrs_http_80())
        .await;

    let response = ctx
        .app
        .get(&format!("/url/find/{}", fixture.provider_address))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "Error"}));
    assert_no_url(&body);
    assert_message_contains(&body, "not been indexed");
}

#[tokio::test]
async fn test_find_url_sp_no_deals() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_no_deals("99991235", multiaddrs_http_80())
        .await;

    ctx.run_discovery_for_provider(&fixture, None).await;

    let response = ctx
        .app
        .get(&format!("/url/find/{}", fixture.provider_address))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "NoDealsFound"}));
    assert_no_url(&body);
}

#[tokio::test]
async fn test_find_url_sp_unreachable_endpoints() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals("99995678", None, multiaddrs_http_8080())
        .await;

    ctx.run_discovery_for_provider(&fixture, None).await;

    let response = ctx
        .app
        .get(&format!("/url/find/{}", fixture.provider_address))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "FailedToGetWorkingUrl"}));
    assert_no_url(&body);
}

#[tokio::test]
async fn test_find_url_sp_success() {
    let ctx = TestContext::new().await;

    let piece_cid = TEST_PIECE_CID;
    ctx.mocks.setup_piece_retrieval_mock(piece_cid, true).await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server("88885000", None, vec![piece_cid], 1.0)
        .await;

    ctx.run_discovery_for_provider(&fixture, None).await;

    let response = ctx
        .app
        .get(&format!("/url/find/{}", fixture.provider_address))
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
async fn test_find_url_sp_partial_retrievability() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            "88886000",
            None,
            vec![TEST_PIECE_CID, TEST_PIECE_CID_2],
            0.5,
        )
        .await;

    ctx.run_discovery_for_provider(&fixture, None).await;

    let response = ctx
        .app
        .get(&format!("/url/find/{}", fixture.provider_address))
        .await;

    let body = assert_json_response_ok(response, json!({"result": "Success"}));

    let url = body["url"]
        .as_str()
        .expect("URL should be present even with partial retrievability");
    assert!(
        url.contains(&ctx.mocks.piece_server_url()),
        "URL should contain mock server address"
    );
    assert!(
        url.contains(TEST_PIECE_CID) || url.contains(TEST_PIECE_CID_2),
        "URL should contain piece CID"
    );
}
