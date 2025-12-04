use crate::common::test_constants::TEST_PIECE_CID;
use crate::common::*;
use serde_json::json;
use url_finder::types::ClientAddress;

#[tokio::test]
async fn test_find_client_invalid_client() {
    let ctx = TestContext::new().await;
    assert_bad_request_error(&ctx.app, "/url/client/invalid", "Invalid client address").await;
}

#[tokio::test]
async fn test_find_client_no_providers() {
    let ctx = TestContext::new().await;
    let client_id = "9998000";

    // No discovery run - client has no results in url_results
    let response = ctx.app.get(&format!("/url/client/f0{client_id}")).await;

    let body = assert_json_response_ok(
        response,
        json!({
            "result": "Error",
            "client": format!("f0{client_id}"),
            "providers": []
        }),
    );
    assert_message_contains(&body, "No providers found");
}

#[tokio::test]
async fn test_find_client_providers_found() {
    let ctx = TestContext::new().await;
    let client_id = "9999000";
    let client_address = ClientAddress::new(format!("f0{client_id}")).unwrap();

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            "99998000",
            Some(client_id),
            vec![TEST_PIECE_CID],
            1.0,
        )
        .await;

    ctx.run_discovery_for_provider(&fixture, Some(client_address))
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
                "retrievability_percent": 100.0
            }]
        }),
    );
}
