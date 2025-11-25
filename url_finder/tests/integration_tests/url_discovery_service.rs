use crate::common::*;
use url_finder::{
    config::Config,
    repository::DealRepository,
    services::url_discovery_service::discover_url,
    types::{ClientAddress, ProviderAddress, ResultCode},
};

fn setup_discovery_params(
    ctx: &TestContext,
    fixture: &ProviderFixture,
) -> (ProviderAddress, ClientAddress, DealRepository, Config) {
    let provider_address = fixture.provider_address.clone();
    let client_address = test_client_address();
    let deal_repo = DealRepository::new(ctx.dbs.app_pool.clone());
    let lotus_url = ctx.mocks.lotus_url();
    let lotus_base = lotus_url.trim_end_matches('/');
    let config = Config::new_for_test(format!("{lotus_base}/rpc/v1"), ctx.mocks.cid_contact_url());
    (provider_address, client_address, deal_repo, config)
}

#[tokio::test]
async fn test_url_discovery_success() {
    let ctx = TestContext::new().await;

    let piece_cid = TEST_PIECE_CID;
    ctx.mocks.setup_piece_retrieval_mock(piece_cid, true).await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            TEST_PROVIDER_1_DB,
            Some(TEST_CLIENT_ID_DB),
            vec![piece_cid],
            1.0,
        )
        .await;

    let (provider_address, client_address, deal_repo, config) =
        setup_discovery_params(&ctx, &fixture);

    let result = discover_url(&config, &provider_address, Some(client_address), &deal_repo).await;

    assert_eq!(result.result_code, ResultCode::Success, "Expected Success");

    let url = result
        .working_url
        .as_ref()
        .expect("Expected working URL to be present");
    assert!(
        url.contains(&ctx.mocks.piece_server_url()),
        "URL should contain mock server address"
    );
    assert!(url.contains(piece_cid), "URL should contain piece CID");

    assert_eq!(
        result.retrievability_percent, 100.0,
        "Should have 100% retrievability"
    );
}

#[tokio::test]
async fn test_url_discovery_partial_retrievability() {
    let ctx = TestContext::new().await;

    let fixture = ctx
        .setup_provider_with_deals_and_mock_server(
            TEST_PROVIDER_2_DB,
            Some(TEST_CLIENT_ID_DB),
            vec![TEST_PIECE_CID, TEST_PIECE_CID_2],
            0.5,
        )
        .await;

    let (provider_address, client_address, deal_repo, config) =
        setup_discovery_params(&ctx, &fixture);

    let result = discover_url(&config, &provider_address, Some(client_address), &deal_repo).await;

    assert_eq!(
        result.result_code,
        ResultCode::Success,
        "Should succeed with partial retrievability"
    );

    let url = result
        .working_url
        .as_ref()
        .expect("Expected working URL to be present");
    assert!(
        url.contains(&ctx.mocks.piece_server_url()),
        "URL should contain mock server address"
    );
    assert!(
        url.contains(TEST_PIECE_CID) || url.contains(TEST_PIECE_CID_2),
        "URL should contain one of the piece CIDs"
    );

    assert_eq!(
        result.retrievability_percent, 50.0,
        "Should have 50% retrievability (1 of 2 pieces)"
    );
}
