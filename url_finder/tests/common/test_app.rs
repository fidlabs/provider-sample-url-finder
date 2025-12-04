#![allow(dead_code)]

use axum::{
    extract::Request,
    middleware::{self, Next},
    response::Response,
};
use axum_test::TestServer;
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicUsize},
};
use url_finder::{
    AppState, config::Config, repository::*, services::provider_service::ProviderService,
};

use super::{TestDatabases, mock_servers::MockExternalServices};

async fn inject_socket_addr(mut request: Request, next: Next) -> Response {
    let mock_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    request.extensions_mut().insert(mock_addr);
    next.run(request).await
}

pub async fn create_test_app(dbs: &TestDatabases, mocks: &MockExternalServices) -> TestServer {
    let active_requests = Arc::new(AtomicUsize::new(0));

    let lotus_url = mocks.lotus_url();
    let lotus_base = lotus_url.trim_end_matches('/');
    let config = Arc::new(Config::new_for_test(
        format!("{lotus_base}/rpc/v1"),
        mocks.cid_contact_url(),
    ));

    let url_repo = Arc::new(UrlResultRepository::new(dbs.app_pool.clone()));
    let bms_repo = Arc::new(BmsBandwidthResultRepository::new(dbs.app_pool.clone()));
    let provider_service = Arc::new(ProviderService::new(url_repo.clone(), bms_repo.clone()));

    let app_state = Arc::new(AppState {
        deal_repo: Arc::new(DealRepository::new(dbs.app_pool.clone())),
        active_requests,
        storage_provider_repo: Arc::new(StorageProviderRepository::new(dbs.app_pool.clone())),
        url_repo,
        bms_repo,
        provider_service,
        config,
    });

    let app = url_finder::routes::create_routes()
        .layer(middleware::from_fn(inject_socket_addr))
        .with_state(app_state.clone());

    TestServer::new(app).expect("Failed to create test server")
}
