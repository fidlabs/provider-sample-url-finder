#![allow(dead_code)]

use axum_test::TestServer;
use sqlx::{Postgres, migrate::MigrateDatabase};
use std::env;
use std::sync::Arc;
use tracing::{debug, warn};
use url_finder::types::{ProviderAddress, ProviderId};

use super::container::{ContainerState, get_or_create_container};
use super::db_setup::{
    POSTGRES_PASSWORD, POSTGRES_USER, TestDatabases, seed_deals, seed_provider,
    setup_test_db_with_port,
};
use super::mock_servers::MockExternalServices;
use super::test_app::create_test_app;
use super::test_constants::TEST_PIECE_CID;

pub struct TestContext {
    pub mocks: MockExternalServices,
    pub dbs: TestDatabases,
    pub app: TestServer,
    _container_ref: Arc<ContainerState>,
}

pub struct ProviderFixture {
    pub provider_id: ProviderId,
    pub provider_address: ProviderAddress,
    pub peer_id: String,
}

impl TestContext {
    pub async fn new() -> Self {
        let container_ref = get_or_create_container().await;
        let port = container_ref.port;

        let mocks = MockExternalServices::start().await;
        let dbs = setup_test_db_with_port(port).await;
        let app = create_test_app(&dbs, &mocks).await;

        Self {
            mocks,
            dbs,
            app,
            _container_ref: container_ref,
        }
    }

    pub async fn setup_provider_with_deals(
        &self,
        provider_id: &str,
        client_id: Option<&str>,
        multiaddrs: Vec<String>,
    ) -> ProviderFixture {
        let peer_id = format!("12D3Koo{}", provider_id);

        seed_provider(&self.dbs.app_pool, provider_id).await;
        seed_deals(
            &self.dbs.app_pool,
            provider_id,
            client_id,
            vec![TEST_PIECE_CID],
        )
        .await;

        self.mocks
            .setup_lotus_peer_id_mock(&format!("f0{provider_id}"), &peer_id, vec![])
            .await;

        self.mocks
            .setup_cid_contact_mock(&peer_id, multiaddrs)
            .await;

        ProviderFixture {
            provider_id: ProviderId::new(provider_id).unwrap(),
            provider_address: ProviderAddress::new(format!("f0{provider_id}")).unwrap(),
            peer_id,
        }
    }

    pub async fn setup_provider_no_deals(
        &self,
        provider_id: &str,
        multiaddrs: Vec<String>,
    ) -> ProviderFixture {
        let peer_id = format!("12D3Koo{}", provider_id);

        seed_provider(&self.dbs.app_pool, provider_id).await;

        self.mocks
            .setup_lotus_peer_id_mock(&format!("f0{provider_id}"), &peer_id, vec![])
            .await;

        self.mocks
            .setup_cid_contact_mock(&peer_id, multiaddrs)
            .await;

        ProviderFixture {
            provider_id: ProviderId::new(provider_id).unwrap(),
            provider_address: ProviderAddress::new(format!("f0{provider_id}")).unwrap(),
            peer_id,
        }
    }

    pub async fn setup_provider_with_deals_and_mock_server(
        &self,
        provider_id: &str,
        client_id: Option<&str>,
        piece_cids: Vec<&str>,
        success_rate: f64,
    ) -> ProviderFixture {
        let peer_id = format!("12D3Koo{}", provider_id);

        seed_provider(&self.dbs.app_pool, provider_id).await;
        seed_deals(
            &self.dbs.app_pool,
            provider_id,
            client_id,
            piece_cids.clone(),
        )
        .await;

        let success_count = (piece_cids.len() as f64 * success_rate).ceil() as usize;
        for (idx, piece_cid) in piece_cids.iter().enumerate() {
            let should_succeed = idx < success_count;
            self.mocks
                .setup_piece_retrieval_mock(piece_cid, should_succeed)
                .await;
        }

        let piece_server_url = self.mocks.piece_server_url();
        let host_and_port = piece_server_url.trim_start_matches("http://");
        let (host, port) = host_and_port
            .split_once(':')
            .expect("MockServer URL should be http://host:port");

        // Build multiaddr pointing to mock server WITHOUT /http suffix
        // The /http protocol auto-adds /tcp/80, so we can't use it for custom ports
        // Instead, just use /ip4/{host}/tcp/{port}
        let multiaddrs = vec![format!(
            "/ip4/{}/tcp/{}",
            if host == "localhost" {
                "127.0.0.1"
            } else {
                host
            },
            port
        )];

        debug!("Mock piece server URL: {}", piece_server_url);
        debug!("Extracted host: {}, port: {}", host, port);
        debug!("Generated multiaddrs: {:?}", multiaddrs);

        self.mocks
            .setup_lotus_peer_id_mock(&format!("f0{provider_id}"), &peer_id, vec![])
            .await;

        self.mocks
            .setup_cid_contact_mock(&peer_id, multiaddrs)
            .await;

        ProviderFixture {
            provider_id: ProviderId::new(provider_id).unwrap(),
            provider_address: ProviderAddress::new(format!("f0{provider_id}")).unwrap(),
            peer_id,
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if env::var("KEEP_TEST_DB").is_ok() {
            println!("Keeping test database: {}", self.dbs.app_db_name);
            return;
        }

        let db_name = self.dbs.app_db_name.clone();
        let postgres_host = self.dbs.postgres_host.clone();
        let pool = self.dbs.app_pool.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create cleanup runtime");

            rt.block_on(async move {
                pool.close().await;
                let db_url = format!(
                    "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{postgres_host}/{db_name}"
                );
                if let Err(e) = Postgres::drop_database(&db_url).await {
                    warn!("Failed to drop test database '{db_name}': {e}");
                }
            });
        });
    }
}
