use crate::{
    config::Config,
    repository::{DealRepository, StorageProviderRepository, UrlResult, UrlResultRepository},
    services::url_discovery_service,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};
use color_eyre::Result;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::Semaphore, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const SCHEDULER_SLEEP_INTERVAL: Duration = Duration::from_secs(3600);
const SCHEDULER_NEXT_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: i64 = 100;
const MAX_CONCURRENT_CLIENT_TESTS: usize = 5;

pub async fn run_url_discovery_scheduler(
    config: Arc<Config>,
    sp_repo: Arc<StorageProviderRepository>,
    url_repo: Arc<UrlResultRepository>,
    deal_repo: Arc<DealRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting URL discovery scheduler loop");

    loop {
        let interval =
            match schedule_url_discoveries(&config, &sp_repo, &url_repo, &deal_repo).await {
                Ok(0) => {
                    info!("No providers due for URL discovery, sleeping...");
                    SCHEDULER_SLEEP_INTERVAL
                }
                Ok(count) => {
                    info!("URL discovery cycle completed: {} providers tested", count);
                    SCHEDULER_NEXT_INTERVAL
                }
                Err(e) => {
                    error!("URL discovery scheduler failed: {:?}", e);
                    SCHEDULER_SLEEP_INTERVAL
                }
            };

        tokio::select! {
            _ = sleep(interval) => {}
            _ = shutdown.cancelled() => {
                info!("URL discovery scheduler received shutdown signal");
                break;
            }
        }
    }

    info!("URL discovery scheduler stopped");
}

async fn schedule_url_discoveries(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    deal_repo: &DealRepository,
) -> Result<usize> {
    let providers = sp_repo.get_due_for_url_discovery(BATCH_SIZE).await?;

    debug!("Found {} providers due for URL discovery", providers.len());

    let mut total_tested = 0;

    for provider in providers {
        if provider.url_discovery_status.as_deref() == Some("pending") {
            warn!(
                "Recovering stale pending provider: {} (pending since {:?})",
                provider.provider_id, provider.url_discovery_pending_since
            );
        }

        sp_repo
            .set_url_discovery_pending(&provider.provider_id)
            .await?;

        let clients = deal_repo
            .get_clients_for_provider(&provider.provider_id)
            .await?;

        debug!(
            "Provider {} has {} clients",
            provider.provider_id,
            clients.len()
        );

        let results =
            test_provider_with_clients(config, &provider.provider_id, clients, deal_repo).await;

        // Extract provider-only result for storage_providers update
        // None case: provider-only discovery missing (panic, filtering, etc.) - default is_consistent
        // to false since consistency was not verified
        let provider_discovery = results.iter().find(|r| r.client_id.is_none());

        let (last_working_url, is_consistent, is_reliable, url_metadata) = match provider_discovery
        {
            Some(r) => (
                r.working_url.clone(),
                r.is_consistent,
                r.is_reliable,
                r.url_metadata.clone(),
            ),
            None => (None, false, false, None),
        };

        let url_results: Vec<UrlResult> = results.into_iter().map(|r| r.into()).collect();

        match url_repo.insert_batch(&url_results).await {
            Ok(count) => debug!(
                "Inserted {} URL results for provider {}",
                count, provider.provider_id
            ),
            Err(e) => error!("Failed to insert URL results: {:?}", e),
        }

        sp_repo
            .update_after_url_discovery(
                &provider.provider_id,
                last_working_url,
                is_consistent,
                is_reliable,
                url_metadata,
            )
            .await?;

        total_tested += 1;
    }

    Ok(total_tested)
}

async fn test_provider_with_clients(
    config: &Config,
    provider_id: &ProviderId,
    client_ids: Vec<ClientId>,
    deal_repo: &DealRepository,
) -> Vec<url_discovery_service::UrlDiscoveryResult> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CLIENT_TESTS));
    let mut tasks = vec![];
    let provider_address: ProviderAddress = provider_id.clone().into();

    let provider_task = {
        let cfg = config.clone();
        let addr = provider_address.clone();
        let repo = deal_repo.clone();
        tokio::spawn(
            async move { url_discovery_service::discover_url(&cfg, &addr, None, &repo).await },
        )
    };
    tasks.push(provider_task);

    for client_id in client_ids {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore should never be closed");
        let cfg = config.clone();
        let provider_addr = provider_address.clone();
        let client_address: ClientAddress = client_id.into();
        let repo = deal_repo.clone();
        tasks.push(tokio::spawn(async move {
            let result = url_discovery_service::discover_url(
                &cfg,
                &provider_addr,
                Some(client_address),
                &repo,
            )
            .await;
            drop(permit); // release semaphore
            result
        }));
    }

    let results = join_all(tasks).await;

    results
        .into_iter()
        .filter_map(|r| {
            r.map_err(|e| {
                error!("URL discovery task panicked: {:?}", e);
                e
            })
            .ok()
        })
        .collect()
}
