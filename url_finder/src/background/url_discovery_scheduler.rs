use crate::{
    repository::{DealRepository, StorageProviderRepository, UrlResult, UrlResultRepository},
    services::url_discovery_service,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};
use chrono::Utc;
use color_eyre::Result;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info};

const SCHEDULER_SLEEP_INTERVAL: Duration = Duration::from_secs(3600);
const SCHEDULER_NEXT_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: i64 = 100;

pub async fn run_url_discovery_scheduler(
    sp_repo: Arc<StorageProviderRepository>,
    url_repo: Arc<UrlResultRepository>,
    deal_repo: Arc<DealRepository>,
) {
    info!("Starting URL discovery scheduler");

    loop {
        let interval = match schedule_url_discoveries(&sp_repo, &url_repo, &deal_repo).await {
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

        sleep(interval).await;
    }
}

async fn schedule_url_discoveries(
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    deal_repo: &DealRepository,
) -> Result<usize> {
    let providers = sp_repo.get_due_for_url_discovery(BATCH_SIZE).await?;

    debug!("Found {} providers due for URL discovery", providers.len());

    let mut total_tested = 0;

    for provider in providers {
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

        let results = test_provider_with_clients(&provider.provider_id, clients, deal_repo).await;

        let url_results: Vec<UrlResult> = results
            .iter()
            .map(|r| UrlResult {
                id: r.id,
                provider_id: r.provider_id.clone(),
                client_id: r.client_id.clone(),
                result_type: r.result_type.clone(),
                working_url: r.working_url.clone(),
                retrievability_percent: r.retrievability_percent,
                result_code: r.result_code.clone(),
                error_code: r.error_code.clone(),
                tested_at: Utc::now(),
            })
            .collect();

        match url_repo.insert_batch(&url_results).await {
            Ok(count) => debug!(
                "Inserted {} URL results for provider {}",
                count, provider.provider_id
            ),
            Err(e) => error!("Failed to insert URL results: {:?}", e),
        }

        let provider_only_result = results.iter().find(|r| r.client_id.is_none());
        let last_working_url = provider_only_result.and_then(|r| r.working_url.clone());

        sp_repo
            .update_after_url_discovery(&provider.provider_id, last_working_url)
            .await?;

        total_tested += 1;
    }

    Ok(total_tested)
}

async fn test_provider_with_clients(
    provider_id: &ProviderId,
    client_ids: Vec<ClientId>,
    deal_repo: &DealRepository,
) -> Vec<url_discovery_service::UrlDiscoveryResult> {
    let mut tasks = vec![];
    let provider_address: ProviderAddress = provider_id.clone().into();

    let provider_task = {
        let addr = provider_address.clone();
        let repo = deal_repo.clone();
        tokio::spawn(async move { url_discovery_service::discover_url(&addr, None, &repo).await })
    };
    tasks.push(provider_task);

    for client_id in client_ids {
        let provider_addr = provider_address.clone();
        let client_address: ClientAddress = client_id.into();
        let repo = deal_repo.clone();
        tasks.push(tokio::spawn(async move {
            url_discovery_service::discover_url(&provider_addr, Some(client_address), &repo).await
        }));
    }

    let results = join_all(tasks).await;

    results
        .into_iter()
        .filter_map(|r| r.ok().and_then(|inner| inner.ok()))
        .collect()
}
