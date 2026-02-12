use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::config::Config;
use crate::lotus_rpc;
use crate::provider_endpoints::{get_provider_endpoints, valid_curio_provider};
use crate::repository::{
    StorageProvider, StorageProviderRepository, UrlResult, UrlResultRepository,
};
use crate::types::{DiscoveryType, ProviderAddress, ResultCode};

const SCHEDULER_INTERVAL: Duration = Duration::from_secs(300);
const CATCHUP_INTERVAL: Duration = Duration::from_secs(5);
const BATCH_SIZE: i64 = 100;
const RATE_LIMIT_DELAY: Duration = Duration::from_millis(100);

pub async fn run_endpoint_scheduler(
    config: Arc<Config>,
    sp_repo: Arc<StorageProviderRepository>,
    url_repo: Arc<UrlResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting endpoint scheduler");

    loop {
        match refresh_endpoints(&config, &sp_repo, &url_repo, &shutdown).await {
            Ok((count, more_pending)) => {
                if count > 0 {
                    info!("Endpoint refresh: {} providers updated", count);
                }

                if more_pending {
                    tokio::select! {
                        _ = sleep(CATCHUP_INTERVAL) => continue,
                        _ = shutdown.cancelled() => break,
                    }
                }
            }
            Err(e) => error!("Endpoint refresh failed: {:?}", e),
        }

        tokio::select! {
            _ = sleep(SCHEDULER_INTERVAL) => {}
            _ = shutdown.cancelled() => {
                info!("Endpoint scheduler received shutdown signal");
                break;
            }
        }
    }

    info!("Endpoint scheduler stopped");
}

async fn refresh_endpoints(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    shutdown: &CancellationToken,
) -> color_eyre::Result<(usize, bool)> {
    let providers = sp_repo.get_providers_needing_endpoints(BATCH_SIZE).await?;
    let batch_was_full = providers.len() as i64 == BATCH_SIZE;

    if !providers.is_empty() {
        info!(
            "Fetching endpoints for {} providers{}",
            providers.len(),
            if batch_was_full {
                " (more pending)"
            } else {
                ""
            }
        );
    }

    let count = process_provider_batch(config, sp_repo, url_repo, providers, shutdown).await;

    Ok((count, batch_was_full))
}

async fn process_provider_batch(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    providers: Vec<StorageProvider>,
    shutdown: &CancellationToken,
) -> usize {
    let mut count = 0;

    for provider in providers {
        if shutdown.is_cancelled() {
            debug!("Endpoint batch processing interrupted by shutdown");
            break;
        }

        match fetch_and_cache_endpoints(config, sp_repo, url_repo, &provider).await {
            Ok(None) => {
                debug!("Cached endpoints for {}", provider.provider_id);
                count += 1;
            }
            Ok(Some(result_code)) => {
                debug!("No endpoints for {}: {}", provider.provider_id, result_code)
            }
            Err(e) => debug!(
                "Failed to fetch endpoints for {}: {:?}",
                provider.provider_id, e
            ),
        }

        sleep(RATE_LIMIT_DELAY).await;
    }

    count
}

/// Phase 1 of URL discovery: resolve peer_id, fetch endpoints, cache or record failure.
/// Returns None on success (endpoints cached), Some(ResultCode) on failure (result recorded).
async fn fetch_and_cache_endpoints(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    provider: &StorageProvider,
) -> color_eyre::Result<Option<ResultCode>> {
    let provider_id = &provider.provider_id;
    let address: ProviderAddress = provider_id.clone().into();

    let peer_id = match valid_curio_provider(config, &address).await {
        Ok(Some(pid)) => pid,
        _ => {
            debug!(
                "Curio lookup failed for {}, falling back to Lotus",
                provider_id
            );
            match lotus_rpc::get_peer_id(config, &address).await {
                Ok(pid) => pid,
                Err(e) => {
                    debug!("Lotus lookup failed for {}: {:?}", provider_id, e);
                    return record_failure(
                        sp_repo,
                        url_repo,
                        provider_id,
                        ResultCode::NoPeerId,
                        None,
                    )
                    .await;
                }
            }
        }
    };

    match get_provider_endpoints(config, &address, Some(peer_id.clone())).await {
        Ok((ResultCode::Success, Some(endpoints))) => {
            sp_repo
                .update_cached_endpoints(provider_id, &peer_id, &endpoints)
                .await?;
            debug!(
                "Cached {} endpoints for {} (peer_id: {})",
                endpoints.len(),
                provider_id,
                peer_id
            );
            Ok(None)
        }
        Ok((result_code, _)) => {
            record_failure(sp_repo, url_repo, provider_id, result_code, None).await
        }
        Err(error_code) => {
            debug!("get_provider_endpoints failed for {provider_id}: {error_code}");
            record_failure(
                sp_repo,
                url_repo,
                provider_id,
                ResultCode::Error,
                Some(error_code),
            )
            .await
        }
    }
}

async fn record_failure(
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    provider_id: &crate::types::ProviderId,
    result_code: ResultCode,
    error_code: Option<crate::types::ErrorCode>,
) -> color_eyre::Result<Option<ResultCode>> {
    sp_repo.mark_endpoint_fetch_failed(provider_id).await?;

    let url_result = UrlResult {
        id: Uuid::new_v4(),
        provider_id: provider_id.clone(),
        client_id: None,
        result_type: DiscoveryType::Provider,
        working_url: None,
        retrievability_percent: None,
        result_code: result_code.clone(),
        error_code,
        tested_at: Utc::now(),
        is_consistent: None,
        is_reliable: None,
        url_metadata: None,
        sector_utilization_percent: None,
        car_files_percent: None,
        large_files_percent: None,
    };

    url_repo.insert_batch(&[url_result]).await?;

    Ok(Some(result_code))
}
