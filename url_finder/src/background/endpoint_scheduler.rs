use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::cid_contact::{self, CidContactError};
use crate::config::Config;
use crate::lotus_rpc;
use crate::multiaddr_parser;
use crate::provider_endpoints::valid_curio_provider;
use crate::repository::{StorageProvider, StorageProviderRepository};
use crate::types::ProviderAddress;

const SCHEDULER_INTERVAL: Duration = Duration::from_secs(300);
const CATCHUP_INTERVAL: Duration = Duration::from_secs(5);
const BATCH_SIZE: i64 = 100;
const RATE_LIMIT_DELAY: Duration = Duration::from_millis(100);

pub async fn run_endpoint_scheduler(
    config: Arc<Config>,
    sp_repo: Arc<StorageProviderRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting endpoint scheduler");

    loop {
        match refresh_endpoints(&config, &sp_repo, &shutdown).await {
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

    let count = process_provider_batch(config, sp_repo, providers, shutdown).await;

    Ok((count, batch_was_full))
}

async fn process_provider_batch(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    providers: Vec<StorageProvider>,
    shutdown: &CancellationToken,
) -> usize {
    let mut count = 0;

    for provider in providers {
        if shutdown.is_cancelled() {
            debug!("Endpoint batch processing interrupted by shutdown");
            break;
        }

        match fetch_and_cache_endpoints(config, sp_repo, &provider).await {
            Ok(true) => {
                debug!("Cached endpoints for {}", provider.provider_id);
                count += 1;
            }
            Ok(false) => debug!("No endpoints found for {}", provider.provider_id),
            Err(e) => debug!(
                "Failed to fetch endpoints for {}: {:?}",
                provider.provider_id, e
            ),
        }

        sleep(RATE_LIMIT_DELAY).await;
    }

    count
}

/// Get peer_id from lotus
/// Get multiaddr from cid.contact
/// Parse and cache HTTP endpoints for storage_provider
async fn fetch_and_cache_endpoints(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    provider: &StorageProvider,
) -> color_eyre::Result<bool> {
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
                    sp_repo.mark_endpoint_fetch_failed(provider_id).await?;
                    return Ok(false);
                }
            }
        }
    };

    let cid_contact_res = match cid_contact::get_contact(config, &peer_id).await {
        Ok(res) => res,
        Err(CidContactError::NoData) => {
            debug!("No cid.contact data for {}", provider_id);
            sp_repo.mark_endpoint_fetch_failed(provider_id).await?;
            return Ok(false);
        }
        Err(e) => {
            debug!("cid.contact failed for {}: {}", provider_id, e);
            sp_repo.mark_endpoint_fetch_failed(provider_id).await?;
            return Ok(false);
        }
    };

    let addrs = cid_contact::get_all_addresses_from_response(cid_contact_res);
    if addrs.is_empty() {
        debug!("No addresses in cid.contact response for {}", provider_id);
        sp_repo.mark_endpoint_fetch_failed(provider_id).await?;
        return Ok(false);
    }

    let mut endpoints = multiaddr_parser::parse(addrs);
    if endpoints.is_empty() {
        debug!("No HTTP endpoints parsed for {}", provider_id);
        sp_repo.mark_endpoint_fetch_failed(provider_id).await?;
        return Ok(false);
    }

    endpoints.sort();
    endpoints.dedup();

    sp_repo
        .update_cached_endpoints(provider_id, &peer_id, &endpoints)
        .await?;

    debug!(
        "Cached {} endpoints for {} (peer_id: {})",
        endpoints.len(),
        provider_id,
        peer_id
    );

    Ok(true)
}
