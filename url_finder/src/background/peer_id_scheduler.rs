use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::lotus_rpc;
use crate::provider_endpoints::valid_curio_provider;
use crate::repository::StorageProviderRepository;
use crate::types::{ProviderAddress, ProviderId};

const SCHEDULER_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const BATCH_SIZE: i64 = 50;
const RATE_LIMIT_DELAY: Duration = Duration::from_millis(200); // ~5 req/sec
const STALE_DAYS: i64 = 7;

pub async fn run_peer_id_scheduler(
    config: Arc<Config>,
    sp_repo: Arc<StorageProviderRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting peer_id scheduler");

    loop {
        match refresh_peer_ids(&config, &sp_repo).await {
            Ok((new_count, stale_count)) => {
                if new_count > 0 || stale_count > 0 {
                    info!(
                        "Peer ID refresh: {} new, {} stale updated",
                        new_count, stale_count
                    );
                }
            }
            Err(e) => error!("Peer ID refresh failed: {:?}", e),
        }

        tokio::select! {
            _ = sleep(SCHEDULER_INTERVAL) => {}
            _ = shutdown.cancelled() => {
                info!("Peer ID scheduler received shutdown signal");
                break;
            }
        }
    }

    info!("Peer ID scheduler stopped");
}

async fn refresh_peer_ids(
    config: &Config,
    sp_repo: &StorageProviderRepository,
) -> color_eyre::Result<(usize, usize)> {
    let new_providers = sp_repo.get_providers_without_peer_id(BATCH_SIZE).await?;
    debug!("Found {} providers without peer_id", new_providers.len());
    let new_count = process_provider_batch(config, sp_repo, new_providers, "Cached").await;

    let stale_providers = sp_repo
        .get_providers_with_stale_peer_id(BATCH_SIZE, STALE_DAYS)
        .await?;
    debug!(
        "Found {} providers with stale peer_id",
        stale_providers.len()
    );
    let stale_count = process_provider_batch(config, sp_repo, stale_providers, "Refreshed").await;

    Ok((new_count, stale_count))
}

async fn process_provider_batch(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    providers: Vec<crate::types::StorageProvider>,
    action: &str,
) -> usize {
    let mut count = 0;

    for provider in providers {
        match fetch_peer_id(config, &provider.provider_id).await {
            Ok(peer_id) => {
                if let Err(e) = sp_repo
                    .update_peer_id(&provider.provider_id, &peer_id)
                    .await
                {
                    error!(
                        "Failed to update peer_id for {}: {:?}",
                        provider.provider_id, e
                    );
                } else {
                    debug!(
                        "{} peer_id for {}: {}",
                        action, provider.provider_id, peer_id
                    );
                    count += 1;
                }
            }
            Err(e) => debug!(
                "Failed to {} peer_id for {}: {:?}",
                action.to_lowercase(),
                provider.provider_id,
                e
            ),
        }
        sleep(RATE_LIMIT_DELAY).await;
    }

    count
}

async fn fetch_peer_id(config: &Config, provider_id: &ProviderId) -> color_eyre::Result<String> {
    let address: ProviderAddress = provider_id.clone().into();

    // Curio first, Lotus fallback (same logic as get_provider_endpoints)
    match valid_curio_provider(config, &address).await {
        Ok(Some(peer_id)) => Ok(peer_id),
        _ => {
            debug!(
                "Curio lookup failed for {}, falling back to Lotus",
                provider_id
            );
            lotus_rpc::get_peer_id(config, &address).await
        }
    }
}
