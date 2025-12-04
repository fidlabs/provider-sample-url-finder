use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::repository::{DealRepository, StorageProviderRepository};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(3600 * 12); // 12 hours
const DMOB_QUERY_TIMEOUT: Duration = Duration::from_secs(1200); // 20 minutes

pub async fn run_provider_discovery(
    sp_repo: Arc<StorageProviderRepository>,
    deal_repo: Arc<DealRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting provider discovery loop");

    loop {
        match discover_and_sync_providers(&sp_repo, &deal_repo).await {
            Ok(count) => info!("Provider discovery completed: {} providers synced", count),
            Err(e) => error!("Provider discovery failed: {:?}", e),
        }

        tokio::select! {
            _ = sleep(DISCOVERY_INTERVAL) => {}
            _ = shutdown.cancelled() => {
                info!("Provider discovery received shutdown signal");
                break;
            }
        }
    }

    info!("Provider discovery stopped");
}

async fn discover_and_sync_providers(
    sp_repo: &StorageProviderRepository,
    deal_repo: &DealRepository,
) -> color_eyre::Result<usize> {
    debug!("Querying dmob for distinct providers...");

    let providers = tokio::time::timeout(DMOB_QUERY_TIMEOUT, deal_repo.get_distinct_providers())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("Timeout querying dmob"))??;

    debug!("Found {} distinct providers in dmob", providers.len());

    sp_repo.insert_batch_if_not_exists(&providers).await
}
