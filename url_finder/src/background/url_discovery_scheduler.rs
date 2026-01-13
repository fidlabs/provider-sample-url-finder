use crate::{
    config::Config,
    repository::{DealRepository, StorageProviderRepository, UrlResult, UrlResultRepository},
    services::url_discovery_service,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};
use color_eyre::Result;
use futures::future::join_all;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{sync::Semaphore, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const SCHEDULER_SLEEP_INTERVAL: Duration = Duration::from_secs(3600);
const SCHEDULER_NEXT_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: i64 = 100;
const MAX_CONCURRENT_CLIENT_TESTS: usize = 5;

// --- Helper Structs ---

struct DiscoveryBatchStats {
    total: usize,
    ok: usize,
    failed: usize,
    total_retrievability: f64,
    consistent: usize,
    started_at: Instant,
}

impl DiscoveryBatchStats {
    fn new() -> Self {
        Self {
            total: 0,
            ok: 0,
            failed: 0,
            total_retrievability: 0.0,
            consistent: 0,
            started_at: Instant::now(),
        }
    }

    fn record(&mut self, outcome: &ProviderOutcome) {
        self.total += 1;
        if outcome.success {
            self.ok += 1;
        } else {
            self.failed += 1;
        }
        self.total_retrievability += outcome.retrievability;
        if outcome.consistent {
            self.consistent += 1;
        }
    }

    fn avg_retrievability(&self) -> f64 {
        if self.total > 0 {
            self.total_retrievability / self.total as f64
        } else {
            0.0
        }
    }

    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    fn is_empty(&self) -> bool {
        self.total == 0
    }

    fn success_percent(&self) -> usize {
        if self.total > 0 {
            (self.ok * 100) / self.total
        } else {
            0
        }
    }
}

struct ProviderOutcome {
    success: bool,
    retrievability: f64,
    consistent: bool,
}

struct ProgressReporter {
    batch_size: usize,
    last_checkpoint: usize,
}

impl ProgressReporter {
    fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            last_checkpoint: 0,
        }
    }

    fn maybe_log(&mut self, stats: &DiscoveryBatchStats, current_provider_id: &ProviderId) {
        if self.batch_size < 4 {
            return;
        }

        let current_percent = (stats.total * 100) / self.batch_size;
        let checkpoint = current_percent / 25;

        if checkpoint > self.last_checkpoint && checkpoint < 4 {
            info!(
                "URL discovery: {}% ({}/{}) current: f0{} | {} ok {} fail",
                checkpoint * 25,
                stats.total,
                self.batch_size,
                current_provider_id,
                stats.ok,
                stats.failed
            );
            self.last_checkpoint = checkpoint;
        }
    }
}

// --- Main Scheduler ---

pub async fn run_url_discovery_scheduler(
    config: Arc<Config>,
    sp_repo: Arc<StorageProviderRepository>,
    url_repo: Arc<UrlResultRepository>,
    deal_repo: Arc<DealRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting URL discovery scheduler loop");

    loop {
        let interval = match schedule_url_discoveries(&config, &sp_repo, &url_repo, &deal_repo)
            .await
        {
            Ok(stats) if stats.is_empty() => {
                debug!("No providers due for URL discovery, sleeping...");
                SCHEDULER_SLEEP_INTERVAL
            }
            Ok(stats) => {
                info!(
                    "URL discovery: done {}/{} ({}%) in {:.0}s | avg_retri: {:.1}% consistent: {}/{}",
                    stats.ok,
                    stats.total,
                    stats.success_percent(),
                    stats.elapsed().as_secs_f64(),
                    stats.avg_retrievability(),
                    stats.consistent,
                    stats.total
                );
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
) -> Result<DiscoveryBatchStats> {
    let providers = sp_repo.get_due_for_url_discovery(BATCH_SIZE).await?;

    debug!("Found {} providers due for URL discovery", providers.len());

    if !providers.is_empty() {
        info!("URL discovery: starting {} providers", providers.len());
    }

    let mut stats = DiscoveryBatchStats::new();
    let mut progress = ProgressReporter::new(providers.len());

    for provider in providers {
        if provider.url_discovery_status.as_deref() == Some("pending") {
            warn!(
                "Recovering stale pending provider: {} (pending since {:?})",
                provider.provider_id, provider.url_discovery_pending_since
            );
        }

        let outcome =
            process_single_provider(config, sp_repo, url_repo, deal_repo, &provider.provider_id)
                .await?;

        stats.record(&outcome);
        progress.maybe_log(&stats, &provider.provider_id);
    }

    Ok(stats)
}

async fn process_single_provider(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    deal_repo: &DealRepository,
    provider_id: &ProviderId,
) -> Result<ProviderOutcome> {
    sp_repo.set_url_discovery_pending(provider_id).await?;

    let clients = deal_repo.get_clients_for_provider(provider_id).await?;

    let client_ids_for_log: Vec<String> = clients
        .iter()
        .map(|c| format!("f0{}", c.as_str()))
        .collect();

    debug!("Provider {} has {} clients", provider_id, clients.len());

    let results = test_provider_with_clients(config, provider_id, clients, deal_repo).await;

    // Extract provider-only result for storage_providers update
    // None case: provider-only discovery missing (panic, filtering, etc.) - default is_consistent
    // to false since consistency was not verified
    let provider_discovery = results.iter().find(|r| r.client_id.is_none());

    let (last_working_url, is_consistent, is_reliable, url_metadata, outcome) =
        match provider_discovery {
            Some(r) => (
                r.working_url.clone(),
                r.is_consistent,
                r.is_reliable,
                r.url_metadata.clone(),
                ProviderOutcome {
                    success: r.working_url.is_some(),
                    retrievability: r.retrievability_percent,
                    consistent: r.is_consistent,
                },
            ),
            None => (
                None,
                false,
                false,
                None,
                ProviderOutcome {
                    success: false,
                    retrievability: 0.0,
                    consistent: false,
                },
            ),
        };

    let url_results: Vec<UrlResult> = results.into_iter().map(|r| r.into()).collect();

    match url_repo.insert_batch(&url_results).await {
        Ok(count) => debug!(
            "Inserted {} URL results for provider {}",
            count, provider_id
        ),
        Err(e) => error!("Failed to insert URL results: {:?}", e),
    }

    sp_repo
        .update_after_url_discovery(
            provider_id,
            last_working_url,
            is_consistent,
            is_reliable,
            url_metadata,
        )
        .await?;

    // Debug per-provider result
    let client_display = if client_ids_for_log.is_empty() {
        "(0 clients)".to_string()
    } else if client_ids_for_log.len() == 1 {
        format!("(1 client) [{}]", client_ids_for_log.join(", "))
    } else {
        format!(
            "({} clients) [{}]",
            client_ids_for_log.len(),
            client_ids_for_log.join(", ")
        )
    };
    let result_str = if outcome.success { "ok" } else { "failed" };
    debug!(
        "f0{} {}: {} retri={:.1}% consistent={}",
        provider_id, client_display, result_str, outcome.retrievability, outcome.consistent
    );

    Ok(outcome)
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
