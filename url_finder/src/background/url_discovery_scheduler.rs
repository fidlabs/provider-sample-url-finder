use crate::{
    config::Config,
    repository::{
        DealRepository, StorageProvider, StorageProviderRepository, UrlResult, UrlResultRepository,
    },
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

const SCHEDULER_SLEEP_INTERVAL: Duration = Duration::from_secs(300);
const SCHEDULER_NEXT_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: i64 = 100;
const MAX_CONCURRENT_CLIENT_TESTS: usize = 5;

// --- Helper Structs ---

struct DiscoveryBatchStats {
    total: usize,
    ok: usize,
    failed: usize,
    skipped: usize,
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
            skipped: 0,
            total_retrievability: 0.0,
            consistent: 0,
            started_at: Instant::now(),
        }
    }

    fn record(&mut self, outcome: &ProviderOutcome) {
        self.total += 1;
        match outcome {
            ProviderOutcome::Processed {
                success,
                retrievability,
                consistent,
            } => {
                if *success {
                    self.ok += 1;
                } else {
                    self.failed += 1;
                }
                self.total_retrievability += retrievability;
                if *consistent {
                    self.consistent += 1;
                }
            }
            ProviderOutcome::Skipped => self.skipped += 1,
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

enum ProviderOutcome {
    Processed {
        success: bool,
        retrievability: f64,
        consistent: bool,
    },
    Skipped,
}

#[allow(dead_code)]
struct ProgressReporter {
    batch_size: usize,
    last_checkpoint: usize,
}

#[allow(dead_code)]
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
        let interval = match schedule_url_discoveries(
            &config, &sp_repo, &url_repo, &deal_repo, &shutdown,
        )
        .await
        {
            Ok(stats) if stats.is_empty() => {
                info!("URL discovery: idle, sleeping 5m");
                SCHEDULER_SLEEP_INTERVAL
            }
            Ok(stats) => {
                info!(
                    "URL discovery: done {}/{} ({}%) in {:.0}s | avg_retri: {:.1}% consistent: {}/{} skipped: {}",
                    stats.ok,
                    stats.total,
                    stats.success_percent(),
                    stats.elapsed().as_secs_f64(),
                    stats.avg_retrievability(),
                    stats.consistent,
                    stats.total,
                    stats.skipped
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
    config: &Arc<Config>,
    sp_repo: &Arc<StorageProviderRepository>,
    url_repo: &Arc<UrlResultRepository>,
    deal_repo: &Arc<DealRepository>,
    shutdown: &CancellationToken,
) -> Result<DiscoveryBatchStats> {
    let providers = sp_repo.get_due_for_url_discovery(BATCH_SIZE).await?;

    if !providers.is_empty() {
        let ready = providers
            .iter()
            .filter(|p| p.cached_http_endpoints.is_some())
            .count();
        let need_endpoints = providers.len() - ready;
        if need_endpoints > 0 {
            info!(
                "URL discovery: starting {} providers ({} ready, {} need endpoints)",
                providers.len(),
                ready,
                need_endpoints
            );
        } else {
            info!("URL discovery: starting {} providers", providers.len());
        }
    }

    let stats = Arc::new(tokio::sync::Mutex::new(DiscoveryBatchStats::new()));

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_providers));
    let mut tasks = vec![];

    for provider in providers {
        if shutdown.is_cancelled() {
            info!("URL discovery batch interrupted by shutdown before spawning");
            break;
        }

        let permit = semaphore.clone().acquire_owned().await?;
        let config = config.clone();
        let sp_repo = sp_repo.clone();
        let url_repo = url_repo.clone();
        let deal_repo = deal_repo.clone();
        let shutdown = shutdown.clone();
        let stats = stats.clone();

        tasks.push(tokio::spawn(async move {
            let outcome = process_single_provider(
                &config, &sp_repo, &url_repo, &deal_repo, &provider, &shutdown,
            )
            .await;

            if let Ok(ref o) = outcome {
                let mut s = stats.lock().await;
                s.record(o);
            }

            drop(permit);
            outcome
        }));
    }

    let results = join_all(tasks).await;

    for result in results {
        match result {
            Ok(Err(e)) => error!("Provider processing error: {:?}", e),
            Err(e) => error!("Provider task panicked: {:?}", e),
            _ => {}
        }
    }

    let final_stats = stats.lock().await;
    Ok(DiscoveryBatchStats {
        total: final_stats.total,
        ok: final_stats.ok,
        failed: final_stats.failed,
        skipped: final_stats.skipped,
        total_retrievability: final_stats.total_retrievability,
        consistent: final_stats.consistent,
        started_at: final_stats.started_at,
    })
}

async fn process_single_provider(
    config: &Config,
    sp_repo: &StorageProviderRepository,
    url_repo: &UrlResultRepository,
    deal_repo: &DealRepository,
    provider: &StorageProvider,
    shutdown: &CancellationToken,
) -> Result<ProviderOutcome> {
    let provider_id = &provider.provider_id;

    if provider.cached_http_endpoints.is_none() {
        warn!(
            "Provider {} has no cached endpoints but was picked up by URL discovery - scheduling mismatch",
            provider_id
        );
        sp_repo
            .reschedule_url_discovery_delayed(provider_id, 86400)
            .await?;
        return Ok(ProviderOutcome::Skipped);
    }

    if provider.url_discovery_status.as_deref() == Some("pending") {
        warn!(
            "Recovering stale pending provider: {} (pending since {:?})",
            provider.provider_id, provider.url_discovery_pending_since
        );
    }

    sp_repo.set_url_discovery_pending(provider_id).await?;

    let clients = deal_repo.get_clients_for_provider(provider_id).await?;

    let client_ids_for_log: Vec<String> = clients
        .iter()
        .map(|c| format!("f0{}", c.as_str()))
        .collect();

    debug!("Provider {} has {} clients", provider_id, clients.len());

    let cached_endpoints = provider.cached_http_endpoints.clone().unwrap_or_default();
    let results = test_provider_with_clients(
        config,
        provider_id,
        clients,
        deal_repo,
        cached_endpoints,
        shutdown,
    )
    .await;

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
                ProviderOutcome::Processed {
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
                ProviderOutcome::Processed {
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

    if let ProviderOutcome::Processed {
        success,
        retrievability,
        consistent,
    } = &outcome
    {
        let clients_count = client_ids_for_log.len();
        if *success {
            info!(
                "f0{} ({} clients): OK retri={:.1}% consistent={}",
                provider_id, clients_count, retrievability, consistent
            );
        } else {
            debug!(
                "f0{} ({} clients): FAIL retri={:.1}%",
                provider_id, clients_count, retrievability
            );
        }
    }

    Ok(outcome)
}

async fn test_provider_with_clients(
    config: &Config,
    provider_id: &ProviderId,
    client_ids: Vec<ClientId>,
    deal_repo: &DealRepository,
    cached_http_endpoints: Vec<String>,
    shutdown: &CancellationToken,
) -> Vec<url_discovery_service::UrlDiscoveryResult> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CLIENT_TESTS));
    let mut tasks = vec![];
    let provider_address: ProviderAddress = provider_id.clone().into();

    let provider_task = {
        let cfg = config.clone();
        let addr = provider_address.clone();
        let repo = deal_repo.clone();
        let endpoints = cached_http_endpoints.clone();
        tokio::spawn(async move {
            url_discovery_service::discover_url(&cfg, &addr, None, &repo, endpoints).await
        })
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
        let endpoints = cached_http_endpoints.clone();
        tasks.push(tokio::spawn(async move {
            let result = url_discovery_service::discover_url(
                &cfg,
                &provider_addr,
                Some(client_address),
                &repo,
                endpoints,
            )
            .await;
            drop(permit);
            result
        }));
    }

    tokio::select! {
        results = join_all(&mut tasks) => {
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
        _ = shutdown.cancelled() => {
            info!("Aborting {} URL discovery tasks for provider {}", tasks.len(), provider_id);
            for task in tasks {
                task.abort();
            }
            vec![]
        }
    }
}
