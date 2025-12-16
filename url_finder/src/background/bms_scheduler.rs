use crate::{
    bms_client::{BmsClient, BmsJobResponse},
    config::Config,
    repository::{BmsBandwidthResult, BmsBandwidthResultRepository, StorageProviderRepository},
};
use chrono::Utc;
use color_eyre::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const JOB_CREATOR_INTERVAL: Duration = Duration::from_secs(60);
const JOB_CREATOR_SLEEP_INTERVAL: Duration = Duration::from_secs(3600);
const RESULT_POLLER_INTERVAL: Duration = Duration::from_secs(30);
const BATCH_SIZE: i64 = 50;
const BMS_JOB_TIMEOUT_HOURS: i64 = 48;

pub async fn run_bms_scheduler(
    config: Arc<Config>,
    bms_client: Arc<BmsClient>,
    sp_repo: Arc<StorageProviderRepository>,
    result_repo: Arc<BmsBandwidthResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting BMS scheduler");
    tokio::join!(
        run_job_creator(
            config.clone(),
            bms_client.clone(),
            sp_repo.clone(),
            result_repo.clone(),
            shutdown.clone()
        ),
        run_result_poller(config, bms_client, result_repo, shutdown),
    );
    info!("BMS scheduler stopped");
}

async fn run_job_creator(
    config: Arc<Config>,
    bms_client: Arc<BmsClient>,
    sp_repo: Arc<StorageProviderRepository>,
    result_repo: Arc<BmsBandwidthResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting BMS job creator loop");

    loop {
        let interval = match create_bms_jobs(&config, &bms_client, &sp_repo, &result_repo).await {
            Ok(0) => {
                debug!("No providers due for BMS test, sleeping...");
                JOB_CREATOR_SLEEP_INTERVAL
            }
            Ok(count) => {
                info!("BMS job creation cycle completed: {count} jobs created");
                JOB_CREATOR_INTERVAL
            }
            Err(e) => {
                error!("BMS job creator failed: {:?}", e);
                JOB_CREATOR_SLEEP_INTERVAL
            }
        };

        tokio::select! {
            _ = sleep(interval) => {}
            _ = shutdown.cancelled() => {
                info!("BMS job creator received shutdown signal");
                break;
            }
        }
    }

    info!("BMS job creator stopped");
}

async fn create_bms_jobs(
    config: &Config,
    bms_client: &BmsClient,
    sp_repo: &StorageProviderRepository,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<usize> {
    let providers = sp_repo.get_due_for_bms_test(BATCH_SIZE).await?;

    debug!("Found {} providers due for BMS test", providers.len());

    let mut jobs_created = 0;

    for provider in providers {
        let url = match &provider.last_working_url {
            Some(url) => url,
            None => {
                warn!(
                    "Provider {} has no last_working_url, skipping BMS test",
                    provider.provider_id
                );
                continue;
            }
        };

        match bms_client
            .create_job(
                url.clone(),
                config.bms_default_worker_count,
                Some(format!("f0{}", provider.provider_id)),
            )
            .await
        {
            Ok(job) => {
                // Insert pending result with routing_key from BMS response
                if let Err(e) = result_repo
                    .insert_pending(
                        &provider.provider_id,
                        job.id,
                        &job.url,
                        &job.routing_key,
                        config.bms_default_worker_count as i32,
                    )
                    .await
                {
                    error!(
                        "Failed to insert pending result for provider {}: {:?}",
                        provider.provider_id, e
                    );
                    continue;
                }

                // Schedule next test to prevent duplicate jobs
                if let Err(e) = sp_repo
                    .schedule_next_bms_test(&provider.provider_id, config.bms_test_interval_days)
                    .await
                {
                    error!(
                        "Failed to schedule next test for provider {}: {:?}",
                        provider.provider_id, e
                    );
                    continue;
                }

                info!(
                    "Created BMS job {} for provider {} (routing_key: {})",
                    job.id, provider.provider_id, job.routing_key
                );
                jobs_created += 1;
            }
            Err(e) => {
                error!(
                    "Failed to create BMS job for provider {}: {:?}",
                    provider.provider_id, e
                );
            }
        }
    }

    Ok(jobs_created)
}

async fn run_result_poller(
    config: Arc<Config>,
    bms_client: Arc<BmsClient>,
    result_repo: Arc<BmsBandwidthResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting BMS result poller loop");

    loop {
        if let Err(e) = poll_bms_results(&config, &bms_client, &result_repo).await {
            error!("BMS result poller failed: {:?}", e);
        }

        tokio::select! {
            _ = sleep(RESULT_POLLER_INTERVAL) => {}
            _ = shutdown.cancelled() => {
                info!("BMS result poller received shutdown signal");
                break;
            }
        }
    }

    info!("BMS result poller stopped");
}

async fn poll_bms_results(
    _config: &Config,
    bms_client: &BmsClient,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<()> {
    let pending_results = result_repo.get_pending().await?;

    debug!("Polling {} pending BMS jobs", pending_results.len());

    for result in pending_results {
        if is_result_timed_out(&result) {
            if let Err(e) = handle_timeout(&result, result_repo).await {
                error!(
                    "Failed to handle timeout for BMS job {} (provider {}): {:?}",
                    result.bms_job_id, result.provider_id, e
                );
            }
            continue;
        }

        if let Err(e) = poll_single_result(&result, bms_client, result_repo).await {
            error!(
                "Failed to poll BMS job {} (provider {}): {:?}",
                result.bms_job_id, result.provider_id, e
            );
        }
    }

    Ok(())
}

fn is_result_timed_out(result: &BmsBandwidthResult) -> bool {
    (Utc::now() - result.created_at).num_hours() >= BMS_JOB_TIMEOUT_HOURS
}

async fn handle_timeout(
    result: &BmsBandwidthResult,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<()> {
    let hours = (Utc::now() - result.created_at).num_hours();
    warn!(
        "BMS job {} for provider {} timed out after {} hours",
        result.bms_job_id, result.provider_id, hours
    );

    result_repo
        .update_completed(result.bms_job_id, "Timeout", None, None, None, None)
        .await
}

async fn poll_single_result(
    result: &BmsBandwidthResult,
    bms_client: &BmsClient,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<()> {
    match bms_client.get_job(result.bms_job_id).await {
        Ok(job_response) => {
            if BmsClient::is_job_finished(&job_response.status) {
                process_completed_job(result, &job_response, result_repo).await?;
            } else {
                debug!(
                    "BMS job {} for provider {} still in progress: {}",
                    result.bms_job_id, result.provider_id, job_response.status
                );
            }
        }
        Err(e) => {
            warn!(
                "Failed to fetch BMS job {} for provider {}: {:?}",
                result.bms_job_id, result.provider_id, e
            );
        }
    }

    Ok(())
}

async fn process_completed_job(
    result: &BmsBandwidthResult,
    job_response: &BmsJobResponse,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<()> {
    info!(
        "BMS job {} completed for provider {} with status: {}",
        job_response.id, result.provider_id, job_response.status
    );

    let (ping_avg_ms, head_avg_ms, ttfb_ms, download_speed_mbps) =
        extract_results_from_job(job_response);

    result_repo
        .update_completed(
            result.bms_job_id,
            &job_response.status,
            ping_avg_ms,
            head_avg_ms,
            ttfb_ms,
            download_speed_mbps,
        )
        .await?;

    Ok(())
}

fn extract_results_from_job(
    job_response: &BmsJobResponse,
) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
    // Find the last completed subjob with worker data (typically the 100% test)
    let worker_data = job_response
        .sub_jobs
        .as_ref()
        .and_then(|subs| {
            subs.iter()
                .rev()
                .find(|s| s.status == "Completed" && s.worker_data.is_some())
        })
        .and_then(|s| s.worker_data.as_ref())
        .and_then(|wd| wd.first());

    match worker_data {
        Some(data) => {
            // Ping avg is in seconds from BMS, convert to ms
            let ping_avg_ms = data.ping.as_ref().and_then(|p| p.avg).map(|v| v * 1000.0);

            // Head avg is already in ms from BMS
            let head_avg_ms = data.head.as_ref().and_then(|h| h.avg);

            // TTFB is in ms from BMS
            let ttfb_ms = data.download.as_ref().and_then(|d| d.time_to_first_byte_ms);

            // Download speed is in Mbps from BMS
            let download_speed_mbps = data.download.as_ref().and_then(|d| d.download_speed);

            (ping_avg_ms, head_avg_ms, ttfb_ms, download_speed_mbps)
        }
        None => (None, None, None, None),
    }
}
