use crate::{
    bms_client::{BmsClient, BmsJobResponse},
    circuit_breaker::CircuitBreaker,
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

// Circuit breaker configuration
const BMS_CIRCUIT_BREAKER_THRESHOLD: usize = 5;
const BMS_CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(300); // 5 minutes

/// Create a circuit breaker for BMS API calls.
pub fn create_bms_circuit_breaker() -> CircuitBreaker {
    CircuitBreaker::new(
        "BMS",
        BMS_CIRCUIT_BREAKER_THRESHOLD,
        BMS_CIRCUIT_BREAKER_COOLDOWN,
    )
}

pub async fn run_bms_scheduler(
    config: Arc<Config>,
    bms_client: Arc<BmsClient>,
    circuit_breaker: Arc<CircuitBreaker>,
    sp_repo: Arc<StorageProviderRepository>,
    result_repo: Arc<BmsBandwidthResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting BMS scheduler");
    tokio::join!(
        run_job_creator(
            config.clone(),
            bms_client.clone(),
            circuit_breaker.clone(),
            sp_repo.clone(),
            result_repo.clone(),
            shutdown.clone()
        ),
        run_result_poller(config, bms_client, circuit_breaker, result_repo, shutdown),
    );
    info!("BMS scheduler stopped");
}

async fn run_job_creator(
    config: Arc<Config>,
    bms_client: Arc<BmsClient>,
    circuit_breaker: Arc<CircuitBreaker>,
    sp_repo: Arc<StorageProviderRepository>,
    result_repo: Arc<BmsBandwidthResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting BMS job creator loop");

    loop {
        let interval = match create_bms_jobs(
            &config,
            &bms_client,
            &circuit_breaker,
            &sp_repo,
            &result_repo,
        )
        .await
        {
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
    circuit_breaker: &CircuitBreaker,
    sp_repo: &StorageProviderRepository,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<usize> {
    let providers = sp_repo.get_due_for_bms_test(BATCH_SIZE).await?;

    debug!("Found {} providers due for BMS test", providers.len());

    let mut jobs_created = 0;

    for provider in providers {
        // Check circuit breaker before each BMS API call
        if let Err(e) = circuit_breaker.check_allowed() {
            warn!(
                "BMS circuit breaker open, skipping provider {}: {}",
                provider.provider_id, e
            );
            continue;
        }

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

        // Schedule next test FIRST to prevent duplicate jobs if later steps fail.
        // This marks the provider as not-due before creating external resources.
        if let Err(e) = sp_repo
            .schedule_next_bms_test(&provider.provider_id, config.bms_test_interval_days)
            .await
        {
            error!(
                "Failed to schedule next BMS test for provider {}: {:?}",
                provider.provider_id, e
            );
            continue;
        }

        match bms_client
            .create_job(
                url.clone(),
                config.bms_default_worker_count,
                Some(format!("f0{}", provider.provider_id)),
            )
            .await
        {
            Ok(job) => {
                circuit_breaker.record_success();

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
                        "Failed to insert pending result for provider {} (BMS job {} created but untracked): {:?}",
                        provider.provider_id, job.id, e
                    );
                    continue;
                }

                debug!(
                    "Created BMS job {} for provider {} (routing_key: {})",
                    job.id, provider.provider_id, job.routing_key
                );
                jobs_created += 1;
            }
            Err(e) => {
                circuit_breaker.record_failure();
                error!(
                    "Failed to create BMS job for provider {}: {} {:?}",
                    provider.provider_id, url, e
                );
            }
        }
    }

    Ok(jobs_created)
}

async fn run_result_poller(
    config: Arc<Config>,
    bms_client: Arc<BmsClient>,
    circuit_breaker: Arc<CircuitBreaker>,
    result_repo: Arc<BmsBandwidthResultRepository>,
    shutdown: CancellationToken,
) {
    info!("Starting BMS result poller loop");

    loop {
        if let Err(e) = poll_bms_results(&config, &bms_client, &circuit_breaker, &result_repo).await
        {
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
    circuit_breaker: &CircuitBreaker,
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

        if let Err(e) = poll_single_result(&result, bms_client, circuit_breaker, result_repo).await
        {
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
    circuit_breaker: &CircuitBreaker,
    result_repo: &BmsBandwidthResultRepository,
) -> Result<()> {
    // Check circuit breaker before polling BMS API
    if let Err(e) = circuit_breaker.check_allowed() {
        debug!(
            "BMS circuit breaker open, skipping poll for job {}: {}",
            result.bms_job_id, e
        );
        return Ok(());
    }

    match bms_client.get_job(result.bms_job_id).await {
        Ok(job_response) => {
            circuit_breaker.record_success();

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
            circuit_breaker.record_failure();
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
    debug!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bms_client::{
        BmsJobDetails, BmsJobResponse, DownloadResult, HeadResult, PingResult, SubJob, WorkerData,
    };
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    fn make_job_response(sub_jobs: Option<Vec<SubJob>>) -> BmsJobResponse {
        BmsJobResponse {
            id: Uuid::new_v4(),
            status: "Completed".to_string(),
            url: "http://example.com/file".to_string(),
            routing_key: "us_east".to_string(),
            details: Some(BmsJobDetails {
                worker_count: Some(3),
                size_mb: Some(100),
            }),
            sub_jobs,
        }
    }

    fn make_worker_data(
        ping_avg: Option<f64>,
        head_avg: Option<f64>,
        ttfb: Option<f64>,
        speed: Option<f64>,
    ) -> WorkerData {
        WorkerData {
            ping: ping_avg.map(|avg| PingResult {
                avg: Some(avg),
                min: Some(avg * 0.9),
                max: Some(avg * 1.1),
            }),
            head: head_avg.map(|avg| HeadResult {
                avg: Some(avg),
                min: Some(avg * 0.9),
                max: Some(avg * 1.1),
            }),
            download: Some(DownloadResult {
                download_speed: speed,
                time_to_first_byte_ms: ttfb,
                total_bytes: Some(100_000_000),
                elapsed_secs: Some(10.0),
            }),
        }
    }

    fn make_subjob(status: &str, worker_data: Option<Vec<WorkerData>>) -> SubJob {
        SubJob {
            id: Uuid::new_v4(),
            status: status.to_string(),
            worker_data,
        }
    }

    // --- extract_results_from_job tests ---

    #[test]
    fn test_extract_results_completed_job_with_all_metrics() {
        // Ping is in seconds from BMS, should be converted to ms
        let worker = make_worker_data(Some(0.025), Some(50.0), Some(100.0), Some(500.0));
        let subjob = make_subjob("Completed", Some(vec![worker]));
        let job = make_job_response(Some(vec![subjob]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        assert_eq!(ping, Some(25.0)); // 0.025s * 1000 = 25ms
        assert_eq!(head, Some(50.0));
        assert_eq!(ttfb, Some(100.0));
        assert_eq!(speed, Some(500.0));
    }

    #[test]
    fn test_extract_results_uses_last_completed_subjob() {
        // First subjob (warmup) - lower values
        let worker1 = make_worker_data(Some(0.010), Some(20.0), Some(50.0), Some(100.0));
        let subjob1 = make_subjob("Completed", Some(vec![worker1]));

        // Second subjob (80% workers) - medium values
        let worker2 = make_worker_data(Some(0.020), Some(40.0), Some(80.0), Some(300.0));
        let subjob2 = make_subjob("Completed", Some(vec![worker2]));

        // Third subjob (100% workers) - highest values, this should be used
        let worker3 = make_worker_data(Some(0.030), Some(60.0), Some(120.0), Some(500.0));
        let subjob3 = make_subjob("Completed", Some(vec![worker3]));

        let job = make_job_response(Some(vec![subjob1, subjob2, subjob3]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        // Should get values from last completed subjob (subjob3)
        assert_eq!(ping, Some(30.0)); // 0.030s * 1000
        assert_eq!(head, Some(60.0));
        assert_eq!(ttfb, Some(120.0));
        assert_eq!(speed, Some(500.0));
    }

    #[test]
    fn test_extract_results_skips_failed_subjobs() {
        // First subjob completed
        let worker1 = make_worker_data(Some(0.010), Some(20.0), Some(50.0), Some(100.0));
        let subjob1 = make_subjob("Completed", Some(vec![worker1]));

        // Second subjob failed (should be skipped)
        let subjob2 = make_subjob("Failed", None);

        // Third subjob cancelled (should be skipped)
        let subjob3 = make_subjob("Cancelled", None);

        let job = make_job_response(Some(vec![subjob1, subjob2, subjob3]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        // Should get values from first completed subjob (subjob1)
        assert_eq!(ping, Some(10.0)); // 0.010s * 1000
        assert_eq!(head, Some(20.0));
        assert_eq!(ttfb, Some(50.0));
        assert_eq!(speed, Some(100.0));
    }

    #[test]
    fn test_extract_results_no_subjobs() {
        let job = make_job_response(None);

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        assert_eq!(ping, None);
        assert_eq!(head, None);
        assert_eq!(ttfb, None);
        assert_eq!(speed, None);
    }

    #[test]
    fn test_extract_results_empty_subjobs() {
        let job = make_job_response(Some(vec![]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        assert_eq!(ping, None);
        assert_eq!(head, None);
        assert_eq!(ttfb, None);
        assert_eq!(speed, None);
    }

    #[test]
    fn test_extract_results_completed_but_no_worker_data() {
        let subjob = make_subjob("Completed", None);
        let job = make_job_response(Some(vec![subjob]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        assert_eq!(ping, None);
        assert_eq!(head, None);
        assert_eq!(ttfb, None);
        assert_eq!(speed, None);
    }

    #[test]
    fn test_extract_results_completed_empty_worker_data() {
        let subjob = make_subjob("Completed", Some(vec![]));
        let job = make_job_response(Some(vec![subjob]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        assert_eq!(ping, None);
        assert_eq!(head, None);
        assert_eq!(ttfb, None);
        assert_eq!(speed, None);
    }

    #[test]
    fn test_extract_results_partial_metrics() {
        // Worker with only download data, no ping/head
        let worker = WorkerData {
            ping: None,
            head: None,
            download: Some(DownloadResult {
                download_speed: Some(500.0),
                time_to_first_byte_ms: Some(100.0),
                total_bytes: None,
                elapsed_secs: None,
            }),
        };
        let subjob = make_subjob("Completed", Some(vec![worker]));
        let job = make_job_response(Some(vec![subjob]));

        let (ping, head, ttfb, speed) = extract_results_from_job(&job);

        assert_eq!(ping, None);
        assert_eq!(head, None);
        assert_eq!(ttfb, Some(100.0));
        assert_eq!(speed, Some(500.0));
    }

    // --- is_result_timed_out tests ---

    fn make_bms_result(created_at: chrono::DateTime<Utc>) -> BmsBandwidthResult {
        BmsBandwidthResult {
            id: Uuid::new_v4(),
            provider_id: "12345".to_string(),
            bms_job_id: Uuid::new_v4(),
            url_tested: "http://example.com".to_string(),
            routing_key: "us_east".to_string(),
            worker_count: 3,
            status: "Pending".to_string(),
            ping_avg_ms: None,
            head_avg_ms: None,
            ttfb_ms: None,
            download_speed_mbps: None,
            created_at,
            completed_at: None,
        }
    }

    #[test]
    fn test_is_result_timed_out_fresh_job() {
        let result = make_bms_result(Utc::now());
        assert!(!is_result_timed_out(&result));
    }

    #[test]
    fn test_is_result_timed_out_47_hours() {
        let result = make_bms_result(Utc::now() - Duration::hours(47));
        assert!(!is_result_timed_out(&result));
    }

    #[test]
    fn test_is_result_timed_out_exactly_48_hours() {
        let result = make_bms_result(Utc::now() - Duration::hours(48));
        assert!(is_result_timed_out(&result));
    }

    #[test]
    fn test_is_result_timed_out_72_hours() {
        let result = make_bms_result(Utc::now() - Duration::hours(72));
        assert!(is_result_timed_out(&result));
    }
}
