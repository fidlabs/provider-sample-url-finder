use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use color_eyre::{Result, eyre::eyre};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::bms_scheduler::extract_results_from_job;
use crate::{
    bms_client::BmsClient,
    circuit_breaker::CircuitBreaker,
    config::Config,
    repository::{DealSliBmsJobCompletion, DealSliRepository, NewDealSliBmsJob},
    services::deal_sli_service::{DealSliService, DealSliServiceError},
};

const DEAL_SLI_SCHEDULER_INTERVAL: Duration = Duration::from_secs(300);
const DEAL_SLI_SCHEDULER_CATCHUP_INTERVAL: Duration = Duration::from_secs(30);
const DEAL_SLI_BMS_RESULT_POLLER_INTERVAL: Duration = Duration::from_secs(30);
const DEAL_SLI_SCHEDULER_BATCH_SIZE: i64 = 25;
const DEAL_SLI_BMS_JOB_TIMEOUT_HOURS: i64 = 48;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DealSliSchedulerStats {
    pub targets_processed: usize,
    pub bms_jobs_created: usize,
}

pub async fn run_deal_sli_scheduler(
    config: Arc<Config>,
    deal_sli_service: Arc<DealSliService>,
    deal_sli_repo: Arc<DealSliRepository>,
    bms_client: Arc<BmsClient>,
    circuit_breaker: Arc<CircuitBreaker>,
    shutdown: CancellationToken,
) {
    info!("Starting Deal SLI scheduler");

    loop {
        let interval = match run_deal_sli_scheduler_once(
            &config,
            &deal_sli_service,
            &deal_sli_repo,
            &bms_client,
            &circuit_breaker,
        )
        .await
        {
            Ok(stats) if stats.targets_processed > 0 => {
                info!(
                    "Deal SLI scheduler processed {} targets and created {} BMS jobs",
                    stats.targets_processed, stats.bms_jobs_created
                );
                DEAL_SLI_SCHEDULER_CATCHUP_INTERVAL
            }
            Ok(_) => {
                debug!("No Deal SLI targets due for measurement");
                DEAL_SLI_SCHEDULER_INTERVAL
            }
            Err(error) => {
                error!("Deal SLI scheduler failed: {:?}", error);
                DEAL_SLI_SCHEDULER_INTERVAL
            }
        };

        tokio::select! {
            _ = sleep(interval) => {}
            _ = shutdown.cancelled() => {
                info!("Deal SLI scheduler received shutdown signal");
                break;
            }
        }
    }

    info!("Deal SLI scheduler stopped");
}

pub async fn run_deal_sli_bms_result_poller(
    deal_sli_repo: Arc<DealSliRepository>,
    bms_client: Arc<BmsClient>,
    circuit_breaker: Arc<CircuitBreaker>,
    shutdown: CancellationToken,
) {
    info!("Starting Deal SLI BMS result poller");

    loop {
        if let Err(error) =
            run_deal_sli_bms_result_poller_once(&deal_sli_repo, &bms_client, &circuit_breaker).await
        {
            error!("Deal SLI BMS result poller failed: {:?}", error);
        }

        tokio::select! {
            _ = sleep(DEAL_SLI_BMS_RESULT_POLLER_INTERVAL) => {}
            _ = shutdown.cancelled() => {
                info!("Deal SLI BMS result poller received shutdown signal");
                break;
            }
        }
    }

    info!("Deal SLI BMS result poller stopped");
}

pub async fn run_deal_sli_bms_result_poller_once(
    deal_sli_repo: &DealSliRepository,
    bms_client: &BmsClient,
    circuit_breaker: &CircuitBreaker,
) -> Result<usize> {
    let pending_jobs = deal_sli_repo.get_pending_deal_sli_bms_jobs().await?;
    let mut completed_jobs = 0;

    debug!("Polling {} pending Deal SLI BMS jobs", pending_jobs.len());

    for job in pending_jobs {
        if is_deal_sli_bms_job_timed_out(&job.created_at) {
            let hours = (Utc::now() - job.created_at).num_hours();
            let error_message = format!("BMS job timed out after {hours} hours");
            warn!(
                "Deal SLI BMS job {} for deal {} run {} piece {} timed out after {} hours",
                job.bms_job_id, job.deal_id, job.run_id, job.piece_cid, hours
            );
            deal_sli_repo
                .update_deal_sli_bms_job_completed(&DealSliBmsJobCompletion {
                    job_id: job.bms_job_id,
                    status: "Timeout",
                    ping_avg_ms: None,
                    head_avg_ms: None,
                    ttfb_ms: None,
                    download_speed_mbps: None,
                    error_message: Some(&error_message),
                })
                .await?;
            completed_jobs += 1;
            continue;
        }

        if let Err(error) = circuit_breaker.check_allowed() {
            debug!(
                "BMS circuit breaker open, skipping Deal SLI BMS job {}: {}",
                job.bms_job_id, error
            );
            continue;
        }

        match bms_client.get_job(job.bms_job_id).await {
            Ok(job_response) => {
                circuit_breaker.record_success();

                if BmsClient::is_job_finished(&job_response.status) {
                    let (ping_avg_ms, head_avg_ms, ttfb_ms, download_speed_mbps) =
                        extract_results_from_job(&job_response);
                    deal_sli_repo
                        .update_deal_sli_bms_job_completed(&DealSliBmsJobCompletion {
                            job_id: job.bms_job_id,
                            status: &job_response.status,
                            ping_avg_ms,
                            head_avg_ms,
                            ttfb_ms,
                            download_speed_mbps,
                            error_message: None,
                        })
                        .await?;
                    completed_jobs += 1;
                } else {
                    debug!(
                        "Deal SLI BMS job {} for deal {} run {} piece {} still in progress: {}",
                        job.bms_job_id, job.deal_id, job.run_id, job.piece_cid, job_response.status
                    );
                }
            }
            Err(error) => {
                circuit_breaker.record_failure();
                warn!(
                    "Failed to fetch Deal SLI BMS job {} for deal {} run {} piece {}: {:?}",
                    job.bms_job_id, job.deal_id, job.run_id, job.piece_cid, error
                );
            }
        }
    }

    Ok(completed_jobs)
}

fn is_deal_sli_bms_job_timed_out(created_at: &chrono::DateTime<Utc>) -> bool {
    (Utc::now() - *created_at).num_hours() >= DEAL_SLI_BMS_JOB_TIMEOUT_HOURS
}

pub async fn run_deal_sli_scheduler_once(
    config: &Config,
    deal_sli_service: &DealSliService,
    deal_sli_repo: &DealSliRepository,
    bms_client: &BmsClient,
    circuit_breaker: &CircuitBreaker,
) -> Result<DealSliSchedulerStats> {
    let interval_days = i32::try_from(config.bms_test_interval_days)
        .map_err(|_| eyre!("BMS test interval days exceeds i32::MAX"))?;
    let worker_count = i32::try_from(config.bms_default_worker_count)
        .map_err(|_| eyre!("BMS worker count exceeds i32::MAX"))?;
    let deal_ids = deal_sli_repo
        .claim_due_scheduled_targets(DEAL_SLI_SCHEDULER_BATCH_SIZE, interval_days)
        .await?;

    let mut stats = DealSliSchedulerStats {
        targets_processed: deal_ids.len(),
        bms_jobs_created: 0,
    };

    for deal_id in deal_ids {
        deal_sli_service
            .create_run(&deal_id)
            .await
            .map_err(map_deal_sli_error)?;

        let Some(run) = deal_sli_repo.get_latest_completed_run(&deal_id).await? else {
            warn!("Deal SLI scheduler created no completed run for deal {deal_id}");
            continue;
        };

        let piece_results = deal_sli_repo
            .get_successful_piece_results_without_bms_jobs(run.id)
            .await?;

        for piece_result in piece_results {
            if let Err(error) = circuit_breaker.check_allowed() {
                warn!(
                    "BMS circuit breaker open, skipping deal {} run {} piece {}: {}",
                    piece_result.deal_id, piece_result.run_id, piece_result.piece_cid, error
                );
                continue;
            }

            let entity = format!(
                "porep-deal:{}:run:{}:piece:{}",
                piece_result.deal_id, piece_result.run_id, piece_result.piece_cid
            );

            match bms_client
                .create_job(
                    piece_result.url_tested.clone(),
                    config.bms_default_worker_count,
                    Some(entity),
                )
                .await
            {
                Ok(job) => {
                    circuit_breaker.record_success();
                    deal_sli_repo
                        .insert_deal_sli_bms_job(&NewDealSliBmsJob {
                            deal_id: piece_result.deal_id,
                            run_id: piece_result.run_id,
                            piece_index: piece_result.piece_index,
                            piece_cid: piece_result.piece_cid,
                            bms_job_id: job.id,
                            url_tested: job.url,
                            routing_key: job.routing_key,
                            worker_count,
                            status: job.status,
                        })
                        .await?;
                    stats.bms_jobs_created += 1;
                }
                Err(error) => {
                    circuit_breaker.record_failure();
                    warn!(
                        "Failed to create BMS job for deal {} run {} piece {} URL {}: {:?}",
                        piece_result.deal_id,
                        piece_result.run_id,
                        piece_result.piece_cid,
                        piece_result.url_tested,
                        error
                    );
                }
            }
        }
    }

    Ok(stats)
}

fn map_deal_sli_error(error: DealSliServiceError) -> color_eyre::Report {
    match error {
        DealSliServiceError::InvalidRequest(message) => {
            eyre!("Deal SLI invalid request: {message}")
        }
        DealSliServiceError::NotFound(message) => eyre!("Deal SLI target not found: {message}"),
        DealSliServiceError::Internal(error) => error,
    }
}
