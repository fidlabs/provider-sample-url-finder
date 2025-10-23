use std::{sync::Arc, time::Duration};

use tokio::time::sleep;
use tracing::{debug, info};

use crate::{
    ErrorCode, Job, JobRepository, JobStatus, ResultCode, deal_repo::DealRepository, deal_service,
    provider_endpoints, url_tester,
};

const LOOP_DELAY: Duration = Duration::from_secs(5);

pub struct JobFailed {
    pub error: Option<ErrorCode>,
    pub result: Option<ResultCode>,
    pub reason: String,
}

pub struct JobSuccessResult {
    pub provider: String,
    pub client: Option<String>,
    pub working_url: Option<String>,
    pub retrievability: f64,
    pub result: ResultCode,
}

pub struct JobErrorResult {
    pub provider: String,
    pub client: Option<String>,
    pub error: Option<ErrorCode>,
    pub result: Option<ResultCode>,
}

pub(super) enum JobHandlerResult {
    Skip(String),
    FailedJob(JobFailed),
    ErrorResult(JobErrorResult),
    SuccessResult(JobSuccessResult),
    MultipleResults(Vec<JobSuccessResult>, Vec<JobErrorResult>),
}

pub async fn job_handler(job_repo: Arc<JobRepository>, deal_repo: Arc<DealRepository>) {
    info!("Starting job handler");

    loop {
        sleep(LOOP_DELAY).await;

        let job = job_repo.get_first_pending().await;
        if job.is_none() {
            continue;
        }

        let job = job.unwrap();

        debug!("Found job: {:?}", job.id);

        match process_pending_job(&deal_repo, &job).await {
            JobHandlerResult::Skip(reason) => {
                debug!("Skipping job: {}", reason);
                continue;
            }
            JobHandlerResult::FailedJob(job_failed) => {
                debug!("Failed job: {}, reason: {}", job.id, job_failed.reason);
                job_repo
                    .fail_job(job.id, job_failed.result, job_failed.error)
                    .await;
            }
            JobHandlerResult::ErrorResult(error_result) => {
                debug!("Error processing job: {}", job.id);
                job_repo
                    .add_error_result(
                        job.id,
                        error_result.provider,
                        error_result.client,
                        error_result.error,
                        error_result.result,
                    )
                    .await;

                job_repo.set_status(job.id, JobStatus::Completed).await;
            }
            JobHandlerResult::SuccessResult(success_result) => {
                debug!("Job completed successfully: {}", job.id);
                job_repo
                    .add_success_result(
                        job.id,
                        success_result.provider,
                        success_result.client,
                        success_result.working_url,
                        success_result.retrievability,
                        success_result.result,
                    )
                    .await;

                job_repo.set_status(job.id, JobStatus::Completed).await;
            }
            JobHandlerResult::MultipleResults(success_results, error_resuls) => {
                debug!("Multiple results processed for job: {}", job.id);

                for success_result in success_results {
                    debug!(
                        "Job success result: provider: {}, client: {:?}, working_url: {:?}, retrievability: {}",
                        success_result.provider,
                        success_result.client,
                        success_result.working_url,
                        success_result.retrievability
                    );
                    job_repo
                        .add_success_result(
                            job.id,
                            success_result.provider,
                            success_result.client,
                            success_result.working_url,
                            success_result.retrievability,
                            success_result.result,
                        )
                        .await;
                }
                for error_result in error_resuls {
                    debug!(
                        "Job error result: provider: {}, client: {:?}",
                        error_result.provider, error_result.client,
                    );
                    job_repo
                        .add_error_result(
                            job.id,
                            error_result.provider,
                            error_result.client,
                            error_result.error,
                            error_result.result,
                        )
                        .await;
                }
                job_repo.set_status(job.id, JobStatus::Completed).await;
            }
        }
    }
}

async fn process_pending_job(deal_repo: &DealRepository, job: &Job) -> JobHandlerResult {
    match (&job.provider, &job.client) {
        (Some(provider), None) => process_job_with_provider(deal_repo, provider).await,
        (None, Some(client)) => process_job_with_client(deal_repo, client).await,
        (Some(provider), Some(client)) => {
            process_job_with_provider_and_client(deal_repo, provider, client).await
        }
        (None, None) => {
            // should not happen
            JobHandlerResult::FailedJob(JobFailed {
                error: Some(ErrorCode::NoProviderOrClient),
                result: Some(ResultCode::Error),
                reason: "No provider or client specified".to_string(),
            })
        }
    }
}

async fn process_job_with_client(deal_repo: &DealRepository, client: &str) -> JobHandlerResult {
    let providers = match deal_service::get_distinct_providers_by_client(deal_repo, client).await {
        Ok(providers) => providers,
        Err(e) => {
            debug!("Failed to get providers for client {}: {:?}", client, e);
            return JobHandlerResult::Skip(format!("Failed to get providers for client {client}"));
        }
    };

    if providers.is_empty() {
        return JobHandlerResult::FailedJob(JobFailed {
            error: Some(ErrorCode::NoProvidersFound),
            result: Some(ResultCode::Error),
            reason: format!("No providers found for client: {client}"),
        });
    }

    let mut success_results = Vec::new();
    let mut error_results = Vec::new();

    for provider in providers {
        debug!("Processing job with provider: {}", &provider);

        match process_job(deal_repo, &provider, Some(client)).await {
            JobHandlerResult::SuccessResult(result) => success_results.push(result),
            JobHandlerResult::ErrorResult(result) => error_results.push(result),
            JobHandlerResult::Skip(reason) => {
                return JobHandlerResult::Skip(reason);
            }
            JobHandlerResult::FailedJob(job_failed) => {
                return JobHandlerResult::FailedJob(job_failed);
            }
            // should not happen here
            JobHandlerResult::MultipleResults(_, _) => continue,
        }
    }

    JobHandlerResult::MultipleResults(success_results, error_results)
}

async fn process_job_with_provider_and_client(
    deal_repo: &DealRepository,
    provider: &str,
    client: &str,
) -> JobHandlerResult {
    debug!(
        "Processing job with provider: {} and client: {}",
        provider, client
    );

    process_job(deal_repo, provider, Some(client)).await
}

async fn process_job_with_provider(deal_repo: &DealRepository, provider: &str) -> JobHandlerResult {
    debug!("Processing job with provider: {}", provider);

    process_job(deal_repo, provider, None).await
}

async fn process_job(
    deal_repo: &DealRepository,
    provider: &str,
    client: Option<&str>,
) -> JobHandlerResult {
    let (_, endpoints) = match provider_endpoints::get_provider_endpoints(provider).await {
        Ok((result_code, _)) if result_code != ResultCode::Success => {
            return JobHandlerResult::ErrorResult(JobErrorResult {
                provider: provider.to_string(),
                client: client.map(|c| c.to_string()),
                result: Some(result_code),
                error: None,
            });
        }
        Ok(result) => result,
        Err(error_code) => {
            return JobHandlerResult::ErrorResult(JobErrorResult {
                provider: provider.to_string(),
                client: client.map(|c| c.to_string()),
                result: Some(ResultCode::Error),
                error: Some(error_code),
            });
        }
    };

    if endpoints.is_none() || endpoints.as_ref().unwrap().is_empty() {
        debug!("No endpoints found");

        return JobHandlerResult::ErrorResult(JobErrorResult {
            provider: provider.to_string(),
            client: client.map(|c| c.to_string()),
            result: Some(ResultCode::NoDealsFound),
            error: None,
        });
    }
    let endpoints = endpoints.unwrap();

    let provider_db = provider.strip_prefix("f0").unwrap_or(provider);
    let client_db = client.as_ref().map(|c| c.strip_prefix("f0").unwrap_or(c));

    let piece_ids =
        match deal_service::get_piece_ids_by_provider(deal_repo, provider_db, client_db).await {
            Ok(ids) => ids,
            Err(e) => {
                debug!("Failed to get piece ids: {:?}", e);

                return JobHandlerResult::Skip("Failed to get piece ids".to_string());
            }
        };

    if piece_ids.is_empty() {
        debug!("No deals found");

        return JobHandlerResult::ErrorResult(JobErrorResult {
            provider: provider.to_string(),
            client: client.map(|c| c.to_string()),
            result: Some(ResultCode::NoDealsFound),
            error: None,
        });
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    let (working_url, retrievability) = url_tester::get_retrivability_with_get(urls).await;

    let result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };

    JobHandlerResult::SuccessResult(JobSuccessResult {
        provider: provider.to_string(),
        client: client.map(|c| c.to_string()),
        working_url,
        retrievability,
        result: result_code,
    })
}
