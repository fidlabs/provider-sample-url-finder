use std::{sync::Arc, time::Duration};

use tokio::time::sleep;
use tracing::{debug, info};

use crate::{
    ErrorCode, Job, JobRepository, JobStatus, ResultCode, provider_endpoints,
    repository::DealRepository,
    services::deal_service,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
    url_tester,
};

const LOOP_DELAY: Duration = Duration::from_secs(5);

pub struct JobFailed {
    pub error: Option<ErrorCode>,
    pub result: Option<ResultCode>,
    pub reason: String,
}

pub struct JobSuccessResult {
    pub provider: ProviderAddress,
    pub client: Option<ClientAddress>,
    pub working_url: Option<String>,
    pub retrievability: f64,
    pub result: ResultCode,
}

pub struct JobErrorResult {
    pub provider: ProviderAddress,
    pub client: Option<ClientAddress>,
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
    info!("Starting job handler loop");

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
        (Some(provider_address), None) => {
            process_job_with_provider(deal_repo, provider_address).await
        }
        (None, Some(client_address)) => process_job_with_client(deal_repo, client_address).await,
        (Some(provider_address), Some(client_address)) => {
            process_job_with_provider_and_client(deal_repo, provider_address, client_address).await
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

async fn process_job_with_client(
    deal_repo: &DealRepository,
    client_address: &ClientAddress,
) -> JobHandlerResult {
    let providers =
        match deal_service::get_distinct_providers_by_client(deal_repo, client_address).await {
            Ok(providers) => providers,
            Err(e) => {
                debug!(
                    "Failed to get providers for client {}: {:?}",
                    client_address, e
                );
                return JobHandlerResult::Skip(format!(
                    "Failed to get providers for client {client_address}"
                ));
            }
        };

    if providers.is_empty() {
        return JobHandlerResult::FailedJob(JobFailed {
            error: Some(ErrorCode::NoProvidersFound),
            result: Some(ResultCode::Error),
            reason: format!("No providers found for client: {client_address}"),
        });
    }

    let mut success_results = Vec::new();
    let mut error_results = Vec::new();

    for provider_address in providers {
        debug!("Processing job with provider: {}", &provider_address);

        match process_job(deal_repo, &provider_address, Some(client_address)).await {
            JobHandlerResult::SuccessResult(result) => success_results.push(result),
            JobHandlerResult::ErrorResult(result) => error_results.push(result),
            JobHandlerResult::Skip(reason) => return JobHandlerResult::Skip(reason),
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
    provider_address: &ProviderAddress,
    client_address: &ClientAddress,
) -> JobHandlerResult {
    debug!(
        "Processing job with provider: {} and client: {}",
        provider_address, client_address
    );

    process_job(deal_repo, provider_address, Some(client_address)).await
}

async fn process_job_with_provider(
    deal_repo: &DealRepository,
    provider_address: &ProviderAddress,
) -> JobHandlerResult {
    debug!("Processing job with provider: {}", provider_address);

    process_job(deal_repo, provider_address, None).await
}

async fn process_job(
    deal_repo: &DealRepository,
    provider_address: &ProviderAddress,
    client_address: Option<&ClientAddress>,
) -> JobHandlerResult {
    let provider_id: ProviderId = provider_address.clone().into();
    let client_id: Option<ClientId> = client_address.map(|c| c.clone().into());

    let (_, endpoints) = match provider_endpoints::get_provider_endpoints(provider_address).await {
        Ok((result_code, _)) if result_code != ResultCode::Success => {
            return JobHandlerResult::ErrorResult(JobErrorResult {
                provider: provider_address.clone(),
                client: client_address.cloned(),
                result: Some(result_code),
                error: None,
            });
        }
        Ok(result) => result,
        Err(error_code) => {
            return JobHandlerResult::ErrorResult(JobErrorResult {
                provider: provider_address.clone(),
                client: client_address.cloned(),
                result: Some(ResultCode::Error),
                error: Some(error_code),
            });
        }
    };

    if endpoints.is_none() || endpoints.as_ref().unwrap().is_empty() {
        debug!("No endpoints found");

        return JobHandlerResult::ErrorResult(JobErrorResult {
            provider: provider_address.clone(),
            client: client_address.cloned(),
            result: Some(ResultCode::NoDealsFound),
            error: None,
        });
    }
    let endpoints = endpoints.unwrap();

    let piece_ids =
        match deal_service::get_piece_ids_by_provider(deal_repo, &provider_id, client_id.as_ref())
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                debug!("Failed to get piece ids: {:?}", e);

                return JobHandlerResult::Skip("Failed to get piece ids".to_string());
            }
        };

    if piece_ids.is_empty() {
        debug!("No deals found");

        return JobHandlerResult::ErrorResult(JobErrorResult {
            provider: provider_address.clone(),
            client: client_address.cloned(),
            result: Some(ResultCode::NoDealsFound),
            error: None,
        });
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    let (working_url, retrievability_percent) =
        url_tester::check_retrievability_with_get(urls, true).await;

    let result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };

    JobHandlerResult::SuccessResult(JobSuccessResult {
        provider: provider_address.clone(),
        client: client_address.cloned(),
        working_url,
        retrievability: retrievability_percent.unwrap_or(0.0),
        result: result_code,
    })
}
