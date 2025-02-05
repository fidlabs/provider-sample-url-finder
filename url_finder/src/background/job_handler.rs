use std::{sync::Arc, time::Duration};

use tokio::time::sleep;
use tracing::{debug, info};

use crate::{
    deal_repo::DealRepository, deal_service, provider_endpoints, url_tester, ErrorCode, Job,
    JobRepository, ResultCode,
};

const LOOP_DELAY: Duration = Duration::from_secs(5);

pub(super) enum JobHandlerError {
    Skip(String),
    FailedJob(Option<ResultCode>, Option<ErrorCode>, String),
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

        match process_pending_job(&job_repo, &deal_repo, &job).await {
            Ok(_) => debug!("Job processed successfully"),
            Err(JobHandlerError::Skip(reason)) => {
                debug!("Skipping job: {}", reason);
                continue;
            }
            Err(JobHandlerError::FailedJob(result_code, error_code, reason)) => {
                debug!("Failed job: {}", reason);
                job_repo.fail_job(job.id, result_code, error_code).await;
            }
        }
    }
}

async fn process_pending_job(
    job_repo: &JobRepository,
    deal_repo: &DealRepository,
    job: &Job,
) -> Result<(), JobHandlerError> {
    let (_, endpoints) = match provider_endpoints::get_provider_endpoints(&job.provider).await {
        Ok((result_code, _)) if result_code != ResultCode::Success => {
            return Err(JobHandlerError::FailedJob(
                Some(result_code),
                None,
                "Provider endpoints not found".to_string(),
            ))
        }
        Ok(result) => result,
        Err(error_code) => {
            return Err(JobHandlerError::FailedJob(
                None,
                Some(error_code.clone()),
                error_code.to_string(),
            ))
        }
    };

    if endpoints.is_none() || endpoints.as_ref().unwrap().is_empty() {
        debug!("No endpoints found");

        return Err(JobHandlerError::FailedJob(
            Some(ResultCode::NoDealsFound),
            None,
            "No endpoints found".to_string(),
        ));
    }
    let endpoints = endpoints.unwrap();

    let provider = job.provider.strip_prefix("f0").unwrap_or(&job.provider);

    let client = job
        .client
        .as_ref()
        .map(|c| c.strip_prefix("f0").unwrap_or(c));

    let piece_ids = deal_service::get_piece_ids_by_provider(deal_repo, provider, client)
        .await
        .map_err(|e| {
            debug!("Failed to get piece ids: {:?}", e);

            JobHandlerError::Skip("Failed to get piece ids".to_string())
        })?;

    if piece_ids.is_empty() {
        debug!("No deals found");

        return Err(JobHandlerError::FailedJob(
            Some(ResultCode::NoDealsFound),
            None,
            "No deals found".to_string(),
        ));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    let (working_url, retrievability) = url_tester::get_retrivability_with_head(urls).await;

    job_repo
        .update_job_result(job.id, working_url, retrievability)
        .await;

    Ok(())
}
