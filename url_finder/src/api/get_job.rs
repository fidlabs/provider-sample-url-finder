use std::sync::Arc;

use axum::extract::{Path, State};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use serde::{Deserialize, Serialize};

use tracing::error;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{AppState, Job};

#[derive(Deserialize, Serialize, ToSchema, IntoParams)]
pub struct GetJobPath {
    id: Uuid,
}

#[derive(Serialize, ToSchema)]
pub struct GetJobResponse {
    #[serde(flatten)]
    job: Job,
}

/// Get a job with working urls or retrievability
#[utoipa::path(
  get,
  path = "/jobs/{job_id}",
  params (GetJobPath),
  description = r#"
**Get a job with working urls or retrievability**
  "#,
  responses(
      (status = 200, description = "Success", body = GetJobResponse),
      (status = 400, description = "Bad Request", body = ErrorResponse),
      (status = 404, description = "Not Found", body = ErrorResponse),
      (status = 500, description = "Internal Server Error", body = ErrorResponse),
  ),
  tags = ["JOB"],
)]
pub async fn handle_get_job(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<GetJobPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<GetJobResponse>, ApiResponse<()>> {
    let mut job = state.job_repo.get_job(path.id).await.map_err(|e| {
        error!("Error getting job: {:?}", e);

        not_found("Failed to get the job".to_string())
    })?;

    // Modify the job to include the first result's working URL and retrievability for FE compatibility
    job.working_url = job.results.first().and_then(|r| r.working_url.clone());
    job.retrievability = job.results.first().map(|r| r.retrievability as i64);

    Ok(ok_response(GetJobResponse { job }))
}
