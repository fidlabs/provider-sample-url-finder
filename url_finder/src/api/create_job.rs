use std::sync::Arc;

use axum::{extract::State, Json};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{provider_endpoints, AppState};

use super::ResultCode;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CreateJobPayload {
    provider: String,
    client: Option<String>,
}

#[derive(Deserialize, Serialize)]
enum JobType {
    WorkingUrl,
    Retrievability,
}

#[derive(Serialize, ToSchema)]
pub struct CreateJobResponse {
    result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Uuid>,
}

/// Create a job to find working urls or retrievability for a given SP and Client address
#[utoipa::path(
    post,
    path = "/url/job",
    request_body(content = CreateJobPayload),
    description = r#"
**Create a job to find working urls or retrievability for a given SP and Client address**
    "#,
    responses(
        (status = 200, description = "Successful job creation", body = CreateJobResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["JOB"],
)]
pub async fn handle_create_job(
    State(state): State<Arc<AppState>>,
    WithRejection(Json(payload), _): WithRejection<
        Json<CreateJobPayload>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<CreateJobResponse>, ApiResponse<()>> {
    debug!(
        "create job provider: {:?}, client: {:?}",
        &payload.provider, &payload.client
    );

    // validate provider and client addresses
    let address_pattern = Regex::new(r"^f0\d{1,8}$").unwrap();
    if !address_pattern.is_match(&payload.provider)
        || (payload.client.is_some() && !address_pattern.is_match(payload.client.as_ref().unwrap()))
    {
        return Err(bad_request(
            "Invalid provider or client address".to_string(),
        ));
    }

    // Verify that we have http endpoint for the provider
    let _ = match provider_endpoints::get_provider_endpoints(&payload.provider).await {
        Ok((result_code, endpoints)) if endpoints.is_none() => {
            debug!("No endpoints found");
            return Ok(ok_response(CreateJobResponse {
                result: result_code,
                id: None,
            }));
        }
        Err(e) => return Err(internal_server_error(e.to_string())),
        Ok(result) => result,
    };

    let job = state
        .job_repo
        .create_job(payload.provider, payload.client)
        .await
        .map_err(|e| {
            error!("Failed to create job: {}", e);
            internal_server_error("Failed to create job: {}")
        })?;

    Ok(ok_response(CreateJobResponse {
        result: ResultCode::JobCreated,
        id: Some(job.id),
    }))
}
