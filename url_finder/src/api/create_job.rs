use std::sync::Arc;

use axum::{Json, extract::State};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    AppState, provider_endpoints,
    types::{ClientAddress, ProviderAddress},
};

use super::ResultCode;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CreateJobPayload {
    provider: Option<String>,
    client: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct CreateJobResponse {
    result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Uuid>,
}

/// Create a job to find working urls or retrievability for
/// Either by SP or Client address or both
#[utoipa::path(
    post,
    path = "/jobs",
    request_body(content = CreateJobPayload),
    description = r#"
**Create a job to find working urls or retrievability for either by SP or Client address or both**
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

    if payload.provider.is_none() && payload.client.is_none() {
        return Err(bad_request(
            "Either provider, client address or both must be provided".to_string(),
        ));
    }

    // Parse and validate client address if provided
    let client_address = if let Some(client) = &payload.client {
        Some(ClientAddress::new(client.clone()).map_err(|e| {
            error!("Invalid client address: {}", e);
            bad_request(format!("Invalid client address: {}", e))
        })?)
    } else {
        None
    };

    // Parse and validate provider address if provided
    let provider_address = if let Some(provider) = &payload.provider {
        let provider_addr = ProviderAddress::new(provider.clone()).map_err(|e| {
            error!("Invalid provider address: {}", e);
            bad_request(format!("Invalid provider address: {}", e))
        })?;

        // Verify that we have http endpoint for the provider
        let _ = match provider_endpoints::get_provider_endpoints(&provider_addr).await {
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

        Some(provider_addr)
    } else {
        None
    };

    let job = state
        .job_repo
        .create_job(provider_address, client_address)
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
