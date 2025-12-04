use std::sync::Arc;

use crate::api_response::*;
use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindRetriByClientAndSpPath {
    pub client: String,
    pub provider: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindRetriByClientAndSpResponse {
    pub result: ResultCode,
    pub retrievability_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Find retrievability of urls for a given SP and Client address
#[utoipa::path(
    get,
    path = "/url/retrievability/{provider}/{client}",
    params (FindRetriByClientAndSpPath),
    description = r#"
**Find retrievabiliy of urls for a given SP and Client address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindRetriByClientAndSpResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_retri_by_client_and_sp(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<FindRetriByClientAndSpPath>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<FindRetriByClientAndSpResponse>, ApiResponse<()>> {
    debug!(
        "find retri for input address: {:?} and client: {:?}",
        &path.provider, &path.client
    );

    // Parse and validate provider and client addresses
    let provider_address = ProviderAddress::new(path.provider)
        .map_err(|e| bad_request(format!("Invalid provider address: {e}")))?;
    let client_address = ClientAddress::new(path.client)
        .map_err(|e| bad_request(format!("Invalid client address: {e}")))?;

    let provider_id: ProviderId = provider_address.into();
    let client_id: ClientId = client_address.into();

    let result = state
        .url_repo
        .get_latest_for_provider_client(&provider_id, &client_id)
        .await
        .map_err(|e| {
            debug!("Failed to query url_results: {:?}", e);
            internal_server_error("Failed to query url results")
        })?;

    match result {
        Some(url_result) => Ok(ok_response(FindRetriByClientAndSpResponse {
            result: url_result.result_code,
            retrievability_percent: url_result.retrievability_percent,
            message: None,
        })),
        None => Ok(ok_response(FindRetriByClientAndSpResponse {
            result: ResultCode::Error,
            retrievability_percent: 0.0,
            message: Some(
                "Provider/client pair has not been indexed yet. Please try again later."
                    .to_string(),
            ),
        })),
    }
}
