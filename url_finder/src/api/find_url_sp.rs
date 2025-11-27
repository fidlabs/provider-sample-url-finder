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
    AppState, ResultCode,
    types::{ProviderAddress, ProviderId},
};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindUrlSpPath {
    pub provider: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindUrlSpResponse {
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Find a working url for a given SP address
#[utoipa::path(
    get,
    path = "/url/find/{provider}",
    params (FindUrlSpPath),
    description = r#"
**Find a working url for a given SP address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindUrlSpResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_url_sp(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<FindUrlSpPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<FindUrlSpResponse>, ApiResponse<()>> {
    debug!("find url input address: {:?}", &path.provider);

    // Parse and validate provider address
    let provider_address = ProviderAddress::new(path.provider)
        .map_err(|e| bad_request(format!("Invalid provider address: {e}")))?;

    let provider_id: ProviderId = provider_address.into();

    let result = state
        .url_repo
        .get_latest_for_provider(&provider_id)
        .await
        .map_err(|e| {
            debug!("Failed to query url_results: {:?}", e);
            internal_server_error("Failed to query url results")
        })?;

    match result {
        Some(url_result) => Ok(ok_response(FindUrlSpResponse {
            result: url_result.result_code.clone(),
            url: url_result.working_url,
            message: url_result.result_code.message().map(String::from),
        })),
        None => Ok(ok_response(FindUrlSpResponse {
            result: ResultCode::Error,
            url: None,
            message: Some("Provider has not been indexed yet. Please try again later.".to_string()),
        })),
    }
}
