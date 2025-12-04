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
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindUrlSpClientPath {
    pub provider: String,
    pub client: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindUrlSpClientResponse {
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Find a working url for a given SP address
#[utoipa::path(
    get,
    path = "/url/find/{provider}/{client}",
    params (FindUrlSpClientPath),
    description = r#"
**Find a working url for a given SP address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindUrlSpClientResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_url_sp_client(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<FindUrlSpClientPath>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<FindUrlSpClientResponse>, ApiResponse<()>> {
    debug!("find url input address: {:?}", &path.provider);

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
        Some(url_result) => Ok(ok_response(FindUrlSpClientResponse {
            result: url_result.result_code,
            url: url_result.working_url,
            message: None,
        })),
        None => Ok(ok_response(FindUrlSpClientResponse {
            result: ResultCode::Error,
            url: None,
            message: Some(
                "Provider/client pair has not been indexed yet. Please try again later."
                    .to_string(),
            ),
        })),
    }
}
