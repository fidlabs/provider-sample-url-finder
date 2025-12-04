use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use serde::Deserialize;
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code, internal_server_error_with_code,
        not_found_with_code, ok_response,
    },
    types::{ClientAddress, ProviderAddress},
};

use super::types::ProviderClientResponse;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct GetProviderClientPath {
    pub id: String,
    pub client_id: String,
}

#[utoipa::path(
    get,
    path = "/providers/{id}/clients/{client_id}",
    params(GetProviderClientPath),
    responses(
        (status = 200, description = "Provider+client found", body = ProviderClientResponse),
        (status = 400, description = "Invalid address", body = ErrorResponse),
        (status = 404, description = "Provider+client not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_get_provider_client(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<GetProviderClientPath>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<ProviderClientResponse>, ApiResponse<()>> {
    debug!("GET /providers/{}/clients/{}", &path.id, &path.client_id);

    let provider_address = ProviderAddress::new(&path.id).map_err(|e| {
        bad_request_with_code(ErrorCode::InvalidAddress, format!("Invalid provider address: {e}"))
    })?;
    let client_address = ClientAddress::new(&path.client_id).map_err(|e| {
        bad_request_with_code(ErrorCode::InvalidAddress, format!("Invalid client address: {e}"))
    })?;

    let provider_id = provider_address.into();
    let client_id = client_address.into();

    let data = state
        .provider_service
        .get_provider_client(&provider_id, &client_id)
        .await
        .map_err(|e| {
            debug!("Failed to query provider+client: {:?}", e);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query provider")
        })?
        .ok_or_else(|| {
            not_found_with_code(
                ErrorCode::NotFound,
                format!(
                    "Provider {} with client {} not found",
                    &path.id, &path.client_id
                ),
            )
        })?;

    Ok(ok_response(data.into()))
}
