use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, Query, State},
};
use axum_extra::extract::WithRejection;
use serde::Deserialize;
use tracing::{debug, error};
use utoipa::{IntoParams, ToSchema};

use super::ExtendedQuery;

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code,
        internal_server_error_with_code, not_found_with_code, ok_response,
    },
    types::ProviderAddress,
};

use super::types::ProviderResponse;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct GetProviderPath {
    pub id: String,
}

#[utoipa::path(
    get,
    path = "/providers/{id}",
    params(GetProviderPath, ExtendedQuery),
    responses(
        (status = 200, description = "Provider found", body = ProviderResponse),
        (status = 400, description = "Invalid provider address", body = ErrorResponse),
        (status = 404, description = "Provider not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_get_provider(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<GetProviderPath>, ApiResponse<ErrorResponse>>,
    Query(query): Query<ExtendedQuery>,
) -> Result<ApiResponse<ProviderResponse>, ApiResponse<()>> {
    debug!("GET /providers/{}?extended={}", &path.id, query.extended);

    let provider_address = ProviderAddress::new(&path.id).map_err(|e| {
        error!("Invalid provider address '{}': {}", &path.id, e);
        bad_request_with_code(ErrorCode::InvalidAddress, "Invalid provider address")
    })?;

    let provider_id = provider_address.into();

    let data = state
        .provider_service
        .get_provider(&provider_id)
        .await
        .map_err(|e| {
            error!("Failed to query provider_id={}: {e:?}", provider_id);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query provider")
        })?
        .ok_or_else(|| {
            not_found_with_code(
                ErrorCode::NotFound,
                format!("Provider {} not found", &path.id),
            )
        })?;

    let scheduling = if query.extended {
        state
            .provider_service
            .get_scheduling_data(&provider_id)
            .await
            .ok()
            .flatten()
    } else {
        None
    };

    Ok(ok_response(ProviderResponse::from_data_with_scheduling(
        data,
        scheduling,
        query.extended,
    )))
}
