use std::sync::Arc;

use axum::{Json, debug_handler, extract::State};
use tracing::debug;

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, bad_request_with_code, internal_server_error_with_code, ok_response,
    },
    types::{ProviderAddress, ProviderId},
};

const MAX_PROVIDER_IDS: usize = 100;

use super::types::{BulkProvidersRequest, BulkProvidersResponse, ProviderResponse};

#[utoipa::path(
    post,
    path = "/providers/bulk",
    request_body = BulkProvidersRequest,
    responses(
        (status = 200, description = "Bulk providers result", body = BulkProvidersResponse),
        (status = 500, description = "Internal error", body = crate::api_response::ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_bulk_providers(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BulkProvidersRequest>,
) -> Result<ApiResponse<BulkProvidersResponse>, ApiResponse<()>> {
    debug!(
        "POST /providers/bulk with {} ids",
        request.provider_ids.len()
    );

    if request.provider_ids.len() > MAX_PROVIDER_IDS {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            format!(
                "Too many provider IDs: {} exceeds maximum of {MAX_PROVIDER_IDS}",
                request.provider_ids.len()
            ),
        ));
    }

    let mut valid_ids: Vec<ProviderId> = Vec::new();
    let mut invalid_ids: Vec<String> = Vec::new();

    for id in &request.provider_ids {
        match ProviderAddress::new(id) {
            Ok(addr) => valid_ids.push(addr.into()),
            Err(_) => invalid_ids.push(id.clone()),
        }
    }

    let result = state
        .provider_service
        .bulk_get_providers(&valid_ids)
        .await
        .map_err(|e| {
            debug!("Failed to bulk query providers: {:?}", e);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query providers")
        })?;

    let providers: Vec<ProviderResponse> = result.providers.into_iter().map(|p| p.into()).collect();

    let mut not_found: Vec<String> = result
        .not_found
        .into_iter()
        .map(|id| ProviderAddress::from(id).to_string())
        .collect();

    not_found.extend(invalid_ids);

    Ok(ok_response(BulkProvidersResponse {
        providers,
        not_found,
    }))
}
