use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Query, State},
};
use serde::Deserialize;
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, internal_server_error_with_code, ok_response,
    },
};

use super::types::{ProviderResponse, ProvidersListResponse};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct ListProvidersQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    100
}

#[utoipa::path(
    get,
    path = "/providers",
    params(ListProvidersQuery),
    responses(
        (status = 200, description = "Providers list", body = ProvidersListResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_list_providers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListProvidersQuery>,
) -> Result<ApiResponse<ProvidersListResponse>, ApiResponse<()>> {
    let limit = query.limit.clamp(1, 500);
    let offset = query.offset.max(0);

    debug!("GET /providers?limit={limit}&offset={offset}");

    let paginated = state
        .provider_service
        .list_providers(limit, offset)
        .await
        .map_err(|e| {
            debug!("Failed to list providers: {:?}", e);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query providers")
        })?;

    let providers: Vec<ProviderResponse> =
        paginated.providers.into_iter().map(|p| p.into()).collect();

    Ok(ok_response(ProvidersListResponse {
        providers,
        total: paginated.total,
        limit: paginated.limit,
        offset: paginated.offset,
    }))
}
