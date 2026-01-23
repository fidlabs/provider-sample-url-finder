use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Query, State},
};
use serde::Deserialize;
use tracing::{debug, warn};
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, internal_server_error_with_code, ok_response,
    },
};

use crate::repository::ProviderFilters;

use super::types::{ProviderResponse, ProvidersListResponse};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct ListProvidersQuery {
    /// Maximum number of providers to return (1-500)
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Number of providers to skip
    #[serde(default)]
    pub offset: i64,
    /// Filter by URL availability: true=has URL, false=no URL, omit=all
    pub has_working_url: Option<bool>,
    /// Filter by URL consistency: true=consistent, false=inconsistent, omit=all
    pub is_consistent: Option<bool>,
    /// Include diagnostic and scheduling details
    #[serde(default)]
    pub extended: bool,
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

    let filters = ProviderFilters {
        has_working_url: query.has_working_url,
        is_consistent: query.is_consistent,
    };

    debug!(
        "GET /providers?limit={limit}&offset={offset}&has_working_url={:?}&is_consistent={:?}&extended={}",
        filters.has_working_url, filters.is_consistent, query.extended
    );

    let paginated = state
        .provider_service
        .list_providers(&filters, limit, offset)
        .await
        .map_err(|e| {
            warn!("Failed to list providers: {:?}", e);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query providers")
        })?;

    let providers: Vec<ProviderResponse> = paginated
        .providers
        .into_iter()
        .map(|p| ProviderResponse::from_data(p, query.extended))
        .collect();

    Ok(ok_response(ProvidersListResponse {
        providers,
        total: paginated.total,
        limit: paginated.limit,
        offset: paginated.offset,
    }))
}
