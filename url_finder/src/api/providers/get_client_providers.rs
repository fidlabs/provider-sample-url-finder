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
    types::ClientAddress,
};

use super::types::{ClientProvidersResponse, ProviderResponse};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct GetClientProvidersPath {
    pub id: String,
}

#[utoipa::path(
    get,
    path = "/clients/{id}/providers",
    params(GetClientProvidersPath, ExtendedQuery),
    responses(
        (status = 200, description = "Client providers found", body = ClientProvidersResponse),
        (status = 400, description = "Invalid client address", body = ErrorResponse),
        (status = 404, description = "Client not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Clients"],
)]
#[debug_handler]
pub async fn handle_get_client_providers(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<GetClientProvidersPath>,
        ApiResponse<ErrorResponse>,
    >,
    Query(query): Query<ExtendedQuery>,
) -> Result<ApiResponse<ClientProvidersResponse>, ApiResponse<()>> {
    debug!(
        "GET /clients/{}/providers?extended={}",
        &path.id, query.extended
    );

    let client_address = ClientAddress::new(&path.id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid client address: {e}"),
        )
    })?;

    let client_id = client_address.clone().into();

    let providers_data = state
        .provider_service
        .get_providers_for_client(&client_id)
        .await
        .map_err(|e| {
            error!(
                "Failed to query client providers for {}: {:?}",
                client_id, e
            );
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query providers")
        })?;

    if providers_data.is_empty() {
        return Err(not_found_with_code(
            ErrorCode::NotFound,
            format!("Client {} has no providers", &path.id),
        ));
    }

    let providers: Vec<ProviderResponse> = providers_data
        .into_iter()
        .map(|p| ProviderResponse::from_data(p, query.extended))
        .collect();
    let total = providers.len() as i64;

    Ok(ok_response(ClientProvidersResponse {
        client_id: client_address.to_string(),
        providers,
        total,
    }))
}
