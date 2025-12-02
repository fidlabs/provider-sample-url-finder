use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState, ResultCode, provider_endpoints,
    services::deal_service,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
    url_tester,
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
        .map_err(|e| bad_request(format!("Invalid provider address: {}", e)))?;
    let client_address = ClientAddress::new(path.client)
        .map_err(|e| bad_request(format!("Invalid client address: {}", e)))?;

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(&provider_address).await {
            Ok(endpoints) => endpoints,
            Err(e) => return Err(internal_server_error(e.to_string())),
        };

    if endpoints.is_none() {
        debug!("No endpoints found");

        return Ok(ok_response(FindUrlSpClientResponse {
            result: result_code,
            url: None,
        }));
    }
    let endpoints = endpoints.unwrap();

    let provider_id: ProviderId = provider_address.into();
    let client_id: ClientId = client_address.into();

    let piece_ids =
        deal_service::get_piece_ids_by_provider(&state.deal_repo, &provider_id, Some(&client_id))
            .await
            .map_err(|e| {
                debug!("Failed to get piece ids: {:?}", e);
                internal_server_error("Failed to get piece ids")
            })?;
    if piece_ids.is_empty() {
        debug!("No deals found");
        return Ok(ok_response(FindUrlSpClientResponse {
            result: ResultCode::NoDealsFound,
            url: None,
        }));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    let (working_url, _) = url_tester::check_retrievability_with_get(urls, false).await;
    if working_url.is_none() {
        debug!("Failed to get working url");
        return Ok(ok_response(FindUrlSpClientResponse {
            result: ResultCode::FailedToGetWorkingUrl,
            url: None,
        }));
    }

    Ok(ok_response(FindUrlSpClientResponse {
        result: ResultCode::Success,
        url: working_url,
    }))
}
