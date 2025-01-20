use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Query, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::{internal_server_error, ok_response, ApiResponse, ErrorResponse};
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{deal_service, provider_endpoints, url_tester, AppState, ResultCode};

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FindUrlInput {
    pub provider: String,
    pub client: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct FindUrlResponse {
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Find a working url for a given SP address
#[utoipa::path(
    get,
    path = "/url/find",
    params (FindUrlInput),
    description = r#"
**Find a working url for a given SP address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindUrlResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["Url"],
)]
#[debug_handler]
pub async fn handle_find_url(
    State(state): State<Arc<AppState>>,
    WithRejection(Query(query), _): WithRejection<Query<FindUrlInput>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<FindUrlResponse>, ApiResponse<()>> {
    debug!("find url input address: {:?}", &query.provider);

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(&query.provider).await {
            Ok(endpoints) => endpoints,
            Err(e) => return Err(internal_server_error(e.to_string())),
        };

    if endpoints.is_none() {
        debug!("No endpoints found");

        return Ok(ok_response(FindUrlResponse {
            result: result_code,
            url: None,
        }));
    }
    let endpoints = endpoints.unwrap();

    let provider = query
        .provider
        .strip_prefix("f0")
        .unwrap_or(&query.provider)
        .to_string();

    let client: Option<&str> = if let Some(client) = &query.client {
        Some(client.strip_prefix("f0").unwrap_or(client))
    } else {
        None
    };

    let piece_ids = deal_service::get_piece_ids_by_provider(&state.deal_repo, &provider, client)
        .await
        .map_err(|e| {
            debug!("Failed to get piece ids: {:?}", e);
            internal_server_error("Failed to get piece ids")
        })?;
    if piece_ids.is_empty() {
        debug!("No deals found");
        return Ok(ok_response(FindUrlResponse {
            result: ResultCode::NoDealsFound,
            url: None,
        }));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    let working_url = url_tester::filter_working_with_head(urls).await;
    if working_url.is_none() {
        debug!("Failed to get working url");
        return Ok(ok_response(FindUrlResponse {
            result: ResultCode::FailedToGetWorkingUrl,
            url: None,
        }));
    }

    Ok(ok_response(FindUrlResponse {
        result: ResultCode::Success,
        url: working_url,
    }))
}
