use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{AppState, ResultCode, deal_service, provider_endpoints, url_tester};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindUrlSpPath {
    pub provider: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindUrlSpResponse {
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
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

    // validate provider and client addresses
    let address_pattern = Regex::new(r"^f0\d{1,8}$").unwrap();
    if !address_pattern.is_match(&path.provider) {
        return Err(bad_request(
            "Invalid provider or client address".to_string(),
        ));
    }

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(&path.provider).await {
            Ok(endpoints) => endpoints,
            Err(e) => return Err(internal_server_error(e.to_string())),
        };

    if endpoints.is_none() {
        debug!("No endpoints found");

        return Ok(ok_response(FindUrlSpResponse {
            result: result_code,
            url: None,
        }));
    }
    let endpoints = endpoints.unwrap();

    let provider = path
        .provider
        .strip_prefix("f0")
        .unwrap_or(&path.provider)
        .to_string();

    let piece_ids = deal_service::get_piece_ids_by_provider(&state.deal_repo, &provider, None)
        .await
        .map_err(|e| {
            debug!("Failed to get piece ids: {:?}", e);
            internal_server_error("Failed to get piece ids")
        })?;
    if piece_ids.is_empty() {
        debug!("No deals found");
        return Ok(ok_response(FindUrlSpResponse {
            result: ResultCode::NoDealsFound,
            url: None,
        }));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;
    let working_url = url_tester::filter_working_with_get(urls).await;

    if working_url.is_none() {
        debug!("Failed to get working url");
        return Ok(ok_response(FindUrlSpResponse {
            result: ResultCode::FailedToGetWorkingUrl,
            url: None,
        }));
    }

    Ok(ok_response(FindUrlSpResponse {
        result: ResultCode::Success,
        url: working_url,
    }))
}
