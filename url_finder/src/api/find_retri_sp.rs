use std::{sync::Arc, time::Duration};

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{provider_endpoints, services::deal_service, url_tester, AppState};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindRetriBySpPath {
    pub provider: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct FindRetriBySpResponse {
    pub result: ResultCode,
    pub retrievability_percent: f64,
}

const RETRIEVABILITY_TIMEOUT_SEC: u64 = 2 * 60; // 2 min

/// Find retrivabiliy of urls for a given SP address
#[utoipa::path(
    get,
    path = "/url/retrievability/{provider}",
    params (FindRetriBySpPath),
    description = r#"
**Find retrievabiliy of urls for a given SP address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindRetriBySpResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_retri_by_sp(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<FindRetriBySpPath>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<FindRetriBySpResponse>, ApiResponse<()>> {
    debug!("find retri for input address: {:?}", &path.provider);

    // validate provider addresses
    let address_pattern = Regex::new(r"^f0\d{1,8}$").unwrap();
    if !address_pattern.is_match(&path.provider) {
        return Err(bad_request("Invalid provider address".to_string()));
    }

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(&path.provider).await {
            Ok(endpoints) => endpoints,
            Err(e) => return Err(internal_server_error(e.to_string())),
        };

    if endpoints.is_none() {
        debug!("No endpoints found");

        return Ok(ok_response(FindRetriBySpResponse {
            result: result_code,
            retrievability_percent: 0.0,
        }));
    }
    let endpoints = endpoints.unwrap();

    let provider = path
        .provider
        .strip_prefix("f0")
        .unwrap_or(&path.provider)
        .to_string();

    let piece_ids = deal_service::get_random_piece_ids_by_provider(&state.deal_repo, &provider)
        .await
        .map_err(|e| {
            debug!("Failed to get piece ids: {:?}", e);
            internal_server_error("Failed to get piece ids")
        })?;

    if piece_ids.is_empty() {
        debug!("No deals found");
        return Ok(ok_response(FindRetriBySpResponse {
            result: ResultCode::NoDealsFound,
            retrievability_percent: 0.0,
        }));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    // Get retrievability percent
    // Make sure that the task is not running for too long
    let (_, retrievability_percent) = match timeout(
        Duration::from_secs(RETRIEVABILITY_TIMEOUT_SEC),
        url_tester::check_retrievability_with_get(urls, true),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            // In case of timeout
            return Ok(ok_response(FindRetriBySpResponse {
                result: ResultCode::TimedOut,
                retrievability_percent: 0.0,
            }));
        }
    };

    Ok(ok_response(FindRetriBySpResponse {
        result: ResultCode::Success,
        retrievability_percent: retrievability_percent.unwrap(),
    }))
}
