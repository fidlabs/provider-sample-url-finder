use std::{sync::Arc, time::Duration};

use axum::{
    debug_handler,
    extract::{Query, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::{internal_server_error, ok_response, ApiResponse, ErrorResponse};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{deal_service, provider_endpoints, url_tester, AppState};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FindRetriByClientAndSpInput {
    pub client: String,
    pub provider: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindRetriByClientAndSpResponse {
    pub result: ResultCode,
    pub retrievability_percent: f64,
}

const RETRIEVABILITY_TIMEOUT_SEC: u64 = 2 * 60; // 2 min

/// Find retrivabiliy of urls for a given SP and Client address
#[utoipa::path(
    get,
    path = "/url/retrievability",
    params (FindRetriByClientAndSpInput),
    description = r#"
**Find retrievabiliy of urls for a given SP and Client address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindRetriByClientAndSpResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["Url"],
)]
#[debug_handler]
pub async fn handle_find_retri_by_client_and_sp(
    State(state): State<Arc<AppState>>,
    WithRejection(Query(query), _): WithRejection<
        Query<FindRetriByClientAndSpInput>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<FindRetriByClientAndSpResponse>, ApiResponse<()>> {
    debug!(
        "find retri for input address: {:?} and client: {:?}",
        &query.provider, &query.client
    );

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(&query.provider).await {
            Ok(endpoints) => endpoints,
            Err(e) => return Err(internal_server_error(e.to_string())),
        };

    if endpoints.is_none() {
        debug!("No endpoints found");

        return Ok(ok_response(FindRetriByClientAndSpResponse {
            result: result_code,
            retrievability_percent: 0.0,
        }));
    }
    let endpoints = endpoints.unwrap();

    let provider = query
        .provider
        .strip_prefix("f0")
        .unwrap_or(&query.provider)
        .to_string();

    let client = query
        .client
        .strip_prefix("f0")
        .unwrap_or(&query.client)
        .to_string();

    let piece_ids =
        deal_service::get_piece_ids_by_provider_and_client(&state.deal_repo, &provider, &client)
            .await
            .map_err(|e| {
                debug!("Failed to get piece ids: {:?}", e);
                internal_server_error("Failed to get piece ids")
            })?;

    if piece_ids.is_empty() {
        debug!("No deals found");
        return Ok(ok_response(FindRetriByClientAndSpResponse {
            result: ResultCode::NoDealsFound,
            retrievability_percent: 0.0,
        }));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    // Get retrievability percent
    // Make sure that the task is not running for too long
    let retrievability_percent = match timeout(
        Duration::from_secs(RETRIEVABILITY_TIMEOUT_SEC),
        url_tester::get_retrivability_with_head(urls),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            // In case of timeout
            return Ok(ok_response(FindRetriByClientAndSpResponse {
                result: ResultCode::TimedOut,
                retrievability_percent: 0.0,
            }));
        }
    };

    Ok(ok_response(FindRetriByClientAndSpResponse {
        result: ResultCode::Success,
        retrievability_percent,
    }))
}
