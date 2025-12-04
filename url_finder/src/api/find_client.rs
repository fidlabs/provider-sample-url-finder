use std::sync::Arc;

use crate::api_response::*;
use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    types::{ClientAddress, ClientId, ProviderAddress},
};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindByClientPath {
    pub client: String,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderResult {
    pub provider: ProviderAddress,
    pub result: ResultCode,
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
}

#[derive(Serialize, ToSchema)]
pub struct FindByClientResponse {
    pub client: String,
    pub result: ResultCode,
    pub providers: Vec<ProviderResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Find retrivabiliy of urls for a given SP and Client address
#[utoipa::path(
    get,
    path = "/url/client/{client}",
    params (FindByClientPath),
    description = r#"
**Find client SPs with working url and retrievabiliy of urls for for each found SP**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindByClientResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_client(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<FindByClientPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<FindByClientResponse>, ApiResponse<()>> {
    debug!(
        "find client working url and retri for input client address: {:?}",
        &path.client
    );

    // Parse and validate client address
    let client_address = ClientAddress::new(path.client.clone())
        .map_err(|e| bad_request(format!("Invalid client address: {e}")))?;

    let client_id: ClientId = client_address.into();

    let url_results = state
        .url_repo
        .get_latest_for_client_all_providers(&client_id)
        .await
        .map_err(|e| {
            debug!("Failed to query url_results: {:?}", e);
            internal_server_error("Failed to query url results")
        })?;

    if url_results.is_empty() {
        return Ok(ok_response(FindByClientResponse {
            result: ResultCode::Error,
            client: path.client.clone(),
            providers: Vec::new(),
            message: Some(
                "No providers found for this client or client has not been indexed yet."
                    .to_string(),
            ),
        }));
    }

    let providers: Vec<ProviderResult> = url_results
        .into_iter()
        .map(|r| ProviderResult {
            provider: r.provider_id.into(),
            result: r.result_code,
            working_url: r.working_url,
            retrievability_percent: r.retrievability_percent,
        })
        .collect();

    Ok(ok_response(FindByClientResponse {
        result: ResultCode::Success,
        client: path.client.clone(),
        providers,
        message: None,
    }))
}
