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
    services::provider_service::ProviderData,
    types::{ClientAddress, ProviderAddress},
};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindRetriByClientAndSpPath {
    pub client: String,
    pub provider: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindRetriByClientAndSpResponse {
    pub result: ResultCode,
    pub retrievability_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub large_files_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl From<ProviderData> for FindRetriByClientAndSpResponse {
    fn from(data: ProviderData) -> Self {
        Self {
            result: data.result_code,
            retrievability_percent: data.retrievability_percent,
            large_files_percent: data.large_files_percent,
            message: None,
        }
    }
}

impl FindRetriByClientAndSpResponse {
    fn not_indexed() -> Self {
        Self {
            result: ResultCode::Error,
            retrievability_percent: None,
            large_files_percent: None,
            message: Some(
                "Provider/client pair has not been indexed yet. Please try again later."
                    .to_string(),
            ),
        }
    }
}

#[utoipa::path(
    get,
    path = "/url/retrievability/{provider}/{client}",
    params(FindRetriByClientAndSpPath),
    description = r#"
**Find retrievability of urls for a given SP and Client address**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindRetriByClientAndSpResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_retri_by_client_and_sp(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<FindRetriByClientAndSpPath>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<FindRetriByClientAndSpResponse>, ApiResponse<()>> {
    debug!(
        "find retri for input address: {:?} and client: {:?}",
        &path.provider, &path.client
    );

    let provider_address = ProviderAddress::new(&path.provider)
        .map_err(|e| bad_request(format!("Invalid provider address: {e}")))?;
    let client_address = ClientAddress::new(&path.client)
        .map_err(|e| bad_request(format!("Invalid client address: {e}")))?;

    let provider_id = provider_address.into();
    let client_id = client_address.into();

    let result = state
        .provider_service
        .get_provider_client(&provider_id, &client_id)
        .await
        .map_err(|e| {
            debug!("Failed to query provider+client: {:?}", e);
            internal_server_error("Failed to query provider data")
        })?;

    Ok(ok_response(match result {
        Some(data) => data.into(),
        None => FindRetriByClientAndSpResponse::not_indexed(),
    }))
}
