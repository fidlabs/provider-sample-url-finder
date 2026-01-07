use std::sync::Arc;

use crate::api_response::*;
use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use utoipa::{IntoParams, ToSchema};

use crate::{AppState, services::provider_service::ProviderData, types::ProviderAddress};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindRetriBySpPath {
    pub provider: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct FindRetriBySpResponse {
    pub result: ResultCode,
    pub retrievability_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl From<ProviderData> for FindRetriBySpResponse {
    fn from(data: ProviderData) -> Self {
        Self {
            result: data.result_code,
            retrievability_percent: data.retrievability_percent,
            message: None,
        }
    }
}

impl FindRetriBySpResponse {
    fn not_indexed() -> Self {
        Self {
            result: ResultCode::Error,
            retrievability_percent: 0.0,
            message: Some("Provider has not been indexed yet. Please try again later.".to_string()),
        }
    }
}

#[utoipa::path(
    get,
    path = "/url/retrievability/{provider}",
    params(FindRetriBySpPath),
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

    let provider_address = ProviderAddress::new(&path.provider)
        .map_err(|e| bad_request(format!("Invalid provider address: {e}")))?;

    let provider_id = provider_address.into();

    let result = state
        .provider_service
        .get_provider(&provider_id)
        .await
        .map_err(|e| {
            warn!("Failed to query provider: {:?}", e);
            internal_server_error("Failed to query provider")
        })?;

    Ok(ok_response(match result {
        Some(data) => data.into(),
        None => FindRetriBySpResponse::not_indexed(),
    }))
}
