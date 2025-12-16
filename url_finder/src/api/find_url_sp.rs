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
    AppState, ResultCode, services::provider_service::ProviderData, types::ProviderAddress,
};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindUrlSpPath {
    pub provider: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindUrlSpResponse {
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl From<ProviderData> for FindUrlSpResponse {
    fn from(data: ProviderData) -> Self {
        Self {
            result: data.result_code.clone(),
            url: data.working_url,
            message: data.result_code.message().map(String::from),
        }
    }
}

impl FindUrlSpResponse {
    fn not_indexed() -> Self {
        Self {
            result: ResultCode::Error,
            url: None,
            message: Some("Provider has not been indexed yet. Please try again later.".to_string()),
        }
    }
}

#[utoipa::path(
    get,
    path = "/url/find/{provider}",
    params(FindUrlSpPath),
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

    let provider_address = ProviderAddress::new(&path.provider)
        .map_err(|e| bad_request(format!("Invalid provider address: {e}")))?;

    let provider_id = provider_address.into();

    let result = state
        .provider_service
        .get_provider(&provider_id)
        .await
        .map_err(|e| {
            debug!("Failed to query provider: {:?}", e);
            internal_server_error("Failed to query provider")
        })?;

    Ok(ok_response(match result {
        Some(data) => data.into(),
        None => FindUrlSpResponse::not_indexed(),
    }))
}
