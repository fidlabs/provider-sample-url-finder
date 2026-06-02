use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;

use super::{DealLatestMeasurementResponse, DealPath};
use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code,
        internal_server_error_with_code, not_found_with_code, ok_response,
    },
    auth::OracleAuth,
    services::deal_sli_service::DealSliServiceError,
};

#[utoipa::path(
    post,
    path = "/deals/{deal_id}/runs",
    description = "Persist a manual synchronous Deal SLI run result and return the stored latest run data.",
    params(DealPath),
    responses(
        (status = 200, description = "Stored latest deal run", body = DealLatestMeasurementResponse),
        (status = 400, description = "Invalid path", body = ErrorResponse),
        (status = 401, description = "Missing or invalid oracle bearer token", body = ErrorResponse),
        (status = 404, description = "Deal target not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    security(("bearer_auth" = [])),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_create_run(
    _auth: OracleAuth,
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<DealLatestMeasurementResponse>, ApiResponse<()>> {
    let response = state.deal_sli_service.create_run(&path.deal_id).await;
    match response {
        Ok(data) => Ok(ok_response(data)),
        Err(DealSliServiceError::InvalidRequest(message)) => {
            Err(bad_request_with_code(ErrorCode::InvalidRequest, message))
        }
        Err(DealSliServiceError::NotFound(message)) => {
            Err(not_found_with_code(ErrorCode::NotFound, message))
        }
        Err(DealSliServiceError::Internal(error)) => Err(internal_server_error_with_code(
            ErrorCode::InternalError,
            error.to_string(),
        )),
    }
}
