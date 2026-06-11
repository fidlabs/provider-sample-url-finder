use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;

use super::{DealLatestMeasurementResponse, DealPath, deal_sli_response};
use crate::{
    AppState,
    api_response::{ApiResponse, ErrorResponse},
    auth::OracleAuth,
};

#[utoipa::path(
    post,
    path = "/deals/{deal_id}/runs",
    description = "Run a synchronous Deal SLI measurement for a stored target using cached provider endpoints, persist the result, and return the latest deal state.",
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
    deal_sli_response(state.deal_sli_service.create_run(&path.deal_id).await)
}
