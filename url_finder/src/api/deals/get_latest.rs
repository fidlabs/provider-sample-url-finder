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
};

#[utoipa::path(
    get,
    path = "/deals/{deal_id}/latest",
    description = "Return the latest stored Deal SLI measurement state for a persisted target.",
    params(DealPath),
    responses(
        (status = 200, description = "Latest stored deal measurement", body = DealLatestMeasurementResponse),
        (status = 400, description = "Invalid path", body = ErrorResponse),
        (status = 404, description = "Deal target not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_get_latest(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<DealLatestMeasurementResponse>, ApiResponse<()>> {
    deal_sli_response(state.deal_sli_service.get_latest(&path.deal_id).await)
}
