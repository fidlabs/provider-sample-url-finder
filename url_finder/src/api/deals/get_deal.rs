use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;

use super::{DealPath, DealTargetResponse, deal_sli_response};
use crate::{
    AppState,
    api_response::{ApiResponse, ErrorResponse},
};

#[utoipa::path(
    get,
    path = "/deals/{deal_id}",
    description = "Return a persisted Deal SLI target and its measurable pieces.",
    params(DealPath),
    responses(
        (status = 200, description = "Stored deal target", body = DealTargetResponse),
        (status = 400, description = "Invalid path", body = ErrorResponse),
        (status = 404, description = "Deal target not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_get_deal(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<DealTargetResponse>, ApiResponse<()>> {
    deal_sli_response(state.deal_sli_service.get_target(&path.deal_id).await)
}
