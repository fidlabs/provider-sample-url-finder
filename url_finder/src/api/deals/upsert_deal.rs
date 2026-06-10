use std::sync::Arc;

use axum::{
    Json, debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;

use super::{DealPath, DealTargetResponse, DealTargetUpsertRequest, deal_sli_response};
use crate::{
    AppState,
    api_response::{ApiResponse, ErrorResponse},
    auth::OracleAuth,
};

#[utoipa::path(
    put,
    path = "/deals/{deal_id}",
    description = "Create or update a persisted Deal SLI target and its measurable pieces.",
    request_body = DealTargetUpsertRequest,
    params(DealPath),
    responses(
        (status = 200, description = "Stored deal target", body = DealTargetResponse),
        (status = 400, description = "Invalid path or request body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid oracle bearer token", body = ErrorResponse),
        (status = 404, description = "Deal target not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    security(("bearer_auth" = [])),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_upsert_deal(
    _auth: OracleAuth,
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
    WithRejection(Json(request), _): WithRejection<
        Json<DealTargetUpsertRequest>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<DealTargetResponse>, ApiResponse<()>> {
    deal_sli_response(
        state
            .deal_sli_service
            .upsert_target(&path.deal_id, request)
            .await,
    )
}
