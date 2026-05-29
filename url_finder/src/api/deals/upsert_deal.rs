use std::sync::Arc;

use axum::{Json, debug_handler, extract::Path};
use axum_extra::extract::WithRejection;

use super::{DealPath, DealTargetResponse, DealTargetUpsertRequest};
use crate::{
    AppState,
    api_response::{ApiResponse, ErrorResponse, ok_response},
    auth::OracleAuth,
};

#[utoipa::path(
    put,
    path = "/deals/{deal_id}",
    description = "Contract shell for oracle-service integration. This route returns the documented response shape, but deal target persistence and measurement execution are not implemented in this slice.",
    request_body = DealTargetUpsertRequest,
    params(DealPath),
    responses(
        (status = 200, description = "Deal target shell response", body = DealTargetResponse),
        (status = 400, description = "Invalid path or request body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid oracle bearer token", body = ErrorResponse),
    ),
    security(("bearer_auth" = [])),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_upsert_deal(
    _auth: OracleAuth,
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
    WithRejection(Json(request), _): WithRejection<
        Json<DealTargetUpsertRequest>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<DealTargetResponse>, ApiResponse<()>> {
    Ok(ok_response(DealTargetResponse::from_upsert_request(
        path.deal_id,
        request,
    )))
}
