use axum::{Json, debug_handler, extract::Path};
use axum_extra::extract::WithRejection;

use super::{DealPath, DealTargetResponse, DealTargetUpsertRequest};
use crate::api_response::{ApiResponse, ErrorResponse, ok_response};

#[utoipa::path(
    put,
    path = "/deals/{deal_id}",
    description = "Contract shell for oracle-service integration. This route returns the documented response shape, but deal target persistence and measurement execution are not implemented in this slice.",
    request_body = DealTargetUpsertRequest,
    params(DealPath),
    responses(
        (status = 200, description = "Deal target shell response", body = DealTargetResponse),
        (status = 400, description = "Invalid path or request body", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler]
pub async fn handle_upsert_deal(
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
