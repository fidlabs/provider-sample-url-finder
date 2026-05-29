use axum::{debug_handler, extract::Path};
use axum_extra::extract::WithRejection;

use super::{DealPath, DealTargetResponse};
use crate::api_response::{ApiResponse, ErrorResponse, ok_response};

#[utoipa::path(
    get,
    path = "/deals/{deal_id}",
    description = "Contract shell for oracle-service integration. This route returns the documented response shape, but deal target persistence and measurement execution are not implemented in this slice.",
    params(DealPath),
    responses(
        (status = 200, description = "Deal target placeholder", body = DealTargetResponse),
        (status = 400, description = "Invalid path", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler]
pub async fn handle_get_deal(
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<DealTargetResponse>, ApiResponse<()>> {
    Ok(ok_response(DealTargetResponse::placeholder(path.deal_id)))
}
