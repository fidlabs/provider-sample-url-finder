use axum::{debug_handler, extract::Path};
use axum_extra::extract::WithRejection;

use super::{DealLatestMeasurementResponse, DealPath};
use crate::api_response::{ApiResponse, ErrorResponse, ok_response};

#[utoipa::path(
    get,
    path = "/deals/{deal_id}/latest",
    description = "Contract shell for oracle-service integration. This route returns the documented response shape, but deal target persistence and measurement execution are not implemented in this slice.",
    params(DealPath),
    responses(
        (status = 200, description = "Latest deal measurement shell response", body = DealLatestMeasurementResponse),
        (status = 400, description = "Invalid path", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler]
pub async fn handle_get_latest(
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<DealLatestMeasurementResponse>, ApiResponse<()>> {
    Ok(ok_response(DealLatestMeasurementResponse::missing(
        path.deal_id,
    )))
}
