use axum::debug_handler;
use serde::Serialize;
use utoipa::ToSchema;

use common::api_response::*;

#[derive(Serialize, ToSchema)]
pub struct HealthcheckResponse {
    #[schema(example = "ok")]
    pub status: String,
}

/// Return simple healthcheck response
#[utoipa::path(
    get,
    path = "/healthcheck",
    description = r#"
**Return simple healthcheck response.**
"#,
    responses(
        (status = 200, description = "Successful Healthcheck", body = HealthcheckResponse),
    ),
    tags = ["Healthcheck"],
)]
#[debug_handler]
pub async fn handle_healthcheck() -> Result<ApiResponse<HealthcheckResponse>, ApiResponse<()>> {
    Ok(ok_response(HealthcheckResponse {
        status: "ok".to_string(),
    }))
}
