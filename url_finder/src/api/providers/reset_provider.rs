use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, Query, State},
};
use axum_extra::extract::WithRejection;
use serde::Deserialize;
use tracing::{debug, error};
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code,
        internal_server_error_with_code, not_found_with_code, ok_response,
    },
    repository::StorageProvider,
    types::ProviderAddress,
};

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleType {
    UrlDiscovery,
    BmsTest,
    All,
}

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct ResetProviderPath {
    pub id: String,
}

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct ResetProviderQuery {
    pub schedule: ScheduleType,
}

#[utoipa::path(
    post,
    path = "/providers/{id}/reset",
    params(ResetProviderPath, ResetProviderQuery),
    responses(
        (status = 200, description = "Schedule reset successfully", body = StorageProvider),
        (status = 400, description = "Invalid provider address or schedule parameter", body = ErrorResponse),
        (status = 404, description = "Provider not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_reset_provider(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<ResetProviderPath>,
        ApiResponse<ErrorResponse>,
    >,
    WithRejection(Query(query), _): WithRejection<
        Query<ResetProviderQuery>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<StorageProvider>, ApiResponse<()>> {
    debug!(
        "POST /providers/{}/reset?schedule={:?}",
        &path.id, query.schedule
    );

    let provider_address = ProviderAddress::new(&path.id).map_err(|e| {
        error!("Invalid provider address '{}': {}", &path.id, e);
        bad_request_with_code(ErrorCode::InvalidAddress, "Invalid provider address")
    })?;

    let provider_id = provider_address.into();

    let result = match query.schedule {
        ScheduleType::UrlDiscovery => {
            state
                .storage_provider_repo
                .reset_url_discovery_schedule(&provider_id)
                .await
        }
        ScheduleType::BmsTest => {
            state
                .storage_provider_repo
                .reset_bms_test_schedule(&provider_id)
                .await
        }
        ScheduleType::All => {
            state
                .storage_provider_repo
                .reset_all_schedules(&provider_id)
                .await
        }
    };

    let provider = result
        .map_err(|e| {
            error!(
                "Failed to reset schedule for provider_id={}: {e:?}",
                provider_id
            );
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to reset schedule")
        })?
        .ok_or_else(|| {
            not_found_with_code(
                ErrorCode::NotFound,
                format!("Provider {} not found", &path.id),
            )
        })?;

    Ok(ok_response(provider))
}
