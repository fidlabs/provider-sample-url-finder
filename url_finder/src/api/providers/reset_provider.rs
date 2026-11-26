use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

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
        internal_server_error_with_code, not_found_with_code, ok_response, too_many_requests,
    },
    repository::StorageProvider,
    types::{ProviderAddress, ProviderId},
};

/// The endpoint is unauthenticated and each reset queues real work (URL discovery, a BMS
/// bandwidth test), so a given provider's schedule can only be reset once per cooldown.
pub const RESET_COOLDOWN: Duration = Duration::from_secs(15 * 60);

// ponytail: in-memory, so it is per instance and cleared on restart, and entries are never
// evicted (bounded by known providers x 2). Move to a DB column if the service runs
// more than one instance.
pub type ResetCooldowns = Mutex<HashMap<(String, &'static str), Instant>>;

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
        (status = 429, description = "This schedule was reset recently", body = ErrorResponse),
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

    let provider_id: ProviderId = provider_address.into();

    let schedules: &[&'static str] = match query.schedule {
        ScheduleType::UrlDiscovery => &["url_discovery"],
        ScheduleType::BmsTest => &["bms_test"],
        ScheduleType::All => &["url_discovery", "bms_test"],
    };
    let cooldown_keys: Vec<(String, &'static str)> = schedules
        .iter()
        .map(|schedule| (provider_id.as_str().to_string(), *schedule))
        .collect();

    let in_cooldown = {
        let cooldowns = state.reset_cooldowns.lock().unwrap();
        cooldown_keys.iter().any(|key| {
            cooldowns
                .get(key)
                .is_some_and(|last_reset| last_reset.elapsed() < RESET_COOLDOWN)
        })
    };
    if in_cooldown {
        return Err(too_many_requests(format!(
            "Provider {} was reset recently, try again later",
            path.id
        )));
    }

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
                format!("Provider {} not found", path.id),
            )
        })?;

    // Recorded only after a successful reset, so unknown provider IDs cannot grow the map.
    let now = Instant::now();
    let mut cooldowns = state.reset_cooldowns.lock().unwrap();
    for key in cooldown_keys {
        cooldowns.insert(key, now);
    }
    drop(cooldowns);

    Ok(ok_response(provider))
}
