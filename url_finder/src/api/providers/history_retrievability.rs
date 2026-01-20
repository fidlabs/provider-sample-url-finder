use std::sync::Arc;

use axum::{
    debug_handler,
    extract::{Path, Query, State},
};
use axum_extra::extract::WithRejection;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code,
        internal_server_error_with_code, ok_response,
    },
    config::MAX_HISTORY_DAYS,
    repository::HistoryRow,
    types::{ClientAddress, ErrorCode as TypesErrorCode, ProviderAddress, ResultCode},
};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct HistoryProviderPath {
    pub id: String,
}

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct HistoryProviderClientPath {
    pub id: String,
    pub client_id: String,
}

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct HistoryQuery {
    /// Start date (YYYY-MM-DD). Defaults to 30 days ago.
    pub from: Option<NaiveDate>,
    /// End date (YYYY-MM-DD). Defaults to today.
    pub to: Option<NaiveDate>,
    /// Include extended test details. Defaults to false.
    #[serde(default)]
    pub extended: bool,
}

#[derive(Serialize, ToSchema)]
pub struct RetrievabilityHistoryResponse {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub data: Vec<RetrievabilityDataPoint>,
}

#[derive(Serialize, ToSchema)]
pub struct RetrievabilityDataPoint {
    pub date: NaiveDate,
    pub retrievability_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sector_utilization_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_consistent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_reliable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_code: Option<ResultCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<TypesErrorCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tested_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url_metadata: Option<serde_json::Value>,
}

impl RetrievabilityDataPoint {
    fn basic(
        date: NaiveDate,
        retrievability_percent: f64,
        sector_utilization_percent: Option<f64>,
    ) -> Self {
        Self {
            date,
            retrievability_percent,
            sector_utilization_percent,
            is_consistent: None,
            is_reliable: None,
            working_url: None,
            result_code: None,
            error_code: None,
            tested_at: None,
            url_metadata: None,
        }
    }
}

impl From<HistoryRow> for RetrievabilityDataPoint {
    fn from(row: HistoryRow) -> Self {
        Self {
            date: row.date,
            retrievability_percent: row.retrievability_percent,
            sector_utilization_percent: row.sector_utilization_percent,
            is_consistent: row.is_consistent,
            is_reliable: row.is_reliable,
            working_url: row.working_url,
            result_code: Some(row.result_code),
            error_code: row.error_code,
            tested_at: Some(row.tested_at),
            url_metadata: row.url_metadata,
        }
    }
}

fn validate_date_range(
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
) -> Result<(NaiveDate, NaiveDate), ApiResponse<()>> {
    let today = Utc::now().date_naive();
    let default_from = today - chrono::Duration::days(MAX_HISTORY_DAYS);

    let from_date = from.unwrap_or(default_from);
    let to_date = to.unwrap_or(today);

    if from_date > to_date {
        return Err(bad_request_with_code(
            ErrorCode::InvalidDateRange,
            "Parameter 'from' must be before or equal to 'to'",
        ));
    }

    let range_days = (to_date - from_date).num_days();
    if range_days > MAX_HISTORY_DAYS {
        return Err(bad_request_with_code(
            ErrorCode::DateRangeExceeded,
            format!("Date range exceeds maximum of {MAX_HISTORY_DAYS} days"),
        ));
    }

    Ok((from_date, to_date))
}

#[utoipa::path(
    get,
    path = "/providers/{id}/history/retrievability",
    params(HistoryProviderPath, HistoryQuery),
    responses(
        (status = 200, description = "Historical retrievability data", body = RetrievabilityHistoryResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_history_retrievability(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<HistoryProviderPath>,
        ApiResponse<ErrorResponse>,
    >,
    Query(query): Query<HistoryQuery>,
) -> Result<ApiResponse<RetrievabilityHistoryResponse>, ApiResponse<()>> {
    debug!(
        "GET /providers/{}/history/retrievability?from={:?}&to={:?}&extended={}",
        &path.id, query.from, query.to, query.extended
    );

    let provider_address = ProviderAddress::new(&path.id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid provider address: {e}"),
        )
    })?;
    let provider_id = provider_address.clone().into();

    let (from_date, to_date) = validate_date_range(query.from, query.to)?;

    let rows = state
        .url_repo
        .get_history_for_provider(&provider_id, from_date, to_date)
        .await
        .map_err(|e| {
            tracing::warn!("Failed to query history: {:?}", e);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query history")
        })?;

    let data: Vec<RetrievabilityDataPoint> = rows
        .into_iter()
        .map(|row| {
            if query.extended {
                row.into()
            } else {
                RetrievabilityDataPoint::basic(
                    row.date,
                    row.retrievability_percent,
                    row.sector_utilization_percent,
                )
            }
        })
        .collect();

    Ok(ok_response(RetrievabilityHistoryResponse {
        provider_id: provider_address.to_string(),
        client_id: None,
        from: from_date,
        to: to_date,
        data,
    }))
}

#[utoipa::path(
    get,
    path = "/providers/{id}/clients/{client_id}/history/retrievability",
    params(HistoryProviderClientPath, HistoryQuery),
    responses(
        (status = 200, description = "Historical retrievability data", body = RetrievabilityHistoryResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_history_retrievability_client(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<HistoryProviderClientPath>,
        ApiResponse<ErrorResponse>,
    >,
    Query(query): Query<HistoryQuery>,
) -> Result<ApiResponse<RetrievabilityHistoryResponse>, ApiResponse<()>> {
    debug!(
        "GET /providers/{}/clients/{}/history/retrievability?from={:?}&to={:?}&extended={}",
        &path.id, &path.client_id, query.from, query.to, query.extended
    );

    let provider_address = ProviderAddress::new(&path.id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid provider address: {e}"),
        )
    })?;
    let client_address = ClientAddress::new(&path.client_id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid client address: {e}"),
        )
    })?;

    let provider_id = provider_address.clone().into();
    let client_id = client_address.clone().into();

    let (from_date, to_date) = validate_date_range(query.from, query.to)?;

    let rows = state
        .url_repo
        .get_history_for_provider_client(&provider_id, &client_id, from_date, to_date)
        .await
        .map_err(|e| {
            tracing::warn!("Failed to query history: {:?}", e);
            internal_server_error_with_code(ErrorCode::InternalError, "Failed to query history")
        })?;

    let data: Vec<RetrievabilityDataPoint> = rows
        .into_iter()
        .map(|row| {
            if query.extended {
                row.into()
            } else {
                RetrievabilityDataPoint::basic(
                    row.date,
                    row.retrievability_percent,
                    row.sector_utilization_percent,
                )
            }
        })
        .collect();

    Ok(ok_response(RetrievabilityHistoryResponse {
        provider_id: provider_address.to_string(),
        client_id: Some(client_address.to_string()),
        from: from_date,
        to: to_date,
        data,
    }))
}
