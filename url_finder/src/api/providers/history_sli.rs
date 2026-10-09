use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

use axum::{
    debug_handler,
    extract::{Path, Query, State},
};
use axum_extra::extract::WithRejection;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    AppState,
    api::{decode_history_cursor, encode_history_cursor, history_limit, history_window},
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code,
        internal_server_error_with_code, ok_response,
    },
    repository::{BmsHistoryEvent, HistoryCursorBoundary, UrlHistoryEvent},
    types::{ClientAddress, ProviderAddress, ProviderId},
};

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct SliHistoryQuery {
    /// First UTC date to include. Defaults to 30 days before `to`.
    #[param(example = "2026-09-01")]
    pub from: Option<NaiveDate>,
    /// Last UTC date to include. Defaults to today.
    #[param(example = "2026-09-08")]
    pub to: Option<NaiveDate>,
    /// Maximum combined URL and BMS events to return.
    #[param(default = 100, minimum = 1, maximum = 500, example = 100)]
    pub limit: Option<i64>,
    /// Opaque cursor returned by the previous page. Keep the same resource and date filters.
    #[param(
        max_length = 2048,
        example = "7b2270726f76696465725f6964223a22663031323334222c2266726f6d223a22323032362d30392d3031222c22746f223a22323032362d30392d3038222c22636c69656e745f6964223a6e756c6c2c226174223a22323032362d30392d30385431323a30303a30305a222c22736f75726365223a2275726c222c226964223a2230303030303030302d303030302d343030302d383030302d303030303030303030303031227d"
    )]
    pub cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct SliHistoryResponse {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub data: BTreeMap<NaiveDate, Vec<SliHistoryEventResponse>>,
    pub next_cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum SliHistoryEventResponse {
    Url {
        id: Uuid,
        provider_id: String,
        client_id: Option<String>,
        result_type: crate::types::DiscoveryType,
        working_url: Option<String>,
        retrievability_percent: Option<f64>,
        result_code: crate::types::ResultCode,
        error_code: Option<crate::types::ErrorCode>,
        tested_at: DateTime<Utc>,
        is_consistent: Option<bool>,
        is_reliable: Option<bool>,
        url_metadata: Option<serde_json::Value>,
        sector_utilization_percent: Option<f64>,
        car_files_percent: Option<f64>,
        large_files_percent: Option<f64>,
    },
    /// BMS is always provider-scoped, including on a client-filtered request.
    ProviderBms {
        id: Uuid,
        provider_id: String,
        bms_job_id: Uuid,
        url_tested: String,
        routing_key: String,
        worker_count: i32,
        status: String,
        ping_avg_ms: Option<f64>,
        head_avg_ms: Option<f64>,
        ttfb_ms: Option<f64>,
        download_speed_mbps: Option<f64>,
        created_at: DateTime<Utc>,
        completed_at: Option<DateTime<Utc>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cursor {
    provider_id: String,
    from: NaiveDate,
    to: NaiveDate,
    client_id: Option<String>,
    at: DateTime<Utc>,
    source: Source,
    id: Uuid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Source {
    Url,
    ProviderBms,
}

enum Event {
    Url(UrlHistoryEvent),
    Bms(BmsHistoryEvent),
}

impl Event {
    fn ordering(&self, other: &Self) -> Ordering {
        other
            .at()
            .cmp(&self.at())
            .then_with(|| self.source().cmp(&other.source()))
            .then_with(|| other.id().cmp(&self.id()))
    }
    fn at(&self) -> DateTime<Utc> {
        match self {
            Self::Url(v) => v.tested_at,
            Self::Bms(v) => v.created_at,
        }
    }
    fn id(&self) -> Uuid {
        match self {
            Self::Url(v) => v.id,
            Self::Bms(v) => v.id,
        }
    }
    fn source(&self) -> Source {
        match self {
            Self::Url(_) => Source::Url,
            Self::Bms(_) => Source::ProviderBms,
        }
    }
    fn response(self) -> SliHistoryEventResponse {
        match self {
            Self::Url(v) => SliHistoryEventResponse::Url {
                id: v.id,
                provider_id: ProviderAddress::from(v.provider_id).to_string(),
                client_id: v
                    .client_id
                    .map(ClientAddress::from)
                    .map(|value| value.to_string()),
                result_type: v.result_type,
                working_url: v.working_url,
                retrievability_percent: v.retrievability_percent,
                result_code: v.result_code,
                error_code: v.error_code,
                tested_at: v.tested_at,
                is_consistent: v.is_consistent,
                is_reliable: v.is_reliable,
                url_metadata: v.url_metadata,
                sector_utilization_percent: v.sector_utilization_percent,
                car_files_percent: v.car_files_percent,
                large_files_percent: v.large_files_percent,
            },
            Self::Bms(v) => SliHistoryEventResponse::ProviderBms {
                id: v.id,
                provider_id: ProviderAddress::from(v.provider_id).to_string(),
                bms_job_id: v.bms_job_id,
                url_tested: v.url_tested,
                routing_key: v.routing_key,
                worker_count: v.worker_count,
                status: v.status,
                ping_avg_ms: decimal_to_f64(v.ping_avg_ms),
                head_avg_ms: decimal_to_f64(v.head_avg_ms),
                ttfb_ms: decimal_to_f64(v.ttfb_ms),
                download_speed_mbps: decimal_to_f64(v.download_speed_mbps),
                created_at: v.created_at,
                completed_at: v.completed_at,
            },
        }
    }
}

fn decimal_to_f64(value: Option<sqlx::types::BigDecimal>) -> Option<f64> {
    value.and_then(|v| v.to_string().parse().ok())
}

fn decode_cursor(
    value: Option<&str>,
    provider_id: &ProviderAddress,
    from: NaiveDate,
    to: NaiveDate,
    client_id: Option<&ClientAddress>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Option<Cursor>, ApiResponse<()>> {
    let cursor: Option<Cursor> = decode_history_cursor(value)?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.provider_id != provider_id.as_str()
        || cursor.from != from
        || cursor.to != to
        || cursor.client_id.as_deref() != client_id.map(ClientAddress::as_str)
    {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            "History cursor does not match request filters",
        ));
    }
    if cursor.at < start || cursor.at >= end {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            "History cursor timestamp is outside the requested date range",
        ));
    }
    Ok(Some(cursor))
}

fn url_boundary(cursor: Option<&Cursor>) -> Option<HistoryCursorBoundary> {
    cursor.map(|cursor| match cursor.source {
        Source::Url => HistoryCursorBoundary::within_source(cursor.at, cursor.id),
        Source::ProviderBms => HistoryCursorBoundary::before_source(cursor.at),
    })
}

fn bms_boundary(cursor: Option<&Cursor>) -> Option<HistoryCursorBoundary> {
    cursor.map(|cursor| match cursor.source {
        Source::Url => HistoryCursorBoundary::after_source(cursor.at),
        Source::ProviderBms => HistoryCursorBoundary::within_source(cursor.at, cursor.id),
    })
}

async fn history(
    state: Arc<AppState>,
    provider: ProviderAddress,
    client: Option<ClientAddress>,
    query: SliHistoryQuery,
) -> Result<ApiResponse<SliHistoryResponse>, ApiResponse<()>> {
    let window = history_window(query.from, query.to)?;
    let limit = history_limit(query.limit)?;
    let provider_id: ProviderId = provider.clone().into();
    let client_id = client.clone().map(Into::into);
    let cursor = decode_cursor(
        query.cursor.as_deref(),
        &provider,
        window.from,
        window.to,
        client.as_ref(),
        window.start,
        window.end,
    )?;
    let fetch_limit = limit + 1;
    let (urls, bms) = tokio::try_join!(
        state.url_repo.get_sli_history_events(
            &provider_id,
            client_id.as_ref(),
            window.start,
            window.end,
            url_boundary(cursor.as_ref()),
            fetch_limit
        ),
        state.bms_repo.get_sli_history_events(
            &provider_id,
            window.start,
            window.end,
            bms_boundary(cursor.as_ref()),
            fetch_limit
        ),
    )
    .map_err(|_| {
        internal_server_error_with_code(ErrorCode::InternalError, "Failed to query SLI history")
    })?;
    let mut events: Vec<Event> = urls
        .into_iter()
        .map(Event::Url)
        .chain(bms.into_iter().map(Event::Bms))
        .collect();
    events.sort_by(Event::ordering);
    let next = if events.len() > limit as usize {
        let event = &events[limit as usize - 1];
        Some(encode_history_cursor(&Cursor {
            provider_id: provider.to_string(),
            from: window.from,
            to: window.to,
            client_id: client.as_ref().map(ToString::to_string),
            at: event.at(),
            source: event.source(),
            id: event.id(),
        })?)
    } else {
        None
    };
    events.truncate(limit as usize);
    let mut data = BTreeMap::new();
    for event in events {
        data.entry(event.at().date_naive())
            .or_insert_with(Vec::new)
            .push(event.response());
    }
    Ok(ok_response(SliHistoryResponse {
        provider_id: provider.to_string(),
        client_id: client.map(|v| v.to_string()),
        from: window.from,
        to: window.to,
        data,
        next_cursor: next,
    }))
}

#[utoipa::path(
    get,
    path = "/providers/{id}/history/sli",
    params(super::HistoryProviderPath, SliHistoryQuery),
    responses(
        (status = 200, description = "Stored provider URL and BMS history", body = SliHistoryResponse),
        (status = 400, description = "Invalid path, query, date range, limit, or cursor", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_history_sli(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<super::HistoryProviderPath>,
        ApiResponse<ErrorResponse>,
    >,
    WithRejection(Query(query), _): WithRejection<
        Query<SliHistoryQuery>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<SliHistoryResponse>, ApiResponse<()>> {
    let provider = ProviderAddress::new(path.id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid provider address: {e}"),
        )
    })?;
    history(state, provider, None, query).await
}

#[utoipa::path(
    get,
    path = "/providers/{id}/clients/{client_id}/history/sli",
    params(super::HistoryProviderClientPath, SliHistoryQuery),
    responses(
        (status = 200, description = "Stored client URL history and provider-scoped BMS history", body = SliHistoryResponse),
        (status = 400, description = "Invalid path, query, date range, limit, or cursor", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Providers"],
)]
#[debug_handler]
pub async fn handle_history_sli_client(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<
        Path<super::HistoryProviderClientPath>,
        ApiResponse<ErrorResponse>,
    >,
    WithRejection(Query(query), _): WithRejection<
        Query<SliHistoryQuery>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<SliHistoryResponse>, ApiResponse<()>> {
    let provider = ProviderAddress::new(path.id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid provider address: {e}"),
        )
    })?;
    let client = ClientAddress::new(path.client_id).map_err(|e| {
        bad_request_with_code(
            ErrorCode::InvalidAddress,
            format!("Invalid client address: {e}"),
        )
    })?;
    history(state, provider, Some(client), query).await
}
