use std::{collections::BTreeMap, collections::HashMap, sync::Arc};

use alloy::primitives::keccak256;
use axum::{
    debug_handler,
    extract::{Path, Query, State},
};
use axum_extra::extract::WithRejection;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use super::{DealPath, DealPorepSliResponse, MeasurementState};
use crate::{
    AppState,
    api::{decode_history_cursor, encode_history_cursor, history_limit, history_window},
    api_response::{
        ApiResponse, ErrorCode, ErrorResponse, bad_request_with_code,
        internal_server_error_with_code, not_found_with_code, ok_response,
    },
    repository::{
        DealSliBmsAggregate, DealSliBmsJob, DealSliHistoryPieceResult, DealSliHistoryRun,
    },
    services::deal_sli_service::{bigdecimal_to_f64, map_porep_sli_aggregates},
    types::{ErrorCode as UrlErrorCode, ResultCode},
};

const BMS_PREVIEW_LIMIT: usize = 10;

#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams)]
pub struct DealRunPath {
    /// Decimal Filecoin deal ID.
    #[schema(example = "1234567890")]
    #[param(value_type = String)]
    pub deal_id: String,
    /// Stored Deal SLI run UUID.
    #[schema(example = "00000000-0000-4000-8000-000000000001")]
    pub run_id: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams)]
pub struct DealSliHistoryQuery {
    /// First UTC date to include. Defaults to 30 days before `to`.
    #[param(example = "2026-09-01")]
    pub from: Option<NaiveDate>,
    /// Last UTC date to include. Defaults to today.
    #[param(example = "2026-09-08")]
    pub to: Option<NaiveDate>,
    /// Maximum completed runs to return.
    #[param(default = 100, minimum = 1, maximum = 500, example = 100)]
    pub limit: Option<i64>,
    /// Omit on the first page. Then copy `next_cursor` and keep the same deal and date filters.
    #[param(max_length = 2048)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams)]
pub struct DealRunDetailQuery {
    /// Maximum piece/URL results to return.
    #[param(default = 100, minimum = 1, maximum = 500, example = 100)]
    pub limit: Option<i64>,
    /// Omit on the first page. Then copy `next_cursor` and keep the same deal and run.
    #[param(max_length = 2048)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams)]
pub struct DealBmsHistoryQuery {
    /// Maximum BMS jobs to return.
    #[param(default = 100, minimum = 1, maximum = 500, example = 100)]
    pub limit: Option<i64>,
    /// Omit on the first page. Then copy `next_cursor` and keep the same deal and run.
    #[param(max_length = 2048)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DealSliHistoryResponse {
    pub deal_id: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub data: BTreeMap<NaiveDate, Vec<DealSliRunResponse>>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DealSliRunDetailResponse {
    pub run: DealSliRunResponse,
    pub piece_results: Vec<DealSliPieceResultResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DealBmsHistoryResponse {
    pub deal_id: String,
    pub run_id: Uuid,
    pub data: Vec<DealSliBmsHistoryResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DealSliRunResponse {
    pub id: Uuid,
    pub deal_id: String,
    pub state: String,
    pub measurement_state: MeasurementState,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tested_at: Option<DateTime<Utc>>,
    pub provider_id: String,
    pub client_id: Option<String>,
    pub working_url: Option<String>,
    pub retrievability_percent: Option<f64>,
    pub large_files_percent: Option<f64>,
    pub car_files_percent: Option<f64>,
    pub sector_utilization_percent: Option<f64>,
    pub manifest_snapshot_id: Option<Uuid>,
    /// Deal size stored with the run, serialized as a base-10 integer string.
    pub deal_size_bytes: Option<String>,
    /// Manifest size stored with the run, serialized as a base-10 integer string.
    pub manifest_size_bytes: Option<String>,
    pub content_matches_deal: Option<bool>,
    pub sampled_piece_count: Option<u32>,
    pub size_matched_percent: Option<f64>,
    pub avg_response_time_ms: Option<f64>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub error_code: Option<UrlErrorCode>,
    pub piece_count: u32,
    pub success_count: u32,
    pub failed_count: u32,
    pub url_metadata: Option<serde_json::Value>,
    pub porep_slis: DealPorepSliResponse,
    pub bms_results: Vec<DealSliBmsHistoryResponse>,
    /// Continue from this cursor on the run's `/bms` endpoint.
    pub bms_next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DealSliBmsHistoryResponse {
    pub id: Uuid,
    pub deal_id: String,
    pub run_id: Uuid,
    pub piece_index: u32,
    pub piece_cid: String,
    pub bms_job_id: Uuid,
    pub url_tested: String,
    pub routing_key: String,
    pub worker_count: u32,
    pub status: String,
    pub ping_avg_ms: Option<f64>,
    pub head_avg_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
    pub download_speed_mbps: Option<f64>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DealSliPieceResultResponse {
    pub run_id: Uuid,
    pub deal_id: String,
    pub piece_index: u32,
    pub piece_cid: String,
    pub url_tested: String,
    pub success: bool,
    pub content_length: Option<i64>,
    pub is_valid_car: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub tested_at: DateTime<Utc>,
    pub manifest_snapshot_id: Option<Uuid>,
    /// Manifest file size, serialized as a base-10 integer string.
    pub file_size_bytes: Option<String>,
    pub observed_size_bytes: Option<i64>,
    pub size_matched: Option<bool>,
    pub manifest_response_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunHistoryCursor {
    deal_key: String,
    from: NaiveDate,
    to: NaiveDate,
    started_at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PieceCursor {
    deal_key: String,
    run_id: Uuid,
    piece_index: i32,
    url_ordinal: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BmsCursor {
    deal_key: String,
    run_id: Uuid,
    created_at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Default)]
struct RunBmsData {
    preview: Vec<DealSliBmsJob>,
    aggregate: Option<DealSliBmsAggregate>,
}

fn validate_deal_id(deal_id: &str) -> Result<(), ApiResponse<()>> {
    if deal_id.is_empty() || !deal_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            format!("deal_id must be a decimal string, got {deal_id}"),
        ));
    }
    Ok(())
}

fn deal_cursor_key(deal_id: &str) -> String {
    hex::encode(keccak256(deal_id.as_bytes()))
}

fn internal_history_error(e: color_eyre::Report) -> ApiResponse<()> {
    tracing::warn!("Failed to query Deal SLI history: {e:?}");
    internal_server_error_with_code(ErrorCode::InternalError, "Failed to query Deal SLI history")
}

fn not_found(deal_id: &str, run_id: Option<Uuid>) -> ApiResponse<()> {
    let message = match run_id {
        Some(run_id) => format!("Deal SLI run {run_id} not found for deal {deal_id}"),
        None => format!("Deal target {deal_id} not found"),
    };
    not_found_with_code(ErrorCode::NotFound, message)
}

fn decode_run_cursor(
    value: Option<&str>,
    deal_id: &str,
    from: NaiveDate,
    to: NaiveDate,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Option<RunHistoryCursor>, ApiResponse<()>> {
    let cursor: Option<RunHistoryCursor> = decode_history_cursor(value)?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.deal_key != deal_cursor_key(deal_id) || cursor.from != from || cursor.to != to {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            "History cursor does not match request filters",
        ));
    }
    if cursor.started_at < start || cursor.started_at >= end {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            "History cursor timestamp is outside the requested date range",
        ));
    }
    Ok(Some(cursor))
}

fn decode_piece_cursor(
    value: Option<&str>,
    deal_id: &str,
    run_id: Uuid,
) -> Result<Option<PieceCursor>, ApiResponse<()>> {
    let cursor: Option<PieceCursor> = decode_history_cursor(value)?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.deal_key != deal_cursor_key(deal_id)
        || cursor.run_id != run_id
        || cursor.piece_index < 0
        || cursor.url_ordinal < 1
    {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            "History cursor does not match requested deal run",
        ));
    }
    Ok(Some(cursor))
}

fn decode_bms_cursor(
    value: Option<&str>,
    deal_id: &str,
    run_id: Uuid,
) -> Result<Option<BmsCursor>, ApiResponse<()>> {
    let cursor: Option<BmsCursor> = decode_history_cursor(value)?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.deal_key != deal_cursor_key(deal_id) || cursor.run_id != run_id {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            "History cursor does not match requested deal run BMS history",
        ));
    }
    Ok(Some(cursor))
}

fn encode_bms_cursor(
    deal_id: &str,
    run_id: Uuid,
    result: &DealSliBmsJob,
) -> Result<String, ApiResponse<()>> {
    encode_history_cursor(&BmsCursor {
        deal_key: deal_cursor_key(deal_id),
        run_id,
        created_at: result.created_at,
        id: result.id,
    })
}

async fn load_run_bms_data(
    state: &AppState,
    deal_id: &str,
    run_ids: &[Uuid],
) -> Result<HashMap<Uuid, RunBmsData>, ApiResponse<()>> {
    let (previews, aggregates) = tokio::try_join!(
        state.deal_sli_repo.get_bms_job_previews_for_runs(
            deal_id,
            run_ids,
            BMS_PREVIEW_LIMIT as i64 + 1,
        ),
        state
            .deal_sli_repo
            .get_bms_aggregates_for_runs(deal_id, run_ids),
    )
    .map_err(internal_history_error)?;

    let mut by_run: HashMap<Uuid, RunBmsData> = HashMap::new();
    for result in previews {
        by_run
            .entry(result.run_id)
            .or_default()
            .preview
            .push(result);
    }
    for aggregate in aggregates {
        let run_id = aggregate.run_id;
        by_run.entry(run_id).or_default().aggregate = Some(aggregate);
    }
    Ok(by_run)
}

fn map_bms_result(result: DealSliBmsJob) -> DealSliBmsHistoryResponse {
    DealSliBmsHistoryResponse {
        id: result.id,
        deal_id: result.deal_id,
        run_id: result.run_id,
        piece_index: result.piece_index as u32,
        piece_cid: result.piece_cid,
        bms_job_id: result.bms_job_id,
        url_tested: result.url_tested,
        routing_key: result.routing_key,
        worker_count: result.worker_count as u32,
        status: result.status,
        ping_avg_ms: result.ping_avg_ms.as_ref().and_then(bigdecimal_to_f64),
        head_avg_ms: result.head_avg_ms.as_ref().and_then(bigdecimal_to_f64),
        ttfb_ms: result.ttfb_ms.as_ref().and_then(bigdecimal_to_f64),
        download_speed_mbps: result
            .download_speed_mbps
            .as_ref()
            .and_then(bigdecimal_to_f64),
        error_message: result.error_message,
        created_at: result.created_at,
        completed_at: result.completed_at,
    }
}

fn map_run(
    run: DealSliHistoryRun,
    mut bms_data: RunBmsData,
) -> Result<DealSliRunResponse, ApiResponse<()>> {
    let bms_next_cursor = if bms_data.preview.len() > BMS_PREVIEW_LIMIT {
        Some(encode_bms_cursor(
            &run.deal_id,
            run.id,
            &bms_data.preview[BMS_PREVIEW_LIMIT - 1],
        )?)
    } else {
        None
    };
    bms_data.preview.truncate(BMS_PREVIEW_LIMIT);
    let (min_download_speed_mbps, max_ttfb_ms) = bms_data
        .aggregate
        .as_ref()
        .map(|aggregate| {
            (
                aggregate
                    .min_download_speed_mbps
                    .as_ref()
                    .and_then(bigdecimal_to_f64),
                aggregate.max_ttfb_ms.as_ref().and_then(bigdecimal_to_f64),
            )
        })
        .unwrap_or((None, None));
    let porep_slis = map_porep_sli_aggregates(
        run.retrievability_percent.as_ref(),
        min_download_speed_mbps,
        max_ttfb_ms,
    );
    Ok(DealSliRunResponse {
        id: run.id,
        deal_id: run.deal_id,
        state: run.state,
        measurement_state: MeasurementState::from_db_value(&run.measurement_state),
        started_at: run.started_at,
        completed_at: run.completed_at,
        tested_at: run.tested_at,
        provider_id: run.provider_id,
        client_id: run.client_id,
        working_url: run.working_url,
        retrievability_percent: run
            .retrievability_percent
            .as_ref()
            .and_then(bigdecimal_to_f64),
        large_files_percent: run.large_files_percent.as_ref().and_then(bigdecimal_to_f64),
        car_files_percent: run.car_files_percent.as_ref().and_then(bigdecimal_to_f64),
        sector_utilization_percent: run
            .sector_utilization_percent
            .as_ref()
            .and_then(bigdecimal_to_f64),
        manifest_snapshot_id: run.manifest_snapshot_id,
        deal_size_bytes: run.deal_size_bytes.map(|value| value.to_string()),
        manifest_size_bytes: run.manifest_size_bytes.map(|value| value.to_string()),
        content_matches_deal: run.content_matches_deal,
        sampled_piece_count: run.sampled_piece_count.map(|value| value as u32),
        size_matched_percent: run
            .size_matched_percent
            .as_ref()
            .and_then(bigdecimal_to_f64),
        avg_response_time_ms: run
            .avg_response_time_ms
            .as_ref()
            .and_then(bigdecimal_to_f64),
        is_consistent: run.is_consistent,
        is_reliable: run.is_reliable,
        result_code: run.result_code,
        error_code: run.error_code,
        piece_count: run.piece_count as u32,
        success_count: run.success_count as u32,
        failed_count: run.failed_count as u32,
        url_metadata: run.url_metadata,
        porep_slis,
        bms_results: bms_data.preview.into_iter().map(map_bms_result).collect(),
        bms_next_cursor,
    })
}

fn map_piece_result(result: DealSliHistoryPieceResult) -> DealSliPieceResultResponse {
    DealSliPieceResultResponse {
        run_id: result.run_id,
        deal_id: result.deal_id,
        piece_index: result.piece_index as u32,
        piece_cid: result.piece_cid,
        url_tested: result.url_tested,
        success: result.success,
        content_length: result.content_length,
        is_valid_car: result.is_valid_car,
        result_code: result.result_code,
        tested_at: result.tested_at,
        manifest_snapshot_id: result.manifest_snapshot_id,
        file_size_bytes: result.file_size_bytes.map(|value| value.to_string()),
        observed_size_bytes: result.observed_size_bytes,
        size_matched: result.size_matched,
        manifest_response_time_ms: result.manifest_response_time_ms,
    }
}

#[utoipa::path(
    get,
    path = "/deals/{deal_id}/history/sli",
    params(DealPath, DealSliHistoryQuery),
    responses(
        (status = 200, description = "Completed stored Deal SLI runs grouped by UTC start date", body = DealSliHistoryResponse),
        (status = 400, description = "Invalid path, query, date range, limit, or cursor", body = ErrorResponse),
        (status = 404, description = "Deal target not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_deal_history_sli(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealPath>, ApiResponse<ErrorResponse>>,
    WithRejection(Query(query), _): WithRejection<
        Query<DealSliHistoryQuery>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<DealSliHistoryResponse>, ApiResponse<()>> {
    validate_deal_id(&path.deal_id)?;
    let window = history_window(query.from, query.to)?;
    let limit = history_limit(query.limit)?;
    let cursor = decode_run_cursor(
        query.cursor.as_deref(),
        &path.deal_id,
        window.from,
        window.to,
        window.start,
        window.end,
    )?;
    if !state
        .deal_sli_repo
        .target_exists(&path.deal_id)
        .await
        .map_err(internal_history_error)?
    {
        return Err(not_found(&path.deal_id, None));
    }
    let boundary = cursor.map(|cursor| (cursor.started_at, cursor.id));
    let mut runs = state
        .deal_sli_repo
        .get_completed_run_history(&path.deal_id, window.start, window.end, boundary, limit + 1)
        .await
        .map_err(internal_history_error)?;
    let next_cursor = if runs.len() > limit as usize {
        let run = &runs[limit as usize - 1];
        Some(encode_history_cursor(&RunHistoryCursor {
            deal_key: deal_cursor_key(&path.deal_id),
            from: window.from,
            to: window.to,
            started_at: run.started_at,
            id: run.id,
        })?)
    } else {
        None
    };
    runs.truncate(limit as usize);

    let run_ids = runs.iter().map(|run| run.id).collect::<Vec<_>>();
    let mut bms_by_run = load_run_bms_data(&state, &path.deal_id, &run_ids).await?;
    let mut data = BTreeMap::new();
    for run in runs {
        let day = run.started_at.date_naive();
        let bms = bms_by_run.remove(&run.id).unwrap_or_default();
        data.entry(day)
            .or_insert_with(Vec::new)
            .push(map_run(run, bms)?);
    }

    Ok(ok_response(DealSliHistoryResponse {
        deal_id: path.deal_id,
        from: window.from,
        to: window.to,
        data,
        next_cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/deals/{deal_id}/runs/{run_id}",
    params(DealRunPath, DealRunDetailQuery),
    responses(
        (status = 200, description = "Stored Deal SLI run with BMS jobs and paginated piece results", body = DealSliRunDetailResponse),
        (status = 400, description = "Invalid path, limit, or cursor", body = ErrorResponse),
        (status = 404, description = "Deal run not found for this deal", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_get_deal_run(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealRunPath>, ApiResponse<ErrorResponse>>,
    WithRejection(Query(query), _): WithRejection<
        Query<DealRunDetailQuery>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<DealSliRunDetailResponse>, ApiResponse<()>> {
    validate_deal_id(&path.deal_id)?;
    let run_id = Uuid::parse_str(&path.run_id).map_err(|_| {
        bad_request_with_code(ErrorCode::InvalidRequest, "run_id must be a valid UUID")
    })?;
    let limit = history_limit(query.limit)?;
    let cursor = decode_piece_cursor(query.cursor.as_deref(), &path.deal_id, run_id)?;
    let run = state
        .deal_sli_repo
        .get_run_for_deal(&path.deal_id, run_id)
        .await
        .map_err(internal_history_error)?
        .ok_or_else(|| not_found(&path.deal_id, Some(run_id)))?;
    let mut bms_by_run = load_run_bms_data(&state, &path.deal_id, &[run_id]).await?;
    let boundary = cursor
        .as_ref()
        .map(|cursor| (cursor.piece_index, cursor.url_ordinal));
    let mut piece_results = state
        .deal_sli_repo
        .get_piece_results_for_run(&path.deal_id, run_id, boundary, limit + 1)
        .await
        .map_err(internal_history_error)?;
    let next_cursor = if piece_results.len() > limit as usize {
        let result = &piece_results[limit as usize - 1];
        Some(encode_history_cursor(&PieceCursor {
            deal_key: deal_cursor_key(&path.deal_id),
            run_id,
            piece_index: result.piece_index,
            url_ordinal: result.url_ordinal,
        })?)
    } else {
        None
    };
    piece_results.truncate(limit as usize);

    Ok(ok_response(DealSliRunDetailResponse {
        run: map_run(run, bms_by_run.remove(&run_id).unwrap_or_default())?,
        piece_results: piece_results.into_iter().map(map_piece_result).collect(),
        next_cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/deals/{deal_id}/runs/{run_id}/bms",
    params(DealRunPath, DealBmsHistoryQuery),
    responses(
        (status = 200, description = "Paginated BMS jobs stored for this Deal SLI run", body = DealBmsHistoryResponse),
        (status = 400, description = "Invalid path, limit, or cursor", body = ErrorResponse),
        (status = 404, description = "Deal run not found for this deal", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    ),
    tags = ["Deals"],
)]
#[debug_handler(state = Arc<AppState>)]
pub async fn handle_get_deal_run_bms(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<DealRunPath>, ApiResponse<ErrorResponse>>,
    WithRejection(Query(query), _): WithRejection<
        Query<DealBmsHistoryQuery>,
        ApiResponse<ErrorResponse>,
    >,
) -> Result<ApiResponse<DealBmsHistoryResponse>, ApiResponse<()>> {
    validate_deal_id(&path.deal_id)?;
    let run_id = Uuid::parse_str(&path.run_id).map_err(|_| {
        bad_request_with_code(ErrorCode::InvalidRequest, "run_id must be a valid UUID")
    })?;
    let limit = history_limit(query.limit)?;
    let cursor = decode_bms_cursor(query.cursor.as_deref(), &path.deal_id, run_id)?;
    if state
        .deal_sli_repo
        .get_run_for_deal(&path.deal_id, run_id)
        .await
        .map_err(internal_history_error)?
        .is_none()
    {
        return Err(not_found(&path.deal_id, Some(run_id)));
    }
    let boundary = cursor.map(|cursor| (cursor.created_at, cursor.id));
    let mut jobs = state
        .deal_sli_repo
        .get_bms_jobs_for_run_page(&path.deal_id, run_id, boundary, limit + 1)
        .await
        .map_err(internal_history_error)?;
    let next_cursor = if jobs.len() > limit as usize {
        Some(encode_bms_cursor(
            &path.deal_id,
            run_id,
            &jobs[limit as usize - 1],
        )?)
    } else {
        None
    };
    jobs.truncate(limit as usize);

    Ok(ok_response(DealBmsHistoryResponse {
        deal_id: path.deal_id,
        run_id,
        data: jobs.into_iter().map(map_bms_result).collect(),
        next_cursor,
    }))
}
