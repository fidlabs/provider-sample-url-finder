use chrono::{DateTime, Utc};
use color_eyre::Result;
use sqlx::types::BigDecimal;
use uuid::Uuid;

use super::{DealSliBmsJob, DealSliRepository};
use crate::{ErrorCode, ResultCode};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliHistoryRun {
    pub id: Uuid,
    pub deal_id: String,
    pub state: String,
    pub measurement_state: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tested_at: Option<DateTime<Utc>>,
    pub provider_id: String,
    pub client_id: Option<String>,
    pub working_url: Option<String>,
    pub retrievability_percent: Option<BigDecimal>,
    pub large_files_percent: Option<BigDecimal>,
    pub car_files_percent: Option<BigDecimal>,
    pub sector_utilization_percent: Option<BigDecimal>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub error_code: Option<ErrorCode>,
    pub piece_count: i32,
    pub success_count: i32,
    pub failed_count: i32,
    pub url_metadata: Option<serde_json::Value>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub deal_size_bytes: Option<BigDecimal>,
    pub manifest_size_bytes: Option<BigDecimal>,
    pub content_matches_deal: Option<bool>,
    pub sampled_piece_count: Option<i32>,
    pub size_matched_percent: Option<BigDecimal>,
    pub avg_response_time_ms: Option<BigDecimal>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliHistoryPieceResult {
    pub run_id: Uuid,
    pub deal_id: String,
    pub piece_index: i32,
    pub piece_cid: String,
    pub url_tested: String,
    pub success: bool,
    pub content_length: Option<i64>,
    pub is_valid_car: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub tested_at: DateTime<Utc>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub file_size_bytes: Option<BigDecimal>,
    pub observed_size_bytes: Option<i64>,
    pub size_matched: Option<bool>,
    pub manifest_response_time_ms: Option<i64>,
    pub url_ordinal: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliBmsAggregate {
    pub run_id: Uuid,
    pub min_download_speed_mbps: Option<BigDecimal>,
    pub max_ttfb_ms: Option<BigDecimal>,
}

impl DealSliRepository {
    pub async fn target_exists(&self, deal_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar!(
            r#"SELECT EXISTS (
                    SELECT 1
                    FROM deal_sli_targets
                    WHERE deal_id = $1
                ) AS "exists!""#,
            deal_id
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn get_completed_run_history(
        &self,
        deal_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        boundary: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<DealSliHistoryRun>> {
        let (boundary_at, boundary_id) = boundary
            .map(|(at, id)| (Some(at), Some(id)))
            .unwrap_or((None, None));
        Ok(sqlx::query_as!(
            DealSliHistoryRun,
            r#"SELECT
                    id,
                    deal_id,
                    state,
                    measurement_state,
                    started_at,
                    completed_at,
                    tested_at,
                    provider_id,
                    client_id,
                    working_url,
                    retrievability_percent,
                    large_files_percent,
                    car_files_percent,
                    sector_utilization_percent,
                    is_consistent,
                    is_reliable,
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    piece_count,
                    success_count,
                    failed_count,
                    url_metadata,
                    manifest_snapshot_id,
                    deal_size_bytes,
                    manifest_size_bytes,
                    content_matches_deal,
                    sampled_piece_count,
                    size_matched_percent,
                    avg_response_time_ms
               FROM deal_sli_runs
               WHERE deal_id = $1
                 AND state = 'completed'
                 AND started_at >= $2
                 AND started_at < $3
                 AND ($4::timestamptz IS NULL OR (started_at, id) < ($4, $5))
               ORDER BY started_at DESC, id DESC
               LIMIT $6"#,
            deal_id,
            start,
            end,
            boundary_at,
            boundary_id,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_run_for_deal(
        &self,
        deal_id: &str,
        run_id: Uuid,
    ) -> Result<Option<DealSliHistoryRun>> {
        Ok(sqlx::query_as!(
            DealSliHistoryRun,
            r#"SELECT
                    id,
                    deal_id,
                    state,
                    measurement_state,
                    started_at,
                    completed_at,
                    tested_at,
                    provider_id,
                    client_id,
                    working_url,
                    retrievability_percent,
                    large_files_percent,
                    car_files_percent,
                    sector_utilization_percent,
                    is_consistent,
                    is_reliable,
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    piece_count,
                    success_count,
                    failed_count,
                    url_metadata,
                    manifest_snapshot_id,
                    deal_size_bytes,
                    manifest_size_bytes,
                    content_matches_deal,
                    sampled_piece_count,
                    size_matched_percent,
                    avg_response_time_ms
               FROM deal_sli_runs
               WHERE deal_id = $1 AND id = $2"#,
            deal_id,
            run_id
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_bms_job_previews_for_runs(
        &self,
        deal_id: &str,
        run_ids: &[Uuid],
        limit_per_run: i64,
    ) -> Result<Vec<DealSliBmsJob>> {
        if run_ids.is_empty() {
            return Ok(vec![]);
        }
        Ok(sqlx::query_as!(
            DealSliBmsJob,
            r#"SELECT jobs.*
               FROM UNNEST($2::uuid[]) AS requested(run_id)
               CROSS JOIN LATERAL (
                    SELECT
                        id,
                        deal_id,
                        run_id,
                        piece_index,
                        piece_cid,
                        bms_job_id,
                        url_tested,
                        routing_key,
                        worker_count,
                        status,
                        ping_avg_ms,
                        head_avg_ms,
                        ttfb_ms,
                        download_speed_mbps,
                        error_message,
                        created_at,
                        completed_at
                    FROM deal_sli_bms_jobs
                    WHERE deal_id = $1 AND run_id = requested.run_id
                    ORDER BY created_at DESC, id DESC
                    LIMIT $3
               ) jobs
               ORDER BY jobs.run_id, jobs.created_at DESC, jobs.id DESC"#,
            deal_id,
            run_ids,
            limit_per_run
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_bms_aggregates_for_runs(
        &self,
        deal_id: &str,
        run_ids: &[Uuid],
    ) -> Result<Vec<DealSliBmsAggregate>> {
        if run_ids.is_empty() {
            return Ok(vec![]);
        }
        Ok(sqlx::query_as!(
            DealSliBmsAggregate,
            r#"SELECT
                    requested.run_id AS "run_id!",
                    MIN(jobs.download_speed_mbps) FILTER (
                        WHERE jobs.status = 'Completed'
                    ) AS min_download_speed_mbps,
                    MAX(jobs.ttfb_ms) FILTER (
                        WHERE jobs.status = 'Completed'
                    ) AS max_ttfb_ms
               FROM UNNEST($2::uuid[]) AS requested(run_id)
               LEFT JOIN deal_sli_bms_jobs jobs
                 ON jobs.deal_id = $1 AND jobs.run_id = requested.run_id
               GROUP BY requested.run_id"#,
            deal_id,
            run_ids
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_bms_jobs_for_run_page(
        &self,
        deal_id: &str,
        run_id: Uuid,
        boundary: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<DealSliBmsJob>> {
        let (boundary_at, boundary_id) = boundary
            .map(|(created_at, id)| (Some(created_at), Some(id)))
            .unwrap_or((None, None));
        Ok(sqlx::query_as!(
            DealSliBmsJob,
            r#"SELECT
                    id,
                    deal_id,
                    run_id,
                    piece_index,
                    piece_cid,
                    bms_job_id,
                    url_tested,
                    routing_key,
                    worker_count,
                    status,
                    ping_avg_ms,
                    head_avg_ms,
                    ttfb_ms,
                    download_speed_mbps,
                    error_message,
                    created_at,
                    completed_at
               FROM deal_sli_bms_jobs
               WHERE deal_id = $1
                 AND run_id = $2
                 AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4))
               ORDER BY created_at DESC, id DESC
               LIMIT $5"#,
            deal_id,
            run_id,
            boundary_at,
            boundary_id,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_piece_results_for_run(
        &self,
        deal_id: &str,
        run_id: Uuid,
        boundary: Option<(i32, i64)>,
        limit: i64,
    ) -> Result<Vec<DealSliHistoryPieceResult>> {
        // The ordinal preserves the piece_index/url_tested order without putting an
        // unbounded stored URL into the public cursor.
        let (boundary_index, boundary_ordinal) = boundary
            .map(|(piece_index, url_ordinal)| (Some(piece_index), Some(url_ordinal)))
            .unwrap_or((None, None));
        Ok(sqlx::query_as!(
            DealSliHistoryPieceResult,
            r#"WITH ordered AS (
                    SELECT
                        run_id,
                        deal_id,
                        piece_index,
                        piece_cid,
                        url_tested,
                        success,
                        content_length,
                        is_valid_car,
                        result_code,
                        tested_at,
                        manifest_snapshot_id,
                        file_size_bytes,
                        observed_size_bytes,
                        size_matched,
                        manifest_response_time_ms,
                        ROW_NUMBER() OVER (
                            PARTITION BY piece_index
                            ORDER BY url_tested ASC
                        ) AS url_ordinal
                    FROM deal_sli_piece_results
                    WHERE deal_id = $1 AND run_id = $2
                )
                SELECT
                    run_id,
                    deal_id,
                    piece_index,
                    piece_cid,
                    url_tested,
                    success,
                    content_length,
                    is_valid_car,
                    result_code AS "result_code: ResultCode",
                    tested_at,
                    manifest_snapshot_id,
                    file_size_bytes,
                    observed_size_bytes,
                    size_matched,
                    manifest_response_time_ms,
                    url_ordinal AS "url_ordinal!"
               FROM ordered
               WHERE $3::integer IS NULL OR (piece_index, url_ordinal) > ($3, $4)
               ORDER BY piece_index ASC, url_ordinal ASC
               LIMIT $5"#,
            deal_id,
            run_id,
            boundary_index,
            boundary_ordinal,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }
}
