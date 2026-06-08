use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use color_eyre::{Result, eyre::eyre};
use sqlx::PgPool;
use sqlx::types::BigDecimal;
use uuid::Uuid;

use crate::{ErrorCode, ResultCode};

#[derive(Clone)]
pub struct DealSliRepository {
    pool: PgPool,
}

#[derive(Debug, Clone, Default)]
pub struct DealSliRequirementValues {
    pub retrievability_bps: Option<i32>,
    pub bandwidth_mbps: Option<i32>,
    pub latency_ms: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct NewDealSliTarget {
    pub deal_id: String,
    pub deal_version: String,
    pub provider_id: String,
    pub client_id: Option<String>,
    pub deal_size_bytes: Option<BigDecimal>,
    pub manifest_hash: Option<String>,
    pub manifest_location: Option<String>,
    pub requirements: DealSliRequirementValues,
    pub pieces: Vec<NewDealSliPiece>,
}

#[derive(Debug, Clone)]
pub struct NewDealSliManifestSnapshot {
    pub deal_id: String,
    pub manifest_hash: String,
    pub manifest_location: String,
    pub raw_content: String,
    pub parsed_content: serde_json::Value,
    pub content_byte_length: i64,
    pub computed_hash: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliManifestSnapshot {
    pub id: Uuid,
    pub deal_id: String,
    pub manifest_hash: String,
    pub manifest_location: String,
    pub raw_content: String,
    pub parsed_content: serde_json::Value,
    pub fetched_at: DateTime<Utc>,
    pub content_byte_length: i64,
    pub computed_hash: String,
}

#[derive(Debug, Clone)]
pub struct NewDealSliPiece {
    pub piece_index: i32,
    pub piece_cid: String,
    pub piece_size_bytes: Option<BigDecimal>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub file_size_bytes: Option<BigDecimal>,
    pub root_cid: Option<String>,
    pub storage_path: Option<String>,
    pub piece_type: Option<String>,
    pub allocation_id: Option<String>,
    pub claim_id: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliTarget {
    pub deal_id: String,
    pub deal_version: String,
    pub provider_id: String,
    pub client_id: Option<String>,
    pub deal_size_bytes: Option<BigDecimal>,
    pub manifest_hash: Option<String>,
    pub manifest_location: Option<String>,
    pub active_manifest_snapshot_id: Option<Uuid>,
    pub retrievability_bps: Option<i32>,
    pub bandwidth_mbps: Option<i32>,
    pub latency_ms: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliPiece {
    pub deal_id: String,
    pub piece_index: i32,
    pub piece_cid: String,
    pub piece_size_bytes: Option<BigDecimal>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub file_size_bytes: Option<BigDecimal>,
    pub root_cid: Option<String>,
    pub storage_path: Option<String>,
    pub piece_type: Option<String>,
    pub allocation_id: Option<String>,
    pub claim_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DealSliTargetWithPieces {
    pub target: DealSliTarget,
    pub manifest_snapshot: Option<DealSliManifestSnapshot>,
    pub pieces: Vec<DealSliPiece>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DealSliLatestRun {
    pub deal_id: String,
    pub measurement_state: String,
    pub tested_at: Option<DateTime<Utc>>,
    pub working_url: Option<String>,
    pub retrievability_percent: Option<BigDecimal>,
    pub large_files_percent: Option<BigDecimal>,
    pub car_files_percent: Option<BigDecimal>,
    pub sector_utilization_percent: Option<BigDecimal>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub deal_size_bytes: Option<BigDecimal>,
    pub manifest_size_bytes: Option<BigDecimal>,
    pub content_matches_deal: Option<bool>,
    pub sampled_piece_count: Option<i32>,
    pub size_matched_percent: Option<BigDecimal>,
    pub avg_response_time_ms: Option<BigDecimal>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub error_code: Option<ErrorCode>,
    pub piece_count: i32,
    pub success_count: i32,
    pub failed_count: i32,
}

#[derive(Debug, Clone)]
pub struct NewDealSliPieceResult {
    pub deal_id: String,
    pub piece_index: i32,
    pub piece_cid: String,
    pub url_tested: String,
    pub success: bool,
    pub content_length: Option<i64>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub file_size_bytes: Option<BigDecimal>,
    pub observed_size_bytes: Option<i64>,
    pub size_matched: Option<bool>,
    pub manifest_response_time_ms: Option<i64>,
    pub is_valid_car: bool,
    pub result_code: ResultCode,
}

#[derive(Debug, Clone)]
pub struct NewCompletedDealSliRun {
    pub deal_id: String,
    pub target_pieces: Vec<DealSliRunPieceSnapshot>,
    pub measurement_state: String,
    pub provider_id: String,
    pub client_id: Option<String>,
    pub working_url: Option<String>,
    pub retrievability_percent: Option<BigDecimal>,
    pub large_files_percent: Option<BigDecimal>,
    pub car_files_percent: Option<BigDecimal>,
    pub sector_utilization_percent: Option<BigDecimal>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub deal_size_bytes: Option<BigDecimal>,
    pub manifest_size_bytes: Option<BigDecimal>,
    pub content_matches_deal: Option<bool>,
    pub sampled_piece_count: Option<i32>,
    pub size_matched_percent: Option<BigDecimal>,
    pub avg_response_time_ms: Option<BigDecimal>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub result_code: ResultCode,
    pub piece_count: i32,
    pub success_count: i32,
    pub failed_count: i32,
    pub piece_results: Vec<NewDealSliPieceResult>,
}

#[derive(Debug, Clone)]
pub struct DealSliRunPieceSnapshot {
    pub piece_index: i32,
    pub piece_cid: String,
    pub piece_size_bytes: Option<BigDecimal>,
    pub manifest_snapshot_id: Option<Uuid>,
    pub file_size_bytes: Option<BigDecimal>,
}

#[derive(Debug, sqlx::FromRow)]
struct InsertedDealSliRun {
    id: Uuid,
    deal_id: String,
    measurement_state: String,
    tested_at: Option<DateTime<Utc>>,
    working_url: Option<String>,
    retrievability_percent: Option<BigDecimal>,
    large_files_percent: Option<BigDecimal>,
    car_files_percent: Option<BigDecimal>,
    sector_utilization_percent: Option<BigDecimal>,
    manifest_snapshot_id: Option<Uuid>,
    deal_size_bytes: Option<BigDecimal>,
    manifest_size_bytes: Option<BigDecimal>,
    content_matches_deal: Option<bool>,
    sampled_piece_count: Option<i32>,
    size_matched_percent: Option<BigDecimal>,
    avg_response_time_ms: Option<BigDecimal>,
    is_consistent: Option<bool>,
    is_reliable: Option<bool>,
    result_code: Option<ResultCode>,
    error_code: Option<ErrorCode>,
    piece_count: i32,
    success_count: i32,
    failed_count: i32,
}

impl DealSliRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_target(&self, target: &NewDealSliTarget) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query_scalar!(
            r#"SELECT
                    deal_id
               FROM
                    deal_sli_targets
               WHERE
                    deal_id = $1
               FOR UPDATE
            "#,
            &target.deal_id
        )
        .fetch_optional(&mut *tx)
        .await?;

        let existing_pieces = sqlx::query_as!(
            DealSliPiece,
            r#"SELECT
                    deal_id,
                    piece_index,
                    piece_cid,
                    piece_size_bytes,
                    manifest_snapshot_id,
                    file_size_bytes,
                    root_cid,
                    storage_path,
                    piece_type,
                    allocation_id,
                    claim_id
               FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
               ORDER BY
                    piece_index ASC
            "#,
            &target.deal_id
        )
        .fetch_all(&mut *tx)
        .await?;

        let has_runs = sqlx::query_scalar!(
            r#"SELECT
                    EXISTS (
                        SELECT
                            1
                        FROM
                            deal_sli_runs
                        WHERE
                            deal_id = $1
                    ) AS "exists!"
            "#,
            &target.deal_id
        )
        .fetch_one(&mut *tx)
        .await?;

        if has_runs {
            validate_measured_pieces_are_unchanged(&target.pieces, &existing_pieces)?;
        }

        sqlx::query!(
            r#"INSERT INTO
                    deal_sli_targets (
                        deal_id,
                        deal_version,
                        provider_id,
                        client_id,
                        deal_size_bytes,
                        manifest_hash,
                        manifest_location,
                        retrievability_bps,
                        bandwidth_mbps,
                        latency_ms
                    )
               VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (deal_id) DO UPDATE SET
                    deal_version = EXCLUDED.deal_version,
                    provider_id = EXCLUDED.provider_id,
                    client_id = EXCLUDED.client_id,
                    deal_size_bytes = EXCLUDED.deal_size_bytes,
                    manifest_hash = EXCLUDED.manifest_hash,
                    manifest_location = EXCLUDED.manifest_location,
                    retrievability_bps = EXCLUDED.retrievability_bps,
                    bandwidth_mbps = EXCLUDED.bandwidth_mbps,
                    latency_ms = EXCLUDED.latency_ms,
                    updated_at = NOW()
            "#,
            &target.deal_id,
            &target.deal_version,
            &target.provider_id,
            target.client_id.as_deref(),
            target.deal_size_bytes.as_ref(),
            target.manifest_hash.as_deref(),
            target.manifest_location.as_deref(),
            target.requirements.retrievability_bps,
            target.requirements.bandwidth_mbps,
            target.requirements.latency_ms
        )
        .execute(&mut *tx)
        .await?;

        if !has_runs {
            let piece_indexes = target
                .pieces
                .iter()
                .map(|piece| piece.piece_index)
                .collect::<Vec<_>>();

            sqlx::query!(
                r#"DELETE FROM
                        deal_sli_pieces
                   WHERE
                        deal_id = $1
                        AND NOT (piece_index = ANY($2::int4[]))
                "#,
                &target.deal_id,
                &piece_indexes
            )
            .execute(&mut *tx)
            .await?;
        }

        for piece in &target.pieces {
            sqlx::query!(
                r#"INSERT INTO
                        deal_sli_pieces (
                            deal_id,
                            piece_index,
                            piece_cid,
                            piece_size_bytes,
                            manifest_snapshot_id,
                            file_size_bytes,
                            root_cid,
                            storage_path,
                            piece_type,
                            allocation_id,
                            claim_id
                        )
                   VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                   ON CONFLICT (deal_id, piece_index) DO UPDATE SET
                        piece_cid = EXCLUDED.piece_cid,
                        piece_size_bytes = EXCLUDED.piece_size_bytes,
                        manifest_snapshot_id = EXCLUDED.manifest_snapshot_id,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        root_cid = EXCLUDED.root_cid,
                        storage_path = EXCLUDED.storage_path,
                        piece_type = EXCLUDED.piece_type,
                        allocation_id = EXCLUDED.allocation_id,
                        claim_id = EXCLUDED.claim_id
                "#,
                &target.deal_id,
                piece.piece_index,
                &piece.piece_cid,
                piece.piece_size_bytes.as_ref(),
                piece.manifest_snapshot_id,
                piece.file_size_bytes.as_ref(),
                piece.root_cid.as_deref(),
                piece.storage_path.as_deref(),
                piece.piece_type.as_deref(),
                piece.allocation_id.as_deref(),
                piece.claim_id.as_deref()
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_manifest_target(
        &self,
        target: &NewDealSliTarget,
        snapshot: &NewDealSliManifestSnapshot,
        pieces: &[NewDealSliPiece],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let existing_target = sqlx::query_as!(
            DealSliTarget,
            r#"SELECT
                    deal_id,
                    deal_version,
                    provider_id,
                    client_id,
                    deal_size_bytes,
                    manifest_hash,
                    manifest_location,
                    active_manifest_snapshot_id,
                    retrievability_bps,
                    bandwidth_mbps,
                    latency_ms,
                    created_at,
                    updated_at
               FROM
                    deal_sli_targets
               WHERE
                    deal_id = $1
               FOR UPDATE
            "#,
            &target.deal_id
        )
        .fetch_optional(&mut *tx)
        .await?;

        let existing_pieces = sqlx::query_as!(
            DealSliPiece,
            r#"SELECT
                    deal_id,
                    piece_index,
                    piece_cid,
                    piece_size_bytes,
                    manifest_snapshot_id,
                    file_size_bytes,
                    root_cid,
                    storage_path,
                    piece_type,
                    allocation_id,
                    claim_id
               FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
               ORDER BY
                    piece_index ASC
            "#,
            &target.deal_id
        )
        .fetch_all(&mut *tx)
        .await?;

        let has_runs = sqlx::query_scalar!(
            r#"SELECT
                    EXISTS (
                        SELECT
                            1
                        FROM
                            deal_sli_runs
                        WHERE
                            deal_id = $1
                    ) AS "exists!"
            "#,
            &target.deal_id
        )
        .fetch_one(&mut *tx)
        .await?;

        if has_runs {
            let existing_target = existing_target.ok_or_else(|| {
                eyre!(
                    "deal SLI target with runs is missing target row {}",
                    target.deal_id
                )
            })?;
            validate_measured_target_is_unchanged(target, &existing_target)?;
            validate_measured_pieces_are_unchanged(pieces, &existing_pieces)?;
            tx.commit().await?;
            return Ok(());
        }

        sqlx::query!(
            r#"INSERT INTO
                    deal_sli_targets (
                        deal_id,
                        deal_version,
                        provider_id,
                        client_id,
                        deal_size_bytes,
                        manifest_hash,
                        manifest_location,
                        retrievability_bps,
                        bandwidth_mbps,
                        latency_ms
                    )
               VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (deal_id) DO UPDATE SET
                    deal_version = EXCLUDED.deal_version,
                    provider_id = EXCLUDED.provider_id,
                    client_id = EXCLUDED.client_id,
                    deal_size_bytes = EXCLUDED.deal_size_bytes,
                    manifest_hash = EXCLUDED.manifest_hash,
                    manifest_location = EXCLUDED.manifest_location,
                    retrievability_bps = EXCLUDED.retrievability_bps,
                    bandwidth_mbps = EXCLUDED.bandwidth_mbps,
                    latency_ms = EXCLUDED.latency_ms,
                    updated_at = NOW()
            "#,
            &target.deal_id,
            &target.deal_version,
            &target.provider_id,
            target.client_id.as_deref(),
            target.deal_size_bytes.as_ref(),
            target.manifest_hash.as_deref(),
            target.manifest_location.as_deref(),
            target.requirements.retrievability_bps,
            target.requirements.bandwidth_mbps,
            target.requirements.latency_ms
        )
        .execute(&mut *tx)
        .await?;

        let snapshot_id = sqlx::query_scalar!(
            r#"INSERT INTO
                    deal_sli_manifest_snapshots (
                        deal_id,
                        manifest_hash,
                        manifest_location,
                        raw_content,
                        parsed_content,
                        content_byte_length,
                        computed_hash
                    )
               VALUES
                    ($1, $2, $3, $4, $5, $6, $7)
               RETURNING
                    id
            "#,
            &snapshot.deal_id,
            &snapshot.manifest_hash,
            &snapshot.manifest_location,
            &snapshot.raw_content,
            &snapshot.parsed_content,
            snapshot.content_byte_length,
            &snapshot.computed_hash
        )
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query!(
            r#"UPDATE
                    deal_sli_targets
               SET
                    active_manifest_snapshot_id = $2,
                    updated_at = NOW()
               WHERE
                    deal_id = $1
            "#,
            &target.deal_id,
            snapshot_id
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            r#"DELETE FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
            "#,
            &target.deal_id
        )
        .execute(&mut *tx)
        .await?;

        for piece in pieces {
            sqlx::query!(
                r#"INSERT INTO
                        deal_sli_pieces (
                            deal_id,
                            piece_index,
                            piece_cid,
                            piece_size_bytes,
                            manifest_snapshot_id,
                            file_size_bytes,
                            root_cid,
                            storage_path,
                            piece_type,
                            allocation_id,
                            claim_id
                        )
                   VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
                &target.deal_id,
                piece.piece_index,
                &piece.piece_cid,
                piece.piece_size_bytes.as_ref(),
                snapshot_id,
                piece.file_size_bytes.as_ref(),
                piece.root_cid.as_deref(),
                piece.storage_path.as_deref(),
                piece.piece_type.as_deref(),
                piece.allocation_id.as_deref(),
                piece.claim_id.as_deref()
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_target(&self, deal_id: &str) -> Result<Option<DealSliTargetWithPieces>> {
        let target = sqlx::query_as!(
            DealSliTarget,
            r#"SELECT
                    deal_id,
                    deal_version,
                    provider_id,
                    client_id,
                    deal_size_bytes,
                    manifest_hash,
                    manifest_location,
                    active_manifest_snapshot_id,
                    retrievability_bps,
                    bandwidth_mbps,
                    latency_ms,
                    created_at,
                    updated_at
               FROM
                    deal_sli_targets
               WHERE
                    deal_id = $1
            "#,
            deal_id
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(target) = target else {
            return Ok(None);
        };

        let pieces = sqlx::query_as!(
            DealSliPiece,
            r#"SELECT
                    deal_id,
                    piece_index,
                    piece_cid,
                    piece_size_bytes,
                    manifest_snapshot_id,
                    file_size_bytes,
                    root_cid,
                    storage_path,
                    piece_type,
                    allocation_id,
                    claim_id
               FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
               ORDER BY
                    piece_index ASC
            "#,
            deal_id
        )
        .fetch_all(&self.pool)
        .await?;

        let manifest_snapshot = match target.active_manifest_snapshot_id {
            Some(snapshot_id) => Some(
                sqlx::query_as!(
                    DealSliManifestSnapshot,
                    r#"SELECT
                            id,
                            deal_id,
                            manifest_hash,
                            manifest_location,
                            raw_content,
                            parsed_content,
                            fetched_at,
                            content_byte_length,
                            computed_hash
                       FROM
                            deal_sli_manifest_snapshots
                       WHERE
                            id = $1
                    "#,
                    snapshot_id
                )
                .fetch_one(&self.pool)
                .await?,
            ),
            None => None,
        };

        Ok(Some(DealSliTargetWithPieces {
            target,
            manifest_snapshot,
            pieces,
        }))
    }

    pub async fn get_latest_completed_run(
        &self,
        deal_id: &str,
    ) -> Result<Option<DealSliLatestRun>> {
        Ok(sqlx::query_as!(
            DealSliLatestRun,
            r#"SELECT
                    deal_id,
                    measurement_state,
                    tested_at,
                    working_url,
                    retrievability_percent,
                    large_files_percent,
                    car_files_percent,
                    sector_utilization_percent,
                    manifest_snapshot_id,
                    deal_size_bytes,
                    manifest_size_bytes,
                    content_matches_deal,
                    sampled_piece_count,
                    size_matched_percent,
                    avg_response_time_ms,
                    is_consistent,
                    is_reliable,
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    piece_count,
                    success_count,
                    failed_count
               FROM
                    deal_sli_runs
               WHERE
                    deal_id = $1
                    AND state = 'completed'
               ORDER BY
                    completed_at DESC NULLS LAST,
                    started_at DESC,
                    id DESC
               LIMIT
                    1
            "#,
            deal_id
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_manifest_piece_count(
        &self,
        deal_id: &str,
        manifest_snapshot_id: Uuid,
    ) -> Result<i64> {
        Ok(sqlx::query_scalar!(
            r#"SELECT
                    COUNT(*) AS "count!"
               FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
                    AND manifest_snapshot_id = $2
            "#,
            deal_id,
            manifest_snapshot_id
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn sample_manifest_pieces(
        &self,
        deal_id: &str,
        manifest_snapshot_id: Uuid,
        sample_size: i64,
    ) -> Result<Vec<DealSliPiece>> {
        Ok(sqlx::query_as!(
            DealSliPiece,
            r#"SELECT
                    deal_id,
                    piece_index,
                    piece_cid,
                    piece_size_bytes,
                    manifest_snapshot_id,
                    file_size_bytes,
                    root_cid,
                    storage_path,
                    piece_type,
                    allocation_id,
                    claim_id
               FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
                    AND manifest_snapshot_id = $2
               ORDER BY
                    random()
               LIMIT
                    $3
            "#,
            deal_id,
            manifest_snapshot_id,
            sample_size
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn insert_completed_run_with_piece_results(
        &self,
        run: &NewCompletedDealSliRun,
    ) -> Result<DealSliLatestRun> {
        let mut tx = self.pool.begin().await?;

        sqlx::query_scalar!(
            r#"SELECT
                    deal_id
               FROM
                    deal_sli_targets
               WHERE
                    deal_id = $1
               FOR UPDATE
            "#,
            &run.deal_id
        )
        .fetch_one(&mut *tx)
        .await?;

        let current_pieces = sqlx::query_as!(
            DealSliPiece,
            r#"SELECT
                    deal_id,
                    piece_index,
                    piece_cid,
                    piece_size_bytes,
                    manifest_snapshot_id,
                    file_size_bytes,
                    root_cid,
                    storage_path,
                    piece_type,
                    allocation_id,
                    claim_id
               FROM
                    deal_sli_pieces
               WHERE
                    deal_id = $1
               ORDER BY
                    piece_index ASC
            "#,
            &run.deal_id
        )
        .fetch_all(&mut *tx)
        .await?;
        validate_run_target_pieces_are_current(&run.target_pieces, &current_pieces)?;

        let inserted = sqlx::query_as!(
            InsertedDealSliRun,
            r#"INSERT INTO
                    deal_sli_runs (
                        deal_id,
                        state,
                        measurement_state,
                        completed_at,
                        tested_at,
                        provider_id,
                        client_id,
                        working_url,
                        retrievability_percent,
                        large_files_percent,
                        car_files_percent,
                        sector_utilization_percent,
                        manifest_snapshot_id,
                        deal_size_bytes,
                        manifest_size_bytes,
                        content_matches_deal,
                        sampled_piece_count,
                        size_matched_percent,
                        avg_response_time_ms,
                        is_consistent,
                        is_reliable,
                        result_code,
                        piece_count,
                        success_count,
                        failed_count
                    )
               VALUES
                    ($1, 'completed', $2, NOW(), NOW(), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
               RETURNING
                    id,
                    deal_id,
                    measurement_state,
                    tested_at,
                    working_url,
                    retrievability_percent,
                    large_files_percent,
                    car_files_percent,
                    sector_utilization_percent,
                    manifest_snapshot_id,
                    deal_size_bytes,
                    manifest_size_bytes,
                    content_matches_deal,
                    sampled_piece_count,
                    size_matched_percent,
                    avg_response_time_ms,
                    is_consistent,
                    is_reliable,
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    piece_count,
                    success_count,
                    failed_count
            "#,
            run.deal_id,
            run.measurement_state,
            run.provider_id,
            run.client_id.as_deref(),
            run.working_url.as_deref(),
            run.retrievability_percent.as_ref(),
            run.large_files_percent.as_ref(),
            run.car_files_percent.as_ref(),
            run.sector_utilization_percent.as_ref(),
            run.manifest_snapshot_id,
            run.deal_size_bytes.as_ref(),
            run.manifest_size_bytes.as_ref(),
            run.content_matches_deal,
            run.sampled_piece_count,
            run.size_matched_percent.as_ref(),
            run.avg_response_time_ms.as_ref(),
            run.is_consistent,
            run.is_reliable,
            run.result_code.clone() as ResultCode,
            run.piece_count,
            run.success_count,
            run.failed_count,
        )
        .fetch_one(&mut *tx)
        .await?;

        for piece_result in &run.piece_results {
            sqlx::query!(
                r#"INSERT INTO
                        deal_sli_piece_results (
                            run_id,
                            deal_id,
                            piece_index,
                            piece_cid,
                            url_tested,
                            success,
                            content_length,
                            manifest_snapshot_id,
                            file_size_bytes,
                            observed_size_bytes,
                            size_matched,
                            manifest_response_time_ms,
                            is_valid_car,
                            result_code,
                            tested_at
                        )
                   VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
                "#,
                inserted.id,
                piece_result.deal_id,
                piece_result.piece_index,
                piece_result.piece_cid,
                piece_result.url_tested,
                piece_result.success,
                piece_result.content_length,
                piece_result.manifest_snapshot_id,
                piece_result.file_size_bytes.as_ref(),
                piece_result.observed_size_bytes,
                piece_result.size_matched,
                piece_result.manifest_response_time_ms,
                piece_result.is_valid_car,
                piece_result.result_code.clone() as ResultCode,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(DealSliLatestRun {
            deal_id: inserted.deal_id,
            measurement_state: inserted.measurement_state,
            tested_at: inserted.tested_at,
            working_url: inserted.working_url,
            retrievability_percent: inserted.retrievability_percent,
            large_files_percent: inserted.large_files_percent,
            car_files_percent: inserted.car_files_percent,
            sector_utilization_percent: inserted.sector_utilization_percent,
            manifest_snapshot_id: inserted.manifest_snapshot_id,
            deal_size_bytes: inserted.deal_size_bytes,
            manifest_size_bytes: inserted.manifest_size_bytes,
            content_matches_deal: inserted.content_matches_deal,
            sampled_piece_count: inserted.sampled_piece_count,
            size_matched_percent: inserted.size_matched_percent,
            avg_response_time_ms: inserted.avg_response_time_ms,
            is_consistent: inserted.is_consistent,
            is_reliable: inserted.is_reliable,
            result_code: inserted.result_code,
            error_code: inserted.error_code,
            piece_count: inserted.piece_count,
            success_count: inserted.success_count,
            failed_count: inserted.failed_count,
        })
    }
}

fn validate_measured_pieces_are_unchanged(
    requested_pieces: &[NewDealSliPiece],
    existing_pieces: &[DealSliPiece],
) -> Result<()> {
    let requested_by_index = requested_pieces
        .iter()
        .map(|piece| (piece.piece_index, piece))
        .collect::<BTreeMap<_, _>>();

    if requested_by_index.len() != requested_pieces.len() {
        return Err(eyre!(
            "deal SLI pieces contain duplicate piece_index values"
        ));
    }

    if requested_by_index.len() != existing_pieces.len() {
        return Err(eyre!(
            "deal SLI pieces cannot be added or removed after measurement results exist"
        ));
    }

    for existing in existing_pieces {
        let Some(requested) = requested_by_index.get(&existing.piece_index) else {
            return Err(eyre!(
                "deal SLI piece {} cannot be removed after measurement results exist",
                existing.piece_index
            ));
        };

        if existing.piece_cid != requested.piece_cid
            || existing.piece_size_bytes != requested.piece_size_bytes
            || existing.file_size_bytes != requested.file_size_bytes
        {
            return Err(eyre!(
                "deal SLI piece {} cid or size cannot change after measurement results exist",
                existing.piece_index
            ));
        }
    }

    Ok(())
}

fn validate_measured_target_is_unchanged(
    requested: &NewDealSliTarget,
    existing: &DealSliTarget,
) -> Result<()> {
    if existing.deal_version != requested.deal_version
        || existing.provider_id != requested.provider_id
        || existing.client_id != requested.client_id
        || existing.deal_size_bytes != requested.deal_size_bytes
        || existing.manifest_hash != requested.manifest_hash
        || existing.manifest_location != requested.manifest_location
        || existing.retrievability_bps != requested.requirements.retrievability_bps
        || existing.bandwidth_mbps != requested.requirements.bandwidth_mbps
        || existing.latency_ms != requested.requirements.latency_ms
    {
        return Err(eyre!(
            "deal SLI target identity cannot change after measurement results exist"
        ));
    }

    Ok(())
}

fn validate_run_target_pieces_are_current(
    expected_pieces: &[DealSliRunPieceSnapshot],
    current_pieces: &[DealSliPiece],
) -> Result<()> {
    if expected_pieces.len() != current_pieces.len() {
        return Err(eyre!(
            "deal SLI run target pieces changed before run insertion"
        ));
    }

    for (expected, current) in expected_pieces.iter().zip(current_pieces) {
        if expected.piece_index != current.piece_index
            || expected.piece_cid != current.piece_cid
            || expected.piece_size_bytes != current.piece_size_bytes
            || expected.manifest_snapshot_id != current.manifest_snapshot_id
            || expected.file_size_bytes != current.file_size_bytes
        {
            return Err(eyre!(
                "deal SLI run target piece {} changed before run insertion",
                expected.piece_index
            ));
        }
    }

    Ok(())
}
