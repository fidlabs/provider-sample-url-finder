use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use sqlx::types::BigDecimal;
use uuid::Uuid;

use crate::{
    api::deals::{
        DealLatestMeasurementResponse, DealManifestSnapshotResponse, DealPieceTarget,
        DealSliRequirements, DealTargetResponse, DealTargetUpsertRequest, DealVersion,
        MeasurementState,
    },
    config::Config,
    http_client::build_client,
    repository::{
        DealSliLatestRun, DealSliManifestSnapshot, DealSliPiece, DealSliRepository,
        DealSliRequirementValues, DealSliRunPieceSnapshot, DealSliRunTarget,
        DealSliTargetWithPieces, NewCompletedDealSliRun, NewDealSliManifestSnapshot,
        NewDealSliPiece, NewDealSliPieceResult, NewDealSliTarget, StorageProviderRepository,
    },
    services::deal_manifest::{FetchedManifestSnapshot, fetch_manifest_snapshot},
    types::{ErrorCode, ProviderAddress, ProviderId, ResultCode},
    url_tester::{ManifestUrlTestResult, test_manifest_urls_double_tap},
};

const MAX_MANUAL_RUN_URL_TESTS: usize = 2_048;
const MANIFEST_SAMPLE_SIZE: i64 = 100;
const MAX_NUMERIC_DIGITS: usize = 78;

#[derive(Debug)]
pub enum DealSliServiceError {
    InvalidRequest(String),
    NotFound(String),
    Internal(color_eyre::Report),
}

impl From<color_eyre::Report> for DealSliServiceError {
    fn from(error: color_eyre::Report) -> Self {
        Self::Internal(error)
    }
}

#[derive(Clone)]
pub struct DealSliService {
    repo: Arc<DealSliRepository>,
    storage_provider_repo: Arc<StorageProviderRepository>,
    config: Arc<Config>,
}

impl DealSliService {
    pub fn new(
        repo: Arc<DealSliRepository>,
        storage_provider_repo: Arc<StorageProviderRepository>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            repo,
            storage_provider_repo,
            config,
        }
    }

    pub async fn upsert_target(
        &self,
        deal_id: &str,
        request: DealTargetUpsertRequest,
    ) -> std::result::Result<DealTargetResponse, DealSliServiceError> {
        validate_deal_id(deal_id)?;
        let target = map_upsert_request(deal_id, request)?;
        let client = build_client(&self.config)
            .map_err(|error| DealSliServiceError::Internal(color_eyre::Report::from(error)))?;
        let fetched_manifest = fetch_manifest_snapshot(
            &client,
            target.manifest_location.as_deref().unwrap_or_default(),
            target.manifest_hash.as_deref().unwrap_or_default(),
        )
        .await
        .map_err(|error| DealSliServiceError::InvalidRequest(error.to_string()))?;
        let snapshot = map_manifest_snapshot(deal_id, &fetched_manifest);
        let pieces = map_manifest_pieces(fetched_manifest);

        self.repo
            .upsert_manifest_target(&target, &snapshot, &pieces)
            .await
            .map_err(map_target_write_error)?;

        let stored = self.repo.get_target(deal_id).await?.ok_or_else(|| {
            DealSliServiceError::NotFound(format!("Deal target {deal_id} not found"))
        })?;

        Ok(map_target_response(stored))
    }

    pub async fn get_target(
        &self,
        deal_id: &str,
    ) -> std::result::Result<DealTargetResponse, DealSliServiceError> {
        validate_deal_id(deal_id)?;

        let stored = self.repo.get_target(deal_id).await?.ok_or_else(|| {
            DealSliServiceError::NotFound(format!("Deal target {deal_id} not found"))
        })?;

        Ok(map_target_response(stored))
    }

    pub async fn get_latest(
        &self,
        deal_id: &str,
    ) -> std::result::Result<DealLatestMeasurementResponse, DealSliServiceError> {
        validate_deal_id(deal_id)?;

        let stored = self.repo.get_target(deal_id).await?.ok_or_else(|| {
            DealSliServiceError::NotFound(format!("Deal target {deal_id} not found"))
        })?;

        let latest = self.repo.get_latest_completed_run(deal_id).await?;
        Ok(match latest {
            Some(run) => map_latest_response(run),
            None => DealLatestMeasurementResponse::missing_with_piece_count(
                deal_id.to_string(),
                stored.pieces.len() as u32,
            ),
        })
    }

    pub async fn create_run(
        &self,
        deal_id: &str,
    ) -> std::result::Result<DealLatestMeasurementResponse, DealSliServiceError> {
        validate_deal_id(deal_id)?;

        let run_target = self.repo.get_run_target(deal_id).await?.ok_or_else(|| {
            DealSliServiceError::NotFound(format!("Deal target {deal_id} not found"))
        })?;

        let provider_id =
            ProviderId::new(run_target.target.provider_id.clone()).map_err(|error| {
                DealSliServiceError::InvalidRequest(format!("Invalid provider_id: {error}"))
            })?;

        let cached_endpoints = self
            .storage_provider_repo
            .get_by_provider_id(&provider_id)
            .await?
            .and_then(|provider| provider.cached_http_endpoints)
            .filter(|endpoints| !endpoints.is_empty());

        let latest = match cached_endpoints {
            Some(endpoints) => {
                self.run_cached_endpoint_measurement(deal_id, &run_target, endpoints)
                    .await?
            }
            None => {
                let run = build_no_endpoint_run(deal_id, &run_target)?;
                self.repo
                    .insert_completed_run_with_piece_results(&run)
                    .await
                    .map_err(map_run_insert_error)?
            }
        };

        Ok(map_latest_response(latest))
    }

    async fn run_cached_endpoint_measurement(
        &self,
        deal_id: &str,
        run_target: &DealSliRunTarget,
        endpoints: Vec<String>,
    ) -> std::result::Result<DealSliLatestRun, DealSliServiceError> {
        let client = build_client(&self.config)
            .map_err(|error| DealSliServiceError::Internal(color_eyre::Report::from(error)))?;
        let manifest_snapshot_id =
            run_target
                .target
                .active_manifest_snapshot_id
                .ok_or_else(|| {
                    DealSliServiceError::InvalidRequest("missing manifest snapshot".to_string())
                })?;
        let total_piece_count = usize::try_from(run_target.manifest_piece_count).map_err(|_| {
            DealSliServiceError::InvalidRequest(
                "manifest piece count exceeds usize::MAX".to_string(),
            )
        })?;
        let sample_size = total_piece_count.min(MANIFEST_SAMPLE_SIZE as usize);
        let sampled_pieces = self
            .repo
            .sample_manifest_pieces(deal_id, manifest_snapshot_id, sample_size as i64)
            .await?;
        let planned_url_tests = endpoints
            .len()
            .checked_mul(sampled_pieces.len())
            .ok_or_else(|| {
                DealSliServiceError::InvalidRequest(
                    "manual Deal SLI run URL fanout overflowed usize".to_string(),
                )
            })?;
        if planned_url_tests > MAX_MANUAL_RUN_URL_TESTS {
            return Err(DealSliServiceError::InvalidRequest(format!(
                "manual Deal SLI run would test {planned_url_tests} URLs, limit is {MAX_MANUAL_RUN_URL_TESTS}"
            )));
        }
        let test_contexts = build_piece_test_contexts(&endpoints, &sampled_pieces);

        let tests = test_contexts
            .iter()
            .map(|context| {
                (
                    context.url.clone(),
                    context.expected_file_size_bytes.unwrap_or(-1),
                )
            })
            .collect::<Vec<_>>();
        let url_results = test_manifest_urls_double_tap(&client, tests).await;
        let aggregate = aggregate_manifest_results(&test_contexts, &url_results);

        let run = build_manifest_measurement_run(
            deal_id,
            run_target,
            &sampled_pieces,
            &test_contexts,
            &url_results,
            &aggregate,
        )?;

        self.repo
            .insert_completed_run_with_piece_results(&run)
            .await
            .map_err(map_run_insert_error)
    }
}

#[derive(Debug)]
struct DealSliPieceTestContext {
    piece_index: i32,
    piece_cid: String,
    manifest_snapshot_id: Option<Uuid>,
    file_size_bytes: Option<BigDecimal>,
    expected_file_size_bytes: Option<i64>,
    url: String,
}

fn build_piece_test_contexts(
    endpoints: &[String],
    pieces: &[DealSliPiece],
) -> Vec<DealSliPieceTestContext> {
    endpoints
        .iter()
        .flat_map(|endpoint| {
            let endpoint = endpoint.trim_end_matches('/');
            pieces.iter().map(move |piece| DealSliPieceTestContext {
                piece_index: piece.piece_index,
                piece_cid: piece.piece_cid.clone(),
                manifest_snapshot_id: piece.manifest_snapshot_id,
                file_size_bytes: piece.file_size_bytes.clone(),
                expected_file_size_bytes: piece
                    .file_size_bytes
                    .as_ref()
                    .and_then(bigdecimal_to_i64),
                url: format!("{endpoint}/piece/{}", urlencoding::encode(&piece.piece_cid)),
            })
        })
        .collect()
}

fn build_no_endpoint_run(
    deal_id: &str,
    run_target: &DealSliRunTarget,
) -> std::result::Result<NewCompletedDealSliRun, DealSliServiceError> {
    let piece_count = i32::try_from(run_target.manifest_piece_count).map_err(|_| {
        DealSliServiceError::InvalidRequest("manifest piece count exceeds i32::MAX".to_string())
    })?;
    let content_matches_deal = content_matches_deal(
        run_target.target.deal_size_bytes.as_ref(),
        run_target.manifest_size_bytes.as_ref(),
    );

    Ok(NewCompletedDealSliRun {
        deal_id: deal_id.to_string(),
        target_pieces: Vec::new(),
        measurement_state: MeasurementState::Failed.as_str().to_string(),
        provider_id: run_target.target.provider_id.clone(),
        client_id: run_target.target.client_id.clone(),
        working_url: None,
        retrievability_percent: None,
        large_files_percent: None,
        car_files_percent: None,
        sector_utilization_percent: None,
        manifest_snapshot_id: run_target.target.active_manifest_snapshot_id,
        deal_size_bytes: run_target.target.deal_size_bytes.clone(),
        manifest_size_bytes: run_target.manifest_size_bytes.clone(),
        content_matches_deal: Some(content_matches_deal),
        sampled_piece_count: Some(0),
        size_matched_percent: None,
        avg_response_time_ms: None,
        is_consistent: None,
        is_reliable: None,
        result_code: ResultCode::MissingHttpAddrFromCidContact,
        piece_count,
        success_count: 0,
        failed_count: 0,
        piece_results: Vec::new(),
    })
}

fn build_manifest_measurement_run(
    deal_id: &str,
    run_target: &DealSliRunTarget,
    sampled_pieces: &[DealSliPiece],
    test_contexts: &[DealSliPieceTestContext],
    url_results: &[ManifestUrlTestResult],
    aggregate: &ManifestResultAggregate,
) -> std::result::Result<NewCompletedDealSliRun, DealSliServiceError> {
    let manifest_snapshot_id = run_target
        .target
        .active_manifest_snapshot_id
        .ok_or_else(|| {
            DealSliServiceError::InvalidRequest("missing manifest snapshot".to_string())
        })?;
    let piece_count = i32::try_from(run_target.manifest_piece_count).map_err(|_| {
        DealSliServiceError::InvalidRequest("manifest piece count exceeds i32::MAX".to_string())
    })?;
    let sampled_piece_count = i32::try_from(sampled_pieces.len()).map_err(|_| {
        DealSliServiceError::InvalidRequest("sampled piece count exceeds i32::MAX".to_string())
    })?;
    let success_count = aggregate.size_matched_count;
    let failed_count = sampled_piece_count - success_count;
    let piece_results = test_contexts
        .iter()
        .zip(url_results.iter())
        .map(|(context, result)| map_manifest_piece_result(deal_id, context, result))
        .collect::<Vec<_>>();
    let working_url = url_results
        .iter()
        .filter(|result| result.size_matched)
        .max_by_key(|result| result.observed_size_bytes)
        .map(|result| result.url.clone());
    let result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };
    let content_matches_deal = content_matches_deal(
        run_target.target.deal_size_bytes.as_ref(),
        run_target.manifest_size_bytes.as_ref(),
    );

    Ok(NewCompletedDealSliRun {
        deal_id: deal_id.to_string(),
        target_pieces: target_piece_snapshots(sampled_pieces),
        measurement_state: MeasurementState::Fresh.as_str().to_string(),
        provider_id: run_target.target.provider_id.clone(),
        client_id: run_target.target.client_id.clone(),
        working_url,
        retrievability_percent: percent(aggregate.retrievable_count, sampled_piece_count),
        large_files_percent: None,
        car_files_percent: None,
        sector_utilization_percent: None,
        manifest_snapshot_id: Some(manifest_snapshot_id),
        deal_size_bytes: run_target.target.deal_size_bytes.clone(),
        manifest_size_bytes: run_target.manifest_size_bytes.clone(),
        content_matches_deal: Some(content_matches_deal),
        sampled_piece_count: Some(sampled_piece_count),
        size_matched_percent: percent(aggregate.size_matched_count, sampled_piece_count),
        avg_response_time_ms: average_response_time_ms(url_results),
        is_consistent: None,
        is_reliable: Some(success_count == sampled_piece_count),
        result_code,
        piece_count,
        success_count,
        failed_count,
        piece_results,
    })
}

fn target_piece_snapshots(pieces: &[DealSliPiece]) -> Vec<DealSliRunPieceSnapshot> {
    pieces
        .iter()
        .map(|piece| DealSliRunPieceSnapshot {
            piece_index: piece.piece_index,
            piece_cid: piece.piece_cid.clone(),
            piece_size_bytes: piece.piece_size_bytes.clone(),
            manifest_snapshot_id: piece.manifest_snapshot_id,
            file_size_bytes: piece.file_size_bytes.clone(),
        })
        .collect()
}

fn map_manifest_piece_result(
    deal_id: &str,
    context: &DealSliPieceTestContext,
    result: &ManifestUrlTestResult,
) -> NewDealSliPieceResult {
    NewDealSliPieceResult {
        deal_id: deal_id.to_string(),
        piece_index: context.piece_index,
        piece_cid: context.piece_cid.clone(),
        url_tested: result.url.clone(),
        success: result.size_matched,
        content_length: result.observed_size_bytes,
        manifest_snapshot_id: context.manifest_snapshot_id,
        file_size_bytes: context.file_size_bytes.clone(),
        observed_size_bytes: result.observed_size_bytes,
        size_matched: Some(result.size_matched),
        manifest_response_time_ms: result.response_time_ms,
        is_valid_car: false,
        result_code: if result.size_matched {
            ResultCode::Success
        } else {
            ResultCode::FailedToGetWorkingUrl
        },
    }
}

#[derive(Debug, Default)]
struct ManifestResultAggregate {
    retrievable_count: i32,
    size_matched_count: i32,
}

fn aggregate_manifest_results(
    contexts: &[DealSliPieceTestContext],
    results: &[ManifestUrlTestResult],
) -> ManifestResultAggregate {
    let mut by_piece = BTreeMap::<i32, (bool, bool)>::new();

    for (context, result) in contexts.iter().zip(results) {
        let entry = by_piece.entry(context.piece_index).or_default();
        entry.0 |= result.retrievable;
        entry.1 |= context.expected_file_size_bytes.is_some() && result.size_matched;
    }

    ManifestResultAggregate {
        retrievable_count: by_piece
            .values()
            .filter(|(retrievable, _)| *retrievable)
            .count() as i32,
        size_matched_count: by_piece
            .values()
            .filter(|(_, size_matched)| *size_matched)
            .count() as i32,
    }
}

fn percent(numerator: i32, denominator: i32) -> Option<BigDecimal> {
    if denominator == 0 {
        return None;
    }

    BigDecimal::from_str(&format!(
        "{:.2}",
        f64::from(numerator) * 100.0 / f64::from(denominator)
    ))
    .ok()
}

fn average_response_time_ms(results: &[ManifestUrlTestResult]) -> Option<BigDecimal> {
    let response_times = results
        .iter()
        .filter_map(|result| result.response_time_ms)
        .collect::<Vec<_>>();

    if response_times.is_empty() {
        return None;
    }

    BigDecimal::from_str(&format!(
        "{:.2}",
        response_times.iter().sum::<i64>() as f64 / response_times.len() as f64
    ))
    .ok()
}

fn content_matches_deal(
    deal_size_bytes: Option<&BigDecimal>,
    manifest_size_bytes: Option<&BigDecimal>,
) -> bool {
    match (deal_size_bytes, manifest_size_bytes) {
        (Some(deal_size_bytes), Some(manifest_size_bytes)) => {
            deal_size_bytes == manifest_size_bytes
        }
        _ => false,
    }
}

fn bigdecimal_to_i64(value: &BigDecimal) -> Option<i64> {
    value.to_string().parse().ok()
}

fn validate_deal_id(deal_id: &str) -> std::result::Result<(), DealSliServiceError> {
    if deal_id.is_empty() || !deal_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(DealSliServiceError::InvalidRequest(format!(
            "deal_id must be a decimal string, got {deal_id}"
        )));
    }

    Ok(())
}

fn map_target_write_error(error: color_eyre::Report) -> DealSliServiceError {
    let message = error.to_string();
    if message.contains("deal SLI pieces contain duplicate")
        || message.contains("deal SLI pieces cannot be added or removed")
        || message.contains("cid or size cannot change")
        || message.contains("target identity cannot change")
    {
        return DealSliServiceError::InvalidRequest(message);
    }

    DealSliServiceError::Internal(error)
}

fn map_run_insert_error(error: color_eyre::Report) -> DealSliServiceError {
    let message = error.to_string();
    if message.contains("changed before run insertion") {
        return DealSliServiceError::InvalidRequest(message);
    }

    DealSliServiceError::Internal(error)
}

fn parse_decimal_string(
    value: String,
    field: &str,
) -> std::result::Result<BigDecimal, DealSliServiceError> {
    if value.is_empty()
        || value.len() > MAX_NUMERIC_DIGITS
        || !value.chars().all(|c| c.is_ascii_digit())
    {
        return Err(DealSliServiceError::InvalidRequest(format!(
            "{field} must be a base-10 integer string"
        )));
    }

    value.parse::<BigDecimal>().map_err(|error| {
        DealSliServiceError::InvalidRequest(format!(
            "{field} must be a base-10 integer string: {error}"
        ))
    })
}

fn u32_to_i32(value: u32, field: &str) -> std::result::Result<i32, DealSliServiceError> {
    i32::try_from(value).map_err(|_| {
        DealSliServiceError::InvalidRequest(format!(
            "{field} must be less than or equal to i32::MAX"
        ))
    })
}

fn retrievability_bps_to_i32(value: u16) -> std::result::Result<i32, DealSliServiceError> {
    if value > 10_000 {
        return Err(DealSliServiceError::InvalidRequest(
            "retrievability_bps must be less than or equal to 10000".to_string(),
        ));
    }

    Ok(i32::from(value))
}

fn normalize_provider_id(value: String) -> std::result::Result<String, DealSliServiceError> {
    if value.starts_with("f0") {
        let address = ProviderAddress::new(value)
            .map_err(|error| DealSliServiceError::InvalidRequest(format!("{error}")))?;
        let id: ProviderId = address.into();
        return Ok(id.as_str().to_string());
    }

    ProviderId::new(value)
        .map(|id| id.as_str().to_string())
        .map_err(|error| DealSliServiceError::InvalidRequest(format!("{error}")))
}

fn map_upsert_request(
    deal_id: &str,
    request: DealTargetUpsertRequest,
) -> std::result::Result<NewDealSliTarget, DealSliServiceError> {
    if request.manifest_hash.trim().is_empty() {
        return Err(DealSliServiceError::InvalidRequest(
            "manifest_hash must not be empty".to_string(),
        ));
    }
    if request.manifest_location.trim().is_empty() {
        return Err(DealSliServiceError::InvalidRequest(
            "manifest_location must not be empty".to_string(),
        ));
    }
    let deal_size_bytes = parse_decimal_string(request.deal_size_bytes, "deal_size_bytes")?;

    let requirements = match request.requirements {
        Some(requirements) => DealSliRequirementValues {
            retrievability_bps: Some(retrievability_bps_to_i32(requirements.retrievability_bps)?),
            bandwidth_mbps: requirements
                .bandwidth_mbps
                .map(|value| u32_to_i32(value, "bandwidth_mbps"))
                .transpose()?,
            latency_ms: requirements
                .latency_ms
                .map(|value| u32_to_i32(value, "latency_ms"))
                .transpose()?,
        },
        None => DealSliRequirementValues::default(),
    };

    Ok(NewDealSliTarget {
        deal_id: deal_id.to_string(),
        deal_version: match request.deal_version {
            DealVersion::V2 => "v2".to_string(),
        },
        provider_id: normalize_provider_id(request.provider_id)?,
        client_id: request.client,
        deal_size_bytes: Some(deal_size_bytes),
        manifest_hash: Some(request.manifest_hash),
        manifest_location: Some(request.manifest_location),
        requirements,
    })
}

fn map_manifest_snapshot(
    deal_id: &str,
    fetched: &FetchedManifestSnapshot,
) -> NewDealSliManifestSnapshot {
    NewDealSliManifestSnapshot {
        deal_id: deal_id.to_string(),
        manifest_hash: fetched.manifest_hash.clone(),
        manifest_location: fetched.manifest_location.clone(),
        raw_content: fetched.raw_content.clone(),
        parsed_content: fetched.parsed_content.clone(),
        content_byte_length: fetched.content_byte_length,
        computed_hash: fetched.computed_hash.clone(),
    }
}

fn map_manifest_pieces(fetched: FetchedManifestSnapshot) -> Vec<NewDealSliPiece> {
    fetched
        .pieces
        .into_iter()
        .map(|piece| NewDealSliPiece {
            piece_index: piece.piece_index,
            piece_cid: piece.piece_cid,
            piece_size_bytes: piece.piece_size_bytes,
            manifest_snapshot_id: None,
            file_size_bytes: piece.file_size_bytes,
            root_cid: piece.root_cid,
            storage_path: piece.storage_path,
            piece_type: piece.piece_type,
            allocation_id: None,
            claim_id: None,
        })
        .collect()
}

fn map_target_response(stored: DealSliTargetWithPieces) -> DealTargetResponse {
    let piece_count = stored.pieces.len() as u32;
    DealTargetResponse {
        deal_id: stored.target.deal_id,
        deal_version: DealVersion::V2,
        provider_id: Some(stored.target.provider_id),
        client: stored.target.client_id,
        deal_size_bytes: stored.target.deal_size_bytes.map(|value| value.to_string()),
        manifest_hash: stored.target.manifest_hash,
        manifest_location: stored.target.manifest_location,
        manifest_snapshot: stored
            .manifest_snapshot
            .map(|snapshot| map_manifest_snapshot_response(snapshot, piece_count)),
        requirements: map_requirements(
            stored.target.retrievability_bps,
            stored.target.bandwidth_mbps,
            stored.target.latency_ms,
        ),
        pieces: stored
            .pieces
            .into_iter()
            .map(|piece| DealPieceTarget {
                piece_cid: piece.piece_cid,
                piece_size_bytes: piece.piece_size_bytes.map(|value| value.to_string()),
                file_size_bytes: piece.file_size_bytes.map(|value| value.to_string()),
                root_cid: piece.root_cid,
                storage_path: piece.storage_path,
                piece_type: piece.piece_type,
                allocation_id: piece.allocation_id,
                claim_id: piece.claim_id,
            })
            .collect(),
        created_at: Some(stored.target.created_at),
        updated_at: Some(stored.target.updated_at),
    }
}

fn map_manifest_snapshot_response(
    snapshot: DealSliManifestSnapshot,
    piece_count: u32,
) -> DealManifestSnapshotResponse {
    DealManifestSnapshotResponse {
        id: snapshot.id.to_string(),
        manifest_hash: snapshot.manifest_hash,
        manifest_location: snapshot.manifest_location,
        fetched_at: snapshot.fetched_at,
        content_byte_length: snapshot.content_byte_length,
        piece_count,
    }
}

fn map_requirements(
    retrievability_bps: Option<i32>,
    bandwidth_mbps: Option<i32>,
    latency_ms: Option<i32>,
) -> Option<DealSliRequirements> {
    if retrievability_bps.is_none() && bandwidth_mbps.is_none() && latency_ms.is_none() {
        return None;
    }

    Some(DealSliRequirements {
        retrievability_bps: retrievability_bps.unwrap_or_default() as u16,
        bandwidth_mbps: bandwidth_mbps.map(|value| value as u32),
        latency_ms: latency_ms.map(|value| value as u32),
    })
}

fn map_latest_response(run: DealSliLatestRun) -> DealLatestMeasurementResponse {
    DealLatestMeasurementResponse {
        deal_id: run.deal_id,
        measurement_state: MeasurementState::from_db_value(&run.measurement_state),
        tested_at: run.tested_at,
        working_url: run.working_url,
        retrievability_percent: run
            .retrievability_percent
            .as_ref()
            .and_then(bigdecimal_to_f64),
        manifest_snapshot_id: run.manifest_snapshot_id.map(|id| id.to_string()),
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
        is_reliable: run.is_reliable,
        result_code: run.result_code,
        error_code: run.error_code,
        piece_count: run.piece_count as u32,
        success_count: run.success_count as u32,
        failed_count: run.failed_count as u32,
    }
}

fn bigdecimal_to_f64(value: &BigDecimal) -> Option<f64> {
    value.to_string().parse().ok()
}

impl MeasurementState {
    pub fn from_db_value(value: &str) -> Self {
        match value {
            "fresh" => Self::Fresh,
            "stale" => Self::Stale,
            "failed" => Self::Failed,
            "skipped" => Self::Skipped,
            _ => Self::Missing,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

#[allow(dead_code)]
fn _preserve_type_reachability(_: Option<ErrorCode>) {}
