use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use sqlx::types::BigDecimal;

use crate::{
    api::deals::{
        DealLatestMeasurementResponse, DealPerformanceResponse, DealPieceTarget,
        DealSliRequirements, DealTargetResponse, DealTargetUpsertRequest, DealVersion,
        MeasurementState,
    },
    config::{Config, MIN_VALID_CONTENT_LENGTH},
    http_client::build_client,
    repository::{
        DealSliLatestRun, DealSliPiece, DealSliRepository, DealSliRequirementValues,
        DealSliRunPieceSnapshot, DealSliTargetWithPieces, NewCompletedDealSliRun, NewDealSliPiece,
        NewDealSliPieceResult, NewDealSliTarget, StorageProviderRepository,
    },
    types::{ErrorCode, ProviderAddress, ProviderId, ResultCode, UrlTestResult},
    url_tester::test_urls_double_tap,
};

const MAX_MANUAL_RUN_URL_TESTS: usize = 512;

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

        self.repo
            .upsert_target(&target)
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

        let stored = self.repo.get_target(deal_id).await?.ok_or_else(|| {
            DealSliServiceError::NotFound(format!("Deal target {deal_id} not found"))
        })?;

        let provider_id = ProviderId::new(stored.target.provider_id.clone()).map_err(|error| {
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
                self.run_cached_endpoint_measurement(deal_id, &stored, endpoints)
                    .await?
            }
            None => self
                .repo
                .insert_completed_run_with_piece_results(&NewCompletedDealSliRun {
                    deal_id: deal_id.to_string(),
                    target_pieces: target_piece_snapshots(&stored.pieces),
                    measurement_state: MeasurementState::Failed.as_str().to_string(),
                    provider_id: stored.target.provider_id.clone(),
                    client_id: stored.target.client_id.clone(),
                    working_url: None,
                    retrievability_percent: None,
                    large_files_percent: None,
                    car_files_percent: None,
                    sector_utilization_percent: None,
                    is_consistent: None,
                    is_reliable: None,
                    result_code: ResultCode::MissingHttpAddrFromCidContact,
                    piece_count: stored.pieces.len() as i32,
                    success_count: 0,
                    failed_count: stored.pieces.len() as i32,
                    piece_results: Vec::new(),
                })
                .await
                .map_err(map_run_insert_error)?,
        };

        Ok(map_latest_response(latest))
    }

    async fn run_cached_endpoint_measurement(
        &self,
        deal_id: &str,
        stored: &DealSliTargetWithPieces,
        endpoints: Vec<String>,
    ) -> std::result::Result<DealSliLatestRun, DealSliServiceError> {
        let client = build_client(&self.config)
            .map_err(|error| DealSliServiceError::Internal(color_eyre::Report::from(error)))?;
        let test_contexts = build_piece_test_contexts(&endpoints, &stored.pieces);
        if test_contexts.len() > MAX_MANUAL_RUN_URL_TESTS {
            return Err(DealSliServiceError::InvalidRequest(format!(
                "manual Deal SLI run would test {} URLs, limit is {}",
                test_contexts.len(),
                MAX_MANUAL_RUN_URL_TESTS
            )));
        }

        let urls = test_contexts
            .iter()
            .map(|context| context.url.clone())
            .collect::<Vec<_>>();
        let url_results = test_urls_double_tap(&client, urls).await;

        let successful_piece_indexes = test_contexts
            .iter()
            .zip(url_results.iter())
            .filter_map(|(context, result)| result.success.then_some(context.piece_index))
            .collect::<BTreeSet<_>>();
        let large_piece_indexes = test_contexts
            .iter()
            .zip(url_results.iter())
            .filter_map(|(context, result)| {
                (result.content_length.unwrap_or_default() >= MIN_VALID_CONTENT_LENGTH)
                    .then_some(context.piece_index)
            })
            .collect::<BTreeSet<_>>();
        let car_piece_indexes = test_contexts
            .iter()
            .zip(url_results.iter())
            .filter_map(|(context, result)| result.is_valid_car.then_some(context.piece_index))
            .collect::<BTreeSet<_>>();

        let piece_count = stored.pieces.len() as i32;
        let success_count = successful_piece_indexes.len() as i32;
        let failed_count = piece_count - success_count;
        let result_code = if success_count == piece_count {
            ResultCode::Success
        } else {
            ResultCode::FailedToGetWorkingUrl
        };
        let piece_results = test_contexts
            .iter()
            .zip(url_results.iter())
            .map(|(context, result)| map_piece_result(deal_id, context, result))
            .collect::<Vec<_>>();
        let working_url = url_results
            .iter()
            .filter(|result| result.success)
            .max_by_key(|result| result.content_length)
            .map(|result| result.url.clone());

        self.repo
            .insert_completed_run_with_piece_results(&NewCompletedDealSliRun {
                deal_id: deal_id.to_string(),
                target_pieces: target_piece_snapshots(&stored.pieces),
                measurement_state: MeasurementState::Fresh.as_str().to_string(),
                provider_id: stored.target.provider_id.clone(),
                client_id: stored.target.client_id.clone(),
                working_url,
                retrievability_percent: percent(success_count, piece_count),
                large_files_percent: percent(large_piece_indexes.len() as i32, piece_count),
                car_files_percent: percent(car_piece_indexes.len() as i32, piece_count),
                sector_utilization_percent: None,
                is_consistent: Some(url_results.iter().all(|result| result.consistent)),
                is_reliable: Some(success_count == piece_count),
                result_code,
                piece_count,
                success_count,
                failed_count,
                piece_results,
            })
            .await
            .map_err(map_run_insert_error)
    }
}

#[derive(Debug)]
struct DealSliPieceTestContext {
    piece_index: i32,
    piece_cid: String,
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
                url: format!("{endpoint}/piece/{}", piece.piece_cid),
            })
        })
        .collect()
}

fn target_piece_snapshots(pieces: &[DealSliPiece]) -> Vec<DealSliRunPieceSnapshot> {
    pieces
        .iter()
        .map(|piece| DealSliRunPieceSnapshot {
            piece_index: piece.piece_index,
            piece_cid: piece.piece_cid.clone(),
            piece_size_bytes: piece.piece_size_bytes.clone(),
        })
        .collect()
}

fn map_piece_result(
    deal_id: &str,
    context: &DealSliPieceTestContext,
    result: &UrlTestResult,
) -> NewDealSliPieceResult {
    NewDealSliPieceResult {
        deal_id: deal_id.to_string(),
        piece_index: context.piece_index,
        piece_cid: context.piece_cid.clone(),
        url_tested: result.url.clone(),
        success: result.success,
        content_length: result
            .content_length
            .and_then(|value| i64::try_from(value).ok()),
        is_valid_car: result.is_valid_car,
        result_code: if result.success {
            ResultCode::Success
        } else {
            ResultCode::FailedToGetWorkingUrl
        },
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

fn parse_piece_size_bytes(value: String) -> std::result::Result<BigDecimal, DealSliServiceError> {
    if value.is_empty() || !value.chars().all(|c| c.is_ascii_digit()) {
        return Err(DealSliServiceError::InvalidRequest(
            "piece_size_bytes must be a base-10 integer string".to_string(),
        ));
    }

    value.parse::<BigDecimal>().map_err(|error| {
        DealSliServiceError::InvalidRequest(format!(
            "piece_size_bytes must be a base-10 integer string: {error}"
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
    if request.pieces.is_empty() {
        return Err(DealSliServiceError::InvalidRequest(
            "pieces must not be empty".to_string(),
        ));
    }

    let pieces = request
        .pieces
        .into_iter()
        .enumerate()
        .map(
            |(index, piece)| -> std::result::Result<NewDealSliPiece, DealSliServiceError> {
                let piece_size_bytes = piece
                    .piece_size_bytes
                    .map(parse_piece_size_bytes)
                    .transpose()?;

                Ok(NewDealSliPiece {
                    piece_index: index as i32,
                    piece_cid: piece.piece_cid,
                    piece_size_bytes,
                    allocation_id: piece.allocation_id,
                    claim_id: piece.claim_id,
                })
            },
        )
        .collect::<std::result::Result<Vec<_>, _>>()?;

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
        manifest_hash: request.manifest_hash,
        manifest_location: request.manifest_location,
        requirements,
        pieces,
    })
}

fn map_target_response(stored: DealSliTargetWithPieces) -> DealTargetResponse {
    DealTargetResponse {
        deal_id: stored.target.deal_id,
        deal_version: DealVersion::V2,
        provider_id: Some(stored.target.provider_id),
        client: stored.target.client_id,
        manifest_hash: stored.target.manifest_hash,
        manifest_location: stored.target.manifest_location,
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
                allocation_id: piece.allocation_id,
                claim_id: piece.claim_id,
            })
            .collect(),
        created_at: Some(stored.target.created_at),
        updated_at: Some(stored.target.updated_at),
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
        large_files_percent: run.large_files_percent.as_ref().and_then(bigdecimal_to_f64),
        car_files_percent: run.car_files_percent.as_ref().and_then(bigdecimal_to_f64),
        sector_utilization_percent: run
            .sector_utilization_percent
            .as_ref()
            .and_then(bigdecimal_to_f64),
        is_consistent: run.is_consistent,
        is_reliable: run.is_reliable,
        result_code: run.result_code,
        error_code: run.error_code,
        piece_count: run.piece_count as u32,
        success_count: run.success_count as u32,
        failed_count: run.failed_count as u32,
        performance: DealPerformanceResponse::default(),
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
