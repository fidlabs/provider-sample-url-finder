use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::types::{ErrorCode as UrlErrorCode, ResultCode};

#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams)]
pub struct DealPath {
    #[param(value_type = String)]
    pub deal_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum DealVersion {
    #[default]
    V2,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementState {
    Missing,
    Fresh,
    Stale,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DealSliRequirements {
    pub retrievability_bps: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_mbps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealPieceTarget {
    pub piece_cid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub piece_size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_cid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub piece_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DealTargetUpsertRequest {
    #[serde(default)]
    pub deal_version: DealVersion,
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    pub deal_size_bytes: String,
    pub manifest_hash: String,
    pub manifest_location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requirements: Option<DealSliRequirements>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealManifestSnapshotResponse {
    pub id: String,
    pub manifest_hash: String,
    pub manifest_location: String,
    pub fetched_at: DateTime<Utc>,
    pub content_byte_length: i64,
    pub piece_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealTargetResponse {
    pub deal_id: String,
    pub deal_version: DealVersion,
    pub provider_id: Option<String>,
    pub client: Option<String>,
    pub deal_size_bytes: Option<String>,
    pub manifest_hash: Option<String>,
    pub manifest_location: Option<String>,
    pub manifest_snapshot: Option<DealManifestSnapshotResponse>,
    pub requirements: Option<DealSliRequirements>,
    #[serde(default)]
    pub pieces: Vec<DealPieceTarget>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealLatestMeasurementResponse {
    pub deal_id: String,
    pub measurement_state: MeasurementState,
    pub tested_at: Option<DateTime<Utc>>,
    pub working_url: Option<String>,
    pub retrievability_percent: Option<f64>,
    pub manifest_snapshot_id: Option<String>,
    pub deal_size_bytes: Option<String>,
    pub manifest_size_bytes: Option<String>,
    pub content_matches_deal: Option<bool>,
    pub sampled_piece_count: Option<u32>,
    pub size_matched_percent: Option<f64>,
    pub avg_response_time_ms: Option<f64>,
    pub is_reliable: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub error_code: Option<UrlErrorCode>,
    pub porep_slis: DealPorepSliResponse,
    #[serde(default)]
    pub bms_results: Vec<DealBmsResultResponse>,
    pub piece_count: u32,
    pub success_count: u32,
    pub failed_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealPorepSliResponse {
    pub retrievability_bps: Option<u16>,
    pub bandwidth_mbps: Option<u32>,
    pub latency_ms: Option<u32>,
    pub indexing_pct: Option<u8>,
}

impl DealPorepSliResponse {
    pub fn empty() -> Self {
        Self {
            retrievability_bps: None,
            bandwidth_mbps: None,
            latency_ms: None,
            indexing_pct: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealBmsResultResponse {
    pub piece_index: u32,
    pub piece_cid: String,
    pub bms_job_id: String,
    pub url_tested: String,
    pub routing_key: String,
    pub worker_count: u32,
    pub status: String,
    pub ping_avg_ms: Option<f64>,
    pub head_avg_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
    pub download_speed_mbps: Option<f64>,
    pub error_message: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl DealLatestMeasurementResponse {
    pub fn missing(deal_id: String) -> Self {
        Self::missing_with_piece_count(deal_id, 0)
    }

    pub fn missing_with_piece_count(deal_id: String, piece_count: u32) -> Self {
        Self {
            deal_id,
            measurement_state: MeasurementState::Missing,
            tested_at: None,
            working_url: None,
            retrievability_percent: None,
            manifest_snapshot_id: None,
            deal_size_bytes: None,
            manifest_size_bytes: None,
            content_matches_deal: None,
            sampled_piece_count: None,
            size_matched_percent: None,
            avg_response_time_ms: None,
            is_reliable: None,
            result_code: None,
            error_code: None,
            porep_slis: DealPorepSliResponse::empty(),
            bms_results: vec![],
            piece_count,
            success_count: 0,
            failed_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn serializes_upsert_request_with_snake_case_fields() {
        let request = DealTargetUpsertRequest {
            deal_version: DealVersion::V2,
            provider_id: "f01234".to_string(),
            client: Some("f05678".to_string()),
            deal_size_bytes: "7112600059904".to_string(),
            manifest_hash: "43ff1a93b66d742e9f9efc3305acaa51c9297b7000145f35e968e2b42e7bf328"
                .to_string(),
            manifest_location: "https://example.com/manifest.json".to_string(),
            requirements: Some(DealSliRequirements {
                retrievability_bps: 9_500,
                bandwidth_mbps: Some(200),
                latency_ms: Some(150),
            }),
        };

        let value = serde_json::to_value(request).expect("request should serialize");

        assert_eq!(
            value,
            json!({
                "deal_version": "v2",
                "provider_id": "f01234",
                "client": "f05678",
                "deal_size_bytes": "7112600059904",
                "manifest_hash": "43ff1a93b66d742e9f9efc3305acaa51c9297b7000145f35e968e2b42e7bf328",
                "manifest_location": "https://example.com/manifest.json",
                "requirements": {
                    "retrievability_bps": 9500,
                    "bandwidth_mbps": 200,
                    "latency_ms": 150
                }
            })
        );
    }

    #[test]
    fn serializes_latest_placeholder_with_missing_state() {
        let value = serde_json::to_value(DealLatestMeasurementResponse::missing(
            "12345678901234567890".to_string(),
        ))
        .expect("latest shell response should serialize");

        assert_eq!(
            value,
            json!({
                "deal_id": "12345678901234567890",
                "measurement_state": "missing",
                "tested_at": null,
                "working_url": null,
                "retrievability_percent": null,
                "manifest_snapshot_id": null,
                "deal_size_bytes": null,
                "manifest_size_bytes": null,
                "content_matches_deal": null,
                "sampled_piece_count": null,
                "size_matched_percent": null,
                "avg_response_time_ms": null,
                "is_reliable": null,
                "result_code": null,
                "error_code": null,
                "porep_slis": {
                    "retrievability_bps": null,
                    "bandwidth_mbps": null,
                    "latency_ms": null,
                    "indexing_pct": null
                },
                "bms_results": [],
                "piece_count": 0,
                "success_count": 0,
                "failed_count": 0
            })
        );
    }
}
