use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use serde_json::json;
use utoipa::{IntoParams, ToSchema};

use crate::types::{ErrorCode as UrlErrorCode, ResultCode};

#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams)]
pub struct DealPath {
    /// Decimal Filecoin deal ID.
    #[schema(example = "1234567890")]
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
    /// Required retrievability in basis points. `10000` means 100%.
    #[schema(example = 9500, minimum = 0, maximum = 10000)]
    pub retrievability_bps: u16,
    /// Optional minimum bandwidth requirement for completed BMS jobs.
    #[schema(example = 200)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_mbps: Option<u32>,
    /// Optional maximum latency requirement derived from BMS time-to-first-byte.
    #[schema(example = 150)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealPieceTarget {
    /// Piece CID derived from the fetched manifest. Callers do not submit this field.
    #[schema(example = "baga6ea4seaq")]
    pub piece_cid: String,
    /// Piece size from the manifest, serialized as a base-10 integer string.
    #[schema(example = "34359738368")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub piece_size_bytes: Option<String>,
    /// File size from the manifest, serialized as a base-10 integer string.
    #[schema(example = "16000000000")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size_bytes: Option<String>,
    /// Root CID from the manifest, when present.
    #[schema(example = "bafybeigdyrzt5sfp7udm7hu76uh7y26dvdwfk4dciwqz2aue3nbxtyr7vm")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_cid: Option<String>,
    /// Storage path from the manifest, when present.
    #[schema(example = "piece.car")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_path: Option<String>,
    /// Manifest piece type, when present.
    #[schema(example = "dag")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub piece_type: Option<String>,
    /// Allocation ID reserved for future chain-backed target metadata.
    #[schema(example = "12345")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<String>,
    /// Claim ID reserved for future chain-backed target metadata.
    #[schema(example = "67890")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
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
}))]
#[serde(deny_unknown_fields)]
pub struct DealTargetUpsertRequest {
    /// Deal contract version. Only `v2` is accepted.
    #[serde(default)]
    pub deal_version: DealVersion,
    /// Storage provider ID. Accepts `f01234` or `1234`; responses normalize to `1234`.
    #[schema(example = "f01234")]
    pub provider_id: String,
    /// Optional client ID associated with the deal.
    #[schema(example = "f05678")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    /// Deal size in bytes, serialized as a base-10 integer string.
    #[schema(example = "7112600059904")]
    pub deal_size_bytes: String,
    /// Expected Keccak-256 hash of the manifest body. A leading `0x` is accepted.
    #[schema(example = "43ff1a93b66d742e9f9efc3305acaa51c9297b7000145f35e968e2b42e7bf328")]
    pub manifest_hash: String,
    /// HTTP or HTTPS URL of the PoRep manifest to fetch and snapshot.
    #[schema(example = "https://example.com/manifest.json")]
    pub manifest_location: String,
    /// Optional SLI thresholds expected by PoRep Market.
    #[schema(inline)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requirements: Option<DealSliRequirements>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealManifestSnapshotResponse {
    /// Internal manifest snapshot ID.
    #[schema(example = "018f6fd1-64f8-7c30-9e0f-f43a1d8df9b1")]
    pub id: String,
    /// Verified manifest hash stored for this snapshot.
    #[schema(example = "43ff1a93b66d742e9f9efc3305acaa51c9297b7000145f35e968e2b42e7bf328")]
    pub manifest_hash: String,
    /// Manifest URL fetched for this snapshot.
    #[schema(example = "https://example.com/manifest.json")]
    pub manifest_location: String,
    /// Time when RPA fetched and stored this manifest.
    pub fetched_at: DateTime<Utc>,
    /// Raw manifest body length in bytes.
    #[schema(example = 2048)]
    pub content_byte_length: i64,
    /// Number of pieces derived from the manifest.
    #[schema(example = 2)]
    pub piece_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealTargetResponse {
    /// Decimal Filecoin deal ID.
    #[schema(example = "1234567890")]
    pub deal_id: String,
    pub deal_version: DealVersion,
    /// Normalized provider ID without the `f0` prefix.
    #[schema(example = "1234")]
    pub provider_id: Option<String>,
    /// Optional client ID associated with the deal.
    #[schema(example = "f05678")]
    pub client: Option<String>,
    /// Deal size in bytes, serialized as a base-10 integer string.
    #[schema(example = "7112600059904")]
    pub deal_size_bytes: Option<String>,
    /// Verified manifest hash stored for the active target.
    #[schema(example = "43ff1a93b66d742e9f9efc3305acaa51c9297b7000145f35e968e2b42e7bf328")]
    pub manifest_hash: Option<String>,
    /// Manifest URL fetched for the active target.
    #[schema(example = "https://example.com/manifest.json")]
    pub manifest_location: Option<String>,
    /// Active manifest snapshot used to derive pieces.
    pub manifest_snapshot: Option<DealManifestSnapshotResponse>,
    /// Optional SLI thresholds expected by PoRep Market.
    #[schema(inline)]
    pub requirements: Option<DealSliRequirements>,
    /// Pieces derived from the active manifest snapshot.
    #[serde(default)]
    pub pieces: Vec<DealPieceTarget>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealLatestMeasurementResponse {
    /// Decimal Filecoin deal ID.
    #[schema(example = "1234567890")]
    pub deal_id: String,
    /// Latest measurement state for the stored target.
    pub measurement_state: MeasurementState,
    /// Time when the latest run tested the target.
    pub tested_at: Option<DateTime<Utc>>,
    /// Largest size-matched piece URL found in the latest run.
    #[schema(example = "https://provider.example/piece/baga6ea4seaq")]
    pub working_url: Option<String>,
    /// Percent of sampled pieces that returned any retrievable response.
    #[schema(example = 50.0)]
    pub retrievability_percent: Option<f64>,
    /// Manifest snapshot measured by the latest run.
    #[schema(example = "018f6fd1-64f8-7c30-9e0f-f43a1d8df9b1")]
    pub manifest_snapshot_id: Option<String>,
    /// Deal size in bytes, serialized as a base-10 integer string.
    #[schema(example = "7112600059904")]
    pub deal_size_bytes: Option<String>,
    /// Sum of manifest file sizes, serialized as a base-10 integer string.
    #[schema(example = "7112600059904")]
    pub manifest_size_bytes: Option<String>,
    /// Whether `deal_size_bytes` exactly equals the sum of manifest file sizes.
    #[schema(example = true)]
    pub content_matches_deal: Option<bool>,
    /// Number of manifest pieces sampled in the latest run.
    #[schema(example = 100)]
    pub sampled_piece_count: Option<u32>,
    /// Percent of sampled pieces whose observed size matched manifest `fileSize`.
    #[schema(example = 50.0)]
    pub size_matched_percent: Option<f64>,
    /// Average response time across successful ranged GET checks.
    #[schema(example = 125.0)]
    pub avg_response_time_ms: Option<f64>,
    /// Legacy reliability flag for URL checks, when available.
    pub is_reliable: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub error_code: Option<UrlErrorCode>,
    /// PoRep-facing SLI values derived from retrievability and completed BMS jobs.
    pub porep_slis: DealPorepSliResponse,
    /// Completed or pending BMS jobs linked to successful piece URLs for this run.
    #[serde(default)]
    pub bms_results: Vec<DealBmsResultResponse>,
    /// Total manifest piece count for the target.
    #[schema(example = 250)]
    pub piece_count: u32,
    /// Number of sampled pieces with exact size-matched responses.
    #[schema(example = 50)]
    pub success_count: u32,
    /// Number of sampled pieces without exact size-matched responses.
    #[schema(example = 50)]
    pub failed_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealPorepSliResponse {
    /// Retrievability basis points derived from latest run percentage.
    #[schema(example = 5000)]
    pub retrievability_bps: Option<u16>,
    /// Minimum completed BMS download speed across linked piece jobs.
    #[schema(example = 500)]
    pub bandwidth_mbps: Option<u32>,
    /// Maximum completed BMS time-to-first-byte across linked piece jobs.
    #[schema(example = 100)]
    pub latency_ms: Option<u32>,
    /// Reserved for future indexing SLI support.
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
    /// Manifest piece index measured by this BMS job.
    #[schema(example = 0)]
    pub piece_index: u32,
    /// Piece CID measured by this BMS job.
    #[schema(example = "baga6ea4seaq")]
    pub piece_cid: String,
    /// BMS job UUID.
    #[schema(example = "018f6fd1-64f8-7c30-9e0f-f43a1d8df9b1")]
    pub bms_job_id: String,
    /// Piece URL submitted to BMS.
    #[schema(example = "https://provider.example/piece/baga6ea4seaq")]
    pub url_tested: String,
    /// BMS routing key returned by the job API.
    #[schema(example = "us_east")]
    pub routing_key: String,
    /// Number of BMS workers requested.
    #[schema(example = 10)]
    pub worker_count: u32,
    /// BMS job status.
    #[schema(example = "Completed")]
    pub status: String,
    /// Average ping from completed BMS worker data.
    #[schema(example = 25.0)]
    pub ping_avg_ms: Option<f64>,
    /// Average HEAD latency from completed BMS worker data.
    #[schema(example = 50.0)]
    pub head_avg_ms: Option<f64>,
    /// Time to first byte from completed BMS worker data.
    #[schema(example = 100.0)]
    pub ttfb_ms: Option<f64>,
    /// Download speed from completed BMS worker data.
    #[schema(example = 500.0)]
    pub download_speed_mbps: Option<f64>,
    /// Timeout or BMS error details, when the job failed locally.
    pub error_message: Option<String>,
    /// Completion timestamp when the BMS job reached a terminal state.
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
