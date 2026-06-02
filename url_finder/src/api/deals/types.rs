use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    api::providers::{BandwidthTestResponse, GeolocationTestResponse},
    types::{ErrorCode as UrlErrorCode, ResultCode},
};

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
    pub allocation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealTargetUpsertRequest {
    #[serde(default)]
    pub deal_version: DealVersion,
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requirements: Option<DealSliRequirements>,
    #[serde(default)]
    pub pieces: Vec<DealPieceTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealTargetResponse {
    pub deal_id: String,
    pub deal_version: DealVersion,
    pub provider_id: Option<String>,
    pub client: Option<String>,
    pub manifest_hash: Option<String>,
    pub manifest_location: Option<String>,
    pub requested_size_bytes: Option<String>,
    pub requirements: Option<DealSliRequirements>,
    #[serde(default)]
    pub pieces: Vec<DealPieceTarget>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct DealPerformanceResponse {
    pub bandwidth: Option<BandwidthTestResponse>,
    pub geolocation: Option<GeolocationTestResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DealLatestMeasurementResponse {
    pub deal_id: String,
    pub measurement_state: MeasurementState,
    pub tested_at: Option<DateTime<Utc>>,
    pub working_url: Option<String>,
    pub retrievability_percent: Option<f64>,
    pub large_files_percent: Option<f64>,
    pub car_files_percent: Option<f64>,
    pub sector_utilization_percent: Option<f64>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub result_code: Option<ResultCode>,
    pub error_code: Option<UrlErrorCode>,
    pub piece_count: u32,
    pub success_count: u32,
    pub failed_count: u32,
    #[serde(default)]
    pub performance: DealPerformanceResponse,
}

impl DealTargetResponse {
    pub fn placeholder(deal_id: String) -> Self {
        Self {
            deal_id,
            deal_version: DealVersion::V2,
            provider_id: None,
            client: None,
            manifest_hash: None,
            manifest_location: None,
            requested_size_bytes: None,
            requirements: None,
            pieces: Vec::new(),
            created_at: None,
            updated_at: None,
        }
    }

    pub fn from_upsert_request(deal_id: String, request: DealTargetUpsertRequest) -> Self {
        Self {
            deal_id,
            deal_version: request.deal_version,
            provider_id: Some(request.provider_id),
            client: request.client,
            manifest_hash: request.manifest_hash,
            manifest_location: request.manifest_location,
            requested_size_bytes: request.requested_size_bytes,
            requirements: request.requirements,
            pieces: request.pieces,
            created_at: None,
            updated_at: None,
        }
    }
}

impl DealLatestMeasurementResponse {
    pub fn missing(deal_id: String) -> Self {
        Self {
            deal_id,
            measurement_state: MeasurementState::Missing,
            tested_at: None,
            working_url: None,
            retrievability_percent: None,
            large_files_percent: None,
            car_files_percent: None,
            sector_utilization_percent: None,
            is_consistent: None,
            is_reliable: None,
            result_code: None,
            error_code: None,
            piece_count: 0,
            success_count: 0,
            failed_count: 0,
            performance: DealPerformanceResponse::default(),
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
            manifest_hash: Some("bafy-manifest".to_string()),
            manifest_location: Some("https://example.com/manifest.car".to_string()),
            requested_size_bytes: Some("2048".to_string()),
            requirements: Some(DealSliRequirements {
                retrievability_bps: 9_500,
                bandwidth_mbps: Some(200),
                latency_ms: Some(150),
            }),
            pieces: vec![DealPieceTarget {
                piece_cid: "baga6ea4seaq".to_string(),
                piece_size_bytes: Some("1024".to_string()),
                allocation_id: Some("44".to_string()),
                claim_id: Some("55".to_string()),
            }],
        };

        let value = serde_json::to_value(request).expect("request should serialize");

        assert_eq!(
            value,
            json!({
                "deal_version": "v2",
                "provider_id": "f01234",
                "client": "f05678",
                "manifest_hash": "bafy-manifest",
                "manifest_location": "https://example.com/manifest.car",
                "requested_size_bytes": "2048",
                "requirements": {
                    "retrievability_bps": 9500,
                    "bandwidth_mbps": 200,
                    "latency_ms": 150
                },
                "pieces": [{
                    "piece_cid": "baga6ea4seaq",
                    "piece_size_bytes": "1024",
                    "allocation_id": "44",
                    "claim_id": "55"
                }]
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
                "large_files_percent": null,
                "car_files_percent": null,
                "sector_utilization_percent": null,
                "is_consistent": null,
                "is_reliable": null,
                "result_code": null,
                "error_code": null,
                "piece_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "performance": {
                    "bandwidth": null,
                    "geolocation": null
                }
            })
        );
    }
}
