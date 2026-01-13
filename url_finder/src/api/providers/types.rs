use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::services::provider_service::{
    BandwidthResult, PerformanceData, ProviderData, SchedulingData,
};
use crate::types::{ErrorCode, ProviderAddress, ResultCode};

/// Common query parameters for extended response
#[derive(Debug, Clone, Deserialize, ToSchema, IntoParams, Default)]
pub struct ExtendedQuery {
    /// Include diagnostic and scheduling details
    #[serde(default)]
    pub extended: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BandwidthTestResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tested_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ping_avg_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_avg_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttfb_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_speed_mbps: Option<f64>,
    // Extended fields (only populated when extended=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url_tested: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GeolocationTestResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tested_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct PerformanceResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth: Option<BandwidthTestResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geolocation: Option<GeolocationTestResponse>,
}

/// Breakdown of inconsistent URL tests by cause (extended only)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InconsistentBreakdown {
    /// VALID+FAIL or FAIL+VALID - strategic timeout pattern
    pub gaming: usize,
    /// FAIL+FAIL - both taps failed
    pub both_failed: usize,
    /// SMALL+anything - error pages returned
    pub error_pages: usize,
    /// VALID+VALID but different Content-Length
    pub size_mismatch: usize,
}

/// Analysis metrics from URL testing (extended only)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AnalysisResponse {
    /// Number of URLs tested
    pub sample_count: usize,
    /// Number of URLs that returned valid data
    pub success_count: usize,
    /// Number of URLs that timed out
    pub timeout_count: usize,
    /// Number of URLs with inconsistent double-tap results
    pub inconsistent_count: usize,
    /// Breakdown of inconsistency causes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inconsistent_breakdown: Option<InconsistentBreakdown>,
    /// When this analysis was performed
    pub validated_at: DateTime<Utc>,
}

/// Diagnostic information (extended only)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DiagnosticsResponse {
    /// Result of the URL discovery
    pub result_code: ResultCode,
    /// Error details if discovery failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<ErrorCode>,
    /// Detailed analysis metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analysis: Option<AnalysisResponse>,
}

/// State of a scheduled task (extended only)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScheduleStateResponse {
    /// When the next run is scheduled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_at: Option<DateTime<Utc>>,
    /// Current status (null = ready, "pending" = in progress)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// When task entered pending state (URL discovery only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_since: Option<DateTime<Utc>>,
}

/// Scheduler state for all tasks (extended only)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SchedulingResponse {
    /// URL discovery schedule
    pub url_discovery: ScheduleStateResponse,
    /// BMS bandwidth test schedule
    pub bms_test: ScheduleStateResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProviderResponse {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tested_at: Option<DateTime<Utc>>,
    // New: always returned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_consistent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_reliable: Option<bool>,
    #[serde(default)]
    pub performance: PerformanceResponse,
    // Extended only (omitted when extended=false)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<DiagnosticsResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<SchedulingResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProviderClientResponse {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tested_at: Option<DateTime<Utc>>,
    // New: always returned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_consistent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_reliable: Option<bool>,
    #[serde(default)]
    pub performance: PerformanceResponse,
    // Extended only (omitted when extended=false)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<DiagnosticsResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<SchedulingResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClientProvidersResponse {
    pub client_id: String,
    pub providers: Vec<ProviderResponse>,
    pub total: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProvidersListResponse {
    pub providers: Vec<ProviderResponse>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BulkProvidersRequest {
    pub provider_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BulkProvidersResponse {
    pub providers: Vec<ProviderResponse>,
    pub not_found: Vec<String>,
}

// --- Impl blocks with conversion methods ---

impl ProviderResponse {
    pub fn from_data(data: ProviderData, extended: bool) -> Self {
        Self::from_data_with_scheduling(data, None, extended)
    }

    pub fn from_data_with_scheduling(
        data: ProviderData,
        scheduling: Option<SchedulingData>,
        extended: bool,
    ) -> Self {
        let provider_address: ProviderAddress = data.provider_id.into();

        let diagnostics = if extended {
            Some(DiagnosticsResponse {
                result_code: data.result_code,
                error_code: data.error_code,
                analysis: Self::parse_analysis(&data.url_metadata),
            })
        } else {
            None
        };

        let scheduling_response = if extended {
            scheduling.map(|s| SchedulingResponse {
                url_discovery: ScheduleStateResponse {
                    next_at: s.url_discovery_next_at,
                    status: s.url_discovery_status,
                    pending_since: s.url_discovery_pending_since,
                },
                bms_test: ScheduleStateResponse {
                    next_at: s.bms_test_next_at,
                    status: s.bms_test_status,
                    pending_since: None,
                },
            })
        } else {
            None
        };

        Self {
            provider_id: provider_address.to_string(),
            working_url: data.working_url,
            retrievability_percent: data.retrievability_percent,
            tested_at: Some(data.tested_at),
            is_consistent: data.is_consistent,
            is_reliable: data.is_reliable,
            performance: PerformanceResponse::from_data(data.performance, extended),
            diagnostics,
            scheduling: scheduling_response,
        }
    }

    fn parse_analysis(metadata: &Option<serde_json::Value>) -> Option<AnalysisResponse> {
        let meta = metadata.as_ref()?;
        let analysis = meta.get("analysis")?;

        let breakdown = analysis.get("inconsistent_breakdown").and_then(|b| {
            Some(InconsistentBreakdown {
                gaming: b.get("gaming")?.as_u64()? as usize,
                both_failed: b.get("both_failed")?.as_u64()? as usize,
                error_pages: b.get("error_pages")?.as_u64()? as usize,
                size_mismatch: b.get("size_mismatch")?.as_u64()? as usize,
            })
        });

        Some(AnalysisResponse {
            sample_count: analysis.get("sample_count")?.as_u64()? as usize,
            success_count: analysis.get("success_count")?.as_u64()? as usize,
            timeout_count: analysis.get("timeout_count")?.as_u64()? as usize,
            inconsistent_count: analysis
                .get("inconsistent_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
            inconsistent_breakdown: breakdown,
            validated_at: {
                let ts = meta.get("validated_at")?.as_str()?;
                DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&Utc)
            },
        })
    }
}

impl From<ProviderData> for ProviderResponse {
    fn from(data: ProviderData) -> Self {
        Self::from_data(data, false)
    }
}

impl ProviderClientResponse {
    pub fn from_data(data: ProviderData, extended: bool) -> Self {
        Self::from_data_with_scheduling(data, None, extended)
    }

    pub fn from_data_with_scheduling(
        data: ProviderData,
        scheduling: Option<SchedulingData>,
        extended: bool,
    ) -> Self {
        let provider_address: ProviderAddress = data.provider_id.into();
        let client_id = data
            .client_id
            .map(|c| crate::types::ClientAddress::from(c).to_string());

        let diagnostics = if extended {
            Some(DiagnosticsResponse {
                result_code: data.result_code,
                error_code: data.error_code,
                analysis: ProviderResponse::parse_analysis(&data.url_metadata),
            })
        } else {
            None
        };

        let scheduling_response = if extended {
            scheduling.map(|s| SchedulingResponse {
                url_discovery: ScheduleStateResponse {
                    next_at: s.url_discovery_next_at,
                    status: s.url_discovery_status,
                    pending_since: s.url_discovery_pending_since,
                },
                bms_test: ScheduleStateResponse {
                    next_at: s.bms_test_next_at,
                    status: s.bms_test_status,
                    pending_since: None,
                },
            })
        } else {
            None
        };

        Self {
            provider_id: provider_address.to_string(),
            client_id,
            working_url: data.working_url,
            retrievability_percent: data.retrievability_percent,
            tested_at: Some(data.tested_at),
            is_consistent: data.is_consistent,
            is_reliable: data.is_reliable,
            performance: PerformanceResponse::from_data(data.performance, extended),
            diagnostics,
            scheduling: scheduling_response,
        }
    }
}

impl From<ProviderData> for ProviderClientResponse {
    fn from(data: ProviderData) -> Self {
        Self::from_data(data, false)
    }
}

impl BandwidthTestResponse {
    pub fn from_data(b: BandwidthResult, extended: bool) -> Self {
        Self {
            status: b.status,
            tested_at: b.tested_at,
            ping_avg_ms: b.ping_avg_ms,
            head_avg_ms: b.head_avg_ms,
            ttfb_ms: b.ttfb_ms,
            download_speed_mbps: b.download_speed_mbps,
            worker_count: if extended { b.worker_count } else { None },
            routing_key: if extended { b.routing_key } else { None },
            url_tested: if extended { b.url_tested } else { None },
        }
    }
}

impl From<BandwidthResult> for BandwidthTestResponse {
    fn from(b: BandwidthResult) -> Self {
        Self::from_data(b, false)
    }
}

impl PerformanceResponse {
    pub fn from_data(data: PerformanceData, extended: bool) -> Self {
        Self {
            bandwidth: data
                .bandwidth
                .map(|b| BandwidthTestResponse::from_data(b, extended)),
            geolocation: None,
        }
    }
}

impl From<PerformanceData> for PerformanceResponse {
    fn from(data: PerformanceData) -> Self {
        Self::from_data(data, false)
    }
}
