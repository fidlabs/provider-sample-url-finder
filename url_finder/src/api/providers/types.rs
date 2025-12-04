use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::services::provider_service::{BandwidthResult, PerformanceData, ProviderData};
use crate::types::ProviderAddress;

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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProviderResponse {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tested_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub performance: PerformanceResponse,
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
    #[serde(default)]
    pub performance: PerformanceResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClientProvidersResponse {
    pub client_id: String,
    pub providers: Vec<ProviderResponse>,
    pub total: usize,
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

// --- From implementations ---

impl From<ProviderData> for ProviderResponse {
    fn from(data: ProviderData) -> Self {
        let provider_address: ProviderAddress = data.provider_id.into();
        Self {
            provider_id: provider_address.to_string(),
            working_url: data.working_url,
            retrievability_percent: data.retrievability_percent,
            tested_at: Some(data.tested_at),
            performance: data.performance.into(),
        }
    }
}

impl From<ProviderData> for ProviderClientResponse {
    fn from(data: ProviderData) -> Self {
        let provider_address: ProviderAddress = data.provider_id.into();
        let client_id = data
            .client_id
            .map(|c| crate::types::ClientAddress::from(c).to_string());
        Self {
            provider_id: provider_address.to_string(),
            client_id,
            working_url: data.working_url,
            retrievability_percent: data.retrievability_percent,
            tested_at: Some(data.tested_at),
            performance: data.performance.into(),
        }
    }
}

impl From<BandwidthResult> for BandwidthTestResponse {
    fn from(b: BandwidthResult) -> Self {
        Self {
            status: b.status,
            tested_at: b.tested_at,
            ping_avg_ms: b.ping_avg_ms,
            head_avg_ms: b.head_avg_ms,
            ttfb_ms: b.ttfb_ms,
            download_speed_mbps: b.download_speed_mbps,
        }
    }
}

impl From<PerformanceData> for PerformanceResponse {
    fn from(data: PerformanceData) -> Self {
        Self {
            bandwidth: data.bandwidth.map(Into::into),
            geolocation: None,
        }
    }
}
