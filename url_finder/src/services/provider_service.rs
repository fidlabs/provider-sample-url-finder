use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use color_eyre::Result;
use sqlx::types::BigDecimal;

use crate::repository::{
    BmsBandwidthResult, BmsBandwidthResultRepository, ProviderFilters, StorageProviderRepository,
    UrlResult, UrlResultRepository,
};
use crate::types::{ClientId, ErrorCode, ProviderId, ResultCode};

// --- Domain Types ---

#[derive(Debug, Clone)]
pub struct ProviderData {
    pub provider_id: ProviderId,
    pub client_id: Option<ClientId>,
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    pub tested_at: DateTime<Utc>,
    pub result_code: ResultCode,
    pub error_code: Option<ErrorCode>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub url_metadata: Option<serde_json::Value>,
    pub sector_utilization_percent: Option<f64>,
    pub performance: PerformanceData,
}

#[derive(Debug, Clone, Default)]
pub struct PerformanceData {
    pub bandwidth: Option<BandwidthResult>,
    pub geolocation: Option<GeolocationResult>,
}

#[derive(Debug, Clone, Default)]
pub struct SchedulingData {
    pub url_discovery_next_at: Option<DateTime<Utc>>,
    pub url_discovery_status: Option<String>,
    pub url_discovery_pending_since: Option<DateTime<Utc>>,
    pub bms_test_next_at: Option<DateTime<Utc>>,
    pub bms_test_status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BandwidthResult {
    pub status: String,
    pub tested_at: Option<DateTime<Utc>>,
    pub ping_avg_ms: Option<f64>,
    pub head_avg_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
    pub download_speed_mbps: Option<f64>,
    pub worker_count: Option<i32>,
    pub routing_key: Option<String>,
    pub url_tested: Option<String>,
}

impl From<BmsBandwidthResult> for BandwidthResult {
    fn from(b: BmsBandwidthResult) -> Self {
        Self {
            status: b.status,
            tested_at: b.completed_at,
            ping_avg_ms: b.ping_avg_ms.as_ref().and_then(bigdecimal_to_f64),
            head_avg_ms: b.head_avg_ms.as_ref().and_then(bigdecimal_to_f64),
            ttfb_ms: b.ttfb_ms.as_ref().and_then(bigdecimal_to_f64),
            download_speed_mbps: b.download_speed_mbps.as_ref().and_then(bigdecimal_to_f64),
            worker_count: Some(b.worker_count),
            routing_key: Some(b.routing_key),
            url_tested: Some(b.url_tested),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GeolocationResult {
    pub status: String,
    pub tested_at: Option<DateTime<Utc>>,
    pub routing_key: Option<String>,
    pub region: Option<String>,
    pub country: Option<String>,
    pub city: Option<String>,
}

pub struct PaginatedProviders {
    pub providers: Vec<ProviderData>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

pub struct BulkProviderResult {
    pub providers: Vec<ProviderData>,
    pub not_found: Vec<ProviderId>,
}

// --- Service ---

pub struct ProviderService {
    url_repo: Arc<UrlResultRepository>,
    bms_repo: Arc<BmsBandwidthResultRepository>,
    sp_repo: Arc<StorageProviderRepository>,
}

impl ProviderService {
    pub fn new(
        url_repo: Arc<UrlResultRepository>,
        bms_repo: Arc<BmsBandwidthResultRepository>,
        sp_repo: Arc<StorageProviderRepository>,
    ) -> Self {
        Self {
            url_repo,
            bms_repo,
            sp_repo,
        }
    }

    pub async fn get_provider(&self, id: &ProviderId) -> Result<Option<ProviderData>> {
        let url_result = self.url_repo.get_latest_for_provider(id).await?;

        let Some(url_result) = url_result else {
            return Ok(None);
        };

        let bms_result = self.bms_repo.get_latest_completed_for_provider(id).await?;

        Ok(Some(self.enrich(url_result, bms_result)))
    }

    pub async fn get_provider_client(
        &self,
        provider: &ProviderId,
        client: &ClientId,
    ) -> Result<Option<ProviderData>> {
        let url_result = self
            .url_repo
            .get_latest_for_provider_client(provider, client)
            .await?;

        let Some(url_result) = url_result else {
            return Ok(None);
        };

        let bms_result = self
            .bms_repo
            .get_latest_completed_for_provider(provider)
            .await?;

        Ok(Some(self.enrich(url_result, bms_result)))
    }

    pub async fn get_providers_for_client(&self, client: &ClientId) -> Result<Vec<ProviderData>> {
        let url_results = self
            .url_repo
            .get_latest_for_client_all_providers(client)
            .await?;
        self.enrich_batch(url_results).await
    }

    pub async fn list_providers(
        &self,
        filters: &ProviderFilters,
        limit: i64,
        offset: i64,
    ) -> Result<PaginatedProviders> {
        let total = self.url_repo.count_all_providers(filters).await?;
        let url_results = self
            .url_repo
            .get_all_providers_paginated(filters, limit, offset)
            .await?;
        let providers = self.enrich_batch(url_results).await?;

        Ok(PaginatedProviders {
            providers,
            total,
            limit,
            offset,
        })
    }

    pub async fn bulk_get_providers(&self, ids: &[ProviderId]) -> Result<BulkProviderResult> {
        let id_strings: Vec<String> = ids.iter().map(|id| id.as_str().to_string()).collect();

        let url_results = self.url_repo.get_latest_for_providers(&id_strings).await?;

        let found_ids: HashSet<String> = url_results
            .iter()
            .map(|r| r.provider_id.as_str().to_string())
            .collect();

        let not_found: Vec<ProviderId> = ids
            .iter()
            .filter(|id| !found_ids.contains(id.as_str()))
            .cloned()
            .collect();

        let providers = self.enrich_batch(url_results).await?;

        Ok(BulkProviderResult {
            providers,
            not_found,
        })
    }

    pub async fn get_scheduling_data(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<SchedulingData>> {
        let sp = self.sp_repo.get_by_provider_id(provider_id).await?;
        Ok(sp.map(|sp| SchedulingData {
            url_discovery_next_at: Some(sp.next_url_discovery_at),
            url_discovery_status: sp.url_discovery_status,
            url_discovery_pending_since: sp.url_discovery_pending_since,
            bms_test_next_at: Some(sp.next_bms_test_at),
            bms_test_status: sp.bms_test_status,
        }))
    }

    // --- Private helpers ---

    fn enrich(
        &self,
        url_result: UrlResult,
        bms_result: Option<BmsBandwidthResult>,
    ) -> ProviderData {
        ProviderData {
            provider_id: url_result.provider_id,
            client_id: url_result.client_id,
            working_url: url_result.working_url,
            retrievability_percent: url_result.retrievability_percent,
            tested_at: url_result.tested_at,
            result_code: url_result.result_code,
            error_code: url_result.error_code,
            is_consistent: url_result.is_consistent,
            is_reliable: url_result.is_reliable,
            url_metadata: url_result.url_metadata,
            sector_utilization_percent: url_result.sector_utilization_percent,
            performance: Self::build_performance(bms_result),
        }
    }

    async fn enrich_batch(&self, url_results: Vec<UrlResult>) -> Result<Vec<ProviderData>> {
        if url_results.is_empty() {
            return Ok(vec![]);
        }

        let provider_ids: Vec<String> = url_results
            .iter()
            .map(|r| r.provider_id.as_str().to_string())
            .collect();

        let bms_results = self
            .bms_repo
            .get_latest_completed_for_providers(&provider_ids)
            .await?;

        let bms_map: HashMap<String, BmsBandwidthResult> = bms_results
            .into_iter()
            .map(|r| (r.provider_id.clone(), r))
            .collect();

        let providers = url_results
            .into_iter()
            .map(|url_result| {
                let bms = bms_map.get(url_result.provider_id.as_str()).cloned();
                self.enrich(url_result, bms)
            })
            .collect();

        Ok(providers)
    }

    fn build_performance(bms: Option<BmsBandwidthResult>) -> PerformanceData {
        match bms {
            Some(b) => PerformanceData {
                bandwidth: Some(b.into()),
                geolocation: None,
            },
            None => PerformanceData::default(),
        }
    }
}

fn bigdecimal_to_f64(val: &BigDecimal) -> Option<f64> {
    use std::str::FromStr;
    f64::from_str(&val.to_string()).ok()
}
