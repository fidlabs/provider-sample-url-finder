use chrono::{DateTime, Utc};

use crate::{
    config::{Config, MIN_VALID_CONTENT_LENGTH},
    http_client::build_client,
    provider_endpoints,
    repository::DealRepository,
    services::{consistency_analyzer::analyze_results, deal_service},
    types::{
        ClientAddress, ClientId, DiscoveryType, ErrorCode, ProviderAddress, ProviderId, ResultCode,
    },
    url_tester::test_urls_double_tap,
};
use tracing::{debug, error, info};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UrlDiscoveryResult {
    pub id: Uuid,
    pub provider_id: ProviderId,
    pub client_id: Option<ClientId>,
    pub result_type: DiscoveryType,
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    pub result_code: ResultCode,
    pub error_code: Option<ErrorCode>,
    pub tested_at: DateTime<Utc>,
    pub is_consistent: bool,
    pub is_reliable: bool,
    pub url_metadata: Option<serde_json::Value>,
}

impl UrlDiscoveryResult {
    pub fn new_provider_only(provider_id: ProviderId) -> Self {
        Self {
            id: Uuid::new_v4(),
            provider_id,
            client_id: None,
            result_type: DiscoveryType::Provider,
            working_url: None,
            retrievability_percent: 0.0,
            result_code: ResultCode::Error,
            error_code: None,
            tested_at: Utc::now(),
            is_consistent: false, // No verification performed yet
            is_reliable: false,   // No verification performed yet
            url_metadata: None,
        }
    }

    pub fn new_provider_client(provider_id: ProviderId, client_id: ClientId) -> Self {
        Self {
            id: Uuid::new_v4(),
            provider_id,
            client_id: Some(client_id),
            result_type: DiscoveryType::ProviderClient,
            working_url: None,
            retrievability_percent: 0.0,
            result_code: ResultCode::Error,
            error_code: None,
            tested_at: Utc::now(),
            is_consistent: false, // No verification performed yet
            is_reliable: false,   // No verification performed yet
            url_metadata: None,
        }
    }
}

pub async fn discover_url(
    config: &Config,
    provider_address: &ProviderAddress,
    client_address: Option<ClientAddress>,
    deal_repo: &DealRepository,
) -> UrlDiscoveryResult {
    let provider_id: ProviderId = provider_address.clone().into();
    let client_id: Option<ClientId> = client_address.clone().map(|c| c.into());

    tracing::info!(
        "discover_url called for provider={}, client={:?}",
        provider_address,
        client_address
    );

    let mut result = match &client_id {
        Some(c) => UrlDiscoveryResult::new_provider_client(provider_id.clone(), c.clone()),
        None => UrlDiscoveryResult::new_provider_only(provider_id.clone()),
    };

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(config, provider_address).await {
            Ok((code, eps)) => (code, eps),
            Err(e) => {
                error!(
                    "Failed to get provider endpoints for {}: {:?}",
                    provider_address, e
                );
                result.result_code = ResultCode::Error;
                result.error_code = Some(e);
                return result;
            }
        };

    let Some(endpoints) = endpoints else {
        result.result_code = result_code;
        return result;
    };

    let piece_ids =
        match deal_service::get_piece_ids_by_provider(deal_repo, &provider_id, client_id.as_ref())
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                error!(
                    "Failed to get piece ids for {} {:?}: {:?}",
                    provider_id, client_id, e
                );
                result.result_code = ResultCode::Error;
                result.error_code = Some(ErrorCode::FailedToGetDeals);
                return result;
            }
        };

    if piece_ids.is_empty() {
        result.result_code = ResultCode::NoDealsFound;
        return result;
    }

    let urls = deal_service::get_piece_url(endpoints.clone(), piece_ids).await;
    debug!(
        "Built {} URLs to test from endpoints: {:?}",
        urls.len(),
        endpoints
    );

    // Build HTTP client for double-tap testing
    let client = match build_client(config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to build HTTP client: {:?}", e);
            result.result_code = ResultCode::Error;
            return result;
        }
    };

    // Double-tap test all URLs
    let test_results = test_urls_double_tap(&client, urls).await;
    debug!("Double-tap tested {} URLs", test_results.len());

    // Analyze results for provider-level metrics
    let analysis = analyze_results(&test_results);
    info!(
        "Provider {} analysis: retrievability={:.1}%, consistent={}, reliable={}, samples={}",
        provider_address,
        analysis.retrievability_percent,
        analysis.is_consistent,
        analysis.is_reliable,
        analysis.sample_count
    );

    // Select best working URL: success && consistent && content_length >= 8GB
    let working_url = test_results
        .iter()
        .filter(|r| r.success && r.consistent)
        .filter(|r| r.content_length.unwrap_or(0) >= MIN_VALID_CONTENT_LENGTH)
        .max_by_key(|r| r.content_length)
        .map(|r| r.url.clone());

    // Build metadata with analysis details
    let url_metadata = serde_json::json!({
        "analysis": {
            "sample_count": analysis.sample_count,
            "success_count": analysis.success_count,
            "timeout_count": analysis.timeout_count,
            "retrievability_percent": analysis.retrievability_percent,
            "is_consistent": analysis.is_consistent,
            "is_reliable": analysis.is_reliable,
        },
        "validated_at": Utc::now().to_rfc3339(),
    });

    result.working_url = working_url.clone();
    result.retrievability_percent = analysis.retrievability_percent;
    result.is_consistent = analysis.is_consistent;
    result.is_reliable = analysis.is_reliable;
    result.url_metadata = Some(url_metadata);

    // Success if we found a valid working URL
    result.result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };

    result
}
