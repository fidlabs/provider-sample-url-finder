use chrono::{DateTime, Utc};

use crate::{
    config::{Config, MIN_VALID_CONTENT_LENGTH},
    http_client::build_client,
    repository::DealRepository,
    services::{consistency_analyzer::analyze_results, deal_service},
    types::{
        ClientAddress, ClientId, DiscoveryType, ErrorCode, ProviderAddress, ProviderId, ResultCode,
    },
    url_tester::test_url_double_tap,
};
use tracing::{debug, error, trace};
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
    pub sector_utilization_percent: Option<f64>,
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
            sector_utilization_percent: None,
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
            sector_utilization_percent: None,
        }
    }
}

pub async fn discover_url(
    config: &Config,
    provider_address: &ProviderAddress,
    client_address: Option<ClientAddress>,
    deal_repo: &DealRepository,
    endpoints: Vec<String>,
    tested_at: Option<DateTime<Utc>>,
) -> UrlDiscoveryResult {
    let provider_id: ProviderId = provider_address.clone().into();
    let client_id: Option<ClientId> = client_address.clone().map(|c| c.into());

    trace!(
        "discover_url: provider={}, client={:?}, endpoints={:?}",
        provider_address, client_address, endpoints
    );

    let mut result = match &client_id {
        Some(c) => UrlDiscoveryResult::new_provider_client(provider_id.clone(), c.clone()),
        None => UrlDiscoveryResult::new_provider_only(provider_id.clone()),
    };
    if let Some(ts) = tested_at {
        result.tested_at = ts;
    }

    if endpoints.is_empty() {
        result.result_code = ResultCode::MissingHttpAddrFromCidContact;
        return result;
    }

    // Get piece contexts (piece_cid + deal_id)
    let piece_contexts = match deal_service::get_piece_contexts_by_provider(
        deal_repo,
        &provider_id,
        client_id.as_ref(),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            error!(
                "Failed to get piece contexts for {} {:?}: {:?}",
                provider_id, client_id, e
            );
            result.result_code = ResultCode::Error;
            result.error_code = Some(ErrorCode::FailedToGetDeals);
            return result;
        }
    };

    if piece_contexts.is_empty() {
        result.result_code = ResultCode::NoDealsFound;
        return result;
    }

    // Build test contexts with deal_id preserved
    let test_contexts = deal_service::build_piece_test_contexts(endpoints.clone(), piece_contexts);
    debug!(
        "Built {} test contexts from endpoints: {:?}",
        test_contexts.len(),
        endpoints
    );

    // Build HTTP client
    let client = match build_client(config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to build HTTP client: {:?}", e);
            result.result_code = ResultCode::Error;
            return result;
        }
    };

    // Double-tap test all URLs, collecting results with context
    let mut test_results = Vec::with_capacity(test_contexts.len());
    for ctx in &test_contexts {
        let url_result = test_url_double_tap(&client, &ctx.url).await;
        test_results.push((ctx.clone(), url_result));
    }
    debug!("Double-tap tested {} URLs", test_results.len());

    // Extract just UrlTestResults for analysis
    let url_results: Vec<_> = test_results.iter().map(|(_, r)| r.clone()).collect();
    let analysis = analyze_results(&url_results);

    // Select working URL (largest valid response)
    let working_url_result = test_results
        .iter()
        .filter(|(_, r)| r.success)
        .filter(|(_, r)| r.content_length.unwrap_or(0) >= MIN_VALID_CONTENT_LENGTH)
        .max_by_key(|(_, r)| r.content_length);

    let working_url = working_url_result.map(|(_, r)| r.url.clone());

    // CAR diagnostics
    let valid_car_count = test_results.iter().filter(|(_, r)| r.is_valid_car).count();
    let small_car_count = test_results
        .iter()
        .filter(|(_, r)| r.is_valid_car && r.content_length.unwrap_or(0) < MIN_VALID_CONTENT_LENGTH)
        .count();

    let working_url_car_info = working_url_result.map(|(_, r)| {
        serde_json::json!({
            "is_valid_car": r.is_valid_car,
            "root_cid": r.root_cid,
            "content_length": r.content_length,
        })
    });

    // Calculate sector utilization
    let utilization_samples: Vec<f64> = test_results
        .iter()
        .filter(|(_, r)| r.success)
        .filter_map(|(ctx, r)| {
            let content_length = r.content_length? as f64;
            let piece_size = ctx.piece_size? as f64;
            if piece_size > 0.0 {
                Some((content_length / piece_size) * 100.0)
            } else {
                None
            }
        })
        .collect();

    let sector_utilization_percent = if utilization_samples.is_empty() {
        None
    } else {
        let sum: f64 = utilization_samples.iter().sum();
        Some(sum / utilization_samples.len() as f64)
    };

    // Build metadata
    let url_metadata = serde_json::json!({
        "analysis": {
            "sample_count": analysis.sample_count,
            "success_count": analysis.success_count,
            "timeout_count": analysis.timeout_count,
            "inconsistent_count": analysis.inconsistent_count,
            "inconsistent_breakdown": {
                "warm_up": analysis.inconsistent_warm_up,
                "flaky": analysis.inconsistent_flaky,
                "small_responses": analysis.inconsistent_small_responses,
                "size_mismatch": analysis.inconsistent_size_mismatch,
            },
            "retrievability_percent": analysis.retrievability_percent,
            "is_consistent": analysis.is_consistent,
            "is_reliable": analysis.is_reliable,
        },
        "car_diagnostics": {
            "responses_parsed": test_results.len(),
            "valid_car_headers": valid_car_count,
            "small_car_responses": small_car_count,
            "working_url_car_info": working_url_car_info,
        },
        "sector_utilization": {
            "sample_count": utilization_samples.len(),
            "min_percent": utilization_samples.iter().cloned().reduce(f64::min),
            "max_percent": utilization_samples.iter().cloned().reduce(f64::max),
        },
        "validated_at": Utc::now().to_rfc3339(),
    });

    result.working_url = working_url.clone();
    result.retrievability_percent = analysis.retrievability_percent;
    result.is_consistent = analysis.is_consistent;
    result.is_reliable = analysis.is_reliable;
    result.url_metadata = Some(url_metadata);
    result.sector_utilization_percent = sector_utilization_percent;

    result.result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };

    result
}
