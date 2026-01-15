use chrono::{DateTime, Utc};

use crate::{
    config::{Config, MIN_VALID_CONTENT_LENGTH},
    http_client::build_client,
    provider_endpoints,
    repository::{DealLabelRepository, DealRepository},
    services::{
        consistency_analyzer::analyze_results,
        deal_service,
        label_verification::{self, VerificationContext},
    },
    types::{
        CarVerificationSummary, ClientAddress, ClientId, DiscoveryType, ErrorCode, ProviderAddress,
        ProviderId, ResultCode,
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
    deal_label_repo: &DealLabelRepository,
) -> UrlDiscoveryResult {
    let provider_id: ProviderId = provider_address.clone().into();
    let client_id: Option<ClientId> = client_address.clone().map(|c| c.into());

    trace!(
        "discover_url: provider={}, client={:?}",
        provider_address, client_address
    );

    let mut result = match &client_id {
        Some(c) => UrlDiscoveryResult::new_provider_client(provider_id.clone(), c.clone()),
        None => UrlDiscoveryResult::new_provider_only(provider_id.clone()),
    };

    // Get endpoints
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

    // Initialize CAR verification summary
    let mut car_summary = CarVerificationSummary {
        responses_parsed: test_results.len(),
        valid_car_headers: test_results.iter().filter(|(_, r)| r.is_valid_car).count(),
        small_car_responses: test_results
            .iter()
            .filter(|(_, r)| {
                r.is_valid_car && r.content_length.unwrap_or(0) < MIN_VALID_CONTENT_LENGTH
            })
            .count(),
        ..Default::default()
    };

    // Select working URL
    let working_url_ctx = test_results
        .iter()
        .filter(|(_, r)| r.success)
        .filter(|(_, r)| r.content_length.unwrap_or(0) >= MIN_VALID_CONTENT_LENGTH)
        .max_by_key(|(_, r)| r.content_length);

    let working_url = working_url_ctx.map(|(_, r)| r.url.clone());

    // Build verification contexts
    let mut to_verify: Vec<VerificationContext> = vec![];

    // Must verify: all small CAR responses
    for (ctx, r) in &test_results {
        if r.is_valid_car && r.content_length.unwrap_or(0) < MIN_VALID_CONTENT_LENGTH {
            to_verify.push(VerificationContext {
                url: r.url.clone(),
                deal_id: ctx.deal_id,
                piece_cid: ctx.piece_cid.clone(),
                root_cid: r.root_cid.clone(),
                is_working_url: false,
            });
        }
    }

    // Must verify: working URL
    if let Some((ctx, r)) = working_url_ctx {
        // Check if already in to_verify (small CAR that's also working - unlikely but possible)
        let already_added = to_verify.iter().any(|v| v.url == r.url);
        if !already_added {
            to_verify.push(VerificationContext {
                url: r.url.clone(),
                deal_id: ctx.deal_id,
                piece_cid: ctx.piece_cid.clone(),
                root_cid: r.root_cid.clone(),
                is_working_url: true,
            });
        } else {
            // Mark the existing one as working URL
            for v in &mut to_verify {
                if v.url == r.url {
                    v.is_working_url = true;
                }
            }
        }
    }

    // Should verify: first valid success (for pipeline validation)
    let first_success = test_results
        .iter()
        .find(|(_, r)| r.success && r.is_valid_car && !to_verify.iter().any(|v| v.url == r.url));
    if let Some((ctx, r)) = first_success {
        to_verify.push(VerificationContext {
            url: r.url.clone(),
            deal_id: ctx.deal_id,
            piece_cid: ctx.piece_cid.clone(),
            root_cid: r.root_cid.clone(),
            is_working_url: false,
        });
    }

    // Run verification
    let working_url_verification =
        label_verification::verify_batch(config, deal_label_repo, to_verify, &mut car_summary)
            .await;

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
                "both_failed": analysis.inconsistent_both_failed,
                "size_mismatch": analysis.inconsistent_size_mismatch,
            },
            "retrievability_percent": analysis.retrievability_percent,
            "is_consistent": analysis.is_consistent,
            "is_reliable": analysis.is_reliable,
        },
        "car_verification": car_summary,
        "working_url_verification": working_url_verification,
        "validated_at": Utc::now().to_rfc3339(),
    });

    result.working_url = working_url.clone();
    result.retrievability_percent = analysis.retrievability_percent;
    result.is_consistent = analysis.is_consistent;
    result.is_reliable = analysis.is_reliable;
    result.url_metadata = Some(url_metadata);

    result.result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };

    result
}
