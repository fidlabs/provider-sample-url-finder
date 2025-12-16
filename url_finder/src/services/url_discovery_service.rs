use chrono::{DateTime, Utc};

use crate::{
    config::Config,
    provider_endpoints,
    repository::DealRepository,
    services::deal_service,
    types::{
        ClientAddress, ClientId, DiscoveryType, ErrorCode, ProviderAddress, ProviderId, ResultCode,
    },
    url_tester,
};
use tracing::{debug, error};
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
    debug!("Testing URLs: {:?}", urls);
    let (working_url, retrievability_percent) =
        url_tester::check_retrievability_with_get(config, urls, true).await;
    debug!(
        "URL test result - working_url: {:?}, retrievability: {:?}",
        working_url, retrievability_percent
    );

    result.working_url = working_url.clone();
    result.retrievability_percent = retrievability_percent.unwrap_or(0.0);
    result.result_code = if working_url.is_some() {
        ResultCode::Success
    } else {
        ResultCode::FailedToGetWorkingUrl
    };

    result
}
