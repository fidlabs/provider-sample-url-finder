use crate::{
    provider_endpoints,
    repository::DealRepository,
    services::deal_service,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
    url_tester, ResultCode,
};
use color_eyre::Result;
use tracing::error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UrlDiscoveryResult {
    pub id: Uuid,
    pub provider_id: ProviderId,
    pub client_id: Option<ClientId>,
    pub result_type: String,
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    pub result_code: String,
    pub error_code: Option<String>,
}

impl UrlDiscoveryResult {
    pub fn new_provider_only(provider_id: ProviderId) -> Self {
        Self {
            id: Uuid::new_v4(),
            provider_id,
            client_id: None,
            result_type: "Provider".to_string(),
            working_url: None,
            retrievability_percent: 0.0,
            result_code: ResultCode::Error.to_string(),
            error_code: None,
        }
    }

    pub fn new_provider_client(provider_id: ProviderId, client_id: ClientId) -> Self {
        Self {
            id: Uuid::new_v4(),
            provider_id,
            client_id: Some(client_id),
            result_type: "ProviderClient".to_string(),
            working_url: None,
            retrievability_percent: 0.0,
            result_code: ResultCode::Error.to_string(),
            error_code: None,
        }
    }
}

pub async fn discover_url(
    provider_address: &ProviderAddress,
    client_address: Option<ClientAddress>,
    deal_repo: &DealRepository,
) -> Result<UrlDiscoveryResult> {
    let provider_id: ProviderId = provider_address.clone().into();
    let client_id: Option<ClientId> = client_address.clone().map(|c| c.into());

    let mut result = match &client_id {
        Some(c) => UrlDiscoveryResult::new_provider_client(provider_id.clone(), c.clone()),
        None => UrlDiscoveryResult::new_provider_only(provider_id.clone()),
    };

    let (result_code, endpoints) =
        match provider_endpoints::get_provider_endpoints(provider_address).await {
            Ok((code, eps)) => (code, eps),
            Err(e) => {
                error!(
                    "Failed to get provider endpoints for {}: {:?}",
                    provider_address, e
                );
                result.result_code = ResultCode::Error.to_string();
                result.error_code = Some(format!("{:?}", e));
                return Ok(result);
            }
        };

    if endpoints.is_none() {
        result.result_code = result_code.to_string();
        return Ok(result);
    }

    let endpoints = endpoints.unwrap();

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
                result.result_code = ResultCode::Error.to_string();
                result.error_code = Some(format!("{:?}", e));
                return Ok(result);
            }
        };

    if piece_ids.is_empty() {
        result.result_code = ResultCode::NoDealsFound.to_string();
        return Ok(result);
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;
    let (working_url, retrievability_percent) = url_tester::get_retrivability_with_head(urls).await;

    result.working_url = working_url.clone();
    result.retrievability_percent = retrievability_percent;
    result.result_code = if working_url.is_some() {
        ResultCode::Success.to_string()
    } else {
        ResultCode::FailedToGetWorkingUrl.to_string()
    };

    Ok(result)
}
