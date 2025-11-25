use color_eyre::Result;

use crate::{
    repository::DealRepository,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};

/// get deals and extract piece_ids
pub async fn get_piece_ids_by_provider(
    deal_repo: &DealRepository,
    provider_id: &ProviderId,
    client_id: Option<&ClientId>,
) -> Result<Vec<String>> {
    let limit = 100;
    let offset = 0;

    let deals = if let Some(client) = client_id {
        deal_repo
            .get_deals_by_provider_and_client(provider_id, client, limit, offset)
            .await?
    } else {
        deal_repo
            .get_deals_by_provider(provider_id, limit, offset)
            .await?
    };

    if deals.is_empty() {
        return Ok(vec![]);
    }

    let piece_ids: Vec<String> = deals
        .iter()
        .filter_map(|deal| deal.piece_cid.clone())
        .collect();

    Ok(piece_ids)
}

pub async fn get_distinct_providers_by_client(
    deal_repo: &DealRepository,
    client_address: &ClientAddress,
) -> Result<Vec<ProviderAddress>> {
    let client_id: ClientId = client_address.clone().into();
    let deals = deal_repo
        .get_distinct_providers_by_client(&client_id)
        .await?;

    if deals.is_empty() {
        return Ok(vec![]);
    }

    let providers: Vec<ProviderAddress> = deals
        .iter()
        .filter_map(|deal| deal.provider_id.clone())
        .filter_map(|provider_id| ProviderId::new(provider_id).ok())
        .map(|provider_id| provider_id.into())
        .collect();

    Ok(providers)
}

pub async fn get_random_piece_ids_by_provider_and_client(
    deal_repo: &DealRepository,
    provider_id: &ProviderId,
    client_id: &ClientId,
) -> Result<Vec<String>> {
    let limit = 100;
    let offset = 0;

    let deals = deal_repo
        .get_random_deals_by_provider_and_client(provider_id, client_id, limit, offset)
        .await?;

    if deals.is_empty() {
        return Ok(vec![]);
    }

    let piece_ids: Vec<String> = deals
        .iter()
        .filter_map(|deal| deal.piece_cid.clone())
        .collect();

    Ok(piece_ids)
}

pub async fn get_random_piece_ids_by_provider(
    deal_repo: &DealRepository,
    provider_id: &ProviderId,
) -> Result<Vec<String>> {
    let limit = 100;
    let offset = 0;

    let deals = deal_repo
        .get_random_deals_by_provider(provider_id, limit, offset)
        .await?;

    if deals.is_empty() {
        return Ok(vec![]);
    }

    let piece_ids: Vec<String> = deals
        .iter()
        .filter_map(|deal| deal.piece_cid.clone())
        .collect();

    Ok(piece_ids)
}

/// construct every piece_cid and endoint combination
pub async fn get_piece_url(endpoints: Vec<String>, piece_ids: Vec<String>) -> Vec<String> {
    endpoints
        .iter()
        .flat_map(|endpoint| {
            let endpoint = endpoint.clone();
            piece_ids
                .iter()
                .map(move |piece_id| format!("{endpoint}/piece/{piece_id}"))
        })
        .collect()
}
