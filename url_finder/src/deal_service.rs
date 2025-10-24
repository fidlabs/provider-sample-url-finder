use color_eyre::Result;

use crate::deal_repo::DealRepository;

/// get deals and extract piece_ids
pub async fn get_piece_ids_by_provider(
    deal_repo: &DealRepository,
    provider: &str,
    client: Option<&str>,
) -> Result<Vec<String>> {
    let limit = 100;
    let offset = 0;

    let deals = if let Some(client) = client {
        deal_repo
            .get_deals_by_provider_and_client(provider, client, limit, offset)
            .await?
    } else {
        deal_repo
            .get_deals_by_provider(provider, limit, offset)
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
    client: &str,
) -> Result<Vec<String>> {
    let client_db = client.strip_prefix("f0").unwrap_or(client);
    let deals = deal_repo
        .get_distinct_providers_by_client(client_db)
        .await?;

    if deals.is_empty() {
        return Ok(vec![]);
    }

    let providers: Vec<String> = deals
        .iter()
        .filter_map(|deal| deal.provider_id.clone())
        .map(|provider| {
            if !provider.starts_with("f0") {
                format!("f0{provider}")
            } else {
                provider
            }
        })
        .collect();

    Ok(providers)
}

pub async fn get_random_piece_ids_by_provider_and_client(
    deal_repo: &DealRepository,
    provider: &str,
    client: &str,
) -> Result<Vec<String>> {
    let limit = 100;
    let offset = 0;

    let deals = deal_repo
        .get_random_deals_by_provider_and_client(provider, client, limit, offset)
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
    provider: &str,
) -> Result<Vec<String>> {
    let limit = 100;
    let offset = 0;

    let deals = deal_repo
        .get_random_deals_by_provider(provider, limit, offset)
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
