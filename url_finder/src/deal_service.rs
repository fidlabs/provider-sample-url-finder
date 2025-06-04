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
    let urls = endpoints
        .iter()
        .flat_map(|endpoint| {
            let endpoint = endpoint.clone();
            piece_ids
                .iter()
                .map(move |piece_id| format!("{}/piece/{}", endpoint, piece_id))
        })
        .collect();

    urls
}
