use color_eyre::Result;

use crate::deal_repo::DealRepository;

/// get deals and extract piece_ids
pub async fn get_piece_ids(deal_repo: &DealRepository, provider: &str) -> Result<Vec<String>> {
    let limit = 20;
    let offset = 0;

    let deals = deal_repo
        .get_unified_verified_deals_by_provider(provider, limit, offset)
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
