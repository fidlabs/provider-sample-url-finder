use color_eyre::Result;
use sqlx::types::BigDecimal;

use crate::{
    repository::DealRepository,
    types::{ClientAddress, ClientId, ProviderAddress, ProviderId},
};

/// Context for testing a piece URL with deal metadata
#[derive(Debug, Clone)]
pub struct PieceTestContext {
    pub piece_cid: String,
    pub deal_id: i32,
    pub piece_size: Option<i64>,
    pub url: String,
}

/// Get deals and extract piece contexts (piece_cid + deal_id + piece_size)
pub async fn get_piece_contexts_by_provider(
    deal_repo: &DealRepository,
    provider_id: &ProviderId,
    client_id: Option<&ClientId>,
) -> Result<Vec<(String, i32, Option<i64>)>> {
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

    let contexts: Vec<(String, i32, Option<i64>)> = deals
        .iter()
        .filter_map(|deal| {
            deal.piece_cid.clone().map(|cid| {
                let piece_size = deal.piece_size.as_ref().and_then(bigdecimal_to_i64);
                (cid, deal.deal_id, piece_size)
            })
        })
        .collect();

    Ok(contexts)
}

fn bigdecimal_to_i64(val: &BigDecimal) -> Option<i64> {
    use std::str::FromStr;
    i64::from_str(&val.to_string()).ok()
}

/// Backward-compatible: get deals and extract piece_ids only
pub async fn get_piece_ids_by_provider(
    deal_repo: &DealRepository,
    provider_id: &ProviderId,
    client_id: Option<&ClientId>,
) -> Result<Vec<String>> {
    let contexts = get_piece_contexts_by_provider(deal_repo, provider_id, client_id).await?;
    Ok(contexts.into_iter().map(|(cid, _, _)| cid).collect())
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

/// Build test contexts: (piece_cid, deal_id, piece_size, url) for each endpoint × piece combination
pub fn build_piece_test_contexts(
    endpoints: Vec<String>,
    piece_contexts: Vec<(String, i32, Option<i64>)>,
) -> Vec<PieceTestContext> {
    endpoints
        .iter()
        .flat_map(|endpoint| {
            let endpoint = endpoint.trim_end_matches('/');
            piece_contexts
                .iter()
                .map(move |(piece_cid, deal_id, piece_size)| PieceTestContext {
                    piece_cid: piece_cid.clone(),
                    deal_id: *deal_id,
                    piece_size: *piece_size,
                    url: format!("{endpoint}/piece/{piece_cid}"),
                })
        })
        .collect()
}

/// Backward-compatible: construct every piece_cid and endpoint combination
pub async fn get_piece_url(endpoints: Vec<String>, piece_ids: Vec<String>) -> Vec<String> {
    endpoints
        .iter()
        .flat_map(|endpoint| {
            let endpoint = endpoint.trim_end_matches('/');
            piece_ids
                .iter()
                .map(move |piece_id| format!("{endpoint}/piece/{piece_id}"))
        })
        .collect()
}
