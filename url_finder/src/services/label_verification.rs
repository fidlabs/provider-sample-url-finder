//! Label verification service for CAR header validation.

use std::collections::HashMap;

use tracing::{debug, warn};

use crate::{
    config::Config,
    lotus_rpc,
    repository::{DealLabel, DealLabelRepository, parse_payload_cid},
    types::{CarVerificationSummary, VerificationStatus, WorkingUrlVerification},
};

/// Context for a URL that needs verification
#[derive(Debug, Clone)]
pub struct VerificationContext {
    pub url: String,
    pub deal_id: i32,
    pub piece_cid: String,
    pub root_cid: Option<String>,
    pub is_working_url: bool,
}

/// Fetch labels for multiple deals, using cache and RPC fallback
pub async fn fetch_labels_batch(
    config: &Config,
    deal_label_repo: &DealLabelRepository,
    deal_ids: &[i32],
) -> (HashMap<i32, DealLabel>, usize) {
    let mut result = HashMap::new();
    let mut rpc_calls = 0;

    if deal_ids.is_empty() {
        return (result, rpc_calls);
    }

    // Check cache first
    let cached = match deal_label_repo.get_by_deal_ids(deal_ids).await {
        Ok(labels) => labels,
        Err(e) => {
            warn!("Failed to query deal_labels cache: {e}");
            vec![]
        }
    };

    for label in cached {
        result.insert(label.deal_id, label);
    }

    // Fetch missing from RPC
    let missing: Vec<i32> = deal_ids
        .iter()
        .filter(|id| !result.contains_key(id))
        .copied()
        .collect();

    for deal_id in missing {
        rpc_calls += 1;
        match lotus_rpc::get_deal_label(config, deal_id).await {
            Ok((label_raw, piece_cid)) => {
                let payload_cid = parse_payload_cid(&label_raw);
                let label = DealLabel {
                    deal_id,
                    piece_cid,
                    label_raw: Some(label_raw),
                    payload_cid,
                };

                // Cache the result
                if let Err(e) = deal_label_repo.upsert(&label).await {
                    warn!("Failed to cache deal label {deal_id}: {e}");
                }

                result.insert(deal_id, label);
            }
            Err(e) => {
                debug!("Failed to fetch label for deal {deal_id}: {e}");
                // Insert placeholder so we don't retry
                let label = DealLabel {
                    deal_id,
                    piece_cid: String::new(),
                    label_raw: None,
                    payload_cid: None,
                };
                result.insert(deal_id, label);
            }
        }
    }

    (result, rpc_calls)
}

/// Verify a single URL against its label
pub fn verify_single(
    ctx: &VerificationContext,
    labels: &HashMap<i32, DealLabel>,
) -> VerificationStatus {
    let root_cid = match &ctx.root_cid {
        Some(cid) => cid,
        None => return VerificationStatus::Unverified,
    };

    let label = match labels.get(&ctx.deal_id) {
        Some(l) => l,
        None => return VerificationStatus::Unverified,
    };

    let payload_cid = match &label.payload_cid {
        Some(cid) => cid,
        None => return VerificationStatus::Unverified,
    };

    if root_cid == payload_cid {
        VerificationStatus::Match
    } else {
        VerificationStatus::Mismatch
    }
}

/// Run verification on a set of contexts, return summary and working URL verification
pub async fn verify_batch(
    config: &Config,
    deal_label_repo: &DealLabelRepository,
    contexts: Vec<VerificationContext>,
    summary: &mut CarVerificationSummary,
) -> Option<WorkingUrlVerification> {
    if contexts.is_empty() {
        return None;
    }

    // Dedupe deal IDs for batch lookup
    let deal_ids: Vec<i32> = contexts
        .iter()
        .map(|c| c.deal_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let (labels, rpc_calls) = fetch_labels_batch(config, deal_label_repo, &deal_ids).await;
    summary.labels_fetched += rpc_calls;

    let mut working_url_verification = None;

    for ctx in &contexts {
        let status = verify_single(ctx, &labels);

        match status {
            VerificationStatus::Match => summary.verified_match += 1,
            VerificationStatus::Mismatch => summary.verified_mismatch += 1,
            VerificationStatus::Unverified => summary.unverifiable += 1,
        }

        if ctx.is_working_url {
            let label = labels.get(&ctx.deal_id);
            working_url_verification = Some(WorkingUrlVerification {
                status,
                root_cid: ctx.root_cid.clone(),
                label_cid: label.and_then(|l| l.payload_cid.clone()),
                deal_id: Some(ctx.deal_id),
            });
        }
    }

    working_url_verification
}
