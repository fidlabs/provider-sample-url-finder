use std::time::Duration;

use color_eyre::{Result, eyre::eyre};
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use crate::{config::Config, types::ProviderAddress, utils::build_reqwest_retry_client};

const LOTUS_RPC_MIN_RETRY_INTERVAL_MS: u64 = 10_000;
const LOTUS_RPC_MAX_RETRY_INTERVAL_MS: u64 = 180_000;
const LOTUS_RPC_TOTAL_TIMEOUT_MS: u64 = 250_000;

pub async fn get_peer_id(config: &Config, address: &ProviderAddress) -> Result<String> {
    debug!("get_peer_id address: {}", address);

    let client = build_reqwest_retry_client(
        LOTUS_RPC_MIN_RETRY_INTERVAL_MS,
        LOTUS_RPC_MAX_RETRY_INTERVAL_MS,
    );
    let res = client
        .post(&config.glif_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "Filecoin.StateMinerInfo",
            "params": [address.as_str(), null]
        }))
        .timeout(Duration::from_millis(LOTUS_RPC_TOTAL_TIMEOUT_MS))
        .send()
        .await?;

    let json = res.json::<serde_json::Value>().await?;
    let message = json
        .get("message")
        .and_then(|m| m.as_str())
        .map(|m| m.to_string());

    debug!("get_peer_id res: {:?}", json);

    let peer_id = json
        .get("result")
        .ok_or_else(|| {
            if let Some(m) = message {
                eyre!("{}", m)
            } else {
                eyre!("Missing lotus rpc result")
            }
        })?
        .get("PeerId")
        .ok_or(eyre!("Missing lotus rpc PeerId"))?
        .as_str()
        .ok_or(eyre!("Missing lotus rpc PeerId"))?;

    Ok(peer_id.to_string())
}

/// Response from StateMarketStorageDeal RPC call
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MarketDealResponse {
    #[serde(rename = "Proposal")]
    proposal: Option<DealProposal>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DealProposal {
    #[serde(rename = "PieceCID")]
    piece_cid: Option<CidWrapper>,
    #[serde(rename = "Label")]
    label: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CidWrapper {
    #[serde(rename = "/")]
    cid: String,
}

/// Fetch deal Label from chain via Lotus RPC
/// Returns (label_raw, piece_cid) on success
#[allow(dead_code)]
pub async fn get_deal_label(config: &Config, deal_id: i32) -> Result<(String, String)> {
    debug!("get_deal_label deal_id: {}", deal_id);

    let client = build_reqwest_retry_client(
        LOTUS_RPC_MIN_RETRY_INTERVAL_MS,
        LOTUS_RPC_MAX_RETRY_INTERVAL_MS,
    );

    let res = client
        .post(&config.glif_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "Filecoin.StateMarketStorageDeal",
            "params": [deal_id, null]
        }))
        .timeout(Duration::from_millis(LOTUS_RPC_TOTAL_TIMEOUT_MS))
        .send()
        .await?;

    let json = res.json::<serde_json::Value>().await?;

    // Check for RPC error
    if let Some(error) = json.get("error") {
        let message = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown RPC error");
        return Err(eyre!("Lotus RPC error for deal {}: {}", deal_id, message));
    }

    let result = json
        .get("result")
        .ok_or_else(|| eyre!("Missing result in RPC response for deal {}", deal_id))?;

    let deal: MarketDealResponse = serde_json::from_value(result.clone())
        .map_err(|e| eyre!("Failed to parse deal response: {}", e))?;

    let proposal = deal
        .proposal
        .ok_or_else(|| eyre!("Missing proposal in deal {}", deal_id))?;

    let piece_cid = proposal
        .piece_cid
        .map(|c| c.cid)
        .ok_or_else(|| eyre!("Missing PieceCID in deal {}", deal_id))?;

    // Label can be string or bytes (FIP-0027)
    let label = match proposal.label {
        Some(serde_json::Value::String(s)) => s,
        Some(serde_json::Value::Object(obj)) => {
            // Bytes encoded as {"bytes": "base64..."} or {"/": "..."}
            obj.get("bytes")
                .or_else(|| obj.get("/"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        }
        _ => String::new(),
    };

    debug!(
        "get_deal_label result: piece_cid={}, label={}",
        piece_cid, label
    );
    Ok((label, piece_cid))
}
