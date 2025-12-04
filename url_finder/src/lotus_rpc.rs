use std::time::Duration;

use color_eyre::{Result, eyre::eyre};
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
