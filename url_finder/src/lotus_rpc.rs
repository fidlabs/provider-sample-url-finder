use std::time::Duration;

use color_eyre::{eyre::eyre, Result};
use serde_json::json;
use tracing::debug;

use crate::{config::CONFIG, types::ProviderAddress, utils::build_reqwest_retry_client};

pub async fn get_peer_id(address: &ProviderAddress) -> Result<String> {
    debug!("get_peer_id address: {}", address);

    let client = build_reqwest_retry_client(10_000, 300_000);
    let res = client
        .post(&CONFIG.glif_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "Filecoin.StateMinerInfo",
            "params": [address.as_str(), null]
        }))
        .timeout(Duration::from_secs(30))
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
