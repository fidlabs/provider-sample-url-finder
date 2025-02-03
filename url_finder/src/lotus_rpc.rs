use color_eyre::{eyre::eyre, Result};
use reqwest::Client;
use serde_json::json;
use tracing::debug;

pub async fn get_peer_id(address: &str) -> Result<String> {
    let client = Client::new();
    let res = client
        .post("https://api.node.glif.io/rpc/v1")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "Filecoin.StateMinerInfo",
            "params": [address, null]
        }))
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
        .ok_or(if let Some(m) = message {
            eyre!(m)
        } else {
            eyre!("Missing lotus rpc result")
        })?
        .get("PeerId")
        .ok_or(eyre!("Missing lotus rpc PeerId"))?
        .as_str()
        .ok_or(eyre!("Missing lotus rpc PeerId"))?;

    Ok(peer_id.to_string())
}
