use color_eyre::Result;
use reqwest::Client;
use serde_json::json;

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
        .await?
        .json::<serde_json::Value>()
        .await?;

    let peer_id = res
        .get("result")
        .unwrap()
        .get("PeerId")
        .unwrap()
        .as_str()
        .unwrap();

    Ok(peer_id.to_string())
}
