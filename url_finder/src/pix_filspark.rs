use color_eyre::{Result, eyre::eyre};
use reqwest::Client;
use tracing::debug;

/// Get the cid from pix.filspark.com
#[allow(dead_code)]
pub async fn get_cid(peer_id: &str, piece_id: &str) -> Result<String> {
    let client = Client::new();
    let url = format!("https://pix.filspark.com/sample/{peer_id}/{piece_id}");

    debug!("pix filspark url: {:?}", url);

    let res = client
        .get(&url)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    debug!("pix filspark res: {:?}", res);

    let cid = res
        .get("samples")
        .ok_or(eyre!("Missing samples"))?
        .get(0)
        .ok_or(eyre!("Missing samples"))?
        .as_str()
        .ok_or(eyre!("Missing samples"))?;

    debug!("cid: {:?}", cid);

    Ok(cid.to_string())
}
