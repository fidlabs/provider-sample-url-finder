use color_eyre::{eyre::bail, Result};
use reqwest::Client;
use tracing::debug;

pub async fn get_contact(peer_id: &str) -> Result<serde_json::Value> {
    let client = Client::new();
    let url = format!("https://cid.contact/providers/{}", peer_id);

    debug!("cid contact url: {:?}", url);

    let res = client
        .get(&url)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(res)
}

pub fn get_all_addresses_from_response(json: serde_json::Value) -> Result<Vec<String>> {
    let mut addresses = vec![];

    if let Some(e_providers) = json
        .get("ExtendedProviders")
        .and_then(|ep| ep.get("Providers"))
        .and_then(|p| p.as_array())
    {
        e_providers
            .iter()
            .filter_map(|provider| provider.get("Addrs"))
            .filter_map(|addrs| addrs.as_array())
            .flat_map(|addrs_arr| addrs_arr.iter())
            .filter_map(|addr| addr.as_str())
            .for_each(|addr| {
                addresses.push(addr.to_string());
            });
    }

    if addresses.is_empty() {
        bail!("No addresses found");
    }

    Ok(addresses)
}
