use color_eyre::Result;
use std::fmt;
use tracing::debug;
use urlencoding::decode;

use crate::utils::build_reqwest_retry_client;

pub enum CidContactError {
    InvalidResponse,
    NoData,
}
impl fmt::Display for CidContactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CidContactError::InvalidResponse => "InvalidResponse",
            CidContactError::NoData => "NoData",
        };
        write!(f, "{s}")
    }
}

pub async fn get_contact(peer_id: &str) -> Result<serde_json::Value, CidContactError> {
    let client = build_reqwest_retry_client(1_000, 30_000);
    let url = format!("https://cid.contact/providers/{peer_id}");

    debug!("cid contact url: {:?}", url);

    let res = client
        .get(&url)
        .header("Accept", "application/json")
        .header("User-Agent", "url-finder/0.1.0")
        .send()
        .await
        .map_err(|_| CidContactError::InvalidResponse)?;

    debug!("cid contact status: {:?}", res.status());

    if !res.status().is_success() {
        debug!(
            "cid contact returned non-success status: {:?}",
            res.status()
        );
        return Err(CidContactError::NoData);
    }

    let json = res.json::<serde_json::Value>().await.map_err(|e| {
        debug!("Failed to parse cid contact response: {:?}", e);
        CidContactError::NoData
    })?;

    debug!("cid contact res: {:?}", json);

    Ok(json)
}

pub fn get_all_addresses_from_response(json: serde_json::Value) -> Vec<String> {
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
    } else if let Some(e_providers) = json
        .get("Publisher")
        .and_then(|p| p.get("Addrs"))
        .and_then(|a| a.as_array())
    {
        e_providers
            .iter()
            .filter_map(|addr| addr.as_str())
            .for_each(|addr: &str| {
                let decoded_addr = decode(addr)
                    .map(|s| s.into_owned())
                    .unwrap_or_else(|_| addr.to_string());

                let cleaned: String = decoded_addr.replace("//", "/");

                let trimmed = match cleaned.find("/http-path") {
                    Some(index) => &cleaned[..index],
                    None => &cleaned,
                };

                let final_addr = if trimmed.ends_with("/https") {
                    trimmed.replace("/https", "/tcp/443/https")
                } else if trimmed.ends_with("/http") {
                    trimmed.replace("/http", "/tcp/80/http")
                } else {
                    trimmed.to_string()
                };

                addresses.push(final_addr);
            });
    }

    addresses
}
