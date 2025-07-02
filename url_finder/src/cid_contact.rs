use std::fmt;

use color_eyre::Result;
use reqwest::Client;
use tracing::debug;

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
        write!(f, "{}", s)
    }
}

pub async fn get_contact(peer_id: &str) -> Result<serde_json::Value, CidContactError> {
    let client = Client::new();
    let url = format!("https://cid.contact/providers/{peer_id}");

    debug!("cid contact url: {:?}", url);

    let res = client
        .get(&url)
        .send()
        .await
        .map_err(|_| CidContactError::InvalidResponse)?;

    if !res.status().is_success() {
        return Err(CidContactError::NoData);
    }

    let json = res
        .json::<serde_json::Value>()
        .await
        .map_err(|_| CidContactError::NoData)?;

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
    }

    addresses
}
