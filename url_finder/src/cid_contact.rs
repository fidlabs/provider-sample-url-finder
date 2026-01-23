use std::{fmt, time::Duration};

use color_eyre::Result;
use tracing::debug;
use urlencoding::decode;

use crate::{config::Config, utils::build_reqwest_retry_client};

const CID_CONTACT_MIN_RETRY_INTERVAL_MS: u64 = 2_000;
const CID_CONTACT_MAX_RETRY_INTERVAL_MS: u64 = 30_000;
const CID_CONTACT_TOTAL_TIMEOUT_MS: u64 = 60_000;

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

pub async fn get_contact(
    config: &Config,
    peer_id: &str,
) -> Result<serde_json::Value, CidContactError> {
    let client = build_reqwest_retry_client(
        CID_CONTACT_MIN_RETRY_INTERVAL_MS,
        CID_CONTACT_MAX_RETRY_INTERVAL_MS,
    );
    let base_url = config.cid_contact_url.trim_end_matches('/');
    let url = format!("{base_url}/providers/{peer_id}");

    debug!("cid contact url: {:?}", url);

    let res = client
        .get(&url)
        .header("Accept", "application/json")
        .header("User-Agent", "url-finder/0.1.0")
        .timeout(Duration::from_millis(CID_CONTACT_TOTAL_TIMEOUT_MS))
        .send()
        .await
        .map_err(|_| CidContactError::InvalidResponse)?;

    let status = res.status();
    debug!("cid contact status: {:?}", status);

    if !status.is_success() {
        debug!("cid contact returned non-success status: {:?}", status);
        // Drain body to allow connection reuse
        let _ = res.text().await;
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

                let has_tcp = trimmed.contains("/tcp/");
                let final_addr = if !has_tcp && trimmed.ends_with("/https") {
                    trimmed.replace("/https", "/tcp/443/https")
                } else if !has_tcp && trimmed.ends_with("/http") {
                    trimmed.replace("/http", "/tcp/80/http")
                } else {
                    trimmed.to_string()
                };

                addresses.push(final_addr);
            });
    }

    addresses
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Real-world example: https://cid.contact/providers/12D3KooWRf7tJR2NfJYE3PQJKXGt1EFqmFBBfQCgPRBLwwR9XL15
    #[test]
    fn transforms_publisher_https_with_http_path() {
        let response = json!({
            "Publisher": {
                "ID": "12D3KooWRf7tJR2NfJYE3PQJKXGt1EFqmFBBfQCgPRBLwwR9XL15",
                "Addrs": [
                    "/dns/adela.myfil.net/https/http-path/%2Fipni-provider%2F12D3KooWRf7tJR2NfJYE3PQJKXGt1EFqmFBBfQCgPRBLwwR9XL15"
                ]
            }
        });

        let addrs = get_all_addresses_from_response(response);

        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0], "/dns/adela.myfil.net/tcp/443/https");
    }

    #[test]
    fn preserves_publisher_addr_with_explicit_tcp() {
        let response = json!({
            "Publisher": {
                "ID": "test-peer-id",
                "Addrs": ["/ip4/1.2.3.4/tcp/8080/http"]
            }
        });

        let addrs = get_all_addresses_from_response(response);

        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0], "/ip4/1.2.3.4/tcp/8080/http");
    }

    // ExtendedProviders path does NOT transform addresses (unlike Publisher path)
    #[test]
    fn extended_providers_not_transformed() {
        let response = json!({
            "ExtendedProviders": {
                "Providers": [{
                    "ID": "test-peer-id",
                    "Addrs": ["/dns/example.com/https"]
                }]
            }
        });

        let addrs = get_all_addresses_from_response(response);

        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0], "/dns/example.com/https");
    }
}
