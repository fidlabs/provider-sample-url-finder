use std::sync::Arc;

use axum::{debug_handler, extract::State, Json};
use axum_extra::extract::WithRejection;
use color_eyre::{
    eyre::{bail, Error},
    Result,
};
use common::api_response::{bad_request, ok_response, ApiResponse, ErrorResponse};
use futures::{stream, StreamExt};
use reqwest::Client;
use tracing::debug;
use utoipa::ToSchema;

use crate::{cid_contact, lotus_rpc, AppState};

#[derive(serde::Deserialize, ToSchema)]
pub struct FindUrlInput {
    pub address: String,
    pub extended_search: Option<bool>,
}

#[derive(serde::Serialize, ToSchema)]
pub struct FindUrlResponse {
    pub url: String,
}

/// Find a working url for a given SP address
#[utoipa::path(
    post,
    path = "/url/find",
    request_body(content = FindUrlInput),
    description = r#"
**Find a working url for a given SP address**
    "#,
    responses(
        (status = 200, description = "Url Found", body = FindUrlResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["Url"],
)]
#[debug_handler]
pub async fn handle_find_url(
    State(state): State<Arc<AppState>>,
    WithRejection(Json(payload), _): WithRejection<Json<FindUrlInput>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<FindUrlResponse>, ApiResponse<()>> {
    // Get miner info from lotus rpc
    let peer_id = lotus_rpc::get_peer_id(&payload.address)
        .await
        .map_err(|e| {
            debug!("Failed to get peer id: {:?}", e);
            bad_request("Failed to get peer id")
        })?;

    // Get cid contact response
    let cid_contact_res = cid_contact::get_contact(&peer_id).await.map_err(|e| {
        debug!("Missing data from cid contact: {:?}", e);
        bad_request("Missing data from cid contact")
    })?;
    debug!("cid contact response: {:?}", cid_contact_res);

    // Get all addresses (containing IP and Port) from cid contact response
    let addrs = cid_contact::get_all_addresses_from_response(cid_contact_res).map_err(|e| {
        debug!("Failed to get addresses: {:?}", e);
        bad_request("Failed to get addresses")
    })?;

    // parse addresses to http endpoints
    let endpoints = parse_addrs_to_endpoints(addrs).map_err(|e| {
        debug!("Failed to parse addrs: {:?}", e);
        bad_request("Failed to parse addrs")
    })?;

    if endpoints.is_empty() {
        debug!("No endpoints found");
        return Err(bad_request("No endpoints found"));
    }

    let provider = payload
        .address
        .strip_prefix("f0")
        .unwrap_or(&payload.address)
        .to_string();

    // Find piece_cid from deals and test if the url is working, returning the first working url
    let working_url = get_working_url(state, endpoints, &provider, payload.extended_search)
        .await
        .map_err(|e| {
            debug!("Failed to get working url: {:?}", e);
            bad_request("Failed to get working url")
        })?;

    Ok(ok_response(FindUrlResponse { url: working_url }))
}

fn parse_addrs_to_endpoints(addrs: Vec<String>) -> Result<Vec<String>, Error> {
    let mut endpoints = vec![];

    for addr in addrs {
        let parts: Vec<&str> = addr.split("/").collect();
        let prot = if addr.contains("https") {
            "https"
        } else {
            "http"
        };
        let host = parts[2];
        let port = parts[4];

        let endpoint = format!("{}://{}:{}", prot, host, port);

        if !addr.contains("http") {
            debug!("skipping non-http endpoint: {:?}", endpoint);
            continue;
        }

        endpoints.push(endpoint);
    }

    Ok(endpoints)
}

/// return first working url
async fn filter_working_url(urls: Vec<String>) -> Option<String> {
    let client = Client::new();

    // Create a stream of requests testing the urls through head requests
    // Run the requests concurrently with a limit
    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            async move {
                match client.head(&url).send().await {
                    Ok(resp) if resp.status().is_success() => Some(url),
                    _ => {
                        debug!("url not working: {:?}", url);
                        None
                    }
                }
            }
        })
        .buffer_unordered(20); // concurency limit

    while let Some(result) = stream.next().await {
        if let Some(url) = result {
            return Some(url);
        }
    }

    None
}

async fn get_working_url(
    state: Arc<AppState>,
    endpoints: Vec<String>,
    provider: &str,
    extended_search: Option<bool>,
) -> Result<String, Error> {
    let limit = 1000;
    let mut offset = 0;
    let max_offset = if extended_search.unwrap_or(false) {
        50 * limit
    } else {
        limit
    };

    loop {
        let deals = state
            .deal_repo
            .get_unified_verified_deals_by_provider(provider, limit, offset)
            .await?;

        if deals.is_empty() {
            break;
        }

        debug!("number of deals: {:?}", deals.len());

        // construct every piece_cid and endoint combination
        let urls: Vec<String> = endpoints
            .iter()
            .flat_map(|endpoint| {
                let endpoint = endpoint.clone();
                deals.iter().filter_map(move |deal| {
                    deal.piece_cid
                        .as_ref()
                        .map(|piece_cid| format!("{}/piece/{}", endpoint, piece_cid))
                })
            })
            .collect();

        let working_url = filter_working_url(urls).await;

        if working_url.is_some() {
            debug!("working url found: {:?}", working_url);
            return Ok(working_url.unwrap());
        }

        offset += limit;
        if offset >= max_offset {
            break;
        }
        debug!("No working url found, fetching more deals");
    }

    bail!("No working url found")
}
