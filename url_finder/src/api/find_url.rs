use std::{fmt, sync::Arc};

use axum::{debug_handler, extract::State, Json};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::{internal_server_error, ok_response, ApiResponse, ErrorResponse};
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use utoipa::ToSchema;

use crate::{
    cid_contact::{self, CidContactError},
    deal_service, lotus_rpc, multiaddr_parser, url_tester, AppState,
};

#[derive(Deserialize, ToSchema)]
pub struct FindUrlInput {
    pub address: String,
}

#[derive(Serialize, ToSchema)]
pub struct FindUrlResponse {
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub enum ResultCode {
    NoCidContactData,
    MissingAddrFromCidContact,
    MissingHttpAddrFromCidContact,
    FailedToGetWorkingUrl,
    NoDealsFound,
    Success,
}

#[derive(Serialize, ToSchema)]
pub enum ErrorCode {
    FailedToRetrieveCidContactData,
    FailedToGetPeerId,
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorCode::FailedToRetrieveCidContactData => "FailedToRetrieveCidContactData",
            ErrorCode::FailedToGetPeerId => "FailedToGetPeerId",
        };
        write!(f, "{}", s)
    }
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
        (status = 200, description = "Successful check", body = FindUrlResponse),
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
    debug!("find url input address: {:?}", &payload.address);
    // get peer_id from miner info from lotus rpc
    let peer_id = lotus_rpc::get_peer_id(&payload.address)
        .await
        .map_err(|e| {
            error!("Failed to get peer id: {:?}", e);

            internal_server_error(ErrorCode::FailedToGetPeerId.to_string())
        })?;

    // get cid contact response
    let cid_contact_res = match cid_contact::get_contact(&peer_id).await {
        Ok(res) => res,
        Err(CidContactError::NoData) => {
            return Ok(ok_response(FindUrlResponse {
                result: ResultCode::NoCidContactData,
                url: None,
            }));
        }
        Err(e) => {
            error!("Failed to get cid contact: {:?}", e.to_string());

            return Err(internal_server_error(
                ErrorCode::FailedToRetrieveCidContactData.to_string(),
            ));
        }
    };

    // Get all addresses (containing IP and Port) from cid contact response
    let addrs = cid_contact::get_all_addresses_from_response(cid_contact_res);
    if addrs.is_empty() {
        debug!("Missing addr from cid contact, No addresses found");
        return Ok(ok_response(FindUrlResponse {
            result: ResultCode::MissingAddrFromCidContact,
            url: None,
        }));
    }

    // parse addresses to http endpoints
    let endpoints = multiaddr_parser::parse(addrs);
    if endpoints.is_empty() {
        debug!("Missing http addr from cid contact, No endpoints found");
        return Ok(ok_response(FindUrlResponse {
            result: ResultCode::MissingHttpAddrFromCidContact,
            url: None,
        }));
    }

    let provider = payload
        .address
        .strip_prefix("f0")
        .unwrap_or(&payload.address)
        .to_string();

    let piece_ids = deal_service::get_piece_ids(&state.deal_repo, &provider)
        .await
        .map_err(|e| {
            debug!("Failed to get piece ids: {:?}", e);
            internal_server_error("Failed to get piece ids")
        })?;
    if piece_ids.is_empty() {
        debug!("No deals found");
        return Ok(ok_response(FindUrlResponse {
            result: ResultCode::NoDealsFound,
            url: None,
        }));
    }

    let urls = deal_service::get_piece_url(endpoints, piece_ids).await;

    let working_url = url_tester::filter_working_with_head(urls).await;
    if working_url.is_none() {
        debug!("Failed to get working url");
        return Ok(ok_response(FindUrlResponse {
            result: ResultCode::FailedToGetWorkingUrl,
            url: None,
        }));
    }

    Ok(ok_response(FindUrlResponse {
        result: ResultCode::Success,
        url: working_url,
    }))
}
