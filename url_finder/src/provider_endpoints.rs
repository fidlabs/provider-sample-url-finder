use tracing::{debug, error};

use crate::{
    cid_contact::{self, CidContactError},
    lotus_rpc, multiaddr_parser, ErrorCode, ResultCode,
};

pub async fn get_provider_endpoints(
    address: &str,
) -> Result<(ResultCode, Option<Vec<String>>), ErrorCode> {
    // get peer_id from miner info from lotus rpc
    let peer_id = lotus_rpc::get_peer_id(address).await.map_err(|e| {
        error!("Failed to get peer id: {:?}", e);

        ErrorCode::FailedToGetPeerId
    })?;

    // get cid contact response
    let cid_contact_res = match cid_contact::get_contact(&peer_id).await {
        Ok(res) => res,
        Err(CidContactError::NoData) => {
            return Ok((ResultCode::NoCidContactData, None));
        }
        Err(e) => {
            error!("Failed to get cid contact: {:?}", e.to_string());

            return Err(ErrorCode::FailedToRetrieveCidContactData);
        }
    };

    // Get all addresses (containing IP and Port) from cid contact response
    let addrs = cid_contact::get_all_addresses_from_response(cid_contact_res);
    if addrs.is_empty() {
        debug!("Missing addr from cid contact, No addresses found");

        return Ok((ResultCode::MissingAddrFromCidContact, None));
    }

    // parse addresses to http endpoints
    let endpoints = multiaddr_parser::parse(addrs);
    if endpoints.is_empty() {
        debug!("Missing http addr from cid contact, No endpoints found");

        return Ok((ResultCode::MissingHttpAddrFromCidContact, None));
    }

    Ok((ResultCode::Success, Some(endpoints)))
}
