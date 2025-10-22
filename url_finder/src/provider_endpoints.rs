use alloy::sol_types::SolType;
use alloy::{
    network::TransactionBuilder,
    primitives::{Address, Bytes},
    providers::{Provider, ProviderBuilder},
    rpc::types::eth::TransactionRequest,
    sol,
    sol_types::SolCall,
};

use color_eyre::{Result, eyre::eyre};
use tracing::{debug, error};

use crate::{
    ErrorCode, ResultCode,
    cid_contact::{self, CidContactError},
    lotus_rpc, multiaddr_parser,
};

sol! {
    struct PeerData {
        string peerID;
        bytes multiaddrs;
    }

    function getPeerData(uint64 minerID) view returns (PeerData);
}

pub async fn valid_curio_provider(address: &str) -> Result<Option<String>> {
    let rpc_url = "https://api.node.glif.io/rpc/v1";

    let rpc_provider = ProviderBuilder::new()
        .connect(rpc_url)
        .await
        .map_err(|err| eyre!("Building provider failed: {}", err))?;

    let contract_address: &str = "0x14183aD016Ddc83D638425D6328009aa390339Ce";

    let miner_peer_id_contract = Address::parse_checksummed(contract_address, None)
        .map_err(|e| eyre!("Failed to parse miner id contact: {e}"))?;

    let miner_id = address
        .strip_prefix("f")
        .unwrap_or(address)
        .parse::<u64>()?;

    let call: Vec<u8> = getPeerDataCall { minerID: miner_id }.abi_encode();
    let input = Bytes::from(call);
    let tx = TransactionRequest::default()
        .with_to(miner_peer_id_contract)
        .with_input(input);

    let response = rpc_provider
        .call(tx)
        .await
        .map_err(|e| eyre!("Transaction failed: {e}"))?;

    let peer_data: PeerData =
        PeerData::abi_decode(response.as_ref()).map_err(|e| eyre!("Decode failed: {e}"))?;

    if peer_data.peerID.is_empty() {
        return Ok(None);
    }

    Ok(Some(peer_data.peerID.to_string()))
}

pub async fn get_provider_endpoints(
    address: &str,
) -> Result<(ResultCode, Option<Vec<String>>), ErrorCode> {
    let peer_id = if let Some(curio_provider) =
        valid_curio_provider(address).await.map_err(|e| {
            error!("Failed to get peer id from curio: {:?}", e);
            ErrorCode::FailedToGetPeerIdFromCurio
        })? {
        curio_provider
    } else {
        // get peer_id from miner info from lotus rpc
        lotus_rpc::get_peer_id(address).await.map_err(|e| {
            error!("Failed to get peer id: {:?}", e);
            ErrorCode::FailedToGetPeerId
        })?
    };

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
