use std::time::Duration;

use alloy::sol_types::SolType;
use alloy::{
    network::TransactionBuilder,
    primitives::{Address, Bytes, address},
    providers::{Provider, ProviderBuilder},
    rpc::types::eth::TransactionRequest,
    sol,
    sol_types::SolCall,
};
use color_eyre::{Result, eyre::eyre};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::{
    ErrorCode, ResultCode,
    cid_contact::{self, CidContactError},
    config::Config,
    lotus_rpc, multiaddr_parser,
    types::ProviderAddress,
};

sol! {
    struct PeerData {
        string peerID;
        bytes multiaddrs;
    }

    function getPeerData(uint64 minerID) view returns (PeerData);
}

pub async fn valid_curio_provider(
    config: &Config,
    address: &ProviderAddress,
) -> Result<Option<String>> {
    let rpc_url = &config.glif_url;

    let rpc_provider = ProviderBuilder::new()
        .connect(rpc_url)
        .await
        .map_err(|err| eyre!("Building provider failed: {}", err))?;

    let miner_peer_id_contract: Address = address!("0x14183aD016Ddc83D638425D6328009aa390339Ce");

    let miner_id = address
        .as_str()
        .strip_prefix("f")
        .ok_or_else(|| eyre!("Address does not start with 'f': {}", address))?
        .parse::<u64>()
        .map_err(|e| eyre!("Failed to parse miner ID from '{}': {}", address, e))?;

    let call: Vec<u8> = getPeerDataCall { minerID: miner_id }.abi_encode();
    let input = Bytes::from(call);
    let tx = TransactionRequest::default()
        .with_to(miner_peer_id_contract)
        .with_input(input);

    let mut response = None;

    for attempt in 1..=3 {
        match rpc_provider.call(tx.clone()).await {
            Ok(res) => {
                response = Some(res);
                break;
            }
            Err(e) => {
                info!("Attempt {attempt}/3 failed: {e} for address: {address}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    let Some(response) = response else {
        warn!("Curio lookup failed after 3 attempts for {address}");
        return Err(eyre!("All 3 attempts failed for address {address}"));
    };

    let peer_data: PeerData = PeerData::abi_decode(response.as_ref())?;

    if peer_data.peerID.is_empty() {
        return Ok(None);
    }

    info!("Curio provider found: {}: {}", &peer_data.peerID, &address);
    Ok(Some(peer_data.peerID.to_string()))
}

pub async fn get_provider_endpoints(
    config: &Config,
    address: &ProviderAddress,
) -> Result<(ResultCode, Option<Vec<String>>), ErrorCode> {
    let peer_id = match valid_curio_provider(config, address).await {
        Ok(Some(curio_provider)) => curio_provider,
        _ => {
            debug!("Falling back to lotus for peer_id lookup");
            lotus_rpc::get_peer_id(config, address).await.map_err(|e| {
                error!("Failed to get peer id from lotus: {e:?}");
                ErrorCode::FailedToGetPeerId
            })?
        }
    };

    // get cid contact response
    let cid_contact_res = match cid_contact::get_contact(config, &peer_id).await {
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
    let mut endpoints = multiaddr_parser::parse(addrs);
    if endpoints.is_empty() {
        debug!("Missing http addr from cid contact, No endpoints found");

        return Ok((ResultCode::MissingHttpAddrFromCidContact, None));
    }

    // Deduplicate endpoints
    let original_count = endpoints.len();
    endpoints.sort();
    endpoints.dedup();
    if endpoints.len() < original_count {
        debug!(
            "Deduplicated endpoints: {} -> {} unique",
            original_count,
            endpoints.len()
        );
    }

    Ok((ResultCode::Success, Some(endpoints)))
}
