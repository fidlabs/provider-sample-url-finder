# Storage Provider Url Finder

The Url Finder service is responsible for finding the URL of a miner given its address.

## Environment Variables

- `DATABASE_URL` - The URL of the DMOB database

## Preface

The Url Finder service is a simple service that is responsible for finding the URL 

of a miner given its address. The service is used by the Storage Provider service to find the URL of a miner when it is required to send a message to the miner.

## Result Codes

- `NoCidContactData` - There is not data in `cid.contact` given `peer_id`
- `MissingAddrFromCidContact` - There are no addresses in `ExtendedProviders` in `cid.contact` response
- `MissingHttpAddrFromCidContact` - There are no http addresses in `ExtendedProviders` in `cid.contact` response
- `NoDealsFound` - No deals found for the given SP address
- `FailedToGetWorkingUrl` - No working URLs found
- `Success` - The URL was found successfully and is returned with the response

## Error Codes

- `FailedToGetPeerId` - Failed to get the peer ID from the lotus rpc
- `FailedToRetrieveCidContactData` - Failed to retrieve the miner info from the database