# Storage Provider Url Finder

## Introduction

The Storage Provider Url Finder is a microservice designed to test the retrievability of files claimed by Storage Providers (SPs) on the Filecoin network. Its primary goal is to verify whether files associated with deals are accessible via HTTP endpoints advertised by SPs. This service focuses exclusively on HTTP retrievability.

## Table of Contents

- [Development](#development)
  - [Environment Variables](#environment-variables)
  - [Result Codes](#result-codes)
  - [Error Codes](#error-codes)
- [How the Service Works](#how-the-service-works)
  - [Two types of Requests](#two-types-of-requests)
  - [High-Level Workflow](#high-level-workflow)
  - [Design Choices](#design-choices)
- [Potential Issues](#potential-issues)
- [Possible Improvements](#possible-improvements)

## Development

### Environment Variables

- `DATABASE_URL` - The URL of the DMOB database

### Result Codes

- `NoCidContactData` - There is not data in `cid.contact` given `peer_id`
- `MissingAddrFromCidContact` - There are no addresses in `ExtendedProviders` in `cid.contact` response
- `MissingHttpAddrFromCidContact` - There are no http addresses in `ExtendedProviders` in `cid.contact` response
- `NoDealsFound` - No deals found for the given SP address
- `FailedToGetWorkingUrl` - No working URLs found
- `Success` - The URL was found successfully and is returned with the response

### Error Codes

- `FailedToGetPeerId` - Failed to get the peer ID from the lotus rpc
- `FailedToRetrieveCidContactData` - Failed to retrieve the miner info from the database

## How the Service Works

### Two types of Requests

1. **Async Job**: User requests a job, which is processed in the background. The user receives a `job_id` that is used to check the status and results later. The jobs are processed one by one, allowing for multiple jobs to be created without blocking the service
2. **Direct Call**: User requests measurement in synchronous mode, which is processed immediately. The result is not stored or cached and its returned as a response to the request. Response might take up to several minutes and might time out if the request takes too long

### High-Level Workflow 

1. **Input**: The service accepts requests for a Storage Provider address, a Client address, or both.
1. **SP Endpoint Discovery**: For each SP, the service:
   - Retrieves the peer ID using Lotus RPC (`Filecoin.StateMinerInfo`)
   - Calls [cid.contact](https://cid.contact) for HTTP endpoints associated with the peer ID (from `ExtendedProviders` section)
   - Parses multiaddresses to extract usable HTTP endpoints
1. **Deal and PieceCID Selection**:
   - Calls the database for deals matching the SP (and optionally the client)
   - Selects up to `100` **random** pieceCIDs per SP/client pair to ensure a representative sample
1. **URL Construction**:
   - Constructs URLs by combining each HTTP endpoint with each pieceCID (`http://{HTTP_ENPOINT}/piece/{pieceCID}`)
1. **Retrievability Testing**:
   - Concurrently tests up to `20` URLs at a time using HTTP HEAD requests
   - Records which URLs are reachable (return a successful HTTP response)
   - Saves one working URL and calculates the retrievability percentage (working URLs / total tested)
1. **Result**:
   - Returns/Saves the working URL and retrievability percentage
   - Provides detailed result codes and error codes when applicable

### Design Choices

- **Random Sampling**: Testing a random subset of pieceCIDs ensures that retrievability metrics are not biased by deal ordering or selection
- **Async Job**: Using async jobs allows the process to run in the background, mitigating timeouts and allowing to create multiple jobs at once that will be processed one by one
- **External Data Sources**: Lotus RPC and cid.contact are authoritative sources for miner info and endpoints

## Potential Issues

- **HTTP Endpoint Reliability**: Not all SPs maintain reliable or up-to-date HTTP endpoints.
- **HEAD Requests Only**: HEAD requests check for basic reachability, but do not guarantee that the file can be fully downloaded or is valid

## Possible Improvements

1. **Scheduled Checks**: Implement a scheduler to periodically re-check URLs for retrievability, ensuring that the data remains current
1. **Database Integration**: Store results in a database to allow for faster response times
1. **File Header Analysis**: Extend the service to analyze file headers for additional metadata e.g. size, diff between deal size and actual file size
1. **Historical Metrics**: Track retrievability metrics over time for trend analysis
