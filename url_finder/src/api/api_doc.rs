use crate::api_response::ErrorResponse;
use utoipa::OpenApi;

use crate::api::providers::*;
use crate::api::*;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Url Finder",
        description = r#"
This is the API documentation for the Url Finder micro-service.

The Url Finder service is responsible for finding the URL of a miner given its address.

## New Providers API

The `/providers/*` and `/clients/*` endpoints serve pre-computed data from the database with combined URL, retrievability, and performance metrics.

## Legacy URL API

The `/url/*` endpoints remain fully backward compatible.

### Result Codes
 - **NoCidContactData** - No entry in cid contact
 - **MissingAddrFromCidContact** - No entry point found in cid contact
 - **MissingHttpAddrFromCidContact** - No HTTP entry point in cid contact
 - **FailedToGetWorkingUrl** - None of tested URLs is working
 - **NoDealsFound** - No deals found for given miner
 - **Success** - Found working URL
 - **Error** - Provider not indexed yet or error occurred
        "#,
        version = "1.0.0"
    ),
    paths(
        // Legacy API
        handle_find_url_sp,
        handle_find_url_sp_client,
        handle_find_retri_by_client_and_sp,
        handle_find_retri_by_sp,
        handle_find_client,
        handle_healthcheck,
        // New Providers API
        handle_get_provider,
        handle_get_provider_client,
        handle_get_client_providers,
        handle_list_providers,
        handle_bulk_providers,
    ),
    components(
        schemas(
            // Legacy URL
            FindUrlSpPath,
            FindUrlSpResponse,
            FindUrlSpClientPath,
            FindUrlSpClientResponse,

            // Legacy Retri
            FindRetriByClientAndSpPath,
            FindRetriByClientAndSpResponse,

            // Legacy Client
            FindByClientPath,
            FindByClientResponse,

            // New Providers API
            GetProviderPath,
            GetProviderClientPath,
            GetClientProvidersPath,
            ListProvidersQuery,
            BulkProvidersRequest,
            ProviderResponse,
            ProviderClientResponse,
            ClientProvidersResponse,
            ProvidersListResponse,
            BulkProvidersResponse,
            PerformanceResponse,
            BandwidthTestResponse,
            GeolocationTestResponse,

            // Misc
            HealthcheckResponse,

            // Common
            ErrorCode,
            ErrorResponse,
        ),
    ),
    tags(
        (name = "Providers", description = "New Providers API - pre-computed data with performance metrics"),
        (name = "Clients", description = "Client endpoints - providers for a specific client"),
        (name = "URL", description = "Legacy URL Finder APIs"),
        (name = "Healthcheck", description = "Health check endpoints"),
    )
)]
pub struct ApiDoc;
