use common::api_response::ErrorResponse;
use utoipa::OpenApi;

use crate::api::*;

#[derive(OpenApi)]
#[openapi(
    // API Metadata
    info(
        title = "Url Finder",
        description = r#"
This is the API documentation for the Url Finder micro-service.

The Url Finder service is responsible for finding the URL of a miner given its address.

The service is using [CID Contact](https://cid.contact) as source of HTTP entry point for any given miner address.

### Result Codes 
 - **NoCidContactData** - No entry in cid contact
 - **MissingAddrFromCidContact** - No entry point found in cid contact
 - **MissingHttpAddrFromCidContact** - No HTTP entry point in cid contact (taken from ExtendedProviders)
 - **FailedToGetWorkingUrl** - None of tested URLs is working and can be downloaded
 - **NoDealsFound** - No deals found for given miner (should not happen, unless miner address is invalid)
 - **TimedOut** - Searching for working URL is taking too long - probably there is no working URL
 - **Success** - Found working URL
        "#,
        version = "1.0.0"
    ),
    // API Handler Functions
    paths(
        handle_find_url,
        handle_find_retri_by_client_and_sp,
    ),
    components(
        schemas(
            // URL
            FindUrlInput,
            FindUrlResponse,
            // Retri
            FindRetriByClientAndSpInput,
            FindRetriByClientAndSpResponse,

            // misc
            HealthcheckResponse,

            // common
            ErrorCode,
            ErrorResponse,
        ),
      ),
    tags(
        // API Categories
        (name = "URL", description = "Url Finder APIs"),
    )
)]
pub struct ApiDoc;
