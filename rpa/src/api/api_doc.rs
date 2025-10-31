use common::api_response::ErrorResponse;
use utoipa::OpenApi;

use crate::api::*;

#[derive(OpenApi)]
#[openapi(
    // API Metadata
    info(
        title = "RPA (Random Piece Availability)",
        description = r#"
This is the API documentation for the RPA (Random Piece Availability) micro-service.

The RPA service is responsible for finding the URL of a miner given its address.

The service is using [CID Contact](https://cid.contact) as source of HTTP entry point for any given miner address.

### Result Codes 
 - **NoCidContactData** - No entry in cid contact
 - **MissingAddrFromCidContact** - No entry point found in cid contact
 - **MissingHttpAddrFromCidContact** - No HTTP entry point in cid contact (taken from ExtendedProviders)
 - **FailedToGetWorkingUrl** - None of tested URLs is working and can be downloaded
 - **NoDealsFound** - No deals found for given miner (should not happen, unless miner address is invalid)
 - **TimedOut** - Searching for working URL is taking too long - probably there is no working URL
 - **JobCreated** - Asynchronous job was created
 - **Success** - Found working URL
 - **Error** - (async only) Error occurred, check error field
        "#,
        version = "1.0.0"
    ),
    // API Handler Functions
    paths(
        handle_find_url_sp,
        handle_find_url_sp_client,
        handle_find_retri_by_client_and_sp,
        handle_find_retri_by_sp,
        handle_find_client,

        handle_create_job,
        handle_get_job,

        handle_healthcheck,
    ),
    components(
        schemas(
            // URL
            FindUrlSpPath,
            FindUrlSpResponse,
            FindUrlSpClientPath,
            FindUrlSpClientResponse,

            // Retri
            FindRetriByClientAndSpPath,
            FindRetriByClientAndSpResponse,

            // Client
            FindByClientPath,
            FindByClientResponse,

            // Job
            CreateJobPayload,
            CreateJobResponse,
            GetJobPath,
            GetJobResponse,

            // misc
            HealthcheckResponse,

            // common
            ErrorCode,
            ErrorResponse,
        ),
      ),
    tags(
        // API Categories
        (name = "JOB", description = "Async RPA APIs"),
        (name = "URL", description = "RPA APIs"),
        (name = "Healthcheck", description = "RPA Misc APIs"),
    )
)]
pub struct ApiDoc;
