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
