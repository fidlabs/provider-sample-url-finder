mod create_run;
mod get_deal;
mod get_latest;
mod types;
mod upsert_deal;

use serde::Serialize;

use crate::{
    api_response::{
        ApiResponse, ErrorCode, bad_request_with_code, internal_server_error_with_code,
        not_found_with_code, ok_response,
    },
    services::deal_sli_service::DealSliServiceError,
};

pub use create_run::*;
pub use get_deal::*;
pub use get_latest::*;
pub use types::*;
pub use upsert_deal::*;

fn deal_sli_response<T: Serialize>(
    response: std::result::Result<T, DealSliServiceError>,
) -> Result<ApiResponse<T>, ApiResponse<()>> {
    match response {
        Ok(data) => Ok(ok_response(data)),
        Err(DealSliServiceError::InvalidRequest(message)) => {
            Err(bad_request_with_code(ErrorCode::InvalidRequest, message))
        }
        Err(DealSliServiceError::NotFound(message)) => {
            Err(not_found_with_code(ErrorCode::NotFound, message))
        }
        Err(DealSliServiceError::Internal(error)) => Err(internal_server_error_with_code(
            ErrorCode::InternalError,
            error.to_string(),
        )),
    }
}
