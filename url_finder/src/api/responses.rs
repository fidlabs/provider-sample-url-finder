use std::fmt;

use serde::Serialize;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub enum ResultCode {
    NoCidContactData,
    MissingAddrFromCidContact,
    MissingHttpAddrFromCidContact,
    FailedToGetWorkingUrl,
    NoDealsFound,
    TimedOut,
    Success,
}

#[derive(Serialize, ToSchema)]
pub enum ErrorCode {
    FailedToRetrieveCidContactData,
    FailedToGetPeerId,
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorCode::FailedToRetrieveCidContactData => "FailedToRetrieveCidContactData",
            ErrorCode::FailedToGetPeerId => "FailedToGetPeerId",
        };
        write!(f, "{}", s)
    }
}
