use std::fmt;

use serde::Serialize;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema, Clone, PartialEq)]
pub enum ResultCode {
    NoCidContactData,
    MissingAddrFromCidContact,
    MissingHttpAddrFromCidContact,
    FailedToGetWorkingUrl,
    NoDealsFound,
    TimedOut,
    Success,
    JobCreated,
}

#[derive(Serialize, ToSchema, Clone)]
pub enum ErrorCode {
    FailedToRetrieveCidContactData,
    FailedToGetPeerId,
    FailedToGetDeals,
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorCode::FailedToRetrieveCidContactData => "FailedToRetrieveCidContactData",
            ErrorCode::FailedToGetPeerId => "FailedToGetPeerId",
            ErrorCode::FailedToGetDeals => "FailedToGetDeals",
        };
        write!(f, "{}", s)
    }
}
