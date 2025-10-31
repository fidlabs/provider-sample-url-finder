use std::fmt;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Clone, PartialEq)]
pub enum ResultCode {
    NoCidContactData,
    MissingAddrFromCidContact,
    MissingHttpAddrFromCidContact,
    FailedToGetWorkingUrl,
    NoDealsFound,
    TimedOut,
    Success,
    JobCreated,
    Error,
}

#[allow(clippy::enum_variant_names)]
#[derive(Serialize, ToSchema, Clone)]
pub enum ErrorCode {
    NoProviderOrClient,
    NoProvidersFound,
    FailedToRetrieveCidContactData,
    FailedToGetPeerId,
    FailedToGetDeals,
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorCode::NoProviderOrClient => "NoProviderOrClient",
            ErrorCode::NoProvidersFound => "NoProvidersFound",
            ErrorCode::FailedToRetrieveCidContactData => "FailedToRetrieveCidContactData",
            ErrorCode::FailedToGetPeerId => "FailedToGetPeerId",
            ErrorCode::FailedToGetDeals => "FailedToGetDeals",
        };
        write!(f, "{s}")
    }
}
