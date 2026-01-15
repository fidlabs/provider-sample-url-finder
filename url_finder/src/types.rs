use color_eyre::{Result, eyre::eyre};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
use sqlx::{Decode, Encode, Postgres, Type};
use std::fmt;
use std::str::FromStr;
use utoipa::ToSchema;

#[derive(Deserialize)]
pub(super) struct DbConnectParams {
    password: String,
    dbname: String,
    engine: String,
    port: u16,
    host: String,
    username: String,
}

impl DbConnectParams {
    pub fn to_url(&self) -> String {
        format!(
            "{}://{}:{}@{}:{}/{}?{}",
            self.engine,
            self.username,
            urlencoding::encode(&self.password),
            self.host,
            self.port,
            self.dbname,
            std::env::var("DB_OPTIONS").unwrap_or_default(),
        )
    }
}

/// Provider address with "f0" prefix (e.g., "f0123456") - for APIs/RPC
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ProviderAddress(String);

impl ProviderAddress {
    pub fn new(addr: impl Into<String>) -> Result<Self> {
        let addr = addr.into();
        let pattern = Regex::new(r"^f0\d{1,8}$").unwrap();
        if !pattern.is_match(&addr) {
            return Err(eyre!("Invalid provider address: {}", addr));
        }
        Ok(Self(addr))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ProviderAddress {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<ProviderId> for ProviderAddress {
    fn from(id: ProviderId) -> Self {
        Self(format!("f0{}", id.0))
    }
}

impl From<ProviderAddress> for ProviderId {
    fn from(addr: ProviderAddress) -> Self {
        // ProviderAddress is validated at construction time, unwrap is safe here
        Self(addr.0.strip_prefix("f0").unwrap().to_string())
    }
}

/// Provider ID without "f0" prefix (e.g., "123456") - for database
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ProviderId(String);

impl ProviderId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if !id.chars().all(|c| c.is_numeric()) || id.is_empty() || id.len() > 8 {
            return Err(eyre!("Invalid provider id: {}", id));
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ProviderId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Type<Postgres> for ProviderId {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }
}

impl<'r> Decode<'r, Postgres> for ProviderId {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let s = <String as Decode<Postgres>>::decode(value)?;
        Ok(Self(s))
    }
}

impl<'q> Encode<'q, Postgres> for ProviderId {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <String as Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}

/// Client address with "f0" prefix (e.g., "f0123456") - for APIs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ClientAddress(String);

impl ClientAddress {
    pub fn new(addr: impl Into<String>) -> Result<Self> {
        let addr = addr.into();
        let pattern = Regex::new(r"^f0\d{1,8}$").unwrap();
        if !pattern.is_match(&addr) {
            return Err(eyre!("Invalid client address: {}", addr));
        }
        Ok(Self(addr))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ClientAddress {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<ClientId> for ClientAddress {
    fn from(id: ClientId) -> Self {
        Self(format!("f0{}", id.0))
    }
}

impl From<ClientAddress> for ClientId {
    fn from(addr: ClientAddress) -> Self {
        // ClientAddress is validated at construction time, unwrap is safe here
        Self(addr.0.strip_prefix("f0").unwrap().to_string())
    }
}

/// Client ID without "f0" prefix (e.g., "123456") - for database
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ClientId(String);

impl ClientId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if !id.chars().all(|c| c.is_numeric()) || id.is_empty() || id.len() > 8 {
            return Err(eyre!("Invalid client id: {}", id));
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ClientId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Type<Postgres> for ClientId {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }
}

impl<'r> Decode<'r, Postgres> for ClientId {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let s = <String as Decode<Postgres>>::decode(value)?;
        Ok(Self(s))
    }
}

impl<'q> Encode<'q, Postgres> for ClientId {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <String as Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DiscoveryType {
    Provider,
    ProviderClient,
}

impl fmt::Display for DiscoveryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider => write!(f, "Provider"),
            Self::ProviderClient => write!(f, "ProviderClient"),
        }
    }
}

impl FromStr for DiscoveryType {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Provider" => Ok(Self::Provider),
            "ProviderClient" => Ok(Self::ProviderClient),
            _ => Err(color_eyre::eyre::eyre!("Invalid discovery type: {}", s)),
        }
    }
}

impl Type<Postgres> for DiscoveryType {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("discovery_type")
    }
}

impl<'r> Decode<'r, Postgres> for DiscoveryType {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let s = <&str as Decode<Postgres>>::decode(value)?;
        s.parse().map_err(Into::into)
    }
}

impl<'q> Encode<'q, Postgres> for DiscoveryType {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <&str as Encode<Postgres>>::encode_by_ref(&self.to_string().as_str(), buf)
    }
}

impl PgHasArrayType for DiscoveryType {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_discovery_type")
    }
}

/// Result codes for URL discovery operations
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq)]
pub enum ResultCode {
    NoCidContactData,
    MissingAddrFromCidContact,
    MissingHttpAddrFromCidContact,
    FailedToGetWorkingUrl,
    NoDealsFound,
    TimedOut,
    Success,
    Error,
}

impl ResultCode {
    // Human-readable message for each result code
    pub fn message(&self) -> Option<&'static str> {
        match self {
            Self::Success => None,
            Self::NoCidContactData => Some("No data available from cid.contact for this provider"),
            Self::MissingAddrFromCidContact => {
                Some("No address information found from cid.contact")
            }
            Self::MissingHttpAddrFromCidContact => {
                Some("No HTTP address found in cid.contact data")
            }
            Self::FailedToGetWorkingUrl => Some("Failed to find a working URL for this provider"),
            Self::NoDealsFound => Some("No deals found for this provider"),
            Self::TimedOut => Some("Request timed out while discovering URL"),
            Self::Error => Some("An error occurred during URL discovery"),
        }
    }
}

impl fmt::Display for ResultCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ResultCode::NoCidContactData => "NoCidContactData",
            ResultCode::MissingAddrFromCidContact => "MissingAddrFromCidContact",
            ResultCode::MissingHttpAddrFromCidContact => "MissingHttpAddrFromCidContact",
            ResultCode::FailedToGetWorkingUrl => "FailedToGetWorkingUrl",
            ResultCode::NoDealsFound => "NoDealsFound",
            ResultCode::TimedOut => "TimedOut",
            ResultCode::Success => "Success",
            ResultCode::Error => "Error",
        };
        write!(f, "{s}")
    }
}

impl FromStr for ResultCode {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NoCidContactData" => Ok(Self::NoCidContactData),
            "MissingAddrFromCidContact" => Ok(Self::MissingAddrFromCidContact),
            "MissingHttpAddrFromCidContact" => Ok(Self::MissingHttpAddrFromCidContact),
            "FailedToGetWorkingUrl" => Ok(Self::FailedToGetWorkingUrl),
            "NoDealsFound" => Ok(Self::NoDealsFound),
            "TimedOut" => Ok(Self::TimedOut),
            "Success" => Ok(Self::Success),
            "Error" => Ok(Self::Error),
            _ => Err(color_eyre::eyre::eyre!("Invalid result code: {}", s)),
        }
    }
}

impl Type<Postgres> for ResultCode {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("result_code")
    }
}

impl<'r> Decode<'r, Postgres> for ResultCode {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let s = <&str as Decode<Postgres>>::decode(value)?;
        s.parse().map_err(Into::into)
    }
}

impl<'q> Encode<'q, Postgres> for ResultCode {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <&str as Encode<Postgres>>::encode_by_ref(&self.to_string().as_str(), buf)
    }
}

impl PgHasArrayType for ResultCode {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_result_code")
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq)]
pub enum ErrorCode {
    NoProviderOrClient,
    NoProvidersFound,
    FailedToRetrieveCidContactData,
    FailedToGetPeerId,
    FailedToGetDeals,
    FailedToGetPeerIdFromCurio,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorCode::NoProviderOrClient => "NoProviderOrClient",
            ErrorCode::NoProvidersFound => "NoProvidersFound",
            ErrorCode::FailedToRetrieveCidContactData => "FailedToRetrieveCidContactData",
            ErrorCode::FailedToGetPeerId => "FailedToGetPeerId",
            ErrorCode::FailedToGetDeals => "FailedToGetDeals",
            ErrorCode::FailedToGetPeerIdFromCurio => "FailedToGetPeerIdFromCurio",
        };
        write!(f, "{s}")
    }
}

impl FromStr for ErrorCode {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NoProviderOrClient" => Ok(Self::NoProviderOrClient),
            "NoProvidersFound" => Ok(Self::NoProvidersFound),
            "FailedToRetrieveCidContactData" => Ok(Self::FailedToRetrieveCidContactData),
            "FailedToGetPeerId" => Ok(Self::FailedToGetPeerId),
            "FailedToGetDeals" => Ok(Self::FailedToGetDeals),
            "FailedToGetPeerIdFromCurio" => Ok(Self::FailedToGetPeerIdFromCurio),
            _ => Err(color_eyre::eyre::eyre!("Invalid error code: {}", s)),
        }
    }
}

impl Type<Postgres> for ErrorCode {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("error_code")
    }
}

impl<'r> Decode<'r, Postgres> for ErrorCode {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let s = <&str as Decode<Postgres>>::decode(value)?;
        s.parse().map_err(Into::into)
    }
}

impl<'q> Encode<'q, Postgres> for ErrorCode {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, BoxDynError> {
        <&str as Encode<Postgres>>::encode_by_ref(&self.to_string().as_str(), buf)
    }
}

impl PgHasArrayType for ErrorCode {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_error_code")
    }
}

/// Error types for URL testing operations
#[derive(Debug, Clone, PartialEq)]
pub enum UrlTestError {
    Timeout,
    ConnectionRefused,
    ConnectionReset,
    DnsFailure,
    TlsError,
    HttpError(u16),
    Other(String),
}

impl std::fmt::Display for UrlTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "timeout"),
            Self::ConnectionRefused => write!(f, "connection_refused"),
            Self::ConnectionReset => write!(f, "connection_reset"),
            Self::DnsFailure => write!(f, "dns_failure"),
            Self::TlsError => write!(f, "tls_error"),
            Self::HttpError(code) => write!(f, "http_{code}"),
            Self::Other(msg) => write!(f, "other: {msg}"),
        }
    }
}

/// Classification of why a URL test was inconsistent
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InconsistencyType {
    /// (Small|Failed, Valid) - Second tap returned valid data after warm-up.
    /// Provider CAN serve data, just needs initial request to "warm up".
    /// This is the pattern double-tap was designed to detect and handle.
    WarmUp,
    /// (Valid, Small|Failed) - First tap valid, second degraded.
    /// Provider is unreliable - served data once then stopped.
    Flaky,
    /// (Small, Small|Failed) or (Failed, Small) - Neither tap returned valid data.
    /// Provider consistently returns small/garbage responses.
    SmallResponses,
    /// (Failed, Failed) - Both taps failed completely.
    /// Provider unreachable or broken.
    BothFailed,
    /// (Valid, Valid) but different Content-Length.
    /// Data integrity issue - file size changed between requests.
    SizeMismatch,
}

/// Result of a double-tap URL test
#[derive(Debug, Clone)]
pub struct UrlTestResult {
    pub url: String,
    pub success: bool,
    pub consistent: bool,
    pub inconsistency_type: Option<InconsistencyType>,
    pub content_length: Option<u64>,
    pub response_time_ms: u64,
    pub error: Option<UrlTestError>,
}

/// Analysis of URL test results for a provider
#[derive(Debug, Clone)]
pub struct ProviderAnalysis {
    pub retrievability_percent: f64,
    pub is_consistent: bool,
    pub is_reliable: bool,
    pub sample_count: usize,
    pub success_count: usize,
    pub timeout_count: usize,
    pub inconsistent_count: usize,
    pub inconsistent_warm_up: usize,
    pub inconsistent_flaky: usize,
    pub inconsistent_small_responses: usize,
    pub inconsistent_both_failed: usize,
    pub inconsistent_size_mismatch: usize,
}

impl ProviderAnalysis {
    pub fn empty() -> Self {
        Self {
            retrievability_percent: 0.0,
            is_consistent: false,
            is_reliable: false,
            sample_count: 0,
            success_count: 0,
            timeout_count: 0,
            inconsistent_count: 0,
            inconsistent_warm_up: 0,
            inconsistent_flaky: 0,
            inconsistent_small_responses: 0,
            inconsistent_both_failed: 0,
            inconsistent_size_mismatch: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_conversions() {
        let provider_address = ProviderAddress::new("f0123456").unwrap();
        let provider_id: ProviderId = provider_address.clone().into();
        assert_eq!(provider_id.as_str(), "123456");

        let provider_address2: ProviderAddress = provider_id.into();
        assert_eq!(provider_address2.as_str(), "f0123456");
    }

    #[test]
    fn test_client_conversions() {
        let client_address = ClientAddress::new("f0789012").unwrap();
        let client_id: ClientId = client_address.clone().into();
        assert_eq!(client_id.as_str(), "789012");

        let client_address2: ClientAddress = client_id.into();
        assert_eq!(client_address2.as_str(), "f0789012");
    }
}
