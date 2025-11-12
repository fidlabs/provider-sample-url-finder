use color_eyre::{eyre::eyre, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
use sqlx::{Decode, Encode, Postgres, Type};
use std::fmt;
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
