//! Shared test constants.
//! Each test file is compiled as a separate binary, so some constants may appear unused in specific binaries.
#![allow(dead_code)]

use url_finder::types::{ClientAddress, ClientId, ProviderAddress, ProviderId};

pub const TEST_PIECE_CID: &str = "baga6ea4seaqao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2mpq";

pub const TEST_PIECE_CID_2: &str =
    "baga6ea4seaqao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2AAA";

// String constants for DB seeding functions
pub const TEST_CLIENT_ID_DB: &str = "1000";
pub const TEST_CLIENT_ID_API: &str = "f01000";
pub const TEST_PROVIDER_1_DB: &str = "88881000";
pub const TEST_PROVIDER_1_API: &str = "f088881000";
pub const TEST_PROVIDER_2_DB: &str = "88882000";
pub const TEST_PROVIDER_2_API: &str = "f088882000";

// Typed helpers for tests
pub fn test_client_id() -> ClientId {
    ClientId::new(TEST_CLIENT_ID_DB).unwrap()
}

pub fn test_client_address() -> ClientAddress {
    ClientAddress::new(TEST_CLIENT_ID_API).unwrap()
}

pub fn test_provider_1_id() -> ProviderId {
    ProviderId::new(TEST_PROVIDER_1_DB).unwrap()
}

pub fn test_provider_1_address() -> ProviderAddress {
    ProviderAddress::new(TEST_PROVIDER_1_API).unwrap()
}

pub fn test_provider_2_id() -> ProviderId {
    ProviderId::new(TEST_PROVIDER_2_DB).unwrap()
}

pub fn test_provider_2_address() -> ProviderAddress {
    ProviderAddress::new(TEST_PROVIDER_2_API).unwrap()
}

pub const TEST_MULTIADDR_HTTP_80: &str = "/ip4/1.2.3.4/tcp/80/http";
pub const TEST_MULTIADDR_HTTP_8080: &str = "/ip4/1.2.3.4/tcp/8080/http";

pub fn multiaddrs_http_80() -> Vec<String> {
    vec![TEST_MULTIADDR_HTTP_80.to_string()]
}

pub fn multiaddrs_http_8080() -> Vec<String> {
    vec![TEST_MULTIADDR_HTTP_8080.to_string()]
}

pub fn multiaddrs_empty() -> Vec<String> {
    vec![]
}

// Provider API test constants
pub const TEST_PROVIDER_3_DB: &str = "88883000";
pub const TEST_PROVIDER_3_API: &str = "f088883000";
pub const TEST_CLIENT_2_DB: &str = "2000";
pub const TEST_CLIENT_2_API: &str = "f02000";

pub const TEST_WORKING_URL: &str = "http://example.com/piece";
pub const TEST_WORKING_URL_2: &str = "http://example2.com/piece";
