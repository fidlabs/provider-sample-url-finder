/// lib exports for integration testing
/// separated to simulate real api call: http request -> api handler -> service -> repo -> db
pub use std::sync::{Arc, atomic::AtomicUsize};

pub mod api;
pub mod api_response;
pub mod background;
pub mod bms_client;
mod cid_contact;
pub mod config;
mod lotus_rpc;
mod multiaddr_parser;
mod pix_filspark;
pub mod provider_endpoints;
pub mod repository;
pub mod routes;
pub mod services;
pub mod types;
mod url_tester;
pub mod utils;

pub use types::{ErrorCode, ResultCode};

pub struct AppState {
    pub deal_repo: Arc<repository::DealRepository>,
    pub active_requests: Arc<AtomicUsize>,
    pub storage_provider_repo: Arc<repository::StorageProviderRepository>,
    pub url_repo: Arc<repository::UrlResultRepository>,
    pub config: Arc<config::Config>,
}
