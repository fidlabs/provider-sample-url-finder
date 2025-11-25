use std::env;

use color_eyre::Result;
use once_cell::sync::Lazy;

use crate::types::DbConnectParams;

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new_from_env().unwrap());

#[derive(Debug)]
pub struct Config {
    pub db_url: String,
    pub dmob_db_url: String,
    pub log_level: String,
    pub glif_url: String,
}
impl Config {
    pub fn new_from_env() -> Result<Self> {
        let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            let json_params = env::var("DB_CONNECT_PARAMS_JSON")
                .expect("DB_CONNECT_PARAMS_JSON environment variable not set");

            let params: DbConnectParams =
                serde_json::from_str(&json_params).expect("Invalid JSON in DB_CONNECT_PARAMS_JSON");

            params.to_url()
        });

        Ok(Self {
            db_url,
            dmob_db_url: env::var("DMOB_DATABASE_URL").expect("DMOB_DATABASE_URL must be set"),
            log_level: env::var("LOG_LEVEL").unwrap_or("info".to_string()),
            glif_url: env::var("GLIF_URL").unwrap_or("https://api.node.glif.io/rpc/v1".to_string()),
        })
    }
}
