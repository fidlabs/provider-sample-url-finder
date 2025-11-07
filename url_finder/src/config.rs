use std::env;

use color_eyre::Result;
use once_cell::sync::Lazy;

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
        Ok(Self {
            db_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
            dmob_db_url: env::var("DMOB_DATABASE_URL").expect("DMOB_DATABASE_URL must be set"),
            log_level: env::var("LOG_LEVEL").unwrap_or("info".to_string()),
            glif_url: env::var("GLIF_URL").unwrap_or("https://api.node.glif.io/rpc/v1".to_string()),
        })
    }
}
