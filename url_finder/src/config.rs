use std::env;

use color_eyre::Result;
use once_cell::sync::Lazy;

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new_from_env().unwrap());

#[derive(Debug)]
pub struct Config {
    pub db_url: String,
    pub log_level: String,
    pub glif_url: String,
    pub proxy_url: Option<String>,
    pub proxy_user: Option<String>,
    pub proxy_password: Option<String>,
}
impl Config {
    pub fn new_from_env() -> Result<Self> {
        Ok(Self {
            db_url: env::var("DATABASE_URL").unwrap(),
            log_level: env::var("LOG_LEVEL").unwrap_or("info".to_string()),
            glif_url: env::var("GLIF_URL").unwrap_or("https://api.node.glif.io/rpc/v1".to_string()),
            proxy_url: env::var("PROXY_URL").unwrap_or("US".to_string()).into(),
            proxy_user: env::var("PROXY_USER").ok(),
            proxy_password: env::var("PROXY_PASSWORD").ok(),
        })
    }
}
