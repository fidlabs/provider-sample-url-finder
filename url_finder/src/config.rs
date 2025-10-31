use std::env;

use color_eyre::Result;
use once_cell::sync::Lazy;

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new_from_env().unwrap());

#[derive(Debug)]
pub struct Config {
    pub db_url: String,
    pub log_level: String,
}
impl Config {
    pub fn new_from_env() -> Result<Self> {
        Ok(Self {
            db_url: env::var("DATABASE_URL").unwrap(),
            log_level: env::var("LOG_LEVEL").unwrap_or("info".to_string()),
        })
    }
}
