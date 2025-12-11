use std::env;

use color_eyre::Result;

use crate::types::DbConnectParams;

#[derive(Debug, Clone)]
pub struct Config {
    pub db_url: String,
    pub dmob_db_url: String,
    pub log_level: String,
    pub glif_url: String,
    pub cid_contact_url: String,
    pub proxy_url: Option<String>,
    pub proxy_user: Option<String>,
    pub proxy_password: Option<String>,
    pub proxy_ip_count: Option<u32>,
    pub proxy_default_port: Option<u32>,
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
            cid_contact_url: env::var("CID_CONTACT_URL")
                .unwrap_or("https://cid.contact".to_string()),
            proxy_url: env::var("PROXY_URL").unwrap_or("US".to_string()).into(),
            proxy_user: env::var("PROXY_USER").ok(),
            proxy_password: env::var("PROXY_PASSWORD").ok(),
            proxy_default_port: env::var("PROXY_DEFAULT_PORT")
                .ok()
                .and_then(|s| s.parse().ok()),
            proxy_ip_count: env::var("PROXY_IP_COUNT").ok().and_then(|s| s.parse().ok()),
        })
    }

    // Test helper
    pub fn new_for_test(glif_url: String, cid_contact_url: String) -> Self {
        Self {
            db_url: "dummy".to_string(),
            dmob_db_url: "dummy".to_string(),
            log_level: "info".to_string(),
            glif_url,
            cid_contact_url,
            proxy_password: None,
            proxy_url: None,
            proxy_user: None,
            proxy_ip_count: None,
            proxy_default_port: None,
        }
    }
}
