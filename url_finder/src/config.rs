use std::env;

use color_eyre::Result;
use tracing::warn;

use crate::types::DbConnectParams;

// Double-tap consistency testing settings
pub const DOUBLE_TAP_DELAY_MS: u64 = 500;
pub const RANGE_REQUEST_BYTES: u64 = 4096;
pub const MAX_CONCURRENT_URL_TESTS: usize = 20;

// Thresholds
pub const RELIABILITY_TIMEOUT_THRESHOLD: f64 = 0.30;
pub const MIN_VALID_CONTENT_LENGTH: u64 = 8 * 1024 * 1024 * 1024; // 8GB

// History endpoint settings
pub const MAX_HISTORY_DAYS: i64 = 30;

fn parse_positive_i64_or_default(env_var: &str, default: i64) -> i64 {
    assert!(default > 0, "default must be positive");
    match env::var(env_var) {
        Ok(s) => match s.parse::<i64>() {
            Ok(v) if v > 0 => v,
            Ok(v) => {
                warn!("{env_var}={v} is not positive, defaulting to {default}");
                default
            }
            Err(e) => {
                warn!("{env_var}='{s}' is not a valid integer ({e}), defaulting to {default}");
                default
            }
        },
        Err(_) => default,
    }
}

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
    pub bms_url: String,
    pub bms_default_worker_count: i64,
    pub bms_test_interval_days: i64,
    pub max_concurrent_providers: usize,
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
            bms_url: env::var("BMS_URL").expect("BMS_URL must be set"),
            bms_default_worker_count: parse_positive_i64_or_default("BMS_WORKER_COUNT", 10),
            bms_test_interval_days: parse_positive_i64_or_default("BMS_TEST_INTERVAL_DAYS", 7),
            max_concurrent_providers: env::var("MAX_CONCURRENT_PROVIDERS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&v| v > 0 && v <= 100)
                .unwrap_or(10),
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
            bms_url: "http://localhost:8080".to_string(),
            bms_default_worker_count: 10,
            bms_test_interval_days: 7,
            max_concurrent_providers: 10,
        }
    }
}
