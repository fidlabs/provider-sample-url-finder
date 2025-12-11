use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use rand::Rng;
use reqwest::{Client, Proxy};
use tracing::info;

const RETRI_TIMEOUT_SEC: u64 = 15;
static ATOMIC_PROXY_PORT: AtomicU32 = AtomicU32::new(8001);
static ATOMIC_PROXY_LAST_CHANGE: AtomicU64 = AtomicU64::new(0);

fn get_sticky_port_atomic(config: &Config) -> u32 {
    let proxy_default_port = config.proxy_default_port.unwrap();
    let ip_count = config.proxy_ip_count.unwrap();

    // if no proxy ip count configured, return default port
    if ip_count == 0 {
        return proxy_default_port;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let last = ATOMIC_PROXY_LAST_CHANGE.load(Ordering::Relaxed);

    let expired = now - last > 24 * 3600; // rotate every 24 hours

    if expired {
        let start = config.proxy_default_port.unwrap();
        let end = start + ip_count - 1;

        let mut rng = rand::rng();
        let new_port = rng.random_range(start..=end);

        ATOMIC_PROXY_PORT.store(new_port, Ordering::Relaxed);
        ATOMIC_PROXY_LAST_CHANGE.store(now, Ordering::Relaxed);

        return new_port as u32;
    }

    ATOMIC_PROXY_PORT.load(Ordering::Relaxed)
}

pub fn build_client(config: &Config) -> Result<Client, reqwest::Error> {
    let mut builder = Client::builder().timeout(std::time::Duration::from_secs(RETRI_TIMEOUT_SEC));

    if let (
        Some(proxy_url),
        Some(proxy_user),
        Some(proxy_password),
        Some(proxy_ip_count),
        Some(proxy_default_port),
    ) = (
        &config.proxy_url,
        &config.proxy_user,
        &config.proxy_password,
        &config.proxy_ip_count,
        &config.proxy_default_port,
    ) {
        info!(
            "Configuring HTTP client with proxy: {} (user: {}, ip_count: {}, default_port: {})",
            proxy_url, proxy_user, proxy_ip_count, proxy_default_port
        );

        let port = get_sticky_port_atomic(config);
        let proxy_url_result = format!("{}:{}", proxy_url, port);

        info!("Start using proxy: {}", proxy_url_result);

        let proxy = (Proxy::http(proxy_url_result))?.basic_auth(proxy_user, proxy_password);
        builder = builder
            .proxy(proxy)
            .pool_idle_timeout(Duration::from_secs(60 * 60 * 24));
    }

    builder.build()
}
