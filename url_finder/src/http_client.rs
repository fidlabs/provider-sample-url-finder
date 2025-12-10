use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use rand::Rng;
use reqwest::{Client, Proxy};

const RETRI_TIMEOUT_SEC: u64 = 15;
static ATOMIC_PROXY_PORT: AtomicU32 = AtomicU32::new(8001);
static ATOMIC_PROXY_LAST_CHANGE: AtomicU64 = AtomicU64::new(0);

fn get_sticky_port_atomic(ip_count: u32) -> u16 {
    // if no proxy ip count configured, return default port
    if ip_count == 0 {
        return 8000;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let last = ATOMIC_PROXY_LAST_CHANGE.load(Ordering::Relaxed);

    let expired = now - last > 24 * 3600; // rotate every 24 hours

    if expired {
        let start = 8001;
        let end = start + ip_count - 1;

        let mut rng = rand::rng();
        let new_port = rng.random_range(start..=end);

        ATOMIC_PROXY_PORT.store(new_port, Ordering::Relaxed);
        ATOMIC_PROXY_LAST_CHANGE.store(now, Ordering::Relaxed);

        return new_port as u16;
    }

    ATOMIC_PROXY_PORT.load(Ordering::Relaxed) as u16
}

pub fn build_client(config: &Config) -> Result<Client, reqwest::Error> {
    let mut builder = Client::builder().timeout(std::time::Duration::from_secs(RETRI_TIMEOUT_SEC));

    if let (Some(proxy_url), Some(proxy_user), Some(proxy_password), Some(proxy_ip_count)) = (
        &config.proxy_url,
        &config.proxy_user,
        &config.proxy_password,
        &config.proxy_ip_count,
    ) {
        let ip_count = *proxy_ip_count as u32;

        let port = get_sticky_port_atomic(ip_count);
        let proxy_url_result = format!("{}:{}", proxy_url, port);

        println!("Using proxy: {}", proxy_url_result);

        let proxy = (Proxy::http(proxy_url_result))?.basic_auth(proxy_user, proxy_password);
        builder = builder
            .proxy(proxy)
            .pool_idle_timeout(Duration::from_secs(60 * 60 * 24));
    }

    builder.build()
}
