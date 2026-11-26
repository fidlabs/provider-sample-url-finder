use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use rand::Rng;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::{Client, Proxy, Url, redirect};
use tracing::info;

const RETRI_TIMEOUT_SEC: u64 = 15;
static ATOMIC_PROXY_PORT: AtomicU32 = AtomicU32::new(8001);
static ATOMIC_PROXY_LAST_CHANGE: AtomicU64 = AtomicU64::new(0);
static PROXY_LOG_ONCE: Once = Once::new();

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

/// Provider endpoints and manifest locations are chosen by third parties, so they must not be
/// able to point this service at loopback, link-local (cloud metadata) or private addresses.
/// Debug builds allow them because local development and tests use localhost mock servers.
const ALLOW_NON_PUBLIC_TARGETS: bool = cfg!(debug_assertions);
const MAX_REDIRECTS: usize = 10;

pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified())
        }
        IpAddr::V6(ip) => {
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local())
        }
    }
}

pub fn is_allowed_target(ip: IpAddr) -> bool {
    ALLOW_NON_PUBLIC_TARGETS || is_public_ip(ip)
}

fn literal_ip(url: &Url) -> Option<IpAddr> {
    url.host_str()?
        .trim_matches(|c| c == '[' || c == ']')
        .parse()
        .ok()
}

/// Drops non-public addresses from DNS answers, so a hostname cannot be used to reach them.
/// Filtering at connect time also covers redirect hops and DNS answers that change between lookups.
struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((name.as_str(), 0))
                .await?
                .filter(|addr| is_allowed_target(addr.ip()))
                .collect();

            if addrs.is_empty() {
                return Err(
                    format!("{} does not resolve to a public address", name.as_str()).into(),
                );
            }

            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

/// The resolver is not consulted for literal IPs, so redirects to them are checked here.
fn redirect_policy() -> redirect::Policy {
    redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            return attempt.error("too many redirects");
        }
        if literal_ip(attempt.url()).is_some_and(|ip| !is_allowed_target(ip)) {
            return attempt.error("redirect to a non-public address");
        }
        attempt.follow()
    })
}

pub fn build_client(config: &Config) -> Result<Client, reqwest::Error> {
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_secs(RETRI_TIMEOUT_SEC))
        .dns_resolver(Arc::new(PublicOnlyResolver))
        .redirect(redirect_policy());

    if let (
        Some(proxy_url),
        Some(proxy_user),
        Some(proxy_password),
        Some(proxy_ip_count),
        Some(_proxy_default_port),
    ) = (
        &config.proxy_url,
        &config.proxy_user,
        &config.proxy_password,
        &config.proxy_ip_count,
        &config.proxy_default_port,
    ) {
        let port = get_sticky_port_atomic(config);
        let proxy_url_result = format!("{}:{}", proxy_url, port);

        PROXY_LOG_ONCE.call_once(|| {
            info!(
                "HTTP client configured with proxy: {} (user: {}, ip_count: {}, port: {})",
                proxy_url, proxy_user, proxy_ip_count, port
            );
        });

        let proxy = (Proxy::http(proxy_url_result))?.basic_auth(proxy_user, proxy_password);
        builder = builder
            .proxy(proxy)
            .pool_idle_timeout(Duration::from_secs(60 * 60 * 24));
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_public_ip_rejects_internal_ranges() {
        for ip in [
            "127.0.0.1",
            "10.0.0.5",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:10.0.0.5",
            "::ffff:169.254.169.254",
        ] {
            assert!(
                !is_public_ip(ip.parse().unwrap()),
                "{ip} must not be public"
            );
        }
    }

    #[test]
    fn is_public_ip_accepts_public_addresses() {
        for ip in ["1.2.3.4", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(is_public_ip(ip.parse().unwrap()), "{ip} must be public");
        }
    }

    #[test]
    fn literal_ip_reads_v4_and_bracketed_v6_hosts() {
        let ip = |url: &str| literal_ip(&Url::parse(url).unwrap());

        assert_eq!(
            ip("http://169.254.169.254/latest"),
            "169.254.169.254".parse().ok()
        );
        assert_eq!(ip("http://[::1]:8080/"), "::1".parse().ok());
        assert_eq!(ip("https://example.com/"), None);
    }

    // Debug builds allow non-public targets, so this only checks the resolver itself works.
    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn resolver_returns_addresses_for_a_hostname() {
        use std::str::FromStr;

        let addrs = PublicOnlyResolver
            .resolve(Name::from_str("localhost").unwrap())
            .await
            .unwrap();

        assert!(addrs.count() > 0);
    }
}
