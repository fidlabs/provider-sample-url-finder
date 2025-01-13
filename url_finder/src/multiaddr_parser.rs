use color_eyre::{eyre::eyre, Result};
use multiaddr::{Multiaddr, Protocol};
use tracing::debug;

pub struct UrlParts {
    protocol: Option<String>,
    host: Option<String>,
    port: Option<String>,
}
impl UrlParts {
    fn new() -> Self {
        Self {
            protocol: None,
            host: None,
            port: None,
        }
    }
    fn to_url(&self) -> Result<String> {
        Ok(format!(
            "{}://{}:{}",
            self.protocol.clone().ok_or(eyre!("Missing protocol"))?,
            self.host.clone().ok_or(eyre!("Missing host"))?,
            self.port.clone().ok_or(eyre!("Missing port"))?
        ))
    }
}
impl std::fmt::Display for UrlParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "protocol: {:?}, host: {:?}, port: {:?}",
            self.protocol, self.host, self.port
        )
    }
}

pub fn parse(addrs: Vec<String>) -> Vec<String> {
    addrs.into_iter().filter_map(parse_addr).collect()
}

fn parse_addr(addr: String) -> Option<String> {
    debug!("parsing addr: {:?}", addr);

    match addr.parse::<Multiaddr>() {
        Ok(multiaddr) => {
            let url_parts = parse_multiaddr(multiaddr);
            debug!("url_parts: {}", url_parts);

            match url_parts.to_url() {
                Ok(endpoint) => {
                    debug!("parsed endpoint: {:?}", endpoint);
                    Some(endpoint)
                }
                Err(e) => {
                    debug!(
                        "Failed to convert multiaddr: {:?} to URL: {:?}",
                        addr,
                        e.to_string()
                    );
                    None
                }
            }
        }
        Err(e) => {
            debug!("Failed to parse multiaddr: {:?} due to {:?}", addr, e);
            None
        }
    }
}

fn parse_multiaddr(multiaddr: Multiaddr) -> UrlParts {
    multiaddr
        .into_iter()
        .fold(UrlParts::new(), |mut url_parts, protocol| {
            match protocol {
                Protocol::Dns(dns) | Protocol::Dns4(dns) | Protocol::Dns6(dns) => {
                    url_parts.host = Some(dns.to_string());
                }
                Protocol::Ip4(ip) => {
                    url_parts.host = Some(ip.to_string());
                }
                Protocol::Ip6(ip) => {
                    url_parts.host = Some(ip.to_string());
                }
                Protocol::Tcp(port) | Protocol::Udp(port) => {
                    url_parts.port = Some(port.to_string());
                }
                Protocol::Http => {
                    url_parts.protocol = Some("http".to_string());
                }
                Protocol::Https => {
                    url_parts.protocol = Some("https".to_string());
                }
                _ => {}
            }
            url_parts
        })
}
