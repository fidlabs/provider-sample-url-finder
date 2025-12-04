use color_eyre::{Result, eyre::eyre};
use multiaddr::{Multiaddr, Protocol};
use tracing::{debug, info, warn};

pub struct UrlParts {
    protocol: Option<String>,
    host: Option<String>,
    port: Option<String>,
    is_tcp: bool,
}
impl UrlParts {
    fn new() -> Self {
        Self {
            protocol: None,
            host: None,
            port: None,
            is_tcp: false,
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
            "protocol: {:?}, host: {:?}, port: {:?}, is_tcp: {}",
            self.protocol, self.host, self.port, self.is_tcp
        )
    }
}

pub fn parse(addrs: Vec<String>) -> Vec<String> {
    addrs.into_iter().filter_map(parse_addr).collect()
}

fn parse_addr(addr: String) -> Option<String> {
    debug!("Parsing multiaddr: {}", addr);

    match addr.parse::<Multiaddr>() {
        Ok(multiaddr) => {
            let url_parts = parse_multiaddr(multiaddr);
            debug!("Multiaddr parsed to url_parts: {}", url_parts);

            match url_parts.to_url() {
                Ok(endpoint) => {
                    debug!("Multiaddr parsed to endpoint: {}", endpoint);
                    Some(endpoint)
                }
                Err(e) => {
                    warn!("Failed to convert multiaddr {} to URL: {}", addr, e);
                    None
                }
            }
        }
        Err(e) => {
            info!("Failed to parse multiaddr {}: {}", addr, e);
            None
        }
    }
}

fn parse_multiaddr(multiaddr: Multiaddr) -> UrlParts {
    let mut url_parts = multiaddr
        .into_iter()
        .fold(UrlParts::new(), |mut url_parts, protocol| {
            debug!("Processing protocol component: {:?}", protocol);
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
                Protocol::Tcp(port) => {
                    if url_parts.port.is_none() {
                        url_parts.port = Some(port.to_string());
                    }
                    url_parts.is_tcp = true;
                }
                Protocol::Udp(port) => {
                    if url_parts.port.is_none() {
                        url_parts.port = Some(port.to_string());
                    }
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
        });

    // Default to HTTP if we have host + TCP port but no explicit protocol.
    // This handles multiaddrs like /ip4/x.x.x.x/tcp/8080 without explicit /http suffix.
    if url_parts.protocol.is_none()
        && url_parts.host.is_some()
        && url_parts.port.is_some()
        && url_parts.is_tcp
    {
        warn!(
            "Inferring HTTP for TCP multiaddr without explicit protocol: {}:{}",
            url_parts.host.as_ref().unwrap(),
            url_parts.port.as_ref().unwrap()
        );
        url_parts.protocol = Some("http".to_string());
    }

    url_parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ip4_tcp_http() {
        assert_eq!(
            parse_addr("/ip4/1.2.3.4/tcp/8080/http".to_string()),
            Some("http://1.2.3.4:8080".to_string())
        );
    }

    #[test]
    fn infers_http_for_tcp_without_protocol() {
        assert_eq!(
            parse_addr("/ip4/1.2.3.4/tcp/8080".to_string()),
            Some("http://1.2.3.4:8080".to_string())
        );
    }

    #[test]
    fn fails_without_tcp_port() {
        assert_eq!(parse_addr("/dns/example.com/https".to_string()), None);
    }

    #[test]
    fn fails_udp_without_protocol() {
        // UDP should NOT auto-infer HTTP (only TCP does)
        assert_eq!(parse_addr("/ip4/1.2.3.4/udp/8080".to_string()), None);
    }

    #[test]
    fn parse_filters_invalid_multiaddrs() {
        let addrs = vec![
            "/ip4/1.2.3.4/tcp/8080/http".to_string(),
            "/dns/host/https".to_string(),
            "/dns/example.com/tcp/443/https".to_string(),
        ];

        let result = parse(addrs);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "http://1.2.3.4:8080");
        assert_eq!(result[1], "https://example.com:443");
    }
}
