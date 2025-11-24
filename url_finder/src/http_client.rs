use crate::config::CONFIG;
use reqwest::{Client, Proxy};

const RETRI_TIMEOUT_SEC: u64 = 15;

pub fn build_client() -> Result<Client, reqwest::Error> {
    let mut builder = Client::builder().timeout(std::time::Duration::from_secs(RETRI_TIMEOUT_SEC));

    if let (Some(proxy_url), Some(proxy_user), Some(proxy_password)) = (
        &CONFIG.proxy_url,
        &CONFIG.proxy_user,
        &CONFIG.proxy_password,
    ) {
        let proxy = (Proxy::http(proxy_url))?.basic_auth(proxy_user, proxy_password);
        builder = builder.proxy(proxy);
    }

    builder.build()
}
