use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::{stream, StreamExt};
use reqwest::Client;
use tracing::debug;

const CONCURENCY_LIMIT: usize = 5;

/// return first working url through head requests
pub async fn filter_working_with_head(urls: Vec<String>) -> Option<String> {
    let client = Client::new();
    let counter = Arc::new(AtomicUsize::new(0));

    // stream of requests with concurency limit
    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                match client.head(&url).send().await {
                    Ok(resp) if resp.status().is_success() => Some(url),
                    _ => {
                        debug!("url not working: {:?}", url);
                        None
                    }
                }
            }
        })
        .buffer_unordered(CONCURENCY_LIMIT);

    while let Some(result) = stream.next().await {
        if let Some(url) = result {
            tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));
            return Some(url);
        }
    }

    tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));

    None
}

/// return the first working url where a file can be downloaded
#[allow(dead_code)]
async fn filter_working_with_get(urls: Vec<String>) -> Option<String> {
    let client = Client::new();
    let counter = Arc::new(AtomicUsize::new(0));

    // stream of requests with concurency limit
    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                match client.get(&url).send().await {
                    Ok(resp)
                        // check if the response has a Content-Disposition header indicating a file
                        if resp.status().is_success()
                            && resp.headers().get("Content-Disposition").is_some() =>
                    {
                        Some(url)
                    }
                    _ => {
                        debug!("url not working: {:?}", url);
                        None
                    }
                }
            }
        })
        .buffer_unordered(CONCURENCY_LIMIT);

    while let Some(result) = stream.next().await {
        if let Some(url) = result {
            tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));

            return Some(url);
        }
    }
    tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));

    None
}
