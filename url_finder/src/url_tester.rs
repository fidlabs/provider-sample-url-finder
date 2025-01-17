use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::{stream, StreamExt};
use reqwest::Client;
use tracing::debug;

const FILTER_CONCURENCY_LIMIT: usize = 5;
const RETRI_CONCURENCY_LIMIT: usize = 20;
const RETRI_TIMEOUT_SEC: u64 = 15;

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
        .buffer_unordered(FILTER_CONCURENCY_LIMIT);

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
        .buffer_unordered(FILTER_CONCURENCY_LIMIT);

    while let Some(result) = stream.next().await {
        if let Some(url) = result {
            tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));

            return Some(url);
        }
    }
    tracing::info!("number of requests: {:?}", counter.load(Ordering::SeqCst));

    None
}

/// return retrivable percent of the urls
pub async fn get_retrivability_with_head(urls: Vec<String>) -> f64 {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(RETRI_TIMEOUT_SEC))
        .build()
        .unwrap();
    let success_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));

    // stream of requests with concurency limit
    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let total_clone = Arc::clone(&total_counter);
            let success_clone = Arc::clone(&success_counter);
            async move {
                total_clone.fetch_add(1, Ordering::SeqCst);
                match client.head(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        tracing::info!("url WORKING: {:?}", url);
                        success_clone.fetch_add(1, Ordering::SeqCst);
                        Some(url)
                    }
                    _ => {
                        tracing::error!("url not working: {:?}", url);
                        debug!("url not working: {:?}", url);
                        None
                    }
                }
            }
        })
        .buffer_unordered(RETRI_CONCURENCY_LIMIT);

    while let Some(_result) = stream.next().await {
        // process the stream
        // we don't really need the resulting url
    }

    let success = success_counter.load(Ordering::SeqCst);
    let total = total_counter.load(Ordering::SeqCst);

    let retri_percentage = if total > 0 {
        (success as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    debug!(
        "Successfully retrieved URLs: {} out of {} ({:.2}%)",
        success, total, retri_percentage
    );

    round_to_two_decimals(retri_percentage)
}

fn round_to_two_decimals(number: f64) -> f64 {
    (number * 100.0).round() / 100.0
}
