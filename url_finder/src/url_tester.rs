use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::{StreamExt, stream};
use reqwest::Client;
use tracing::{debug, info};

const FILTER_CONCURENCY_LIMIT: usize = 5;
const RETRI_CONCURENCY_LIMIT: usize = 20;
const RETRI_TIMEOUT_SEC: u64 = 15;

/// return first working url through head requests
/// let's keep both head and get versions for now
#[allow(dead_code)]
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
                    Ok(resp) => {
                        if resp.status().is_success() {
                            Some(url)
                        } else {
                            debug!("URL::HEAD not working: {:?}", url);
                            None
                        }
                    }
                    Err(err) => {
                        debug!(
                            "Head request for working url failed for {:?}: {:?}",
                            url, err
                        );
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
/// let's keep both head and get versions for now
#[allow(dead_code)]
pub async fn get_retrivability_with_head(urls: Vec<String>) -> (Option<String>, f64) {
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
                        debug!("url WORKING: {:?}", url);
                        success_clone.fetch_add(1, Ordering::SeqCst);
                        Some(url)
                    }
                    _ => {
                        debug!("url not working: {:?}", url);
                        None
                    }
                }
            }
        })
        .buffer_unordered(RETRI_CONCURENCY_LIMIT);

    let mut sample_url: Option<String> = None;

    while let Some(result) = stream.next().await {
        // process the stream

        // save a sample url that is working
        if sample_url.is_none() && result.is_some() {
            sample_url = result;
        }
    }

    let success = success_counter.load(Ordering::SeqCst);
    let total = total_counter.load(Ordering::SeqCst);

    let retri_percentage = if total > 0 {
        (success as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    info!(
        "Successfully retrieved URLs: {} out of {} ({:.2}%)",
        success, total, retri_percentage
    );

    (sample_url, round_to_two_decimals(retri_percentage))
}

pub async fn check_retrievability_with_get(
    urls: Vec<String>,
    with_stats: bool,
) -> (Option<String>, Option<f64>) {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(RETRI_TIMEOUT_SEC))
        .build()
        .expect("Failed to build reqwest client");

    let success_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));

    let mut stream = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            let total_clone = Arc::clone(&total_counter);
            let success_clone = Arc::clone(&success_counter);

            async move {
                total_clone.fetch_add(1, Ordering::SeqCst);

                match client.get(&url).send().await {
                    Ok(resp) => {
                        let content_type = resp
                            .headers()
                            .get("content-type")
                            .and_then(|v| v.to_str().ok());
                        let etag = resp.headers().get("etag");

                        if resp.status().is_success()
                            && matches!(
                                content_type,
                                Some("application/octet-stream") | Some("application/piece")
                            )
                            && etag.is_some()
                        {
                            success_clone.fetch_add(1, Ordering::SeqCst);
                            Some(url)
                        } else {
                            debug!("GET not working or missing headers: {:?}", url);
                            None
                        }
                    }
                    Err(err) => {
                        debug!("GET request failed for {:?}: {:?}", url, err);
                        None
                    }
                }
            }
        })
        .buffer_unordered(RETRI_CONCURENCY_LIMIT);

    let mut sample_url: Option<String> = None;

    while let Some(result) = stream.next().await {
        // save a sample url that is working
        if sample_url.is_none() && result.is_some() {
            sample_url = result;
            if !with_stats {
                return (sample_url, None);
            }
        }
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

    (sample_url, round_to_two_decimals(retri_percentage).into())
}

fn round_to_two_decimals(number: f64) -> f64 {
    (number * 100.0).round() / 100.0
}
