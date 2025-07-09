use std::{sync::Arc, time::Duration};

use axum::{
    debug_handler,
    extract::{Path, State},
};
use axum_extra::extract::WithRejection;
use color_eyre::Result;
use common::api_response::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::{deal_service, provider_endpoints, url_tester, AppState};

use super::ResultCode;

#[derive(Deserialize, ToSchema, IntoParams)]
pub struct FindByClientPath {
    pub client: String,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderResult {
    pub provider: String,
    pub result: ResultCode,
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
}

#[derive(Serialize, ToSchema)]
pub struct FindByClientResponse {
    pub client: String,
    pub result: ResultCode,
    pub providers: Vec<ProviderResult>,
}

const RETRIEVABILITY_TIMEOUT_SEC: u64 = 60; // 1 min for each provider

/// Find retrivabiliy of urls for a given SP and Client address
#[utoipa::path(
    get,
    path = "/url/client/{client}",
    params (FindByClientPath),
    description = r#"
**Find client SPs with working url and retrievabiliy of urls for for each found SP**
    "#,
    responses(
        (status = 200, description = "Successful check", body = FindByClientResponse),
        (status = 400, description = "Bad Request", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse),
    ),
    tags = ["URL"],
)]
#[debug_handler]
pub async fn handle_find_client(
    State(state): State<Arc<AppState>>,
    WithRejection(Path(path), _): WithRejection<Path<FindByClientPath>, ApiResponse<ErrorResponse>>,
) -> Result<ApiResponse<FindByClientResponse>, ApiResponse<()>> {
    debug!(
        "find client working url and retri for input client address: {:?}",
        &path.client
    );

    // validate provider and client addresses
    let address_pattern = Regex::new(r"^f0\d{1,8}$").unwrap();
    if !address_pattern.is_match(&path.client) {
        return Err(bad_request(
            "Invalid provider or client address".to_string(),
        ));
    }

    let providers = match deal_service::get_distinct_providers_by_client(
        &state.deal_repo,
        &path.client,
    )
    .await
    {
        Ok(providers) => providers,
        Err(e) => {
            debug!(
                "Failed to get providers for client {}: {:?}",
                &path.client, e
            );

            return Err(internal_server_error(format!(
                "Failed to get providers for client {}: {:?}",
                &path.client, e
            )));
        }
    };

    if providers.is_empty() {
        debug!("No providers found for client {}", &path.client);

        return Ok(ok_response(FindByClientResponse {
            result: ResultCode::Error,
            client: path.client.clone(),
            providers: Vec::new(),
        }));
    }

    let mut results = Vec::new();

    for provider in providers {
        let (result_code, endpoints) =
            match provider_endpoints::get_provider_endpoints(&provider).await {
                Ok(endpoints) => endpoints,
                Err(e) => return Err(internal_server_error(e.to_string())),
            };

        if endpoints.is_none() {
            debug!("No endpoints found for provider {}", &provider);

            results.push(ProviderResult {
                provider: provider.clone(),
                result: result_code,
                working_url: None,
                retrievability_percent: 0.0,
            });
            continue;
        }
        let endpoints = endpoints.unwrap();

        let provider_db = provider.strip_prefix("f0").unwrap_or(&provider).to_string();
        let client = path
            .client
            .strip_prefix("f0")
            .unwrap_or(&path.client)
            .to_string();

        let piece_ids = deal_service::get_random_piece_ids_by_provider_and_client(
            &state.deal_repo,
            &provider_db,
            &client,
        )
        .await
        .map_err(|e| {
            debug!("Failed to get piece ids: {:?}", e);

            internal_server_error("Failed to get piece ids")
        })?;

        if piece_ids.is_empty() {
            debug!("No deals found for provider {}", &provider);

            results.push(ProviderResult {
                provider: provider.clone(),
                result: ResultCode::NoDealsFound,
                working_url: None,
                retrievability_percent: 0.0,
            });
            continue;
        }

        let urls = deal_service::get_piece_url(endpoints, piece_ids).await;
        let first_url = if !urls.is_empty() {
            if urls[0].is_empty() {
                None
            } else {
                Some(urls[0].clone())
            }
        } else {
            None
        };

        // Get retrievability percent
        // Make sure that the task is not running for too long
        let (_, retrievability_percent) = match timeout(
            Duration::from_secs(RETRIEVABILITY_TIMEOUT_SEC),
            url_tester::get_retrivability_with_head(urls),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                debug!(
                    "Timeout while checking retrievability for provider {}",
                    &provider
                );
                // In case of timeout
                results.push(ProviderResult {
                    provider: provider.clone(),
                    result: ResultCode::TimedOut,
                    working_url: first_url,
                    retrievability_percent: 0.0,
                });
                continue;
            }
        };

        results.push(ProviderResult {
            provider: provider.clone(),
            result: ResultCode::Success,
            working_url: first_url,
            retrievability_percent,
        });
    }

    Ok(ok_response(FindByClientResponse {
        result: ResultCode::Success,
        client: path.client.clone(),
        providers: results,
    }))
}
