use std::sync::Arc;

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, header::AUTHORIZATION, request::Parts},
};

use crate::{
    AppState,
    api_response::{ApiResponse, unauthorized},
};

pub struct OracleAuth;

impl FromRequestParts<Arc<AppState>> for OracleAuth {
    type Rejection = ApiResponse<()>;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        if has_valid_bearer_token(&parts.headers, &state.config.auth_token) {
            Ok(Self)
        } else {
            Err(unauthorized("Unauthorized"))
        }
    }
}

pub fn has_valid_bearer_token(headers: &HeaderMap, expected_token: &str) -> bool {
    let Some(header) = headers.get(AUTHORIZATION) else {
        return false;
    };

    let Ok(header) = header.to_str() else {
        return false;
    };

    header == format!("Bearer {expected_token}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers_with_authorization(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static(value));
        headers
    }

    #[test]
    fn rejects_missing_authorization_header() {
        let headers = HeaderMap::new();

        assert!(!has_valid_bearer_token(&headers, "test-token"));
    }

    #[test]
    fn rejects_wrong_bearer_token() {
        let headers = headers_with_authorization("Bearer wrong-token");

        assert!(!has_valid_bearer_token(&headers, "test-token"));
    }

    #[test]
    fn rejects_non_bearer_authorization_header() {
        let headers = headers_with_authorization("Basic test-token");

        assert!(!has_valid_bearer_token(&headers, "test-token"));
    }

    #[test]
    fn accepts_matching_bearer_token() {
        let headers = headers_with_authorization("Bearer test-token");

        assert!(has_valid_bearer_token(&headers, "test-token"));
    }
}
