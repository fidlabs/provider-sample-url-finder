#![allow(dead_code)]

use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use axum_test::TestResponse;
use axum_test::TestServer;
use pretty_assertions::assert_eq;

pub async fn assert_bad_request_error(app: &TestServer, path: &str, error_contains: &str) {
    let response = app.get(path).await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert!(
        body["error"].as_str().unwrap().contains(error_contains),
        "Expected error containing '{error_contains}', got: {:?}",
        body["error"]
    );
}

pub fn assert_json_response(
    response: TestResponse,
    expected_status: StatusCode,
    expected_json: serde_json::Value,
) -> serde_json::Value {
    assert_eq!(response.status_code(), expected_status);
    let body: serde_json::Value = response.json();
    assert_json_include!(actual: body, expected: expected_json);
    body
}

pub fn assert_json_response_ok(
    response: TestResponse,
    expected_json: serde_json::Value,
) -> serde_json::Value {
    assert_json_response(response, StatusCode::OK, expected_json)
}

pub fn assert_message_contains(body: &serde_json::Value, expected_text: &str) {
    let message = body["message"]
        .as_str()
        .expect("Expected 'message' field in response");
    assert!(
        message.contains(expected_text),
        "Expected message containing '{expected_text}', got: '{message}'"
    );
}

pub fn assert_no_url(body: &serde_json::Value) {
    assert!(
        body.get("url").is_none(),
        "Expected no 'url' field, but got: {:?}",
        body.get("url")
    );
}
