// This is a temporary vendored copy from BMS to allow upgrading to axum 0.8 independently from BMS.

use axum::{
    extract::rejection::{JsonRejection, PathRejection, QueryRejection},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema, Clone, Debug)]
pub struct ErrorResponse {
    error: String,
}

#[derive(Clone, Debug)]
pub enum ApiResponse<T> {
    BadRequest(Json<ErrorResponse>),
    InternalServerError(Json<ErrorResponse>),
    NotFound(Json<ErrorResponse>),
    Unauthorized(Json<ErrorResponse>),
    TooManyRequests(Json<ErrorResponse>),
    OkResponse(Json<T>),
}

impl From<JsonRejection> for ApiResponse<ErrorResponse> {
    fn from(rejection: JsonRejection) -> ApiResponse<ErrorResponse> {
        ApiResponse::BadRequest(Json(ErrorResponse {
            error: rejection.body_text(),
        }))
    }
}

impl From<QueryRejection> for ApiResponse<ErrorResponse> {
    fn from(rejection: QueryRejection) -> ApiResponse<ErrorResponse> {
        ApiResponse::BadRequest(Json(ErrorResponse {
            error: rejection.body_text(),
        }))
    }
}

impl From<PathRejection> for ApiResponse<ErrorResponse> {
    fn from(rejection: PathRejection) -> ApiResponse<ErrorResponse> {
        ApiResponse::BadRequest(Json(ErrorResponse {
            error: rejection.body_text(),
        }))
    }
}

impl<T> IntoResponse for ApiResponse<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        match self {
            ApiResponse::BadRequest(json) => (StatusCode::BAD_REQUEST, json).into_response(),
            ApiResponse::InternalServerError(json) => {
                (StatusCode::INTERNAL_SERVER_ERROR, json).into_response()
            }
            ApiResponse::NotFound(json) => (StatusCode::NOT_FOUND, json).into_response(),
            ApiResponse::OkResponse(json) => (StatusCode::OK, json).into_response(),
            ApiResponse::Unauthorized(json) => (StatusCode::UNAUTHORIZED, json).into_response(),
            ApiResponse::TooManyRequests(json) => {
                (StatusCode::TOO_MANY_REQUESTS, json).into_response()
            }
        }
    }
}

pub fn bad_request<T: Into<String>>(msg: T) -> ApiResponse<()> {
    ApiResponse::BadRequest(Json(ErrorResponse { error: msg.into() }))
}

pub fn internal_server_error<T: Into<String>>(msg: T) -> ApiResponse<()> {
    ApiResponse::InternalServerError(Json(ErrorResponse { error: msg.into() }))
}

pub fn not_found<T: Into<String>>(msg: T) -> ApiResponse<()> {
    ApiResponse::NotFound(Json(ErrorResponse { error: msg.into() }))
}

pub fn ok_response<T: Serialize>(data: T) -> ApiResponse<T> {
    ApiResponse::OkResponse(Json(data))
}

pub fn unauthorized<T: Into<String>>(msg: T) -> ApiResponse<()> {
    ApiResponse::Unauthorized(Json(ErrorResponse { error: msg.into() }))
}

pub fn too_many_requests<T: Into<String>>(msg: T) -> ApiResponse<()> {
    ApiResponse::TooManyRequests(Json(ErrorResponse { error: msg.into() }))
}
