use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub request_id: Option<String>,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: "bad_request".to_owned(),
            message: message.into(),
            request_id: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}
