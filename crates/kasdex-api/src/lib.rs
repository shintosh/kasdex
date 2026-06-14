mod dto;
mod error;

use axum::{Json, Router, extract::Query, routing::get};
pub use dto::*;
pub use error::ApiError;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(health, indexer_status, list_blocks, search),
    components(schemas(
        ApiError,
        BlockPage,
        BlockSummary,
        HealthResponse,
        IndexerStatusResponse,
        SearchResponse,
        SearchResult,
        kasdex_core::IndexedContext,
    )),
    tags(
        (name = "system", description = "System and indexer status"),
        (name = "blocks", description = "Block queries"),
        (name = "search", description = "Search")
    ),
    info(
        title = "Kasdex API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Local-first Kaspa indexer and dashboard API"
    )
)]
pub struct ApiDoc;

pub fn openapi_json_pretty() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&ApiDoc::openapi())
}

pub fn router() -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/indexer/status", get(indexer_status))
        .route("/blocks", get(list_blocks))
        .route("/search", get(search));

    Router::new()
        .nest("/api/v1", api)
        .merge(SwaggerUi::new("/docs").url("/api/v1/openapi.json", ApiDoc::openapi()))
        .layer(TraceLayer::new_for_http())
}

#[utoipa::path(
    get,
    path = "/api/v1/health",
    operation_id = "getHealth",
    tag = "system",
    responses(
        (status = 200, description = "Service health", body = HealthResponse)
    )
)]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/indexer/status",
    operation_id = "getIndexerStatus",
    tag = "system",
    responses(
        (status = 200, description = "Indexer status", body = IndexerStatusResponse)
    )
)]
async fn indexer_status() -> Json<IndexerStatusResponse> {
    Json(IndexerStatusResponse {
        state: "mocked".to_owned(),
        network: "kaspa-mainnet".to_owned(),
        indexed_score: Some("0".to_owned()),
        virtual_daa_score: Some("0".to_owned()),
        lag_blocks: None,
        source: "mock".to_owned(),
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/blocks",
    operation_id = "listBlocks",
    tag = "blocks",
    params(PageQuery),
    responses(
        (status = 200, description = "Paginated block summaries", body = BlockPage),
        (status = 400, description = "Invalid pagination query", body = ApiError)
    )
)]
async fn list_blocks(Query(query): Query<PageQuery>) -> Result<Json<BlockPage>, ApiError> {
    if query.cursor.as_deref().is_some_and(str::is_empty) {
        return Err(ApiError::bad_request("cursor must not be empty"));
    }

    let limit = query.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }

    Ok(Json(mocked_block_page(limit)))
}

#[utoipa::path(
    get,
    path = "/api/v1/search",
    operation_id = "search",
    tag = "search",
    params(SearchQuery),
    responses(
        (status = 200, description = "Search result", body = SearchResponse),
        (status = 400, description = "Invalid search query", body = ApiError)
    )
)]
async fn search(Query(query): Query<SearchQuery>) -> Result<Json<SearchResponse>, ApiError> {
    let q = query.q.trim();
    if q.is_empty() {
        return Err(ApiError::bad_request("q must not be empty"));
    }

    let result = if q.starts_with("kaspa:") {
        Some(SearchResult::Address {
            address: q.to_owned(),
        })
    } else if q.len() == 64 {
        Some(SearchResult::Transaction { txid: q.to_owned() })
    } else {
        None
    };

    Ok(Json(SearchResponse {
        query: q.to_owned(),
        result,
    }))
}
