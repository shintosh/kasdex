mod dto;
mod error;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
pub use dto::*;
pub use error::ApiError;
use kasdex_core::IndexedContext;
use kasdex_store::{BlockSummaryRecord, ChainStore, StoreError, TxSummaryRecord};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(health, indexer_status, list_blocks, get_block, get_transaction, search),
    components(schemas(
        ApiError,
        BlockDetail,
        BlockPage,
        BlockSummary,
        HealthResponse,
        IndexerStatusResponse,
        SearchResponse,
        SearchResult,
        TransactionSummary,
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

#[derive(Clone, Default)]
pub struct ApiState {
    store: Option<Arc<dyn ChainStore>>,
}

impl ApiState {
    pub fn with_store(store: impl ChainStore + 'static) -> Self {
        Self {
            store: Some(Arc::new(store)),
        }
    }
}

pub fn router() -> Router {
    router_with_state(ApiState::default())
}

pub fn router_with_store(store: impl ChainStore + 'static) -> Router {
    router_with_state(ApiState::with_store(store))
}

pub fn router_with_state(state: ApiState) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/indexer/status", get(indexer_status))
        .route("/blocks", get(list_blocks))
        .route("/blocks/{hash}", get(get_block))
        .route("/transactions/{txid}", get(get_transaction))
        .route("/search", get(search));

    Router::new()
        .nest("/api/v1", api)
        .merge(SwaggerUi::new("/docs").url("/api/v1/openapi.json", ApiDoc::openapi()))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
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
async fn indexer_status(
    State(state): State<ApiState>,
) -> Result<Json<IndexerStatusResponse>, ApiError> {
    let Some(store) = state.store.as_deref() else {
        return Ok(Json(IndexerStatusResponse {
            state: "mocked".to_owned(),
            network: "kaspa-mainnet".to_owned(),
            indexed_score: Some("0".to_owned()),
            virtual_daa_score: Some("0".to_owned()),
            lag_blocks: None,
            source: "mock".to_owned(),
        }));
    };

    let checkpoint = store.checkpoint().map_err(store_error)?;
    Ok(Json(match checkpoint {
        Some(checkpoint) => IndexerStatusResponse {
            state: "indexed".to_owned(),
            network: checkpoint.network,
            indexed_score: Some(checkpoint.daa_score.to_string()),
            virtual_daa_score: None,
            lag_blocks: None,
            source: "rocksdb".to_owned(),
        },
        None => IndexerStatusResponse {
            state: "empty".to_owned(),
            network: "unknown".to_owned(),
            indexed_score: None,
            virtual_daa_score: None,
            lag_blocks: None,
            source: "rocksdb".to_owned(),
        },
    }))
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
async fn list_blocks(
    State(state): State<ApiState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<BlockPage>, ApiError> {
    if query.cursor.as_deref().is_some_and(str::is_empty) {
        return Err(ApiError::bad_request("cursor must not be empty"));
    }

    let limit = query.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }

    let Some(store) = state.store.as_deref() else {
        return Ok(Json(mocked_block_page(limit)));
    };

    let cursor = decode_cursor(query.cursor.as_deref())?;
    let page = store
        .recent_blocks(cursor.as_deref(), limit as usize)
        .map_err(store_error)?;

    Ok(Json(BlockPage {
        items: page.items.into_iter().map(block_summary).collect(),
        next_cursor: page.next_cursor.map(hex::encode),
        indexed_context: indexed_context(store)?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/blocks/{hash}",
    operation_id = "getBlock",
    tag = "blocks",
    params(
        ("hash" = String, Path, description = "32-byte block hash as hex")
    ),
    responses(
        (status = 200, description = "Block detail", body = BlockDetail),
        (status = 400, description = "Invalid hash", body = ApiError),
        (status = 404, description = "Block not found", body = ApiError)
    )
)]
async fn get_block(
    State(state): State<ApiState>,
    Path(hash): Path<String>,
) -> Result<Json<BlockDetail>, ApiError> {
    let store = state
        .store
        .as_deref()
        .ok_or_else(|| ApiError::not_found("block not found"))?;
    let hash = parse_hash(&hash)?;
    let block = store.block_by_hash(&hash).map_err(store_error)?;
    block
        .map(block_detail)
        .map(Json)
        .ok_or_else(|| ApiError::not_found("block not found"))
}

#[utoipa::path(
    get,
    path = "/api/v1/transactions/{txid}",
    operation_id = "getTransaction",
    tag = "transactions",
    params(
        ("txid" = String, Path, description = "32-byte transaction id as hex")
    ),
    responses(
        (status = 200, description = "Transaction summary", body = TransactionSummary),
        (status = 400, description = "Invalid transaction id", body = ApiError),
        (status = 404, description = "Transaction not found", body = ApiError)
    )
)]
async fn get_transaction(
    State(state): State<ApiState>,
    Path(txid): Path<String>,
) -> Result<Json<TransactionSummary>, ApiError> {
    let store = state
        .store
        .as_deref()
        .ok_or_else(|| ApiError::not_found("transaction not found"))?;
    let txid = parse_hash(&txid)?;
    let tx = store.tx_by_id(&txid).map_err(store_error)?;
    tx.map(transaction_summary)
        .map(Json)
        .ok_or_else(|| ApiError::not_found("transaction not found"))
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

fn decode_cursor(cursor: Option<&str>) -> Result<Option<Vec<u8>>, ApiError> {
    cursor
        .map(hex::decode)
        .transpose()
        .map_err(|_| ApiError::bad_request("cursor must be hex encoded"))
}

fn indexed_context(store: &dyn ChainStore) -> Result<IndexedContext, ApiError> {
    let checkpoint = store.checkpoint().map_err(store_error)?;
    Ok(match checkpoint {
        Some(checkpoint) => IndexedContext {
            network: checkpoint.network,
            indexed_score: Some(checkpoint.daa_score.to_string()),
            virtual_daa_score: None,
            is_synced: false,
            source: "rocksdb".to_owned(),
        },
        None => IndexedContext {
            network: "unknown".to_owned(),
            indexed_score: None,
            virtual_daa_score: None,
            is_synced: false,
            source: "rocksdb".to_owned(),
        },
    })
}

fn block_summary(block: BlockSummaryRecord) -> BlockSummary {
    BlockSummary {
        hash: hex::encode(block.hash),
        blue_score: block.blue_score.to_string(),
        daa_score: block.daa_score.to_string(),
        tx_count: block.tx_count,
        timestamp: timestamp_ms_to_iso(block.timestamp_ms),
    }
}

fn block_detail(block: BlockSummaryRecord) -> BlockDetail {
    BlockDetail {
        hash: hex::encode(block.hash),
        blue_score: block.blue_score.to_string(),
        daa_score: block.daa_score.to_string(),
        tx_count: block.tx_count,
        timestamp: timestamp_ms_to_iso(block.timestamp_ms),
    }
}

fn transaction_summary(tx: TxSummaryRecord) -> TransactionSummary {
    TransactionSummary {
        txid: hex::encode(tx.txid),
        accepting_block_hash: tx.accepting_block_hash.map(hex::encode),
        input_count: tx.input_count,
        output_count: tx.output_count,
    }
}

fn parse_hash(value: &str) -> Result<[u8; 32], ApiError> {
    let bytes = hex::decode(value).map_err(|_| ApiError::bad_request("hash must be hex"))?;
    bytes
        .try_into()
        .map_err(|_| ApiError::bad_request("hash must be 32 bytes"))
}

fn store_error(err: StoreError) -> ApiError {
    match err {
        StoreError::NotFound => ApiError::not_found("not found"),
        StoreError::Backend(message) | StoreError::Codec(message) => ApiError::internal(message),
    }
}

fn timestamp_ms_to_iso(timestamp_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| timestamp_ms.to_string())
}
