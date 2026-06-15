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
use kasdex_indexer::{IndexerRuntimeStatus, IndexerStatusHandle};
use kasdex_store::{
    AddressHistoryRecord, AddressUtxoRecord, BlockSummaryRecord, ChainStore, CoverageClass,
    CoverageRangeRecord, IndexerStatsRecord, StoreError, TxDetailRecordV1, TxSummaryRecord,
};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

const CURSOR_MAGIC: &[u8; 4] = b"KDXC";
const CURSOR_VERSION: u8 = 1;
const CURSOR_TYPE_RECENT_BLOCKS: u8 = 1;

#[derive(OpenApi)]
#[openapi(
    paths(
        health,
        indexer_status,
        list_blocks,
        get_block,
        get_block_transactions,
        get_transaction,
        get_script_history,
        get_script_utxos,
        search
    ),
    components(schemas(
        ApiError,
        BlockDetail,
        BlockPage,
        BlockSummary,
        CoverageRange,
        HealthResponse,
        IndexerStatusResponse,
        SearchResponse,
        SearchResult,
        ScriptHistoryEvent,
        ScriptHistoryPage,
        ScriptUtxo,
        ScriptUtxoPage,
        TransactionDetail,
        TransactionInput,
        TransactionOutput,
        TransactionPage,
        TransactionSummary,
        kasdex_core::IndexedContext,
    )),
    tags(
        (name = "system", description = "System and indexer status"),
        (name = "blocks", description = "Block queries"),
        (name = "scripts", description = "Script-hash history and UTXO queries"),
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
    indexer_status: Option<IndexerStatusHandle>,
}

impl ApiState {
    pub fn with_store(store: impl ChainStore + 'static) -> Self {
        Self {
            store: Some(Arc::new(store)),
            indexer_status: None,
        }
    }

    pub fn with_store_and_indexer_status(
        store: impl ChainStore + 'static,
        indexer_status: IndexerStatusHandle,
    ) -> Self {
        Self {
            store: Some(Arc::new(store)),
            indexer_status: Some(indexer_status),
        }
    }
}

pub fn router() -> Router {
    router_with_state(ApiState::default())
}

pub fn router_with_store(store: impl ChainStore + 'static) -> Router {
    router_with_state(ApiState::with_store(store))
}

pub fn router_with_store_and_indexer_status(
    store: impl ChainStore + 'static,
    indexer_status: IndexerStatusHandle,
) -> Router {
    router_with_state(ApiState::with_store_and_indexer_status(
        store,
        indexer_status,
    ))
}

pub fn router_with_state(state: ApiState) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/indexer/status", get(indexer_status))
        .route("/blocks", get(list_blocks))
        .route("/blocks/{hash}", get(get_block))
        .route("/blocks/{hash}/transactions", get(get_block_transactions))
        .route("/transactions/{txid}", get(get_transaction))
        .route("/scripts/{script_hash}/history", get(get_script_history))
        .route("/scripts/{script_hash}/utxos", get(get_script_utxos))
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
            lag_daa_score: None,
            source: "mock".to_owned(),
            indexed_block_hash: None,
            node_observed_at: None,
            last_poll_started_at: None,
            last_poll_finished_at: None,
            last_success_at: None,
            last_error_at: None,
            last_error: None,
            last_start_hash: None,
            last_indexed_blocks: None,
            last_indexed_transactions: None,
            total_indexed_blocks: None,
            total_indexed_transactions: None,
            total_write_batches: None,
            total_put_operations: None,
            total_delete_operations: None,
            last_batch_put_operations: None,
            last_batch_delete_operations: None,
            last_checkpoint_hash: None,
            last_poll_duration_ms: None,
            last_blocks_per_second: None,
            last_transactions_per_second: None,
            coverage: None,
            coverage_evaluation: "unknown".to_owned(),
        }));
    };

    let checkpoint = store.checkpoint().map_err(store_error)?;
    let stats = store.indexer_stats().map_err(store_error)?;
    let coverage = store
        .coverage_range("default")
        .map_err(store_error)?
        .map(coverage_range);
    let runtime = state
        .indexer_status
        .as_ref()
        .map(IndexerStatusHandle::snapshot);
    Ok(Json(indexer_status_response(
        checkpoint,
        stats,
        runtime.as_ref(),
        coverage,
    )))
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

    let cursor = decode_block_cursor(query.cursor.as_deref(), CURSOR_TYPE_RECENT_BLOCKS)?;
    let page = store
        .recent_blocks(cursor.as_deref(), limit as usize)
        .map_err(store_error)?;

    Ok(Json(BlockPage {
        items: page.items.into_iter().map(block_summary).collect(),
        next_cursor: page
            .next_cursor
            .map(|cursor| encode_cursor(CURSOR_TYPE_RECENT_BLOCKS, &cursor)),
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
    path = "/api/v1/blocks/{hash}/transactions",
    operation_id = "getBlockTransactions",
    tag = "blocks",
    params(
        ("hash" = String, Path, description = "32-byte block hash as hex"),
        PageQuery
    ),
    responses(
        (status = 200, description = "Block transaction summaries", body = TransactionPage),
        (status = 400, description = "Invalid hash or cursor", body = ApiError),
        (status = 404, description = "Block transaction effects not found", body = ApiError)
    )
)]
async fn get_block_transactions(
    State(state): State<ApiState>,
    Path(hash): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<TransactionPage>, ApiError> {
    let store = state
        .store
        .as_deref()
        .ok_or_else(|| ApiError::not_found("block transactions not found"))?;
    let hash = parse_hash(&hash)?;
    let offset = decode_offset_cursor(query.cursor.as_deref())?;
    let limit = query.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }

    let effect = store
        .block_effect_by_hash(&hash)
        .map_err(store_error)?
        .ok_or_else(|| ApiError::not_found("block transaction effects not found"))?;
    let mut items = Vec::new();
    for txid in effect
        .inserted_txids
        .iter()
        .skip(offset)
        .take(limit as usize)
    {
        if let Some(tx) = store.tx_by_id(txid).map_err(store_error)? {
            let detail = store.tx_detail_by_id(txid).map_err(store_error)?;
            items.push(transaction_summary(tx, detail));
        }
    }
    let next_offset = offset + items.len();
    let next_cursor =
        (next_offset < effect.inserted_txids.len()).then(|| encode_offset_cursor(next_offset));

    Ok(Json(TransactionPage {
        items,
        next_cursor,
        indexed_context: indexed_context(store)?,
    }))
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
    let detail = store.tx_detail_by_id(&txid).map_err(store_error)?;
    tx.map(|tx| transaction_summary(tx, detail))
        .map(Json)
        .ok_or_else(|| ApiError::not_found("transaction not found"))
}

#[utoipa::path(
    get,
    path = "/api/v1/scripts/{script_hash}/history",
    operation_id = "getScriptHistory",
    tag = "scripts",
    params(
        ("script_hash" = String, Path, description = "32-byte script hash as hex"),
        PageQuery
    ),
    responses(
        (status = 200, description = "Coverage-limited script history", body = ScriptHistoryPage),
        (status = 400, description = "Invalid script hash or cursor", body = ApiError)
    )
)]
async fn get_script_history(
    State(state): State<ApiState>,
    Path(script_hash): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ScriptHistoryPage>, ApiError> {
    let store = state
        .store
        .as_deref()
        .ok_or_else(|| ApiError::not_found("script history not found"))?;
    let script_hash = parse_hash(&script_hash)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }
    let page = store
        .address_history(&script_hash, cursor.as_deref(), limit as usize)
        .map_err(store_error)?;

    Ok(Json(ScriptHistoryPage {
        items: page.items.into_iter().map(script_history_event).collect(),
        next_cursor: page.next_cursor.map(hex::encode),
        indexed_context: indexed_context(store)?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/scripts/{script_hash}/utxos",
    operation_id = "getScriptUtxos",
    tag = "scripts",
    params(
        ("script_hash" = String, Path, description = "32-byte script hash as hex"),
        PageQuery
    ),
    responses(
        (status = 200, description = "Coverage-limited script UTXOs", body = ScriptUtxoPage),
        (status = 400, description = "Invalid script hash or cursor", body = ApiError)
    )
)]
async fn get_script_utxos(
    State(state): State<ApiState>,
    Path(script_hash): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ScriptUtxoPage>, ApiError> {
    let store = state
        .store
        .as_deref()
        .ok_or_else(|| ApiError::not_found("script utxos not found"))?;
    let script_hash = parse_hash(&script_hash)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }
    let page = store
        .address_utxos(&script_hash, cursor.as_deref(), limit as usize)
        .map_err(store_error)?;

    Ok(Json(ScriptUtxoPage {
        items: page.items.into_iter().map(script_utxo).collect(),
        next_cursor: page.next_cursor.map(hex::encode),
        indexed_context: indexed_context(store)?,
    }))
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

fn decode_offset_cursor(cursor: Option<&str>) -> Result<usize, ApiError> {
    let Some(bytes) = decode_cursor(cursor)? else {
        return Ok(0);
    };
    if bytes.len() != 8 {
        return Err(ApiError::bad_request("cursor is malformed"));
    }
    let offset = u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| ApiError::bad_request("cursor is malformed"))?,
    );
    usize::try_from(offset).map_err(|_| ApiError::bad_request("cursor is too large"))
}

fn encode_offset_cursor(offset: usize) -> String {
    hex::encode((offset as u64).to_be_bytes())
}

fn decode_block_cursor(
    cursor: Option<&str>,
    expected_cursor_type: u8,
) -> Result<Option<Vec<u8>>, ApiError> {
    let Some(decoded) = decode_cursor(cursor)? else {
        return Ok(None);
    };

    if decoded.starts_with(CURSOR_MAGIC) {
        if decoded.len() < 6 {
            return Err(ApiError::bad_request("cursor is malformed"));
        }
        let version = decoded[4];
        let cursor_type = decoded[5];
        if version != CURSOR_VERSION || cursor_type != expected_cursor_type {
            return Err(ApiError::bad_request(
                "cursor is not valid for this endpoint",
            ));
        }
        return Ok(Some(decoded[6..].to_vec()));
    }

    Ok(Some(decoded))
}

fn encode_cursor(cursor_type: u8, key: &[u8]) -> String {
    let mut cursor = Vec::with_capacity(CURSOR_MAGIC.len() + 2 + key.len());
    cursor.extend_from_slice(CURSOR_MAGIC);
    cursor.push(CURSOR_VERSION);
    cursor.push(cursor_type);
    cursor.extend_from_slice(key);
    hex::encode(cursor)
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

fn indexer_status_response(
    checkpoint: Option<kasdex_store::Checkpoint>,
    stats: Option<IndexerStatsRecord>,
    runtime: Option<&IndexerRuntimeStatus>,
    coverage: Option<CoverageRange>,
) -> IndexerStatusResponse {
    let now = std::time::SystemTime::now();
    let state = runtime
        .map(|runtime| runtime.effective_state(now).as_str().to_owned())
        .unwrap_or_else(|| {
            if checkpoint.is_some() {
                "indexed".to_owned()
            } else {
                "empty".to_owned()
            }
        });

    let network = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.network.clone())
        .unwrap_or_else(|| "unknown".to_owned());
    let indexed_score = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.daa_score.to_string())
        .or_else(|| {
            runtime.and_then(|runtime| {
                runtime
                    .last_checkpoint_daa_score
                    .map(|score| score.to_string())
            })
        });
    let indexed_block_hash = checkpoint
        .as_ref()
        .map(|checkpoint| hex::encode(checkpoint.block_hash));

    let coverage_evaluation = evaluate_coverage(coverage.as_ref(), runtime);

    IndexerStatusResponse {
        state,
        network,
        indexed_score,
        virtual_daa_score: runtime
            .and_then(|runtime| runtime.node_virtual_daa_score)
            .map(|score| score.to_string()),
        lag_blocks: None,
        lag_daa_score: runtime
            .and_then(|runtime| runtime.lag_daa_score)
            .map(|lag| lag.to_string()),
        source: "rocksdb".to_owned(),
        indexed_block_hash,
        node_observed_at: runtime.and_then(|runtime| system_time_rfc3339(runtime.node_observed_at)),
        last_poll_started_at: runtime
            .and_then(|runtime| system_time_rfc3339(runtime.last_poll_started_at)),
        last_poll_finished_at: runtime
            .and_then(|runtime| system_time_rfc3339(runtime.last_poll_finished_at)),
        last_success_at: runtime.and_then(|runtime| system_time_rfc3339(runtime.last_success_at)),
        last_error_at: runtime.and_then(|runtime| system_time_rfc3339(runtime.last_error_at)),
        last_error: runtime.and_then(|runtime| runtime.last_error.clone()),
        last_start_hash: runtime.and_then(|runtime| runtime.last_start_hash.clone()),
        last_indexed_blocks: runtime
            .and_then(|runtime| runtime.last_indexed_blocks)
            .map(|count| count as u64),
        last_indexed_transactions: runtime
            .and_then(|runtime| runtime.last_indexed_transactions)
            .map(|count| count as u64),
        total_indexed_blocks: stats.as_ref().map(|stats| stats.total_indexed_blocks),
        total_indexed_transactions: stats.as_ref().map(|stats| stats.total_indexed_transactions),
        total_write_batches: stats.as_ref().map(|stats| stats.total_write_batches),
        total_put_operations: stats.as_ref().map(|stats| stats.total_put_operations),
        total_delete_operations: stats.as_ref().map(|stats| stats.total_delete_operations),
        last_batch_put_operations: stats.as_ref().map(|stats| stats.last_batch_put_operations),
        last_batch_delete_operations: stats
            .as_ref()
            .map(|stats| stats.last_batch_delete_operations),
        last_checkpoint_hash: runtime.and_then(|runtime| runtime.last_checkpoint_hash.clone()),
        last_poll_duration_ms: runtime.and_then(|runtime| runtime.last_poll_duration_ms),
        last_blocks_per_second: runtime.and_then(|runtime| runtime.last_blocks_per_second),
        last_transactions_per_second: runtime
            .and_then(|runtime| runtime.last_transactions_per_second),
        coverage,
        coverage_evaluation,
    }
}

fn evaluate_coverage(
    coverage: Option<&CoverageRange>,
    runtime: Option<&IndexerRuntimeStatus>,
) -> String {
    let Some(coverage) = coverage else {
        return "unknown".to_owned();
    };
    match coverage.coverage_class.as_str() {
        "archival_verified" => "verified_full".to_owned(),
        "pruned_window" => {
            let lag = runtime.and_then(|runtime| runtime.lag_daa_score);
            if lag.is_some_and(|lag| lag <= 20) {
                "near_tip_pruned_window_gap_status_unknown".to_owned()
            } else {
                "backfilling_pruned_window_gap_status_unknown".to_owned()
            }
        }
        "partial_backfill" => "partial_gap_status_unknown".to_owned(),
        _ => "unknown".to_owned(),
    }
}

fn system_time_rfc3339(time: Option<std::time::SystemTime>) -> Option<String> {
    time.map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339())
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

fn transaction_summary(
    tx: TxSummaryRecord,
    detail: Option<TxDetailRecordV1>,
) -> TransactionSummary {
    let detail_available = detail
        .as_ref()
        .is_some_and(|detail| detail.detail_available);
    let detail_complete = detail.as_ref().is_some_and(|detail| detail.detail_complete);

    TransactionSummary {
        txid: hex::encode(tx.txid),
        accepting_block_hash: tx.accepting_block_hash.map(hex::encode),
        input_count: tx.input_count,
        output_count: tx.output_count,
        detail_available,
        detail_complete,
        detail: detail.map(transaction_detail),
    }
}

fn transaction_detail(detail: TxDetailRecordV1) -> TransactionDetail {
    TransactionDetail {
        accepting_daa_score: detail.accepting_daa_score.to_string(),
        accepting_timestamp: timestamp_ms_to_iso(detail.accepting_timestamp_ms),
        version: detail.version,
        lock_time: detail.lock_time.to_string(),
        subnetwork_id: detail.subnetwork_id,
        gas: detail.gas.to_string(),
        payload_size: detail.payload.len() as u64,
        mass: detail.mass.to_string(),
        storage_mass: detail.storage_mass.to_string(),
        compute_mass: detail.compute_mass.to_string(),
        block_time: detail.block_time.to_string(),
        inputs: detail
            .inputs
            .into_iter()
            .map(|input| TransactionInput {
                previous_txid: input.previous_txid.map(hex::encode),
                previous_output_index: input.previous_output_index,
                sequence: input.sequence.to_string(),
                sig_op_count: input.sig_op_count,
                compute_budget: input.compute_budget,
                previous_output_resolved: input.previous_output_resolved,
            })
            .collect(),
        outputs: detail
            .outputs
            .into_iter()
            .map(|output| TransactionOutput {
                output_index: output.output_index,
                amount: output.amount.to_string(),
                script_public_key_version: output.script_public_key_version,
                script_public_key_type: output.script_public_key_type,
                script_public_key_address: output.script_public_key_address,
            })
            .collect(),
    }
}

fn script_history_event(event: AddressHistoryRecord) -> ScriptHistoryEvent {
    ScriptHistoryEvent {
        script_hash: hex::encode(event.script_hash),
        daa_score: event.daa_score.to_string(),
        txid: hex::encode(event.txid),
        event_index: event.event_index,
        amount: event.amount.to_string(),
        balance_trust_level: "coverage_limited".to_owned(),
    }
}

fn script_utxo(utxo: AddressUtxoRecord) -> ScriptUtxo {
    ScriptUtxo {
        script_hash: hex::encode(utxo.script_hash),
        txid: hex::encode(utxo.txid),
        output_index: utxo.output_index,
        amount: utxo.amount.to_string(),
        created_daa_score: utxo.created_daa_score.to_string(),
        balance_trust_level: "coverage_limited".to_owned(),
    }
}

fn coverage_range(coverage: CoverageRangeRecord) -> CoverageRange {
    CoverageRange {
        range_id: coverage.range_id,
        start_hash: hex::encode(coverage.start_hash),
        start_daa_score: coverage.start_daa_score.map(|score| score.to_string()),
        end_hash: hex::encode(coverage.end_hash),
        end_daa_score: coverage.end_daa_score.to_string(),
        source: coverage.source,
        coverage_class: coverage_class(&coverage.coverage_class).to_owned(),
    }
}

fn coverage_class(coverage_class: &CoverageClass) -> &'static str {
    match coverage_class {
        CoverageClass::PrunedWindow => "pruned_window",
        CoverageClass::ArchivalVerified => "archival_verified",
        CoverageClass::PartialBackfill => "partial_backfill",
        CoverageClass::Unknown => "unknown",
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
