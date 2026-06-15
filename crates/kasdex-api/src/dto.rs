use kasdex_core::{IndexedContext, Page};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct IndexerStatusResponse {
    pub state: String,
    pub network: String,
    pub indexed_score: Option<String>,
    pub virtual_daa_score: Option<String>,
    pub lag_blocks: Option<String>,
    pub lag_daa_score: Option<String>,
    pub source: String,
    pub indexed_block_hash: Option<String>,
    pub node_observed_at: Option<String>,
    pub last_poll_started_at: Option<String>,
    pub last_poll_finished_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error_at: Option<String>,
    pub last_error: Option<String>,
    pub last_start_hash: Option<String>,
    pub last_indexed_blocks: Option<u64>,
    pub last_indexed_transactions: Option<u64>,
    pub last_checkpoint_hash: Option<String>,
    pub last_poll_duration_ms: Option<u64>,
    pub last_blocks_per_second: Option<f64>,
    pub last_transactions_per_second: Option<f64>,
    pub coverage: Option<CoverageRange>,
    pub coverage_evaluation: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CoverageRange {
    pub range_id: String,
    pub start_hash: String,
    pub start_daa_score: Option<String>,
    pub end_hash: String,
    pub end_daa_score: String,
    pub source: String,
    pub coverage_class: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct BlockSummary {
    pub hash: String,
    pub blue_score: String,
    pub daa_score: String,
    pub tx_count: u32,
    pub timestamp: String,
}

pub type BlockPage = Page<BlockSummary>;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct BlockDetail {
    pub hash: String,
    pub blue_score: String,
    pub daa_score: String,
    pub tx_count: u32,
    pub timestamp: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TransactionSummary {
    pub txid: String,
    pub accepting_block_hash: Option<String>,
    pub input_count: u32,
    pub output_count: u32,
    pub detail_available: bool,
    pub detail_complete: bool,
    pub detail: Option<TransactionDetail>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TransactionDetail {
    pub accepting_daa_score: String,
    pub accepting_timestamp: String,
    pub version: u32,
    pub lock_time: String,
    pub subnetwork_id: String,
    pub gas: String,
    pub payload_size: u64,
    pub mass: String,
    pub storage_mass: String,
    pub compute_mass: String,
    pub block_time: String,
    pub inputs: Vec<TransactionInput>,
    pub outputs: Vec<TransactionOutput>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TransactionInput {
    pub previous_txid: Option<String>,
    pub previous_output_index: Option<u32>,
    pub sequence: String,
    pub sig_op_count: u32,
    pub compute_budget: u32,
    pub previous_output_resolved: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TransactionOutput {
    pub output_index: u32,
    pub amount: String,
    pub script_public_key_version: u32,
    pub script_public_key_type: Option<String>,
    pub script_public_key_address: Option<String>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PageQuery {
    pub cursor: Option<String>,
    #[param(minimum = 1, maximum = 100)]
    pub limit: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SearchQuery {
    pub q: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SearchResponse {
    pub query: String,
    pub result: Option<SearchResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SearchResult {
    Address { address: String },
    Block { hash: String },
    Transaction { txid: String },
}

pub fn mocked_block_page(limit: u16) -> BlockPage {
    let context = IndexedContext::mocked();
    let count = limit.min(10);
    let items = (0..count)
        .map(|idx| BlockSummary {
            hash: format!("mock-block-{idx:04}"),
            blue_score: idx.to_string(),
            daa_score: idx.to_string(),
            tx_count: 0,
            timestamp: "1970-01-01T00:00:00Z".to_owned(),
        })
        .collect();

    Page {
        items,
        next_cursor: None,
        indexed_context: context,
    }
}
