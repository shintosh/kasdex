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
    pub source: String,
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
