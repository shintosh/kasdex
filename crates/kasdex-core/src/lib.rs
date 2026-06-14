use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub indexed_context: IndexedContext,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct IndexedContext {
    pub network: String,
    pub indexed_score: Option<String>,
    pub virtual_daa_score: Option<String>,
    pub is_synced: bool,
    pub source: String,
}

impl IndexedContext {
    pub fn mocked() -> Self {
        Self {
            network: "kaspa-mainnet".to_owned(),
            indexed_score: Some("0".to_owned()),
            virtual_daa_score: Some("0".to_owned()),
            is_synced: false,
            source: "mock".to_owned(),
        }
    }
}
