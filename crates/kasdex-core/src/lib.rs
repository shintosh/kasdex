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

pub fn script_hash_from_hex(script_public_key: &str) -> [u8; 32] {
    let script_bytes =
        hex::decode(script_public_key).unwrap_or_else(|_| script_public_key.as_bytes().to_vec());
    *blake3::hash(&script_bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_hash_is_stable_for_hex_scripts() {
        assert_eq!(script_hash_from_hex("51"), script_hash_from_hex("51"));
        assert_ne!(script_hash_from_hex("51"), script_hash_from_hex("52"));
    }
}
