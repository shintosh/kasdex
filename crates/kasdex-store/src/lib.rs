use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("not found")]
    NotFound,
    #[error("backend error: {0}")]
    Backend(String),
    #[error("codec error: {0}")]
    Codec(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Checkpoint {
    pub network: String,
    pub daa_score: u64,
    pub block_hash: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockSummaryRecord {
    pub hash: [u8; 32],
    pub blue_score: u64,
    pub daa_score: u64,
    pub timestamp_ms: i64,
    pub tx_count: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TxSummaryRecord {
    pub txid: [u8; 32],
    pub accepting_block_hash: Option<[u8; 32]>,
    pub input_count: u32,
    pub output_count: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddressHistoryRecord {
    pub script_hash: [u8; 32],
    pub daa_score: u64,
    pub txid: [u8; 32],
    pub event_index: u16,
    pub amount: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddressUtxoRecord {
    pub script_hash: [u8; 32],
    pub txid: [u8; 32],
    pub output_index: u32,
    pub amount: u64,
    pub created_daa_score: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Vec<u8>>,
}

pub trait ChainStore {
    fn checkpoint(&self) -> StoreResult<Option<Checkpoint>>;
    fn put_checkpoint(&self, checkpoint: &Checkpoint) -> StoreResult<()>;

    fn put_block(&self, block: &BlockSummaryRecord) -> StoreResult<()>;
    fn block_by_hash(&self, hash: &[u8; 32]) -> StoreResult<Option<BlockSummaryRecord>>;
    fn blocks_by_score(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>>;

    fn put_tx(&self, tx: &TxSummaryRecord) -> StoreResult<()>;
    fn tx_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxSummaryRecord>>;

    fn put_address_history(&self, event: &AddressHistoryRecord) -> StoreResult<()>;
    fn address_history(
        &self,
        script_hash: &[u8; 32],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<AddressHistoryRecord>>;

    fn put_address_utxo(&self, utxo: &AddressUtxoRecord) -> StoreResult<()>;
    fn delete_address_utxo(
        &self,
        script_hash: &[u8; 32],
        txid: &[u8; 32],
        output_index: u32,
    ) -> StoreResult<()>;
    fn address_utxos(
        &self,
        script_hash: &[u8; 32],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<AddressUtxoRecord>>;
}
