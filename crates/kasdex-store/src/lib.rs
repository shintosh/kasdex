use std::sync::Arc;

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
#[serde(rename_all = "snake_case")]
pub enum CoverageClass {
    PrunedWindow,
    ArchivalVerified,
    PartialBackfill,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CoverageRangeRecord {
    pub schema_version: u16,
    pub range_id: String,
    pub start_hash: [u8; 32],
    pub start_daa_score: Option<u64>,
    pub end_hash: [u8; 32],
    pub end_daa_score: u64,
    pub source: String,
    pub coverage_class: CoverageClass,
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
pub struct TxDetailRecordV1 {
    pub schema_version: u16,
    pub detail_available: bool,
    pub detail_complete: bool,
    pub txid: [u8; 32],
    pub accepting_block_hash: [u8; 32],
    pub accepting_daa_score: u64,
    pub accepting_timestamp_ms: i64,
    pub version: u32,
    pub lock_time: u64,
    pub subnetwork_id: String,
    pub gas: u64,
    pub payload: String,
    pub mass: u64,
    pub storage_mass: u64,
    pub compute_mass: u64,
    pub block_time: u64,
    pub inputs: Vec<TxInputRecordV1>,
    pub outputs: Vec<TxOutputRecordV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TxInputRecordV1 {
    pub previous_txid: Option<[u8; 32]>,
    pub previous_output_index: Option<u32>,
    pub signature_script: String,
    pub sequence: u64,
    pub sig_op_count: u32,
    pub compute_budget: u32,
    pub previous_output_resolved: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TxOutputRecordV1 {
    pub output_index: u32,
    pub amount: u64,
    pub script_public_key_version: u32,
    pub script_public_key: String,
    pub script_public_key_type: Option<String>,
    pub script_public_key_address: Option<String>,
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

pub trait ChainStore: Send + Sync {
    fn checkpoint(&self) -> StoreResult<Option<Checkpoint>>;
    fn put_checkpoint(&self, checkpoint: &Checkpoint) -> StoreResult<()>;
    fn coverage_range(&self, range_id: &str) -> StoreResult<Option<CoverageRangeRecord>>;
    fn put_coverage_range(&self, coverage: &CoverageRangeRecord) -> StoreResult<()>;

    fn put_block(&self, block: &BlockSummaryRecord) -> StoreResult<()>;
    fn block_by_hash(&self, hash: &[u8; 32]) -> StoreResult<Option<BlockSummaryRecord>>;
    fn blocks_by_score(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>>;
    fn recent_blocks(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>>;

    fn put_tx(&self, tx: &TxSummaryRecord) -> StoreResult<()>;
    fn tx_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxSummaryRecord>>;
    fn put_tx_detail(&self, tx: &TxDetailRecordV1) -> StoreResult<()>;
    fn tx_detail_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxDetailRecordV1>>;

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

impl<T> ChainStore for Arc<T>
where
    T: ChainStore + ?Sized,
{
    fn checkpoint(&self) -> StoreResult<Option<Checkpoint>> {
        self.as_ref().checkpoint()
    }

    fn put_checkpoint(&self, checkpoint: &Checkpoint) -> StoreResult<()> {
        self.as_ref().put_checkpoint(checkpoint)
    }

    fn coverage_range(&self, range_id: &str) -> StoreResult<Option<CoverageRangeRecord>> {
        self.as_ref().coverage_range(range_id)
    }

    fn put_coverage_range(&self, coverage: &CoverageRangeRecord) -> StoreResult<()> {
        self.as_ref().put_coverage_range(coverage)
    }

    fn put_block(&self, block: &BlockSummaryRecord) -> StoreResult<()> {
        self.as_ref().put_block(block)
    }

    fn block_by_hash(&self, hash: &[u8; 32]) -> StoreResult<Option<BlockSummaryRecord>> {
        self.as_ref().block_by_hash(hash)
    }

    fn blocks_by_score(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>> {
        self.as_ref().blocks_by_score(cursor, limit)
    }

    fn recent_blocks(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>> {
        self.as_ref().recent_blocks(cursor, limit)
    }

    fn put_tx(&self, tx: &TxSummaryRecord) -> StoreResult<()> {
        self.as_ref().put_tx(tx)
    }

    fn tx_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxSummaryRecord>> {
        self.as_ref().tx_by_id(txid)
    }

    fn put_tx_detail(&self, tx: &TxDetailRecordV1) -> StoreResult<()> {
        self.as_ref().put_tx_detail(tx)
    }

    fn tx_detail_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxDetailRecordV1>> {
        self.as_ref().tx_detail_by_id(txid)
    }

    fn put_address_history(&self, event: &AddressHistoryRecord) -> StoreResult<()> {
        self.as_ref().put_address_history(event)
    }

    fn address_history(
        &self,
        script_hash: &[u8; 32],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<AddressHistoryRecord>> {
        self.as_ref().address_history(script_hash, cursor, limit)
    }

    fn put_address_utxo(&self, utxo: &AddressUtxoRecord) -> StoreResult<()> {
        self.as_ref().put_address_utxo(utxo)
    }

    fn delete_address_utxo(
        &self,
        script_hash: &[u8; 32],
        txid: &[u8; 32],
        output_index: u32,
    ) -> StoreResult<()> {
        self.as_ref()
            .delete_address_utxo(script_hash, txid, output_index)
    }

    fn address_utxos(
        &self,
        script_hash: &[u8; 32],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<AddressUtxoRecord>> {
        self.as_ref().address_utxos(script_hash, cursor, limit)
    }
}
