use std::path::Path;

use kasdex_store::{
    AddressHistoryRecord, AddressUtxoRecord, BlockSummaryRecord, ChainStore, Checkpoint,
    CoverageRangeRecord, Page, StoreError, StoreMetadata, StoreResult, TxDetailRecordV1,
    TxSummaryRecord,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options};
use serde::{Serialize, de::DeserializeOwned};

const META: &str = "meta";
const COVERAGE_RANGES: &str = "coverage_ranges";
const BLOCKS_BY_HASH: &str = "blocks_by_hash";
const BLOCKS_BY_SCORE: &str = "blocks_by_score";
const TX_BY_ID: &str = "tx_by_id";
const TX_DETAIL_BY_ID: &str = "tx_detail_by_id";
const TX_ACCEPTANCE: &str = "tx_acceptance";
const ADDRESS_HISTORY: &str = "address_history";
const ADDRESS_UTXOS: &str = "address_utxos";
const OUTPOINT_STATE: &str = "outpoint_state";
const SPENDS_BY_OUTPOINT: &str = "spends_by_outpoint";
const MEMPOOL: &str = "mempool";

const CHECKPOINT_KEY: &[u8] = b"checkpoint";
const STORE_METADATA_KEY: &[u8] = b"store_metadata";
const STORE_SCHEMA_VERSION: u16 = 1;
const KEY_LAYOUT_VERSION: u16 = 1;

pub fn backend_name() -> &'static str {
    "rocksdb"
}

pub struct RocksStore {
    db: DB,
}

impl RocksStore {
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let families = column_families()
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));

        let store = Self {
            db: DB::open_cf_descriptors(&opts, path, families).map_err(rocks_err)?,
        };
        store.initialize_metadata()?;
        Ok(store)
    }

    fn put_encoded<T: Serialize>(
        &self,
        cf_name: &str,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> StoreResult<()> {
        let cf = self.cf(cf_name)?;
        self.db.put_cf(&cf, key, encode(value)?).map_err(rocks_err)
    }

    fn get_decoded<T: DeserializeOwned>(
        &self,
        cf_name: &str,
        key: impl AsRef<[u8]>,
    ) -> StoreResult<Option<T>> {
        let cf = self.cf(cf_name)?;
        self.db
            .get_cf(&cf, key)
            .map_err(rocks_err)?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn cf(&self, name: &str) -> StoreResult<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| StoreError::Backend(format!("missing column family {name}")))
    }

    fn initialize_metadata(&self) -> StoreResult<()> {
        match self.store_metadata()? {
            Some(metadata)
                if metadata.store_schema_version == STORE_SCHEMA_VERSION
                    && metadata.backend == backend_name()
                    && metadata.key_layout_version == KEY_LAYOUT_VERSION =>
            {
                Ok(())
            }
            Some(metadata) => Err(StoreError::Backend(format!(
                "unsupported store metadata: schema={}, backend={}, key_layout={}",
                metadata.store_schema_version, metadata.backend, metadata.key_layout_version
            ))),
            None => self.put_store_metadata(&StoreMetadata {
                store_schema_version: STORE_SCHEMA_VERSION,
                backend: backend_name().to_owned(),
                key_layout_version: KEY_LAYOUT_VERSION,
            }),
        }
    }
}

impl ChainStore for RocksStore {
    fn checkpoint(&self) -> StoreResult<Option<Checkpoint>> {
        self.get_decoded(META, CHECKPOINT_KEY)
    }

    fn put_checkpoint(&self, checkpoint: &Checkpoint) -> StoreResult<()> {
        self.put_encoded(META, CHECKPOINT_KEY, checkpoint)
    }

    fn store_metadata(&self) -> StoreResult<Option<StoreMetadata>> {
        self.get_decoded(META, STORE_METADATA_KEY)
    }

    fn put_store_metadata(&self, metadata: &StoreMetadata) -> StoreResult<()> {
        self.put_encoded(META, STORE_METADATA_KEY, metadata)
    }

    fn coverage_range(&self, range_id: &str) -> StoreResult<Option<CoverageRangeRecord>> {
        self.get_decoded(COVERAGE_RANGES, range_id)
    }

    fn put_coverage_range(&self, coverage: &CoverageRangeRecord) -> StoreResult<()> {
        self.put_encoded(COVERAGE_RANGES, &coverage.range_id, coverage)
    }

    fn put_block(&self, block: &BlockSummaryRecord) -> StoreResult<()> {
        let hash_cf = self.cf(BLOCKS_BY_HASH)?;
        let score_cf = self.cf(BLOCKS_BY_SCORE)?;
        let encoded = encode(block)?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&hash_cf, block.hash, &encoded);
        batch.put_cf(
            &score_cf,
            block_score_key(block.blue_score, &block.hash),
            &encoded,
        );
        self.db.write(batch).map_err(rocks_err)
    }

    fn block_by_hash(&self, hash: &[u8; 32]) -> StoreResult<Option<BlockSummaryRecord>> {
        self.get_decoded(BLOCKS_BY_HASH, hash)
    }

    fn blocks_by_score(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>> {
        let cf = self.cf(BLOCKS_BY_SCORE)?;
        let start = cursor.unwrap_or(&[]);
        let mut items = Vec::with_capacity(limit);
        let mut next_cursor = None;
        let mut last_key = None;

        for row in self
            .db
            .iterator_cf(&cf, IteratorMode::From(start, Direction::Forward))
        {
            let (key, value) = row.map_err(rocks_err)?;
            if cursor.is_some_and(|cursor| key.as_ref() == cursor) {
                continue;
            }
            if items.len() == limit {
                next_cursor = last_key;
                break;
            }
            last_key = Some(key.to_vec());
            items.push(decode(&value)?);
        }

        Ok(Page { items, next_cursor })
    }

    fn recent_blocks(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<BlockSummaryRecord>> {
        let cf = self.cf(BLOCKS_BY_SCORE)?;
        let start = cursor.unwrap_or(&[0xff; 40]);
        let mut items = Vec::with_capacity(limit);
        let mut next_cursor = None;
        let mut last_key = None;

        for row in self
            .db
            .iterator_cf(&cf, IteratorMode::From(start, Direction::Reverse))
        {
            let (key, value) = row.map_err(rocks_err)?;
            if cursor.is_some_and(|cursor| key.as_ref() == cursor) {
                continue;
            }
            if items.len() == limit {
                next_cursor = last_key;
                break;
            }
            last_key = Some(key.to_vec());
            items.push(decode(&value)?);
        }

        Ok(Page { items, next_cursor })
    }

    fn put_tx(&self, tx: &TxSummaryRecord) -> StoreResult<()> {
        self.put_encoded(TX_BY_ID, tx.txid, tx)
    }

    fn tx_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxSummaryRecord>> {
        self.get_decoded(TX_BY_ID, txid)
    }

    fn put_tx_detail(&self, tx: &TxDetailRecordV1) -> StoreResult<()> {
        self.put_encoded(TX_DETAIL_BY_ID, tx.txid, tx)
    }

    fn tx_detail_by_id(&self, txid: &[u8; 32]) -> StoreResult<Option<TxDetailRecordV1>> {
        self.get_decoded(TX_DETAIL_BY_ID, txid)
    }

    fn put_address_history(&self, event: &AddressHistoryRecord) -> StoreResult<()> {
        self.put_encoded(ADDRESS_HISTORY, address_history_key(event), event)
    }

    fn address_history(
        &self,
        script_hash: &[u8; 32],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<AddressHistoryRecord>> {
        let cf = self.cf(ADDRESS_HISTORY)?;
        page_prefix(&self.db, cf, script_hash, cursor, limit)
    }

    fn put_address_utxo(&self, utxo: &AddressUtxoRecord) -> StoreResult<()> {
        self.put_encoded(
            ADDRESS_UTXOS,
            address_utxo_key(&utxo.script_hash, &utxo.txid, utxo.output_index),
            utxo,
        )
    }

    fn delete_address_utxo(
        &self,
        script_hash: &[u8; 32],
        txid: &[u8; 32],
        output_index: u32,
    ) -> StoreResult<()> {
        let cf = self.cf(ADDRESS_UTXOS)?;
        self.db
            .delete_cf(&cf, address_utxo_key(script_hash, txid, output_index))
            .map_err(rocks_err)
    }

    fn address_utxos(
        &self,
        script_hash: &[u8; 32],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Page<AddressUtxoRecord>> {
        let cf = self.cf(ADDRESS_UTXOS)?;
        page_prefix(&self.db, cf, script_hash, cursor, limit)
    }
}

fn column_families() -> [&'static str; 12] {
    [
        META,
        COVERAGE_RANGES,
        BLOCKS_BY_HASH,
        BLOCKS_BY_SCORE,
        TX_BY_ID,
        TX_DETAIL_BY_ID,
        TX_ACCEPTANCE,
        ADDRESS_HISTORY,
        ADDRESS_UTXOS,
        OUTPOINT_STATE,
        SPENDS_BY_OUTPOINT,
        MEMPOOL,
    ]
}

fn encode<T: Serialize>(value: &T) -> StoreResult<Vec<u8>> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|err| StoreError::Codec(err.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> StoreResult<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(value, _)| value)
        .map_err(|err| StoreError::Codec(err.to_string()))
}

fn rocks_err(err: rocksdb::Error) -> StoreError {
    StoreError::Backend(err.to_string())
}

fn block_score_key(blue_score: u64, hash: &[u8; 32]) -> [u8; 40] {
    let mut key = [0_u8; 40];
    key[..8].copy_from_slice(&blue_score.to_be_bytes());
    key[8..].copy_from_slice(hash);
    key
}

fn address_history_key(event: &AddressHistoryRecord) -> [u8; 74] {
    let mut key = [0_u8; 74];
    key[..32].copy_from_slice(&event.script_hash);
    key[32..40].copy_from_slice(&(!event.daa_score).to_be_bytes());
    key[40..72].copy_from_slice(&event.txid);
    key[72..].copy_from_slice(&event.event_index.to_be_bytes());
    key
}

fn address_utxo_key(script_hash: &[u8; 32], txid: &[u8; 32], output_index: u32) -> [u8; 68] {
    let mut key = [0_u8; 68];
    key[..32].copy_from_slice(script_hash);
    key[32..64].copy_from_slice(txid);
    key[64..].copy_from_slice(&output_index.to_be_bytes());
    key
}

fn page_prefix<T: DeserializeOwned>(
    db: &DB,
    cf: &ColumnFamily,
    prefix: &[u8; 32],
    cursor: Option<&[u8]>,
    limit: usize,
) -> StoreResult<Page<T>> {
    let start = cursor.unwrap_or(prefix);
    let mut items = Vec::with_capacity(limit);
    let mut next_cursor = None;
    let mut last_key = None;

    for row in db.iterator_cf(cf, IteratorMode::From(start, Direction::Forward)) {
        let (key, value) = row.map_err(rocks_err)?;
        if !key.starts_with(prefix) {
            break;
        }
        if cursor.is_some_and(|cursor| key.as_ref() == cursor) {
            continue;
        }
        if items.len() == limit {
            next_cursor = last_key;
            break;
        }
        last_key = Some(key.to_vec());
        items.push(decode(&value)?);
    }

    Ok(Page { items, next_cursor })
}

#[cfg(test)]
mod tests {
    use kasdex_store::{
        ChainStore, CoverageClass, CoverageRangeRecord, TxDetailRecordV1, TxInputRecordV1,
        TxOutputRecordV1,
    };
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn stores_checkpoint_blocks_and_txs() {
        let (_dir, store) = test_store();
        assert_eq!(
            store.store_metadata().unwrap(),
            Some(StoreMetadata {
                store_schema_version: 1,
                backend: "rocksdb".to_owned(),
                key_layout_version: 1,
            })
        );

        let checkpoint = Checkpoint {
            network: "mainnet".to_owned(),
            daa_score: 42,
            block_hash: bytes(1),
        };
        store.put_checkpoint(&checkpoint).unwrap();
        assert_eq!(store.checkpoint().unwrap(), Some(checkpoint));

        let coverage = CoverageRangeRecord {
            schema_version: 1,
            range_id: "default".to_owned(),
            start_hash: bytes(1),
            start_daa_score: Some(42),
            end_hash: bytes(2),
            end_daa_score: 84,
            source: "kaspa-mainnet".to_owned(),
            coverage_class: CoverageClass::PrunedWindow,
        };
        store.put_coverage_range(&coverage).unwrap();
        assert_eq!(
            store.coverage_range(&coverage.range_id).unwrap(),
            Some(coverage)
        );

        let block = BlockSummaryRecord {
            hash: bytes(2),
            blue_score: 7,
            daa_score: 8,
            timestamp_ms: 123,
            tx_count: 2,
        };
        store.put_block(&block).unwrap();
        assert_eq!(
            store.block_by_hash(&block.hash).unwrap(),
            Some(block.clone())
        );
        assert_eq!(store.blocks_by_score(None, 10).unwrap().items, vec![block]);

        let tx = TxSummaryRecord {
            txid: bytes(3),
            accepting_block_hash: Some(bytes(2)),
            input_count: 1,
            output_count: 2,
        };
        store.put_tx(&tx).unwrap();
        assert_eq!(store.tx_by_id(&tx.txid).unwrap(), Some(tx));

        let detail = TxDetailRecordV1 {
            schema_version: 1,
            detail_available: true,
            detail_complete: false,
            txid: bytes(3),
            accepting_block_hash: bytes(2),
            accepting_daa_score: 8,
            accepting_timestamp_ms: 123,
            version: 1,
            lock_time: 0,
            subnetwork_id: "00".to_owned(),
            gas: 0,
            payload: String::new(),
            mass: 10,
            storage_mass: 5,
            compute_mass: 4,
            block_time: 123,
            inputs: vec![TxInputRecordV1 {
                previous_txid: Some(bytes(4)),
                previous_output_index: Some(0),
                signature_script: "abcd".to_owned(),
                sequence: 1,
                sig_op_count: 1,
                compute_budget: 0,
                previous_output_resolved: true,
            }],
            outputs: vec![TxOutputRecordV1 {
                output_index: 0,
                amount: 100,
                script_public_key_version: 0,
                script_public_key: "51".to_owned(),
                script_public_key_type: Some("pubkey".to_owned()),
                script_public_key_address: Some("kaspa:test".to_owned()),
            }],
        };
        store.put_tx_detail(&detail).unwrap();
        assert_eq!(store.tx_detail_by_id(&detail.txid).unwrap(), Some(detail));
    }

    #[test]
    fn pages_recent_blocks_descending_by_blue_score() {
        let (_dir, store) = test_store();
        for blue_score in [10, 12, 11] {
            store
                .put_block(&BlockSummaryRecord {
                    hash: bytes(blue_score),
                    blue_score,
                    daa_score: blue_score + 100,
                    timestamp_ms: blue_score as i64,
                    tx_count: 0,
                })
                .unwrap();
        }

        let page = store.recent_blocks(None, 2).unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.blue_score)
                .collect::<Vec<_>>(),
            vec![12, 11]
        );

        let next = store.recent_blocks(page.next_cursor.as_deref(), 2).unwrap();
        assert_eq!(
            next.items
                .iter()
                .map(|item| item.blue_score)
                .collect::<Vec<_>>(),
            vec![10]
        );
    }

    #[test]
    fn pages_address_history_descending_by_daa_score() {
        let (_dir, store) = test_store();
        let script_hash = bytes(9);
        for daa_score in [10, 12, 11] {
            store
                .put_address_history(&AddressHistoryRecord {
                    script_hash,
                    daa_score,
                    txid: bytes(daa_score),
                    event_index: 0,
                    amount: daa_score as i64,
                })
                .unwrap();
        }

        let page = store.address_history(&script_hash, None, 2).unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.daa_score)
                .collect::<Vec<_>>(),
            vec![12, 11]
        );

        let next = store
            .address_history(&script_hash, page.next_cursor.as_deref(), 2)
            .unwrap();
        assert_eq!(
            next.items
                .iter()
                .map(|item| item.daa_score)
                .collect::<Vec<_>>(),
            vec![10]
        );
    }

    #[test]
    fn creates_pages_and_deletes_address_utxos() {
        let (_dir, store) = test_store();
        let script_hash = bytes(5);
        let first = AddressUtxoRecord {
            script_hash,
            txid: bytes(1),
            output_index: 0,
            amount: 100,
            created_daa_score: 10,
        };
        let second = AddressUtxoRecord {
            txid: bytes(2),
            output_index: 1,
            amount: 200,
            created_daa_score: 11,
            ..first.clone()
        };

        store.put_address_utxo(&first).unwrap();
        store.put_address_utxo(&second).unwrap();
        assert_eq!(
            store
                .address_utxos(&script_hash, None, 10)
                .unwrap()
                .items
                .len(),
            2
        );

        store
            .delete_address_utxo(&script_hash, &first.txid, first.output_index)
            .unwrap();
        let remaining = store.address_utxos(&script_hash, None, 10).unwrap().items;
        assert_eq!(remaining, vec![second]);
    }

    #[test]
    fn duplicate_writes_are_idempotent() {
        let (_dir, store) = test_store();
        let tx = TxSummaryRecord {
            txid: bytes(7),
            accepting_block_hash: None,
            input_count: 0,
            output_count: 1,
        };
        store.put_tx(&tx).unwrap();
        store.put_tx(&tx).unwrap();
        assert_eq!(store.tx_by_id(&tx.txid).unwrap(), Some(tx));
    }

    fn test_store() -> (TempDir, RocksStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksStore::open(dir.path()).unwrap();
        (dir, store)
    }

    fn bytes(seed: u64) -> [u8; 32] {
        let mut out = [0_u8; 32];
        for chunk in 0..4 {
            out[chunk * 8..(chunk + 1) * 8]
                .copy_from_slice(&seed.wrapping_add(chunk as u64).to_be_bytes());
        }
        out
    }
}
