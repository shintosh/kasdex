use std::path::Path;

use kasdex_store::{
    AddressHistoryRecord, AddressUtxoRecord, BlockEffectRecordV1, BlockSummaryRecord, ChainStore,
    Checkpoint, CoverageRangeRecord, IndexedBlockWrite, IndexerStatsRecord, OutpointStateRecord,
    Page, StoreError, StoreMetadata, StoreResult, TxDetailRecordV1, TxSummaryRecord,
    UnresolvedSpendRecord,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options};
use serde::{Serialize, de::DeserializeOwned};

const META: &str = "meta";
const COVERAGE_RANGES: &str = "coverage_ranges";
const BLOCKS_BY_HASH: &str = "blocks_by_hash";
const BLOCKS_BY_SCORE: &str = "blocks_by_score";
const BLOCK_EFFECTS: &str = "block_effects";
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
const INDEXER_STATS_KEY: &[u8] = b"indexer_stats";
const STORE_SCHEMA_VERSION: u16 = 1;
const KEY_LAYOUT_VERSION: u16 = 1;
const INDEXER_STATS_SCHEMA_VERSION: u16 = 2;

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

    fn indexer_stats(&self) -> StoreResult<Option<IndexerStatsRecord>> {
        self.get_decoded(META, INDEXER_STATS_KEY)
    }

    fn put_indexer_stats(&self, stats: &IndexerStatsRecord) -> StoreResult<()> {
        self.put_encoded(META, INDEXER_STATS_KEY, stats)
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

    fn put_indexed_block(&self, write: IndexedBlockWrite<'_>) -> StoreResult<()> {
        let blocks_by_hash_cf = self.cf(BLOCKS_BY_HASH)?;
        let blocks_by_score_cf = self.cf(BLOCKS_BY_SCORE)?;
        let tx_by_id_cf = self.cf(TX_BY_ID)?;
        let tx_detail_by_id_cf = self.cf(TX_DETAIL_BY_ID)?;
        let address_history_cf = self.cf(ADDRESS_HISTORY)?;
        let address_utxos_cf = self.cf(ADDRESS_UTXOS)?;
        let outpoint_state_cf = self.cf(OUTPOINT_STATE)?;
        let spends_by_outpoint_cf = self.cf(SPENDS_BY_OUTPOINT)?;
        let block_effects_cf = self.cf(BLOCK_EFFECTS)?;
        let meta_cf = self.cf(META)?;
        let coverage_cf = self.cf(COVERAGE_RANGES)?;

        let block_already_indexed = self.block_by_hash(&write.block.hash)?.is_some();
        let mut batch = rocksdb::WriteBatch::default();
        let mut put_operations = 0_u64;
        let mut delete_operations = 0_u64;
        let encoded_block = encode(write.block)?;
        batch.put_cf(&blocks_by_hash_cf, write.block.hash, &encoded_block);
        put_operations += 1;
        batch.put_cf(
            &blocks_by_score_cf,
            block_score_key(write.block.blue_score, &write.block.hash),
            &encoded_block,
        );
        put_operations += 1;
        for tx in write.txs {
            batch.put_cf(&tx_by_id_cf, tx.txid, encode(tx)?);
            put_operations += 1;
        }
        for detail in write.tx_details {
            batch.put_cf(&tx_detail_by_id_cf, detail.txid, encode(detail)?);
            put_operations += 1;
        }
        for event in write.address_history {
            batch.put_cf(
                &address_history_cf,
                address_history_key(event),
                encode(event)?,
            );
            put_operations += 1;
        }
        for utxo in write.address_utxos {
            batch.put_cf(
                &address_utxos_cf,
                address_utxo_key(&utxo.script_hash, &utxo.txid, utxo.output_index),
                encode(utxo)?,
            );
            put_operations += 1;
        }
        for utxo in write.spent_address_utxos {
            batch.delete_cf(
                &address_utxos_cf,
                address_utxo_key(&utxo.script_hash, &utxo.txid, utxo.output_index),
            );
            delete_operations += 1;
        }
        for outpoint in write.outpoint_states {
            batch.put_cf(
                &outpoint_state_cf,
                outpoint_key(&outpoint.txid, outpoint.output_index),
                encode(outpoint)?,
            );
            put_operations += 1;
        }
        for spend in write.unresolved_spends {
            batch.put_cf(
                &spends_by_outpoint_cf,
                unresolved_spend_key(spend),
                encode(spend)?,
            );
            put_operations += 1;
        }
        batch.put_cf(
            &block_effects_cf,
            write.effect.block_hash,
            encode(write.effect)?,
        );
        put_operations += 1;
        batch.put_cf(&meta_cf, CHECKPOINT_KEY, encode(write.checkpoint)?);
        put_operations += 1;
        batch.put_cf(
            &coverage_cf,
            &write.coverage.range_id,
            encode(write.coverage)?,
        );
        put_operations += 1;
        put_operations += 1; // indexer_stats metadata record
        let mut stats =
            normalize_indexer_stats(self.indexer_stats()?.unwrap_or(IndexerStatsRecord {
                schema_version: INDEXER_STATS_SCHEMA_VERSION,
                ..IndexerStatsRecord::default()
            }));
        if !block_already_indexed {
            stats.total_indexed_blocks = stats.total_indexed_blocks.saturating_add(1);
            stats.total_indexed_transactions = stats
                .total_indexed_transactions
                .saturating_add(write.txs.len() as u64);
        }
        stats.total_write_batches = stats.total_write_batches.saturating_add(1);
        stats.total_put_operations = stats.total_put_operations.saturating_add(put_operations);
        stats.total_delete_operations = stats
            .total_delete_operations
            .saturating_add(delete_operations);
        stats.last_batch_put_operations = put_operations;
        stats.last_batch_delete_operations = delete_operations;
        stats.last_batch_blocks = u64::from(!block_already_indexed);
        stats.last_batch_transactions = if block_already_indexed {
            0
        } else {
            write.txs.len() as u64
        };
        stats.last_updated_daa_score = Some(write.block.daa_score);
        stats.last_updated_block_hash = Some(write.block.hash);
        batch.put_cf(&meta_cf, INDEXER_STATS_KEY, encode(&stats)?);

        self.db.write(batch).map_err(rocks_err)
    }

    fn block_effect_by_hash(&self, hash: &[u8; 32]) -> StoreResult<Option<BlockEffectRecordV1>> {
        self.get_decoded(BLOCK_EFFECTS, hash)
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

    fn put_outpoint_state(&self, outpoint: &OutpointStateRecord) -> StoreResult<()> {
        self.put_encoded(
            OUTPOINT_STATE,
            outpoint_key(&outpoint.txid, outpoint.output_index),
            outpoint,
        )
    }

    fn outpoint_state(
        &self,
        txid: &[u8; 32],
        output_index: u32,
    ) -> StoreResult<Option<OutpointStateRecord>> {
        self.get_decoded(OUTPOINT_STATE, outpoint_key(txid, output_index))
    }

    fn put_unresolved_spend(&self, spend: &UnresolvedSpendRecord) -> StoreResult<()> {
        self.put_encoded(SPENDS_BY_OUTPOINT, unresolved_spend_key(spend), spend)
    }
}

fn column_families() -> [&'static str; 13] {
    [
        META,
        COVERAGE_RANGES,
        BLOCKS_BY_HASH,
        BLOCKS_BY_SCORE,
        BLOCK_EFFECTS,
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

fn normalize_indexer_stats(mut stats: IndexerStatsRecord) -> IndexerStatsRecord {
    if stats.schema_version < INDEXER_STATS_SCHEMA_VERSION {
        if stats.schema_version == 1 {
            stats.total_put_operations = stats
                .total_put_operations
                .saturating_add(stats.total_write_batches);
        }
        stats.schema_version = INDEXER_STATS_SCHEMA_VERSION;
    }
    stats
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

fn outpoint_key(txid: &[u8; 32], output_index: u32) -> [u8; 36] {
    let mut key = [0_u8; 36];
    key[..32].copy_from_slice(txid);
    key[32..].copy_from_slice(&output_index.to_be_bytes());
    key
}

fn unresolved_spend_key(spend: &UnresolvedSpendRecord) -> [u8; 68] {
    let mut key = [0_u8; 68];
    key[..32].copy_from_slice(&spend.previous_txid);
    key[32..36].copy_from_slice(&spend.previous_output_index.to_be_bytes());
    key[36..68].copy_from_slice(&spend.spending_txid);
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
        AddressHistoryRecord, AddressUtxoRecord, BlockEffectRecordV1, ChainStore, CoverageClass,
        CoverageRangeRecord, IndexedBlockWrite, OutpointStateRecord, TxDetailRecordV1,
        TxInputRecordV1, TxOutputRecordV1,
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

    #[test]
    fn applies_indexed_block_atomically() {
        let (_dir, store) = test_store();
        let block = BlockSummaryRecord {
            hash: bytes(10),
            blue_score: 10,
            daa_score: 11,
            timestamp_ms: 12,
            tx_count: 1,
        };
        let tx = TxSummaryRecord {
            txid: bytes(11),
            accepting_block_hash: Some(block.hash),
            input_count: 0,
            output_count: 1,
        };
        let detail = TxDetailRecordV1 {
            schema_version: 1,
            detail_available: true,
            detail_complete: false,
            txid: tx.txid,
            accepting_block_hash: block.hash,
            accepting_daa_score: block.daa_score,
            accepting_timestamp_ms: block.timestamp_ms,
            version: 1,
            lock_time: 0,
            subnetwork_id: String::new(),
            gas: 0,
            payload: String::new(),
            mass: 0,
            storage_mass: 0,
            compute_mass: 0,
            block_time: 0,
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        let effect = BlockEffectRecordV1 {
            schema_version: 1,
            block_hash: block.hash,
            daa_score: block.daa_score,
            previous_checkpoint_hash: None,
            previous_checkpoint_daa_score: None,
            inserted_txids: vec![tx.txid],
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_event_keys: Vec::new(),
        };
        let history = AddressHistoryRecord {
            script_hash: bytes(12),
            daa_score: block.daa_score,
            txid: tx.txid,
            event_index: 0,
            amount: 100,
        };
        let utxo = AddressUtxoRecord {
            script_hash: history.script_hash,
            txid: tx.txid,
            output_index: 0,
            amount: 100,
            created_daa_score: block.daa_score,
        };
        let outpoint = OutpointStateRecord {
            txid: tx.txid,
            output_index: 0,
            amount: 100,
            script_hash: history.script_hash,
            address: None,
            created_daa_score: block.daa_score,
            spent_by: None,
            spent_daa_score: None,
        };
        let checkpoint = Checkpoint {
            network: "kaspa-mainnet".to_owned(),
            daa_score: block.daa_score,
            block_hash: block.hash,
        };
        let coverage = CoverageRangeRecord {
            schema_version: 1,
            range_id: "default".to_owned(),
            start_hash: block.hash,
            start_daa_score: Some(block.daa_score),
            end_hash: block.hash,
            end_daa_score: block.daa_score,
            source: "kaspa-mainnet".to_owned(),
            coverage_class: CoverageClass::PrunedWindow,
        };

        store
            .put_indexed_block(IndexedBlockWrite {
                block: &block,
                txs: std::slice::from_ref(&tx),
                tx_details: std::slice::from_ref(&detail),
                address_history: std::slice::from_ref(&history),
                address_utxos: std::slice::from_ref(&utxo),
                spent_address_utxos: &[],
                outpoint_states: std::slice::from_ref(&outpoint),
                unresolved_spends: &[],
                effect: &effect,
                checkpoint: &checkpoint,
                coverage: &coverage,
            })
            .unwrap();

        assert_eq!(store.block_by_hash(&block.hash).unwrap(), Some(block));
        assert_eq!(store.tx_by_id(&tx.txid).unwrap(), Some(tx));
        assert_eq!(store.tx_detail_by_id(&detail.txid).unwrap(), Some(detail));
        assert_eq!(
            store.block_effect_by_hash(&effect.block_hash).unwrap(),
            Some(effect)
        );
        assert_eq!(
            store
                .address_history(&history.script_hash, None, 10)
                .unwrap()
                .items,
            vec![history]
        );
        assert_eq!(
            store
                .address_utxos(&utxo.script_hash, None, 10)
                .unwrap()
                .items,
            vec![utxo]
        );
        assert_eq!(
            store
                .outpoint_state(&outpoint.txid, outpoint.output_index)
                .unwrap(),
            Some(outpoint)
        );
        assert_eq!(store.checkpoint().unwrap(), Some(checkpoint));
        assert_eq!(
            store.coverage_range(&coverage.range_id).unwrap(),
            Some(coverage)
        );
        assert_eq!(
            store.indexer_stats().unwrap(),
            Some(IndexerStatsRecord {
                schema_version: 2,
                total_indexed_blocks: 1,
                total_indexed_transactions: 1,
                total_write_batches: 1,
                total_put_operations: 11,
                total_delete_operations: 0,
                last_batch_put_operations: 11,
                last_batch_delete_operations: 0,
                last_batch_blocks: 1,
                last_batch_transactions: 1,
                last_updated_daa_score: Some(11),
                last_updated_block_hash: Some(bytes(10)),
            })
        );
    }

    #[test]
    fn indexed_block_stats_do_not_double_count_duplicate_blocks() {
        let (_dir, store) = test_store();
        let block = BlockSummaryRecord {
            hash: bytes(20),
            blue_score: 20,
            daa_score: 21,
            timestamp_ms: 22,
            tx_count: 1,
        };
        let tx = TxSummaryRecord {
            txid: bytes(21),
            accepting_block_hash: Some(block.hash),
            input_count: 0,
            output_count: 1,
        };
        let detail = TxDetailRecordV1 {
            schema_version: 1,
            detail_available: true,
            detail_complete: false,
            txid: tx.txid,
            accepting_block_hash: block.hash,
            accepting_daa_score: block.daa_score,
            accepting_timestamp_ms: block.timestamp_ms,
            version: 1,
            lock_time: 0,
            subnetwork_id: String::new(),
            gas: 0,
            payload: String::new(),
            mass: 0,
            storage_mass: 0,
            compute_mass: 0,
            block_time: 0,
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        let effect = BlockEffectRecordV1 {
            schema_version: 1,
            block_hash: block.hash,
            daa_score: block.daa_score,
            previous_checkpoint_hash: None,
            previous_checkpoint_daa_score: None,
            inserted_txids: vec![tx.txid],
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_event_keys: Vec::new(),
        };
        let checkpoint = Checkpoint {
            network: "kaspa-mainnet".to_owned(),
            daa_score: block.daa_score,
            block_hash: block.hash,
        };
        let coverage = CoverageRangeRecord {
            schema_version: 1,
            range_id: "default".to_owned(),
            start_hash: block.hash,
            start_daa_score: Some(block.daa_score),
            end_hash: block.hash,
            end_daa_score: block.daa_score,
            source: "kaspa-mainnet".to_owned(),
            coverage_class: CoverageClass::PrunedWindow,
        };

        for _ in 0..2 {
            store
                .put_indexed_block(IndexedBlockWrite {
                    block: &block,
                    txs: std::slice::from_ref(&tx),
                    tx_details: std::slice::from_ref(&detail),
                    address_history: &[],
                    address_utxos: &[],
                    spent_address_utxos: &[],
                    outpoint_states: &[],
                    unresolved_spends: &[],
                    effect: &effect,
                    checkpoint: &checkpoint,
                    coverage: &coverage,
                })
                .unwrap();
        }

        assert_eq!(
            store.indexer_stats().unwrap(),
            Some(IndexerStatsRecord {
                schema_version: 2,
                total_indexed_blocks: 1,
                total_indexed_transactions: 1,
                total_write_batches: 2,
                total_put_operations: 16,
                total_delete_operations: 0,
                last_batch_put_operations: 8,
                last_batch_delete_operations: 0,
                last_batch_blocks: 0,
                last_batch_transactions: 0,
                last_updated_daa_score: Some(21),
                last_updated_block_hash: Some(bytes(20)),
            })
        );
    }

    #[test]
    fn migrates_v1_indexer_stats_to_include_stats_metadata_puts() {
        let (_dir, store) = test_store();
        store
            .put_indexer_stats(&IndexerStatsRecord {
                schema_version: 1,
                total_indexed_blocks: 5,
                total_indexed_transactions: 50,
                total_write_batches: 5,
                total_put_operations: 100,
                total_delete_operations: 3,
                last_batch_put_operations: 20,
                last_batch_delete_operations: 1,
                last_batch_blocks: 1,
                last_batch_transactions: 10,
                last_updated_daa_score: Some(99),
                last_updated_block_hash: Some(bytes(99)),
            })
            .unwrap();

        let block = BlockSummaryRecord {
            hash: bytes(30),
            blue_score: 30,
            daa_score: 31,
            timestamp_ms: 32,
            tx_count: 0,
        };
        let effect = BlockEffectRecordV1 {
            schema_version: 1,
            block_hash: block.hash,
            daa_score: block.daa_score,
            previous_checkpoint_hash: None,
            previous_checkpoint_daa_score: None,
            inserted_txids: Vec::new(),
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_event_keys: Vec::new(),
        };
        let checkpoint = Checkpoint {
            network: "kaspa-mainnet".to_owned(),
            daa_score: block.daa_score,
            block_hash: block.hash,
        };
        let coverage = CoverageRangeRecord {
            schema_version: 1,
            range_id: "default".to_owned(),
            start_hash: block.hash,
            start_daa_score: Some(block.daa_score),
            end_hash: block.hash,
            end_daa_score: block.daa_score,
            source: "kaspa-mainnet".to_owned(),
            coverage_class: CoverageClass::PrunedWindow,
        };

        store
            .put_indexed_block(IndexedBlockWrite {
                block: &block,
                txs: &[],
                tx_details: &[],
                address_history: &[],
                address_utxos: &[],
                spent_address_utxos: &[],
                outpoint_states: &[],
                unresolved_spends: &[],
                effect: &effect,
                checkpoint: &checkpoint,
                coverage: &coverage,
            })
            .unwrap();

        assert_eq!(
            store.indexer_stats().unwrap(),
            Some(IndexerStatsRecord {
                schema_version: 2,
                total_indexed_blocks: 6,
                total_indexed_transactions: 50,
                total_write_batches: 6,
                total_put_operations: 111,
                total_delete_operations: 3,
                last_batch_put_operations: 6,
                last_batch_delete_operations: 0,
                last_batch_blocks: 1,
                last_batch_transactions: 0,
                last_updated_daa_score: Some(31),
                last_updated_block_hash: Some(bytes(30)),
            })
        );
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
