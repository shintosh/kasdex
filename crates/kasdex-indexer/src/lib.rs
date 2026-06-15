use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use kasdex_core::script_hash_from_hex;
use kasdex_node::{GrpcKaspaNode, NodeError, protowire};
use kasdex_store::{
    AddressHistoryRecord, AddressUtxoRecord, BlockEffectRecordV1, BlockSummaryRecord, ChainStore,
    Checkpoint, CoverageClass, CoverageRangeRecord, IndexedBlockWrite, OutpointRef,
    OutpointStateRecord, StoreError, TxDetailRecordV1, TxInputRecordV1, TxOutputRecordV1,
    TxSummaryRecord, UnresolvedSpendRecord,
};

const DEFAULT_COVERAGE_RANGE_ID: &str = "default";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexerState {
    Mocked,
    Empty,
    Backfilling,
    Tailing,
    Stale,
    Stalled,
    Error,
    RepairRequired,
}

impl IndexerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mocked => "mocked",
            Self::Empty => "empty",
            Self::Backfilling => "backfilling",
            Self::Tailing => "tailing",
            Self::Stale => "stale",
            Self::Stalled => "stalled",
            Self::Error => "error",
            Self::RepairRequired => "repair_required",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexerRuntimeConfig {
    pub tail_enter_lag_threshold: u64,
    pub tail_exit_lag_threshold: u64,
    pub stalled_after: Duration,
    pub stale_after: Duration,
}

impl Default for IndexerRuntimeConfig {
    fn default() -> Self {
        Self {
            tail_enter_lag_threshold: 1_000,
            tail_exit_lag_threshold: 2_000,
            stalled_after: Duration::from_secs(120),
            stale_after: Duration::from_secs(120),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexerRuntimeStatus {
    pub state: IndexerState,
    pub config: IndexerRuntimeConfig,
    pub node_virtual_daa_score: Option<u64>,
    pub node_observed_at: Option<SystemTime>,
    pub lag_daa_score: Option<u64>,
    pub current_poll_started_at: Option<SystemTime>,
    pub last_poll_started_at: Option<SystemTime>,
    pub last_poll_finished_at: Option<SystemTime>,
    pub last_success_at: Option<SystemTime>,
    pub last_error_at: Option<SystemTime>,
    pub last_error: Option<String>,
    pub last_start_hash: Option<String>,
    pub last_indexed_blocks: Option<usize>,
    pub last_indexed_transactions: Option<usize>,
    pub last_checkpoint_daa_score: Option<u64>,
    pub last_checkpoint_hash: Option<String>,
    pub last_poll_duration_ms: Option<u64>,
    pub last_blocks_per_second: Option<f64>,
    pub last_transactions_per_second: Option<f64>,
}

impl IndexerRuntimeStatus {
    pub fn new(config: IndexerRuntimeConfig) -> Self {
        Self {
            state: IndexerState::Empty,
            config,
            node_virtual_daa_score: None,
            node_observed_at: None,
            lag_daa_score: None,
            current_poll_started_at: None,
            last_poll_started_at: None,
            last_poll_finished_at: None,
            last_success_at: None,
            last_error_at: None,
            last_error: None,
            last_start_hash: None,
            last_indexed_blocks: None,
            last_indexed_transactions: None,
            last_checkpoint_daa_score: None,
            last_checkpoint_hash: None,
            last_poll_duration_ms: None,
            last_blocks_per_second: None,
            last_transactions_per_second: None,
        }
    }

    pub fn effective_state(&self, now: SystemTime) -> IndexerState {
        if matches!(
            self.state,
            IndexerState::Error | IndexerState::RepairRequired
        ) {
            return self.state.clone();
        }

        if let Some(observed_at) = self.node_observed_at
            && now
                .duration_since(observed_at)
                .is_ok_and(|age| age > self.config.stale_after)
        {
            return IndexerState::Stale;
        }

        if self.last_success_at.is_some_and(|success_at| {
            now.duration_since(success_at)
                .is_ok_and(|age| age > self.config.stalled_after)
        }) && self.lag_daa_score.is_some_and(|lag| lag > 0)
        {
            return IndexerState::Stalled;
        }

        self.state.clone()
    }
}

impl Default for IndexerRuntimeStatus {
    fn default() -> Self {
        Self::new(IndexerRuntimeConfig::default())
    }
}

#[derive(Clone, Debug)]
pub struct IndexerStatusHandle {
    inner: Arc<RwLock<IndexerRuntimeStatus>>,
}

impl IndexerStatusHandle {
    pub fn new(config: IndexerRuntimeConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(IndexerRuntimeStatus::new(config))),
        }
    }

    pub fn snapshot(&self) -> IndexerRuntimeStatus {
        self.inner
            .read()
            .expect("indexer status lock poisoned")
            .clone()
    }

    pub fn mark_poll_started(&self, started_at: SystemTime) {
        let mut status = self.inner.write().expect("indexer status lock poisoned");
        status.state = IndexerState::Backfilling;
        status.current_poll_started_at = Some(started_at);
    }

    pub fn mark_poll_success(
        &self,
        report: &BackfillReport,
        started_at: SystemTime,
        finished_at: SystemTime,
    ) {
        let duration_ms = finished_at
            .duration_since(started_at)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .ok();
        let lag_daa_score = report
            .checkpoint_daa_score
            .map(|indexed| report.virtual_daa_score.saturating_sub(indexed));
        let snapshot = self.snapshot();
        let state = match (snapshot.state, lag_daa_score) {
            (IndexerState::Tailing, Some(lag))
                if lag <= snapshot.config.tail_exit_lag_threshold =>
            {
                IndexerState::Tailing
            }
            (_, Some(lag)) if lag <= snapshot.config.tail_enter_lag_threshold => {
                IndexerState::Tailing
            }
            (_, Some(_)) => IndexerState::Backfilling,
            (_, None) => IndexerState::Empty,
        };
        let duration_secs = duration_ms.map(|duration_ms| duration_ms as f64 / 1_000.0);
        let blocks_per_second = duration_secs
            .filter(|duration_secs| *duration_secs > 0.0)
            .map(|duration_secs| report.indexed_blocks as f64 / duration_secs);
        let transactions_per_second = duration_secs
            .filter(|duration_secs| *duration_secs > 0.0)
            .map(|duration_secs| report.indexed_transactions as f64 / duration_secs);

        let mut status = self.inner.write().expect("indexer status lock poisoned");
        status.state = state;
        status.node_virtual_daa_score = Some(report.virtual_daa_score);
        status.node_observed_at = Some(finished_at);
        status.lag_daa_score = lag_daa_score;
        status.current_poll_started_at = None;
        status.last_poll_started_at = Some(started_at);
        status.last_poll_finished_at = Some(finished_at);
        status.last_success_at = Some(finished_at);
        status.last_start_hash = Some(report.start_hash.clone());
        status.last_indexed_blocks = Some(report.indexed_blocks);
        status.last_indexed_transactions = Some(report.indexed_transactions);
        status.last_checkpoint_daa_score = report.checkpoint_daa_score;
        status.last_checkpoint_hash = report.checkpoint_hash.clone();
        status.last_poll_duration_ms = duration_ms;
        status.last_blocks_per_second = blocks_per_second;
        status.last_transactions_per_second = transactions_per_second;
        status.last_error = None;
    }

    pub fn mark_poll_error(
        &self,
        error: impl ToString,
        started_at: Option<SystemTime>,
        finished_at: SystemTime,
    ) {
        let mut status = self.inner.write().expect("indexer status lock poisoned");
        status.state = IndexerState::Error;
        status.current_poll_started_at = None;
        if let Some(started_at) = started_at {
            status.last_poll_started_at = Some(started_at);
        }
        status.last_poll_finished_at = Some(finished_at);
        status.last_error_at = Some(finished_at);
        status.last_error = Some(error.to_string());
    }
}

impl Default for IndexerStatusHandle {
    fn default() -> Self {
        Self::new(IndexerRuntimeConfig::default())
    }
}

#[derive(Clone, Debug)]
pub struct BackfillConfig {
    pub rpc_url: String,
    pub limit_blocks: usize,
    pub start_hash: Option<String>,
    pub index_addresses: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillReport {
    pub network: String,
    pub start_hash: String,
    pub fetched_chain_blocks: usize,
    pub indexed_blocks: usize,
    pub indexed_transactions: usize,
    pub virtual_daa_score: u64,
    pub checkpoint_daa_score: Option<u64>,
    pub checkpoint_hash: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("node error: {0}")]
    Node(#[from] NodeError),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("invalid hash `{0}`")]
    InvalidHash(String),
    #[error("block `{0}` has no header")]
    MissingBlockHeader(String),
}

pub type IndexerResult<T> = Result<T, IndexerError>;

pub async fn run_bounded_backfill<S: ChainStore>(
    store: &S,
    config: BackfillConfig,
) -> IndexerResult<BackfillReport> {
    let mut node = GrpcKaspaNode::connect(config.rpc_url).await?;
    let dag = node.get_block_dag_info().await?;
    let stored_checkpoint = store.checkpoint()?;
    let coverage_start_hash = stored_checkpoint
        .as_ref()
        .map(|_| dag.pruning_point_hash.clone())
        .unwrap_or_else(|| {
            config
                .start_hash
                .clone()
                .unwrap_or_else(|| dag.pruning_point_hash.clone())
        });
    let start_hash = select_start_hash(
        config.start_hash.as_deref(),
        stored_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.block_hash),
        &dag.pruning_point_hash,
    );
    let virtual_chain = node
        .get_virtual_chain_from_block(start_hash.clone(), true)
        .await?;

    let block_hashes = virtual_chain
        .added_chain_block_hashes
        .iter()
        .take(config.limit_blocks);

    let mut indexed_blocks = 0_usize;
    let mut indexed_transactions = 0_usize;
    let mut checkpoint_daa_score = None;
    let mut checkpoint_hash = None;
    let mut current_checkpoint = stored_checkpoint.clone();
    let mut coverage = initialize_coverage_range(
        store,
        &mut node,
        &dag.network_name,
        &coverage_start_hash,
        stored_checkpoint.as_ref(),
    )
    .await?;

    for hash in block_hashes {
        let block_response = node.get_block(hash.clone(), true).await?;
        let block = block_response
            .block
            .ok_or_else(|| IndexerError::MissingBlockHeader(hash.clone()))?;
        let header = block
            .header
            .ok_or_else(|| IndexerError::MissingBlockHeader(hash.clone()))?;

        let block_hash = parse_hash(&header.hash)?;
        let block_record = BlockSummaryRecord {
            hash: block_hash,
            blue_score: header.blue_score,
            daa_score: header.daa_score,
            timestamp_ms: header.timestamp,
            tx_count: block.transactions.len() as u32,
        };

        let mut tx_records = Vec::new();
        let mut tx_detail_records = Vec::new();
        let mut address_history = Vec::new();
        let mut address_utxos = Vec::new();
        let mut spent_address_utxos = Vec::new();
        let mut outpoint_states = Vec::new();
        let mut unresolved_spends = Vec::new();
        let mut spent_outpoints = Vec::new();
        let mut pending_outpoints = HashMap::new();
        for tx in block.transactions {
            if let Some(verbose) = tx.verbose_data.as_ref() {
                let txid = parse_hash(&verbose.transaction_id)?;
                tx_records.push(TxSummaryRecord {
                    txid,
                    accepting_block_hash: Some(block_hash),
                    input_count: tx.inputs.len() as u32,
                    output_count: tx.outputs.len() as u32,
                });
                tx_detail_records.push(tx_detail_record(
                    &tx,
                    verbose,
                    txid,
                    block_hash,
                    header.daa_score,
                    header.timestamp,
                )?);
                if config.index_addresses {
                    let derived = address_index_records(
                        store,
                        &mut pending_outpoints,
                        &tx,
                        txid,
                        header.daa_score,
                    )?;
                    address_history.extend(derived.address_history);
                    address_utxos.extend(derived.address_utxos);
                    spent_address_utxos.extend(derived.spent_address_utxos);
                    outpoint_states.extend(derived.outpoint_states);
                    unresolved_spends.extend(derived.unresolved_spends);
                    spent_outpoints.extend(derived.spent_outpoints);
                }
                indexed_transactions += 1;
            }
        }

        let checkpoint = Checkpoint {
            network: dag.network_name.clone(),
            daa_score: header.daa_score,
            block_hash,
        };
        coverage.end_hash = block_hash;
        coverage.end_daa_score = header.daa_score;
        let effect = BlockEffectRecordV1 {
            schema_version: 1,
            block_hash,
            daa_score: header.daa_score,
            previous_checkpoint_hash: current_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.block_hash),
            previous_checkpoint_daa_score: current_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.daa_score),
            inserted_txids: tx_records.iter().map(|tx| tx.txid).collect(),
            created_outpoints: outpoint_states
                .iter()
                .map(|outpoint| OutpointRef {
                    txid: outpoint.txid,
                    output_index: outpoint.output_index,
                })
                .collect(),
            spent_outpoints,
            address_event_keys: Vec::new(),
        };
        store.put_indexed_block(IndexedBlockWrite {
            block: &block_record,
            txs: &tx_records,
            tx_details: &tx_detail_records,
            address_history: &address_history,
            address_utxos: &address_utxos,
            spent_address_utxos: &spent_address_utxos,
            outpoint_states: &outpoint_states,
            unresolved_spends: &unresolved_spends,
            effect: &effect,
            checkpoint: &checkpoint,
            coverage: &coverage,
        })?;
        checkpoint_daa_score = Some(header.daa_score);
        checkpoint_hash = Some(header.hash);
        current_checkpoint = Some(checkpoint);
        indexed_blocks += 1;
    }

    Ok(BackfillReport {
        network: dag.network_name,
        start_hash,
        fetched_chain_blocks: virtual_chain.added_chain_block_hashes.len(),
        indexed_blocks,
        indexed_transactions,
        virtual_daa_score: dag.virtual_daa_score,
        checkpoint_daa_score,
        checkpoint_hash,
    })
}

async fn initialize_coverage_range<S: ChainStore>(
    store: &S,
    node: &mut GrpcKaspaNode,
    network: &str,
    coverage_start_hash: &str,
    stored_checkpoint: Option<&Checkpoint>,
) -> IndexerResult<CoverageRangeRecord> {
    if let Some(coverage) = store.coverage_range(DEFAULT_COVERAGE_RANGE_ID)? {
        return Ok(coverage);
    }

    let start_hash = parse_hash(coverage_start_hash)?;
    let (start_daa_score, coverage_class) =
        match coverage_start_daa_score(node, coverage_start_hash).await {
            Some(start_daa_score) => (Some(start_daa_score), CoverageClass::PrunedWindow),
            None => (None, CoverageClass::Unknown),
        };
    let (end_hash, end_daa_score) = stored_checkpoint
        .map(|checkpoint| (checkpoint.block_hash, checkpoint.daa_score))
        .unwrap_or((start_hash, start_daa_score.unwrap_or_default()));

    let coverage = CoverageRangeRecord {
        schema_version: 1,
        range_id: DEFAULT_COVERAGE_RANGE_ID.to_owned(),
        start_hash,
        start_daa_score,
        end_hash,
        end_daa_score,
        source: network.to_owned(),
        coverage_class,
    };
    store.put_coverage_range(&coverage)?;
    Ok(coverage)
}

async fn coverage_start_daa_score(node: &mut GrpcKaspaNode, hash: &str) -> Option<u64> {
    let block = node.get_block(hash.to_owned(), false).await.ok()?.block?;
    Some(block.header?.daa_score)
}

fn select_start_hash(
    configured_start_hash: Option<&str>,
    checkpoint_hash: Option<[u8; 32]>,
    pruning_point_hash: &str,
) -> String {
    configured_start_hash
        .map(str::to_owned)
        .or_else(|| checkpoint_hash.map(hex::encode))
        .unwrap_or_else(|| pruning_point_hash.to_owned())
}

fn parse_hash(hash: &str) -> IndexerResult<[u8; 32]> {
    let bytes = hex::decode(hash).map_err(|_| IndexerError::InvalidHash(hash.to_owned()))?;
    bytes
        .try_into()
        .map_err(|_| IndexerError::InvalidHash(hash.to_owned()))
}

fn parse_optional_hash(hash: &str) -> IndexerResult<Option<[u8; 32]>> {
    if hash.is_empty() {
        return Ok(None);
    }
    parse_hash(hash).map(Some)
}

fn tx_detail_record(
    tx: &protowire::RpcTransaction,
    verbose: &protowire::RpcTransactionVerboseData,
    txid: [u8; 32],
    accepting_block_hash: [u8; 32],
    accepting_daa_score: u64,
    accepting_timestamp_ms: i64,
) -> IndexerResult<TxDetailRecordV1> {
    let inputs = tx
        .inputs
        .iter()
        .map(tx_input_record)
        .collect::<IndexerResult<Vec<_>>>()?;
    let outputs = tx
        .outputs
        .iter()
        .enumerate()
        .map(|(index, output)| tx_output_record(index as u32, output))
        .collect();

    Ok(TxDetailRecordV1 {
        schema_version: 1,
        detail_available: true,
        detail_complete: false,
        txid,
        accepting_block_hash,
        accepting_daa_score,
        accepting_timestamp_ms,
        version: tx.version,
        lock_time: tx.lock_time,
        subnetwork_id: tx.subnetwork_id.clone(),
        gas: tx.gas,
        payload: tx.payload.clone(),
        mass: 0,
        storage_mass: tx.storage_mass,
        compute_mass: verbose.compute_mass,
        block_time: verbose.block_time,
        inputs,
        outputs,
    })
}

fn tx_input_record(input: &protowire::RpcTransactionInput) -> IndexerResult<TxInputRecordV1> {
    let previous_txid = input
        .previous_outpoint
        .as_ref()
        .map(|outpoint| parse_optional_hash(&outpoint.transaction_id))
        .transpose()?
        .flatten();
    let previous_output_index = input
        .previous_outpoint
        .as_ref()
        .map(|outpoint| outpoint.index);
    let previous_output_resolved = input
        .verbose_data
        .as_ref()
        .is_some_and(|verbose| verbose.utxo_entry.is_some());

    Ok(TxInputRecordV1 {
        previous_txid,
        previous_output_index,
        signature_script: input.signature_script.clone(),
        sequence: input.sequence,
        sig_op_count: input.sig_op_count,
        compute_budget: input.compute_budget,
        previous_output_resolved,
    })
}

fn tx_output_record(
    output_index: u32,
    output: &protowire::RpcTransactionOutput,
) -> TxOutputRecordV1 {
    let (script_public_key_version, script_public_key) = output
        .script_public_key
        .as_ref()
        .map(|script| (script.version, script.script_public_key.clone()))
        .unwrap_or_default();
    let script_public_key_type = output
        .verbose_data
        .as_ref()
        .and_then(|verbose| non_empty_string(&verbose.script_public_key_type));
    let script_public_key_address = output
        .verbose_data
        .as_ref()
        .and_then(|verbose| non_empty_string(&verbose.script_public_key_address));

    TxOutputRecordV1 {
        output_index,
        amount: output.amount,
        script_public_key_version,
        script_public_key,
        script_public_key_type,
        script_public_key_address,
    }
}

#[derive(Default)]
struct AddressIndexRecords {
    address_history: Vec<AddressHistoryRecord>,
    address_utxos: Vec<AddressUtxoRecord>,
    spent_address_utxos: Vec<AddressUtxoRecord>,
    outpoint_states: Vec<OutpointStateRecord>,
    unresolved_spends: Vec<UnresolvedSpendRecord>,
    spent_outpoints: Vec<OutpointRef>,
}

fn address_index_records<S: ChainStore>(
    store: &S,
    pending_outpoints: &mut HashMap<([u8; 32], u32), OutpointStateRecord>,
    tx: &protowire::RpcTransaction,
    txid: [u8; 32],
    daa_score: u64,
) -> IndexerResult<AddressIndexRecords> {
    let mut records = AddressIndexRecords::default();
    let mut event_index = 0_u16;

    for (output_index, output) in tx.outputs.iter().enumerate() {
        let Some(script) = output.script_public_key.as_ref() else {
            continue;
        };
        let script_hash = script_hash_from_hex(&script.script_public_key);
        let address = output
            .verbose_data
            .as_ref()
            .and_then(|verbose| non_empty_string(&verbose.script_public_key_address));
        records.address_history.push(AddressHistoryRecord {
            script_hash,
            daa_score,
            txid,
            event_index,
            amount: output.amount.min(i64::MAX as u64) as i64,
        });
        records.address_utxos.push(AddressUtxoRecord {
            script_hash,
            txid,
            output_index: output_index as u32,
            amount: output.amount,
            created_daa_score: daa_score,
        });
        let outpoint = OutpointStateRecord {
            txid,
            output_index: output_index as u32,
            amount: output.amount,
            script_hash,
            address,
            created_daa_score: daa_score,
            spent_by: None,
            spent_daa_score: None,
        };
        pending_outpoints.insert((txid, output_index as u32), outpoint.clone());
        records.outpoint_states.push(outpoint);
        event_index = event_index.saturating_add(1);
    }

    for input in &tx.inputs {
        let Some(previous) = input.previous_outpoint.as_ref() else {
            continue;
        };
        let Some(previous_txid) = parse_optional_hash(&previous.transaction_id)? else {
            continue;
        };

        let resolved_outpoint =
            if let Some(outpoint) = pending_outpoints.get(&(previous_txid, previous.index)) {
                Some(outpoint.clone())
            } else {
                store.outpoint_state(&previous_txid, previous.index)?
            };

        match resolved_outpoint {
            Some(mut outpoint) if outpoint.spent_by.is_none() => {
                outpoint.spent_by = Some(txid);
                outpoint.spent_daa_score = Some(daa_score);
                records.address_history.push(AddressHistoryRecord {
                    script_hash: outpoint.script_hash,
                    daa_score,
                    txid,
                    event_index,
                    amount: -(outpoint.amount.min(i64::MAX as u64) as i64),
                });
                records.spent_address_utxos.push(AddressUtxoRecord {
                    script_hash: outpoint.script_hash,
                    txid: outpoint.txid,
                    output_index: outpoint.output_index,
                    amount: outpoint.amount,
                    created_daa_score: outpoint.created_daa_score,
                });
                records.spent_outpoints.push(OutpointRef {
                    txid: previous_txid,
                    output_index: previous.index,
                });
                pending_outpoints.insert((previous_txid, previous.index), outpoint.clone());
                records.outpoint_states.push(outpoint);
                event_index = event_index.saturating_add(1);
            }
            _ => records.unresolved_spends.push(UnresolvedSpendRecord {
                previous_txid,
                previous_output_index: previous.index,
                spending_txid: txid,
                spending_daa_score: daa_score,
            }),
        }
    }

    Ok(records)
}

fn non_empty_string(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

#[allow(dead_code)]
fn _assert_block_shape(block: protowire::RpcBlock) -> protowire::RpcBlock {
    block
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_hash() {
        let hash = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let parsed = parse_hash(hash).unwrap();
        assert_eq!(parsed[0], 0);
        assert_eq!(parsed[31], 31);
    }

    #[test]
    fn rejects_invalid_hash() {
        assert!(parse_hash("abc").is_err());
        assert!(
            parse_hash("zz0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f").is_err()
        );
    }

    #[test]
    fn selects_configured_start_hash_first() {
        assert_eq!(
            select_start_hash(Some("configured"), Some([7; 32]), "pruning"),
            "configured"
        );
    }

    #[test]
    fn selects_checkpoint_before_pruning_point() {
        assert_eq!(
            select_start_hash(None, Some([7; 32]), "pruning"),
            hex::encode([7; 32])
        );
    }

    #[test]
    fn selects_pruning_point_for_empty_store() {
        assert_eq!(select_start_hash(None, None, "pruning"), "pruning");
    }
}
