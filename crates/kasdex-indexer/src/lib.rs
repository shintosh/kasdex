use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use kasdex_node::{GrpcKaspaNode, NodeError, protowire};
use kasdex_store::{BlockSummaryRecord, ChainStore, Checkpoint, StoreError, TxSummaryRecord};

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
    pub tail_lag_threshold: u64,
    pub stalled_after: Duration,
    pub stale_after: Duration,
}

impl Default for IndexerRuntimeConfig {
    fn default() -> Self {
        Self {
            tail_lag_threshold: 1_000,
            stalled_after: Duration::from_secs(120),
            stale_after: Duration::from_secs(120),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexerRuntimeStatus {
    pub state: IndexerState,
    pub config: IndexerRuntimeConfig,
    pub node_virtual_daa_score: Option<u64>,
    pub node_observed_at: Option<SystemTime>,
    pub lag_daa_score: Option<u64>,
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
}

impl IndexerRuntimeStatus {
    pub fn new(config: IndexerRuntimeConfig) -> Self {
        Self {
            state: IndexerState::Empty,
            config,
            node_virtual_daa_score: None,
            node_observed_at: None,
            lag_daa_score: None,
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
        status.last_poll_started_at = Some(started_at);
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
        let state = match lag_daa_score {
            Some(lag) if lag <= self.snapshot().config.tail_lag_threshold => IndexerState::Tailing,
            Some(_) => IndexerState::Backfilling,
            None => IndexerState::Empty,
        };

        let mut status = self.inner.write().expect("indexer status lock poisoned");
        status.state = state;
        status.node_virtual_daa_score = Some(report.virtual_daa_score);
        status.node_observed_at = Some(finished_at);
        status.lag_daa_score = lag_daa_score;
        status.last_poll_started_at = Some(started_at);
        status.last_poll_finished_at = Some(finished_at);
        status.last_success_at = Some(finished_at);
        status.last_start_hash = Some(report.start_hash.clone());
        status.last_indexed_blocks = Some(report.indexed_blocks);
        status.last_indexed_transactions = Some(report.indexed_transactions);
        status.last_checkpoint_daa_score = report.checkpoint_daa_score;
        status.last_checkpoint_hash = report.checkpoint_hash.clone();
        status.last_poll_duration_ms = duration_ms;
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
        store.put_block(&block_record)?;

        for tx in block.transactions {
            if let Some(verbose) = tx.verbose_data {
                let txid = parse_hash(&verbose.transaction_id)?;
                store.put_tx(&TxSummaryRecord {
                    txid,
                    accepting_block_hash: Some(block_hash),
                    input_count: tx.inputs.len() as u32,
                    output_count: tx.outputs.len() as u32,
                })?;
                indexed_transactions += 1;
            }
        }

        store.put_checkpoint(&Checkpoint {
            network: dag.network_name.clone(),
            daa_score: header.daa_score,
            block_hash,
        })?;
        checkpoint_daa_score = Some(header.daa_score);
        checkpoint_hash = Some(header.hash);
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
