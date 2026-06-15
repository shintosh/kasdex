use kasdex_node::{GrpcKaspaNode, NodeError, protowire};
use kasdex_store::{BlockSummaryRecord, ChainStore, Checkpoint, StoreError, TxSummaryRecord};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexerState {
    Mocked,
    Backfilling,
    Tailing,
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
