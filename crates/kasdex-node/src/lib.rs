use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;

pub mod protowire {
    tonic::include_proto!("protowire");
}

use protowire::{
    GetBlockDagInfoRequestMessage, GetBlockDagInfoResponseMessage, GetBlockRequestMessage,
    GetBlockResponseMessage, GetInfoRequestMessage, GetInfoResponseMessage,
    GetVirtualChainFromBlockRequestMessage, GetVirtualChainFromBlockResponseMessage, KaspadRequest,
    KaspadResponse, kaspad_request, kaspad_response, rpc_client::RpcClient,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeProbeStatus {
    pub endpoint: String,
    pub network: String,
    pub server_version: String,
    pub is_synced: bool,
    pub is_archival: Option<bool>,
    pub has_utxo_index: bool,
    pub virtual_daa_score: u64,
    pub pruning_point_hash: String,
    pub sink: String,
    pub virtual_chain_sample_start: String,
    pub virtual_chain_added: usize,
    pub accepted_transaction_batches: usize,
    pub sink_block_transaction_count: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("transport error: {0}")]
    Transport(#[source] Box<tonic::transport::Error>),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("rpc status: {0}")]
    Status(#[source] Box<tonic::Status>),
    #[error("request stream closed")]
    RequestStreamClosed,
    #[error("response stream ended before response id {0}")]
    ResponseStreamEnded(u64),
    #[error("unexpected response payload for request id {0}")]
    UnexpectedResponse(u64),
    #[error("rpc error: {0}")]
    Rpc(String),
}

pub type NodeResult<T> = Result<T, NodeError>;

impl From<tonic::transport::Error> for NodeError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(Box::new(error))
    }
}

impl From<tonic::Status> for NodeError {
    fn from(error: tonic::Status) -> Self {
        Self::Status(Box::new(error))
    }
}

pub struct GrpcKaspaNode {
    endpoint: String,
    sender: mpsc::Sender<KaspadRequest>,
    responses: tonic::Streaming<KaspadResponse>,
    next_id: u64,
}

impl GrpcKaspaNode {
    pub async fn connect(endpoint: impl Into<String>) -> NodeResult<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|err| NodeError::InvalidEndpoint(err.to_string()))?
            .connect()
            .await?;
        let mut client = RpcClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
        let (sender, receiver) = mpsc::channel(32);
        let responses = client
            .message_stream(ReceiverStream::new(receiver))
            .await?
            .into_inner();

        Ok(Self {
            endpoint,
            sender,
            responses,
            next_id: 1,
        })
    }

    pub async fn probe(mut self) -> NodeResult<NodeProbeStatus> {
        let info = self.get_info().await?;
        let dag = self.get_block_dag_info().await?;
        let virtual_chain_sample_start = dag.pruning_point_hash.clone();
        let virtual_chain = self
            .get_virtual_chain_from_block(virtual_chain_sample_start.clone(), true)
            .await?;
        let sink_block = self.get_block(dag.sink.clone(), true).await?;
        let sink_block_transaction_count = sink_block
            .block
            .as_ref()
            .map(|block| block.transactions.len())
            .unwrap_or_default();

        Ok(NodeProbeStatus {
            endpoint: self.endpoint,
            network: dag.network_name,
            server_version: info.server_version,
            is_synced: info.is_synced,
            is_archival: None,
            has_utxo_index: info.is_utxo_indexed,
            virtual_daa_score: dag.virtual_daa_score,
            pruning_point_hash: dag.pruning_point_hash,
            sink: dag.sink,
            virtual_chain_sample_start,
            virtual_chain_added: virtual_chain.added_chain_block_hashes.len(),
            accepted_transaction_batches: virtual_chain.accepted_transaction_ids.len(),
            sink_block_transaction_count,
        })
    }

    async fn get_info(&mut self) -> NodeResult<GetInfoResponseMessage> {
        let id = self
            .send(kaspad_request::Payload::GetInfoRequest(
                GetInfoRequestMessage {},
            ))
            .await?;
        match self.recv_payload(id).await? {
            kaspad_response::Payload::GetInfoResponse(response) => {
                ensure_no_rpc_error(response.error.as_ref())?;
                Ok(response)
            }
            _ => Err(NodeError::UnexpectedResponse(id)),
        }
    }

    pub async fn get_block_dag_info(&mut self) -> NodeResult<GetBlockDagInfoResponseMessage> {
        let id = self
            .send(kaspad_request::Payload::GetBlockDagInfoRequest(
                GetBlockDagInfoRequestMessage {},
            ))
            .await?;
        match self.recv_payload(id).await? {
            kaspad_response::Payload::GetBlockDagInfoResponse(response) => {
                ensure_no_rpc_error(response.error.as_ref())?;
                Ok(response)
            }
            _ => Err(NodeError::UnexpectedResponse(id)),
        }
    }

    pub async fn get_virtual_chain_from_block(
        &mut self,
        start_hash: String,
        include_accepted_transaction_ids: bool,
    ) -> NodeResult<GetVirtualChainFromBlockResponseMessage> {
        let id = self
            .send(kaspad_request::Payload::GetVirtualChainFromBlockRequest(
                GetVirtualChainFromBlockRequestMessage {
                    start_hash,
                    include_accepted_transaction_ids,
                    min_confirmation_count: None,
                },
            ))
            .await?;
        match self.recv_payload(id).await? {
            kaspad_response::Payload::GetVirtualChainFromBlockResponse(response) => {
                ensure_no_rpc_error(response.error.as_ref())?;
                Ok(response)
            }
            _ => Err(NodeError::UnexpectedResponse(id)),
        }
    }

    pub async fn get_block(
        &mut self,
        hash: String,
        include_transactions: bool,
    ) -> NodeResult<GetBlockResponseMessage> {
        let id = self
            .send(kaspad_request::Payload::GetBlockRequest(
                GetBlockRequestMessage {
                    hash,
                    include_transactions,
                },
            ))
            .await?;
        match self.recv_payload(id).await? {
            kaspad_response::Payload::GetBlockResponse(response) => {
                ensure_no_rpc_error(response.error.as_ref())?;
                Ok(response)
            }
            _ => Err(NodeError::UnexpectedResponse(id)),
        }
    }

    async fn send(&mut self, payload: kaspad_request::Payload) -> NodeResult<u64> {
        let id = self.next_id;
        self.next_id += 1;
        self.sender
            .send(KaspadRequest {
                id,
                payload: Some(payload),
            })
            .await
            .map_err(|_| NodeError::RequestStreamClosed)?;
        Ok(id)
    }

    async fn recv_payload(&mut self, id: u64) -> NodeResult<kaspad_response::Payload> {
        while let Some(response) = self.responses.next().await {
            let response = response?;
            if response.id != id {
                continue;
            }
            return response.payload.ok_or(NodeError::UnexpectedResponse(id));
        }

        Err(NodeError::ResponseStreamEnded(id))
    }
}

fn ensure_no_rpc_error(error: Option<&protowire::RpcError>) -> NodeResult<()> {
    match error {
        Some(error) => Err(NodeError::Rpc(error.message.clone())),
        None => Ok(()),
    }
}
