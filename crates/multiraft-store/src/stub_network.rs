//! Minimal in-process network stub for single-node smoke tests.
//!
//! Single-node clusters do not issue peer RPCs after `initialize`; this factory
//! only satisfies `Raft::new`. Full shared routing lands in `multiraft-net`.

use std::future::Future;
use std::io::Cursor;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;

use multiraft_core::NodeId;
use multiraft_core::TypeConfig;

fn unreachable<E>() -> RPCError<TypeConfig, E>
where
    E: std::error::Error + openraft::OptionalSend + openraft::OptionalSync + 'static,
{
    RPCError::Unreachable(Unreachable::from_string(
        "stub network: no peers (single-node test)",
    ))
}

/// Network client that always reports unreachable peers.
#[derive(Clone, Debug, Default)]
pub struct StubNetwork;

impl RaftNetworkV2<TypeConfig> for StubNetwork {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(unreachable())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(unreachable())
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::Unreachable(Unreachable::from_string(
            "stub network: no peers (single-node test)",
        )))
    }

    async fn transfer_leader(
        &mut self,
        _req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(unreachable())
    }
}

/// Factory producing [`StubNetwork`] clients.
#[derive(Clone, Debug, Default)]
pub struct StubNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for StubNetworkFactory {
    type Network = StubNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        StubNetwork
    }
}
