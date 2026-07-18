//! `GroupRouter` / `RaftNetworkFactory` adapters over the shared [`Router`].
//!
//! Adapted from openraft `examples/multi-raft-kv/src/network.rs` at tag
//! `v0.10.0-alpha.30`.

use std::future::Future;

use openraft::OptionalSend;
use openraft::alias::SnapshotOf;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::Backoff;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft_multi::GroupNetworkAdapter;
use openraft_multi::GroupRouter;

use crate::router::Router;
use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_core::TypeConfig;
use multiraft_core::typ;

impl GroupRouter<TypeConfig, GroupId> for Router {
    type SnapshotData = typ::SnapshotData;

    async fn append_entries(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.send(target, group_id, "/raft/append", rpc)
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn vote(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.send(target, group_id, "/raft/vote", rpc)
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn full_snapshot(
        &self,
        target: NodeId,
        group_id: GroupId,
        vote: typ::Vote,
        snapshot: SnapshotOf<TypeConfig, typ::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let data: Vec<u8> = snapshot.snapshot.into_inner();
        self.send(
            target,
            group_id,
            "/raft/snapshot",
            (vote, snapshot.meta, data),
        )
        .await
        .map_err(StreamingError::Unreachable)
    }

    async fn transfer_leader(
        &self,
        target: NodeId,
        group_id: GroupId,
        req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.send(target, group_id, "/raft/transfer_leader", req)
            .await
            .map_err(RPCError::Unreachable)
    }

    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(std::time::Duration::from_millis(
            500,
        ))))
    }
}

/// Local network factory wrapping shared [`Router`] + `group_id`.
///
/// Mirrors `openraft_multi::GroupNetworkFactory` but is a local type so we can
/// implement [`RaftNetworkFactory`] (orphan rules: `TypeConfig` lives in
/// `multiraft-core`).
#[derive(Clone)]
pub struct NetworkFactory {
    pub router: Router,
    pub group_id: GroupId,
}

impl NetworkFactory {
    pub fn new(router: Router, group_id: GroupId) -> Self {
        Self { router, group_id }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = GroupNetworkAdapter<TypeConfig, GroupId, Router>;

    async fn new_client(&mut self, target: NodeId, _node: &openraft::BasicNode) -> Self::Network {
        // Binding (target, group) must NOT open a new peer link — groups share
        // the router's per-node channel (see `Router::unique_peer_links`).
        GroupNetworkAdapter::new(self.router.clone(), target, self.group_id)
    }
}
