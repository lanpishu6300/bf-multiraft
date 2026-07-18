//! Raft protocol handlers for the in-process node dispatcher.
//!
//! Adapted from openraft `examples/multi-raft-kv/src/api.rs` at tag
//! `v0.10.0-alpha.30` (Raft paths only — app KV paths omitted).

use std::io::Cursor;

use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;

use crate::decode;
use crate::encode;
use crate::node::GroupApp;
use multiraft_core::typ::*;
use multiraft_fsm::StateMachine;

pub async fn vote<S: StateMachine>(app: &mut GroupApp<S>, req: String) -> String {
    let res = app.raft.vote(decode(&req)).await;
    encode(res)
}

pub async fn append<S: StateMachine>(app: &mut GroupApp<S>, req: String) -> String {
    let res = app.raft.append_entries(decode(&req)).await;
    encode(res)
}

pub async fn snapshot<S: StateMachine>(app: &mut GroupApp<S>, req: String) -> String {
    let (vote, snapshot_meta, snapshot_data): (Vote, SnapshotMeta, Vec<u8>) = decode(&req);
    let snapshot = Snapshot {
        meta: snapshot_meta,
        snapshot: Cursor::new(snapshot_data),
    };
    let res = app
        .raft
        .install_full_snapshot(vote, snapshot)
        .await
        .map_err(RaftError::<Infallible>::Fatal);
    encode(res)
}

pub async fn transfer_leader<S: StateMachine>(app: &mut GroupApp<S>, req: String) -> String {
    let transfer_req: TransferLeaderRequest<multiraft_core::TypeConfig> = decode(&req);
    let res: Result<TransferLeaderResponse<multiraft_core::TypeConfig>, RaftError> = app
        .raft
        .handle_transfer_leader(transfer_req)
        .await
        .map_err(RaftError::Fatal);
    encode(res)
}
