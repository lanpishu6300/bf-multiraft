//! Public errors and propose results for MultiRaft.

use thiserror::Error;

use crate::NodeId;

/// Errors returned by MultiRaft propose / group APIs.
#[derive(Debug, Error)]
pub enum MultiRaftError {
    #[error("not leader; hint={hint:?}")]
    NotLeader { hint: Option<NodeId> },

    #[error("unknown group {0}")]
    UnknownGroup(u64),

    #[error("stale queries disabled (set ClusterConfig::enable_stale_queries)")]
    StaleQueriesDisabled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Successful propose: log index and term after quorum commit + apply
/// (linearizable write for that group).
#[derive(Debug, Clone)]
pub struct ProposeOk {
    pub index: u64,
    pub term: u64,
}

/// Result of a local FSM read for Standby service offload.
///
/// Never linearizable: `applied_index` is this node's last applied log only.
#[derive(Debug, Clone)]
pub struct StaleRead<T> {
    pub value: T,
    pub applied_index: u64,
    pub applied_term: u64,
}
