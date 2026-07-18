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
