//! Shared types and OpenRaft TypeConfig for multiraft.
//!
//! Config / error types for the public MultiRaft API live here.
//! The `MultiRaft` runtime facade is implemented in `multiraft-net`
//! (`use multiraft_net::MultiRaft`) to avoid a core↔net dependency cycle.

mod config;
mod error;
mod multiraft;
mod type_config;

pub use config::ClusterConfig;
pub use error::MultiRaftError;
pub use error::ProposeOk;
pub use type_config::GroupId;
pub use type_config::NodeId;
pub use type_config::Raft;
pub use type_config::Request;
pub use type_config::Response;
pub use type_config::SnapshotData;
pub use type_config::TypeConfig;
pub use type_config::typ;
