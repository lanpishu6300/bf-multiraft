//! Raft log / state storage for multiraft.
//!
//! Memory log + SM bridge adapted from openraft `examples/{log-mem,sm-mem,multi-raft-kv}`
//! at tag `v0.10.0-alpha.30`.

mod log_file;
mod log_mem;
mod sm_bridge;
pub mod stub_network;

pub use log_file::FileLogStore;
pub use log_mem::LogStore;
pub use sm_bridge::StateMachineStore;
pub use stub_network::StubNetworkFactory;

pub use multiraft_core::GroupId;
pub use multiraft_core::NodeId;
pub use multiraft_core::Request;
pub use multiraft_core::Response;
pub use multiraft_core::TypeConfig;

/// Concrete memory log store for multiraft's [`TypeConfig`].
pub type MemLogStore = LogStore<TypeConfig>;

/// Concrete file-backed log store for multiraft's [`TypeConfig`].
pub type FileLogStoreOf = FileLogStore<TypeConfig>;

/// Raft handle backed by a bridged [`multiraft_fsm::StateMachine`].
pub type Raft<S> = openraft::Raft<TypeConfig, StateMachineStore<S>>;

/// Convenience alias for the demo counter FSM.
pub type CounterRaft = Raft<multiraft_fsm::CounterFsm>;
