//! Shared Multi-Raft networking: O(nodes) peer channels, not O(groups).
//!
//! In-process [`Router`] adapted from openraft `examples/multi-raft-kv` at tag
//! `v0.10.0-alpha.30`, implementing [`openraft_multi::GroupRouter`].
//!
//! Cross-process: [`GrpcRouter`] + tonic [`GrpcServer`] (UTF-8 JSON payloads).
//!
//! Public orchestration facade: [`MultiRaft`] (`use multiraft_net::MultiRaft`).

mod api;
mod conn_metrics;
mod grpc;
mod multiraft;
mod network;
mod node;
mod router;
mod snapshot_fetch;
mod standby_throttle;

pub use conn_metrics::ConnMetrics;
pub use grpc::GrpcRouter;
pub use grpc::GrpcServer;
pub use multiraft::MultiRaft;
pub use multiraft::SharedFabric;
pub use multiraft::wait_for_leader;
pub use network::GrpcNetworkFactory;
pub use network::NetworkFactory;
pub use node::GroupApp;
pub use node::Node;
pub use node::create_node;
pub use router::NodeMessage;
pub use router::NodeRx;
pub use router::NodeTx;
pub use router::Router;
pub use router::RouterError;
pub use snapshot_fetch::FetchedSnapshot;
pub use snapshot_fetch::pull_snapshot_chunked;
pub use standby_throttle::StandbyThrottle;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn encode<T: Serialize>(t: T) -> String {
    serde_json::to_string(&t).unwrap()
}

pub fn decode<T: DeserializeOwned>(s: &str) -> T {
    serde_json::from_str(s).unwrap()
}
