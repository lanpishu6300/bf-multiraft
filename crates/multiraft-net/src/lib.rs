//! Shared Multi-Raft networking: O(nodes) peer channels, not O(groups).
//!
//! In-process [`Router`] adapted from openraft `examples/multi-raft-kv` at tag
//! `v0.10.0-alpha.30`, implementing [`openraft_multi::GroupRouter`].
//!
//! Public orchestration facade: [`MultiRaft`] (`use multiraft_net::MultiRaft`).

mod api;
mod conn_metrics;
mod multiraft;
mod network;
mod node;
mod router;

pub use conn_metrics::ConnMetrics;
pub use multiraft::MultiRaft;
pub use multiraft::wait_for_leader;
pub use network::NetworkFactory;
pub use node::GroupApp;
pub use node::Node;
pub use node::create_node;
pub use router::NodeMessage;
pub use router::NodeRx;
pub use router::NodeTx;
pub use router::Router;
pub use router::RouterError;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn encode<T: Serialize>(t: T) -> String {
    serde_json::to_string(&t).unwrap()
}

pub fn decode<T: DeserializeOwned>(s: &str) -> T {
    serde_json::from_str(s).unwrap()
}
