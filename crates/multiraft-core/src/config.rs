//! Cluster configuration for MultiRaft.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::NodeId;

/// Static cluster membership and Raft timing knobs.
///
/// Phase-1 in-process networking ignores `peers` socket addresses (channel
/// Router linking). `data_dir` is unused until file persistence.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>,
    pub data_dir: PathBuf,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
}

impl ClusterConfig {
    /// Sensible defaults for local / in-process tests.
    pub fn for_test(node_id: NodeId, peer_ids: &[NodeId]) -> Self {
        let peers = peer_ids
            .iter()
            .map(|&id| {
                (
                    id,
                    format!("127.0.0.1:{}", 19000 + id)
                        .parse()
                        .expect("static test addr"),
                )
            })
            .collect();
        Self {
            node_id,
            peers,
            data_dir: PathBuf::from(format!("/tmp/multiraft-test-{node_id}")),
            heartbeat_interval_ms: 100,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
        }
    }
}
