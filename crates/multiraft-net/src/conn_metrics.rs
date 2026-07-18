//! Connection cardinality metrics for shared Multi-Raft networking.
//!
//! Peer links are counted **per node**, never per Raft group. Opening a channel
//! to the same peer from many groups must not inflate this counter.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use multiraft_core::NodeId;

/// Tracks distinct peer node IDs that have an open in-process channel.
#[derive(Clone, Debug, Default)]
pub struct ConnMetrics {
    peers: Arc<Mutex<BTreeSet<NodeId>>>,
}

impl ConnMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the first open channel to `peer`. Subsequent calls for the same
    /// peer (e.g. many groups routing to it) are no-ops.
    pub fn record_peer(&self, peer: NodeId) {
        let mut peers = self.peers.lock().unwrap();
        peers.insert(peer);
    }

    /// Number of distinct peer node ids with an open channel.
    pub fn unique_peer_links(&self) -> usize {
        self.peers.lock().unwrap().len()
    }
}
