//! Cluster configuration for MultiRaft.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::NodeId;

/// Local Raft role for this process/node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NodeRole {
    #[default]
    Voter,
    /// openraft Learner; never becomes Leader. Used for async snapshot offload.
    Standby,
}

/// How snapshots are produced on this cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Legacy: voters may sync-dump FSM in `build_snapshot`.
    #[default]
    Disabled,
    /// Voters never hot-dump FSM; Standby builds durable snapshots asynchronously.
    StandbyOffload,
}

/// Static cluster membership and Raft timing knobs.
///
/// Phase-1 in-process networking ignores `peers` socket addresses (channel
/// Router linking). Empty `data_dir` → memory log; non-empty → file store
/// under `{data_dir}/group-{id}/`.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>,
    /// When empty, MultiRaft uses an in-memory log. When set, uses a file-backed
    /// log at `{data_dir}/group-{id}/`.
    pub data_dir: PathBuf,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Local role (voter vs standby learner).
    pub role: NodeRole,
    /// Snapshot policy; see [`SnapshotMode`].
    pub snapshot_mode: SnapshotMode,
    /// How many durable catalog snapshots to retain per group (StandbyOffload).
    pub snapshot_keep: usize,
    /// Admin HTTP bind / advertise address for snapshot fetch URLs (demo / recovery).
    pub admin_advertise_addr: Option<SocketAddr>,
    /// Max outstanding AppendEntries toward Standby node ids (soft).
    pub standby_max_inflight: u32,
    /// Artificial delay before sending RPC to standby peers (ms).
    pub standby_replicate_delay_ms: u64,
    /// Node ids treated as standby for throttling (seed; also updated at runtime).
    pub standby_node_ids: Vec<NodeId>,
    /// Chunk size for HTTP Range snapshot downloads (bytes). Default 64 KiB.
    pub snapshot_fetch_chunk_bytes: usize,
    /// Background daisy sync interval when [`Self::daisy_upstream_base`] is set (ms).
    pub daisy_sync_interval_ms: u64,
    /// If set on a Standby, pull snapshots from this upstream Standby's base URL
    /// (e.g. `"http://127.0.0.1:23103"`) instead of only following the leader.
    /// Path used: `{base}/snapshots/{group}/latest`.
    pub daisy_upstream_base: Option<String>,
}

impl ClusterConfig {
    /// Sensible defaults for local / in-process tests (memory log).
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
            data_dir: PathBuf::new(),
            heartbeat_interval_ms: 100,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            role: NodeRole::Voter,
            snapshot_mode: SnapshotMode::Disabled,
            snapshot_keep: 2,
            admin_advertise_addr: None,
            standby_max_inflight: 8,
            standby_replicate_delay_ms: 0,
            standby_node_ids: Vec::new(),
            snapshot_fetch_chunk_bytes: 65_536,
            daisy_sync_interval_ms: 2_000,
            daisy_upstream_base: None,
        }
    }
}
