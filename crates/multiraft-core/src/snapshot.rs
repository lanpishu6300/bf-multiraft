//! Standby async snapshot trigger + advertisement types (Aeron-aligned).

use serde::Deserialize;
use serde::Serialize;

/// Magic payload: Leader proposes this; Standby applies it and schedules an async snapshot.
pub const STANDBY_SNAPSHOT_TRIGGER: &[u8] = b"\0multiraft.standby_snapshot_trigger\0";

/// Returns true when `data` is the standby snapshot trigger magic bytes.
pub fn is_standby_snapshot_trigger(data: &[u8]) -> bool {
    data == STANDBY_SNAPSHOT_TRIGGER
}

/// Advertisement published by a Standby after a durable snapshot is written.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SnapshotAdvertisement {
    pub group: u64,
    pub last_index: u64,
    pub last_term: u64,
    pub snapshot_id: String,
    pub size: u64,
    pub sha256_hex: String,
    /// e.g. `http://127.0.0.1:23103/snapshots/0/latest`
    pub fetch_url: String,
}
