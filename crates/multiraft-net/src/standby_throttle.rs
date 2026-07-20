//! Throttle outbound Raft RPCs toward Standby (learner) peers.
//!
//! Does not affect voter↔voter replication: only targets in the standby set
//! are delayed / gated.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use multiraft_core::ClusterConfig;
use multiraft_core::NodeId;

/// Shared standby peer set + soft replication limits.
#[derive(Clone, Debug)]
pub struct StandbyThrottle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    standby_ids: Mutex<HashSet<NodeId>>,
    delay_ms: AtomicU64,
    max_inflight: AtomicU32,
    semaphores: Mutex<HashMap<NodeId, Arc<Semaphore>>>,
}

impl Default for StandbyThrottle {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                standby_ids: Mutex::new(HashSet::new()),
                delay_ms: AtomicU64::new(0),
                max_inflight: AtomicU32::new(8),
                semaphores: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl StandbyThrottle {
    /// Build from [`ClusterConfig`] seed fields.
    pub fn from_config(config: &ClusterConfig) -> Self {
        let t = Self::default();
        t.apply_config(config);
        t
    }

    /// Update delay / max-inflight / seed ids from config (keeps existing runtime ids).
    pub fn apply_config(&self, config: &ClusterConfig) {
        self.inner
            .delay_ms
            .store(config.standby_replicate_delay_ms, Ordering::Relaxed);
        let max = if config.standby_max_inflight == 0 {
            8
        } else {
            config.standby_max_inflight
        };
        self.inner.max_inflight.store(max, Ordering::Relaxed);
        {
            let mut ids = self.inner.standby_ids.lock().unwrap();
            for &id in &config.standby_node_ids {
                ids.insert(id);
            }
        }
        // Reset per-target semaphores so capacity matches new max.
        self.inner.semaphores.lock().unwrap().clear();
    }

    pub fn insert(&self, id: NodeId) {
        self.inner.standby_ids.lock().unwrap().insert(id);
    }

    pub fn remove(&self, id: NodeId) {
        self.inner.standby_ids.lock().unwrap().remove(&id);
        self.inner.semaphores.lock().unwrap().remove(&id);
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.inner.standby_ids.lock().unwrap().contains(&id)
    }

    pub fn standby_ids(&self) -> HashSet<NodeId> {
        self.inner.standby_ids.lock().unwrap().clone()
    }

    /// If `target` is a standby peer: sleep `delay_ms`, then acquire an inflight permit.
    ///
    /// Returns a permit that must be held until the RPC completes. `None` when the
    /// target is not a standby (no throttle).
    pub async fn before_send(&self, target: NodeId) -> Option<OwnedSemaphorePermit> {
        if !self.contains(target) {
            return None;
        }
        let delay = self.inner.delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let max = self.inner.max_inflight.load(Ordering::Relaxed).max(1) as usize;
        let sem = {
            let mut map = self.inner.semaphores.lock().unwrap();
            map.entry(target)
                .or_insert_with(|| Arc::new(Semaphore::new(max)))
                .clone()
        };
        match sem.acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => None,
        }
    }
}
