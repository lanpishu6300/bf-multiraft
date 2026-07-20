//! In-process Multi-Raft router with per-node connection sharing.
//!
//! Adapted from openraft `examples/multi-raft-kv/src/router.rs` at tag
//! `v0.10.0-alpha.30`. Route key = `(target_node_id, group_id)`; the channel
//! itself is shared across all groups on a node.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use futures::SinkExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use openraft::error::Unreachable;

use crate::conn_metrics::ConnMetrics;
use crate::decode;
use crate::encode;
use crate::standby_throttle::StandbyThrottle;
use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_core::TypeConfig;
use multiraft_core::typ::RaftError;

pub type NodeTx = mpsc::Sender<NodeMessage>;
pub type NodeRx = mpsc::Receiver<NodeMessage>;

#[derive(Debug)]
pub struct RouterError(pub String);

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RouterError {}

/// Message sent through a node connection; `group_id` selects the Raft group.
pub struct NodeMessage {
    pub group_id: GroupId,
    pub path: String,
    pub payload: String,
    pub response_tx: oneshot::Sender<String>,
}

/// Multi-Raft router: one channel per node, shared by all groups on that node.
#[derive(Clone, Default)]
pub struct Router {
    /// Map from node_id to node connection.
    pub nodes: Arc<Mutex<BTreeMap<NodeId, NodeTx>>>,
    metrics: ConnMetrics,
    throttle: StandbyThrottle,
}

impl Router {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(BTreeMap::new())),
            metrics: ConnMetrics::new(),
            throttle: StandbyThrottle::default(),
        }
    }

    /// Standby replication throttle shared by all groups on this fabric.
    pub fn throttle(&self) -> &StandbyThrottle {
        &self.throttle
    }

    /// Register a node connection. All groups on this node share it.
    ///
    /// Increments [`unique_peer_links`](Self::unique_peer_links) on first
    /// registration of `node_id` only (never per group).
    pub fn register_node(&self, node_id: NodeId, tx: NodeTx) {
        {
            let mut nodes = self.nodes.lock().unwrap();
            nodes.insert(node_id, tx);
        }
        self.metrics.record_peer(node_id);
    }

    /// Unregister a node connection.
    pub fn unregister_node(&self, node_id: NodeId) -> Option<NodeTx> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(&node_id)
    }

    /// Distinct peer node ids with an open channel (O(nodes), not O(groups)).
    pub fn unique_peer_links(&self) -> usize {
        self.metrics.unique_peer_links()
    }

    /// Send a request to a specific `(node, group)`.
    pub async fn send<Req, Resp>(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        path: &str,
        req: Req,
    ) -> Result<Resp, Unreachable<TypeConfig>>
    where
        Req: serde::Serialize,
        Result<Resp, RaftError>: serde::de::DeserializeOwned,
    {
        let _standby_permit = self.throttle.before_send(to_node).await;

        let (resp_tx, resp_rx) = oneshot::channel();

        let encoded_req = encode(&req);
        tracing::debug!(
            "send to: node={}, group={}, path={}, req={}",
            to_node,
            to_group,
            path,
            encoded_req
        );

        let mut tx = {
            let nodes = self.nodes.lock().unwrap();
            nodes
                .get(&to_node)
                .ok_or_else(|| {
                    Unreachable::new(&RouterError(format!("node {} not connected", to_node)))
                })?
                .clone()
        };

        let msg = NodeMessage {
            group_id: to_group,
            path: path.to_string(),
            payload: encoded_req,
            response_tx: resp_tx,
        };

        tx.send(msg)
            .await
            .map_err(|e| Unreachable::new(&RouterError(e.to_string())))?;

        let resp_str = resp_rx
            .await
            .map_err(|e| Unreachable::new(&RouterError(e.to_string())))?;
        tracing::debug!(
            "resp from: node={}, group={}, path={}, resp={}",
            to_node,
            to_group,
            path,
            resp_str
        );

        let res = decode::<Result<Resp, RaftError>>(&resp_str);
        res.map_err(|e| Unreachable::new(&RouterError(e.to_string())))
    }

    pub fn has_node(&self, node_id: NodeId) -> bool {
        let nodes = self.nodes.lock().unwrap();
        nodes.contains_key(&node_id)
    }
}
