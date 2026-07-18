//! In-process Multi-Raft node: one shared receive channel, many Raft groups.
//!
//! Adapted from openraft `examples/multi-raft-kv/src/app.rs` + `create_node`
//! at tag `v0.10.0-alpha.30`.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use futures::channel::mpsc;
use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_fsm::StateMachine;
use multiraft_store::MemLogStore;
use multiraft_store::Raft;
use multiraft_store::StateMachineStore;
use openraft::Config;

use crate::api;
use crate::encode;
use crate::network::NetworkFactory;
use crate::router::NodeMessage;
use crate::router::NodeRx;
use crate::router::NodeTx;
use crate::router::Router;
use multiraft_core::typ;

/// A node manages multiple Raft groups that share one inbound connection.
pub struct Node<S: StateMachine> {
    pub node_id: NodeId,
    pub groups: BTreeMap<GroupId, GroupApp<S>>,
    pub rx: NodeRx,
    pub router: Router,
}

impl<S: StateMachine> Node<S> {
    pub fn new(node_id: NodeId, router: Router) -> (Self, NodeTx) {
        let (tx, rx) = mpsc::channel(1024);
        router.register_node(node_id, tx.clone());

        let node = Self {
            node_id,
            groups: BTreeMap::new(),
            rx,
            router,
        };
        (node, tx)
    }

    pub fn add_group(
        &mut self,
        group_id: GroupId,
        raft: Raft<S>,
        state_machine: StateMachineStore<S>,
    ) {
        let app = GroupApp {
            node_id: self.node_id,
            group_id,
            raft,
            state_machine,
        };
        self.groups.insert(group_id, app);
    }

    pub fn get_raft(&self, group_id: GroupId) -> Option<&Raft<S>> {
        self.groups.get(&group_id).map(|g| &g.raft)
    }

    /// Dispatch inbound messages to the correct group by `group_id`.
    pub async fn run(mut self) -> Option<()> {
        loop {
            let msg = self.rx.next().await?;

            let NodeMessage {
                group_id,
                path,
                payload,
                response_tx,
            } = msg;

            let group = match self.groups.get_mut(&group_id) {
                Some(g) => g,
                None => {
                    let _ = response_tx.send(encode::<Result<(), typ::RaftError>>(Err(
                        typ::RaftError::Fatal(openraft::error::Fatal::Stopped),
                    )));
                    continue;
                }
            };

            let res = match path.as_str() {
                "/raft/append" => api::append(group, payload).await,
                "/raft/snapshot" => api::snapshot(group, payload).await,
                "/raft/vote" => api::vote(group, payload).await,
                "/raft/transfer_leader" => api::transfer_leader(group, payload).await,
                _ => {
                    tracing::warn!("unknown path: {}", path);
                    encode::<Result<(), typ::RaftError>>(Err(typ::RaftError::Fatal(
                        openraft::error::Fatal::Stopped,
                    )))
                }
            };

            let _ = response_tx.send(res);
        }
    }
}

/// Single Raft group's application context on a node.
pub struct GroupApp<S: StateMachine> {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub raft: Raft<S>,
    pub state_machine: StateMachineStore<S>,
}

/// Create a node with multiple Raft groups sharing one router connection.
pub async fn create_node<S, F>(
    node_id: NodeId,
    group_ids: &[GroupId],
    router: Router,
    mut make_fsm: F,
) -> Node<S>
where
    S: StateMachine,
    F: FnMut(GroupId) -> S,
{
    let (mut node, _tx) = Node::new(node_id, router.clone());

    for &group_id in group_ids {
        let config = Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        };
        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let state_machine_store = StateMachineStore::new(group_id, make_fsm(group_id));
        let network = NetworkFactory::new(router.clone(), group_id);

        let raft = openraft::Raft::new(
            node_id,
            config,
            network,
            log_store,
            state_machine_store.clone(),
        )
        .await
        .unwrap();

        node.add_group(group_id, raft, state_machine_store);
    }

    node
}
