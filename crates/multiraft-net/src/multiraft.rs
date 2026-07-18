//! Public MultiRaft facade over the in-process shared-[`Router`] harness.
//!
//! Lives in `multiraft-net` (not `multiraft-core`) to avoid a core↔net dependency
//! cycle: net already depends on core for [`TypeConfig`].
//!
//! # In-process cluster
//!
//! - [`MultiRaft::start`] starts **one** node with its own [`Router`].
//! - [`MultiRaft::start_cluster`] starts N nodes sharing one [`Router`] (preferred for tests).
//!
//! `ClusterConfig::peers` socket addresses are unused in phase-1 (channel linking).
//! Non-empty `data_dir` enables file-backed Raft logs under `{data_dir}/group-{id}/`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::async_runtime::WatchReceiver;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::type_config::TypeConfigExt;

use crate::network::NetworkFactory;
use crate::node::GroupApp;
use crate::node::GroupMap;
use crate::node::Node;
use crate::router::Router;
use multiraft_core::ClusterConfig;
use multiraft_core::GroupId;
use multiraft_core::MultiRaftError;
use multiraft_core::NodeId;
use multiraft_core::ProposeOk;
use multiraft_core::Request;
use multiraft_core::TypeConfig;
use multiraft_fsm::CounterFsm;
use multiraft_fsm::StateMachine;
use multiraft_store::FileLogStoreOf;
use multiraft_store::MemLogStore;
use multiraft_store::Raft;
use multiraft_store::StateMachineStore;

type LeaderCb = Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>;

/// Coordinates `create_group` so membership is initialized once all peers are local.
#[derive(Clone, Default)]
struct ClusterGlue {
    /// group -> nodes that have created the local raft
    ready: Arc<Mutex<HashMap<GroupId, HashSet<NodeId>>>>,
    /// groups that already claimed initialize
    claimed_init: Arc<Mutex<HashSet<GroupId>>>,
}

impl ClusterGlue {
    fn mark_ready(&self, group: GroupId, node_id: NodeId, members: &[NodeId]) -> bool {
        let mut ready = self.ready.lock().unwrap();
        let set = ready.entry(group).or_default();
        set.insert(node_id);
        members.iter().all(|m| set.contains(m))
    }

    fn try_claim_init(&self, group: GroupId) -> bool {
        self.claimed_init.lock().unwrap().insert(group)
    }
}

/// Multi-Raft handle for one node (many groups).
pub struct MultiRaft<S: StateMachine = CounterFsm> {
    node_id: NodeId,
    config: ClusterConfig,
    router: Router,
    groups: GroupMap<S>,
    glue: ClusterGlue,
    make_fsm: Arc<dyn Fn(GroupId) -> S + Send + Sync>,
    leader_cbs: Arc<Mutex<Vec<LeaderCb>>>,
}

impl MultiRaft<CounterFsm> {
    /// Start a single in-process node with a private [`Router`].
    pub async fn start(config: ClusterConfig) -> anyhow::Result<Self> {
        Self::start_inner(config, Router::new(), ClusterGlue::default(), |_| {
            CounterFsm::new()
        })
        .await
    }

    /// Start N nodes sharing one [`Router`] (in-process multi-node harness).
    ///
    /// `SocketAddr` peers in each config are unused; nodes are linked via the shared router.
    pub async fn start_cluster(configs: Vec<ClusterConfig>) -> anyhow::Result<Vec<Self>> {
        let router = Router::new();
        let glue = ClusterGlue::default();
        let mut nodes = Vec::with_capacity(configs.len());
        for config in configs {
            nodes.push(
                Self::start_inner(config, router.clone(), glue.clone(), |_| CounterFsm::new())
                    .await?,
            );
        }
        Ok(nodes)
    }
}

impl<S: StateMachine> MultiRaft<S> {
    async fn start_inner<F>(
        config: ClusterConfig,
        router: Router,
        glue: ClusterGlue,
        make_fsm: F,
    ) -> anyhow::Result<Self>
    where
        F: Fn(GroupId) -> S + Send + Sync + 'static,
    {
        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let (node, _tx) = Node::with_groups(config.node_id, router.clone(), groups.clone());
        TypeConfig::spawn(node.run());

        Ok(Self {
            node_id: config.node_id,
            config,
            router,
            groups,
            glue,
            make_fsm: Arc::new(make_fsm),
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Create (or idempotently ensure) a local Raft group peer.
    ///
    /// When every `members` node in this in-process cluster has created the group,
    /// membership is initialized once (any racing callers see `NotAllowed` and ignore).
    pub async fn create_group(&self, group: u64, members: &[u64]) -> Result<(), MultiRaftError> {
        if members.is_empty() {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "create_group requires at least one member"
            )));
        }
        if !members.contains(&self.node_id) {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "local node {} is not in members {:?}",
                self.node_id,
                members
            )));
        }

        let needs_spawn = !self.groups.lock().unwrap().contains_key(&group);
        if needs_spawn {
            self.spawn_local_group(group).await?;
        }

        let all_ready = self.glue.mark_ready(group, self.node_id, members);
        if all_ready && self.glue.try_claim_init(group) {
            let raft = self
                .raft(group)
                .ok_or(MultiRaftError::UnknownGroup(group))?;
            let nodes = self.membership_nodes(members);
            match raft.initialize(nodes).await {
                Ok(()) => {}
                Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
                Err(e) => {
                    return Err(MultiRaftError::Other(anyhow::anyhow!(
                        "initialize group: {e}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Propose application bytes via openraft `client_write`.
    /// Non-leader → [`MultiRaftError::NotLeader`].
    pub async fn propose(&self, group: u64, data: Vec<u8>) -> Result<ProposeOk, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;

        match raft.client_write(Request::new(data)).await {
            Ok(resp) => Ok(ProposeOk {
                index: resp.log_id.index(),
                term: resp.log_id.committed_leader_id().term,
            }),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                Err(MultiRaftError::Other(anyhow::anyhow!("client_write: {e}")))
            }
        }
    }

    pub fn is_leader(&self, group: u64) -> bool {
        self.raft(group).map(|r| r.is_leader()).unwrap_or(false)
    }

    pub fn leader(&self, group: u64) -> Option<u64> {
        self.raft(group)
            .and_then(|r| r.metrics().borrow_watched().current_leader)
    }

    /// Register a leader-change callback `(group_id, current_leader)`.
    ///
    /// Best-effort metrics watcher per group (spawned on create_group and for
    /// groups already present when this is called).
    pub fn on_leader_change<F>(&self, cb: F)
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static,
    {
        let cb: LeaderCb = Arc::new(cb);
        self.leader_cbs.lock().unwrap().push(cb);

        let groups: Vec<(GroupId, Raft<S>)> = self
            .groups
            .lock()
            .unwrap()
            .iter()
            .map(|(&gid, app)| (gid, app.raft.clone()))
            .collect();

        for (gid, raft) in groups {
            Self::spawn_leader_watch(gid, raft, self.leader_cbs.clone());
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn router(&self) -> &Router {
        &self.router
    }

    /// Shut down all local Raft groups cleanly (flush / stop core tasks).
    ///
    /// Also unregisters this node from the shared [`Router`] so peers observe
    /// it as unreachable (used by demo admin leader-loss simulation).
    pub async fn shutdown(&self) -> Result<(), MultiRaftError> {
        let rafts: Vec<Raft<S>> = self
            .groups
            .lock()
            .unwrap()
            .values()
            .map(|g| g.raft.clone())
            .collect();
        for raft in rafts {
            raft.shutdown()
                .await
                .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("shutdown: {e}")))?;
        }
        self.groups.lock().unwrap().clear();
        let _ = self.router.unregister_node(self.node_id);
        Ok(())
    }

    /// Wait until the state machine has recovered at least the persisted commit
    /// point after a restart (no-op when the log was empty).
    pub async fn wait_for_recovery(
        &self,
        group: GroupId,
        timeout: Duration,
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        raft.wait_for_recovery(Some(timeout))
            .await
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("wait_for_recovery: {e}")))?;
        Ok(())
    }

    /// Inspect the local FSM for `group` (tests / local reads).
    pub async fn with_fsm<R>(&self, group: GroupId, f: impl FnOnce(&S) -> R) -> Option<R> {
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())?;
        Some(sm.with_fsm(f).await)
    }

    fn raft(&self, group: GroupId) -> Option<Raft<S>> {
        self.groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.raft.clone())
    }

    async fn spawn_local_group(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let config = Config {
            heartbeat_interval: self.config.heartbeat_interval_ms,
            election_timeout_min: self.config.election_timeout_min_ms,
            election_timeout_max: self.config.election_timeout_max_ms,
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| MultiRaftError::Other(anyhow::anyhow!(e.to_string())))?,
        );

        let fsm = (self.make_fsm)(group);
        let state_machine_store = StateMachineStore::new(group, fsm);
        let network = NetworkFactory::new(self.router.clone(), group);

        let raft = if self.config.data_dir.as_os_str().is_empty() {
            let log_store = MemLogStore::default();
            openraft::Raft::new(
                self.node_id,
                config,
                network,
                log_store,
                state_machine_store.clone(),
            )
            .await
        } else {
            let dir = self.config.data_dir.join(format!("group-{group}"));
            let log_store = FileLogStoreOf::open(&dir)
                .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("open file log: {e}")))?;
            openraft::Raft::new(
                self.node_id,
                config,
                network,
                log_store,
                state_machine_store.clone(),
            )
            .await
        }
        .map_err(|e| MultiRaftError::Other(anyhow::anyhow!(e.to_string())))?;

        {
            let mut g = self.groups.lock().unwrap();
            g.insert(
                group,
                GroupApp {
                    node_id: self.node_id,
                    group_id: group,
                    raft: raft.clone(),
                    state_machine: state_machine_store,
                },
            );
        }

        Self::spawn_leader_watch(group, raft, self.leader_cbs.clone());
        Ok(())
    }

    fn membership_nodes(&self, members: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        let mut nodes = BTreeMap::new();
        for &id in members {
            let addr = self
                .config
                .peers
                .iter()
                .find(|(n, _)| *n == id)
                .map(|(_, a)| a.to_string())
                .unwrap_or_default();
            nodes.insert(id, BasicNode { addr });
        }
        nodes
    }

    fn spawn_leader_watch(group: GroupId, raft: Raft<S>, cbs: Arc<Mutex<Vec<LeaderCb>>>) {
        TypeConfig::spawn(async move {
            let mut rx = raft.metrics();
            let mut last: Option<Option<NodeId>> = None;
            loop {
                let cur = rx.borrow_watched().current_leader;
                if last.as_ref() != Some(&cur) {
                    last = Some(cur);
                    let callbacks: Vec<LeaderCb> = cbs.lock().unwrap().clone();
                    for cb in callbacks {
                        cb(group, cur);
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }
}

/// Wait until any handle reports a leader for `group` (test helper).
pub async fn wait_for_leader(
    nodes: &[MultiRaft],
    group: GroupId,
    timeout: Duration,
) -> Option<NodeId> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for n in nodes {
            if let Some(leader) = n.leader(group) {
                if nodes.iter().any(|x| x.is_leader(group)) {
                    return Some(leader);
                }
            }
        }
        TypeConfig::sleep(Duration::from_millis(50)).await;
    }
    None
}
