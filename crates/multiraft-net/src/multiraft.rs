//! Public MultiRaft facade over in-process [`Router`] or cross-process [`GrpcRouter`].
//!
//! Lives in `multiraft-net` (not `multiraft-core`) to avoid a core↔net dependency
//! cycle: net already depends on core for [`TypeConfig`].
//!
//! # In-process cluster
//!
//! - [`MultiRaft::start`] starts **one** node with its own [`Router`].
//! - [`MultiRaft::start_cluster`] starts N nodes sharing one [`Router`] (preferred for tests).
//! - [`SharedFabric`] exposes the shared [`Router`] + glue so chaos tests can
//!   `shutdown` a node and [`SharedFabric::start_node`] it again with the same
//!   `node_id` / `data_dir` / peers.
//!
//! # Cross-process (gRPC)
//!
//! - [`MultiRaft::start_grpc`] binds a tonic server on this node's peer addr and
//!   uses [`GrpcRouter`] for outbound Raft RPCs.

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

use crate::grpc::GrpcRouter;
use crate::grpc::GrpcServer;
use crate::network::GrpcNetworkFactory;
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

/// Shared in-process fabric: one [`Router`] + cluster glue for many nodes.
///
/// Use this when a test needs to restart a node after [`MultiRaft::shutdown`]
/// without losing the peer mesh (unregister on shutdown; re-register on start).
#[derive(Clone, Default)]
pub struct SharedFabric {
    router: Router,
    glue: ClusterGlue,
}

impl SharedFabric {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn router(&self) -> &Router {
        &self.router
    }

    pub async fn start_node(&self, config: ClusterConfig) -> anyhow::Result<MultiRaft> {
        MultiRaft::start_inner(
            config,
            self.router.clone(),
            self.glue.clone(),
            |_| CounterFsm::new(),
        )
        .await
    }
}

enum NetBackend {
    InProcess {
        router: Router,
        glue: ClusterGlue,
    },
    Grpc {
        router: GrpcRouter,
    },
}

/// Multi-Raft handle for one node (many groups).
pub struct MultiRaft<S: StateMachine = CounterFsm> {
    node_id: NodeId,
    config: ClusterConfig,
    net: NetBackend,
    groups: GroupMap<S>,
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
    /// Internally uses [`SharedFabric`]; prefer that type when tests need node restart.
    pub async fn start_cluster(configs: Vec<ClusterConfig>) -> anyhow::Result<Vec<Self>> {
        let fabric = SharedFabric::new();
        let mut nodes = Vec::with_capacity(configs.len());
        for config in configs {
            nodes.push(fabric.start_node(config).await?);
        }
        Ok(nodes)
    }

    /// Start one node with cross-process tonic transport.
    ///
    /// Binds a gRPC server on this node's address from `config.peers` and uses
    /// [`GrpcRouter`] for outbound Raft RPCs to other peers.
    pub async fn start_grpc(config: ClusterConfig) -> anyhow::Result<Self> {
        Self::start_grpc_inner(config, |_| CounterFsm::new()).await
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
            net: NetBackend::InProcess { router, glue },
            groups,
            make_fsm: Arc::new(make_fsm),
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
        })
    }

    async fn start_grpc_inner<F>(config: ClusterConfig, make_fsm: F) -> anyhow::Result<Self>
    where
        F: Fn(GroupId) -> S + Send + Sync + 'static,
    {
        let self_addr = config
            .peers
            .iter()
            .find(|(id, _)| *id == config.node_id)
            .map(|(_, a)| *a)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "start_grpc: node {} missing from config.peers",
                    config.node_id
                )
            })?;

        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let grpc_router = GrpcRouter::new(config.peers.clone(), config.node_id);

        let groups_for_server = groups.clone();
        let listener = tokio::net::TcpListener::bind(self_addr).await?;
        tokio::spawn(async move {
            if let Err(e) = GrpcServer::serve_with_listener(listener, groups_for_server).await {
                tracing::error!("grpc server on {self_addr} exited: {e:#}");
            }
        });

        // Give the accept loop a moment to register with the runtime.
        TypeConfig::sleep(Duration::from_millis(20)).await;

        Ok(Self {
            node_id: config.node_id,
            config,
            net: NetBackend::Grpc {
                router: grpc_router,
            },
            groups,
            make_fsm: Arc::new(make_fsm),
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Create (or idempotently ensure) a local Raft group peer.
    ///
    /// **In-process:** when every `members` node has created the group, membership
    /// is initialized once (racing callers see `NotAllowed` and ignore).
    ///
    /// **gRPC:** each process spawns the local raft then tries `initialize`;
    /// `NotAllowed` is ignored (no cross-process ClusterGlue).
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

        match &self.net {
            NetBackend::InProcess { glue, .. } => {
                let all_ready = glue.mark_ready(group, self.node_id, members);
                if all_ready && glue.try_claim_init(group) {
                    self.try_initialize(group, members).await?;
                }
            }
            NetBackend::Grpc { .. } => {
                // Cross-process: every node attempts initialize; loser gets NotAllowed.
                self.try_initialize(group, members).await?;
            }
        }

        Ok(())
    }

    async fn try_initialize(
        &self,
        group: GroupId,
        members: &[NodeId],
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let nodes = self.membership_nodes(members);
        match raft.initialize(nodes).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(e) => Err(MultiRaftError::Other(anyhow::anyhow!(
                "initialize group: {e}"
            ))),
        }
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

    /// In-process shared [`Router`] (panics if this node was started with gRPC).
    pub fn router(&self) -> &Router {
        match &self.net {
            NetBackend::InProcess { router, .. } => router,
            NetBackend::Grpc { .. } => {
                panic!("router() is only available for in-process MultiRaft::start / start_cluster")
            }
        }
    }

    /// Distinct peer links: in-process router channels or gRPC peer channels.
    pub fn unique_peer_links(&self) -> usize {
        match &self.net {
            NetBackend::InProcess { router, .. } => router.unique_peer_links(),
            NetBackend::Grpc { router } => router.unique_peer_links(),
        }
    }

    /// Shut down all local Raft groups cleanly (flush / stop core tasks).
    ///
    /// For in-process mode, also unregisters this node from the shared [`Router`]
    /// so peers observe it as unreachable (used by demo admin leader-loss simulation).
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
        if let NetBackend::InProcess { router, .. } = &self.net {
            let _ = router.unregister_node(self.node_id);
        }
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

        let raft = match &self.net {
            NetBackend::InProcess { router, .. } => {
                let network = NetworkFactory::new(router.clone(), group);
                if self.config.data_dir.as_os_str().is_empty() {
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
                    let log_store = FileLogStoreOf::open(&dir).map_err(|e| {
                        MultiRaftError::Other(anyhow::anyhow!("open file log: {e}"))
                    })?;
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                }
            }
            NetBackend::Grpc { router } => {
                let network = GrpcNetworkFactory::new(router.clone(), group);
                if self.config.data_dir.as_os_str().is_empty() {
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
                    let log_store = FileLogStoreOf::open(&dir).map_err(|e| {
                        MultiRaftError::Other(anyhow::anyhow!("open file log: {e}"))
                    })?;
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                }
            }
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
