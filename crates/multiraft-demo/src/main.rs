//! MultiRaft demo: in-process cluster or multi-process gRPC nodes.
//!
//! - `--mode cluster` (default): one OS process, 3 logical nodes via
//!   [`MultiRaft::start_cluster`] (shared in-process Router).
//! - `--mode node`: one OS process = one Raft node via [`MultiRaft::start_grpc`].

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use axum::Router as AxumRouter;
use axum::extract::Path;
use axum::extract::State;
use axum::routing::get;
use axum::routing::post;
use clap::Parser;
use clap::ValueEnum;
use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;
use serde::Serialize;
use tracing::info;
use tracing::warn;

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum Mode {
    /// Single OS process, N logical nodes via `start_cluster`.
    #[default]
    Cluster,
    /// One OS process = one Raft node via `start_grpc`.
    Node,
}

#[derive(Debug, Parser)]
#[command(
    name = "multiraft-demo",
    about = "multiraft demo: 3-node × N-group CounterFsm cluster"
)]
struct Args {
    /// `cluster` = one process with N logical nodes; `node` = multi-process gRPC.
    #[arg(long, value_enum, default_value_t = Mode::Cluster)]
    mode: Mode,

    /// Local node id (required for `--mode node`; ignored in cluster mode).
    #[arg(long)]
    node_id: Option<u64>,

    /// Base port for Raft gRPC: node N binds `127.0.0.1:(base_port + N - 1)`.
    /// Admin HTTP for `--mode node` binds `127.0.0.1:(base_port + 100 + N - 1)`.
    /// In `--mode cluster`, admin binds at `base_port`.
    #[arg(long, default_value_t = 21000)]
    base_port: u16,

    /// Number of Raft groups.
    #[arg(long, default_value_t = 10)]
    groups: u64,

    /// Data directory.
    ///
    /// `--mode cluster`: each logical node uses `{data-dir}/node-{id}/`.
    /// `--mode node`: this process uses `{data-dir}/` directly (script passes
    /// `.../node-{id}`).
    #[arg(long, default_value = ".demo-data")]
    data_dir: PathBuf,

    /// Peer / logical node count (default 3).
    #[arg(long, default_value_t = 3)]
    nodes: u64,
}

struct DemoState {
    nodes: Vec<MultiRaft>,
    group_ids: Vec<u64>,
    idem: AtomicU64,
}

#[derive(Serialize)]
struct GroupValueResp {
    group: u64,
    value: i64,
    leader: Option<u64>,
}

#[derive(Serialize)]
struct LinksResp {
    unique_peer_links: usize,
}

#[derive(Serialize)]
struct ShutdownNodeResp {
    node_id: u64,
    ok: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    match args.mode {
        Mode::Cluster => run_cluster(args).await,
        Mode::Node => run_node(args).await,
    }
}

fn peer_addrs(base_port: u16, nodes: u64) -> Vec<(u64, SocketAddr)> {
    (1..=nodes)
        .map(|pid| {
            let port = base_port.saturating_add((pid as u16).saturating_sub(1));
            let addr: SocketAddr = format!("127.0.0.1:{port}")
                .parse()
                .expect("peer addr");
            (pid, addr)
        })
        .collect()
}

fn admin_addr_for_node(base_port: u16, node_id: u64) -> SocketAddr {
    let port = base_port
        .saturating_add(100)
        .saturating_add((node_id as u16).saturating_sub(1));
    ([127, 0, 0, 1], port).into()
}

async fn run_cluster(args: Args) -> anyhow::Result<()> {
    validate_counts(&args)?;

    let peer_ids: Vec<u64> = (1..=args.nodes).collect();
    let group_ids: Vec<u64> = (0..args.groups).collect();
    let peers = peer_addrs(args.base_port, args.nodes);

    std::fs::create_dir_all(&args.data_dir)?;

    let configs: Vec<ClusterConfig> = peer_ids
        .iter()
        .map(|&id| ClusterConfig {
            node_id: id,
            peers: peers.clone(),
            data_dir: args.data_dir.join(format!("node-{id}")),
            heartbeat_interval_ms: 100,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
        })
        .collect();

    info!(
        nodes = args.nodes,
        groups = args.groups,
        base_port = args.base_port,
        data_dir = %args.data_dir.display(),
        "starting single-process MultiRaft cluster (in-process Router)"
    );

    let nodes = MultiRaft::start_cluster(configs).await?;
    let members = peer_ids.clone();

    for &gid in &group_ids {
        for n in &nodes {
            n.create_group(gid, &members).await?;
        }
    }

    for &gid in &group_ids {
        let leader = wait_for_leader(&nodes, gid, Duration::from_secs(10))
            .await
            .ok_or_else(|| anyhow::anyhow!("no leader elected for group {gid}"))?;
        info!(group = gid, leader, "group ready");
    }

    for n in &nodes {
        let nid = n.node_id();
        n.on_leader_change(move |group, leader| {
            info!(node = nid, group, ?leader, "leader change");
        });
    }

    let state = Arc::new(DemoState {
        nodes,
        group_ids: group_ids.clone(),
        idem: AtomicU64::new(1),
    });

    let admin_addr: SocketAddr = ([127, 0, 0, 1], args.base_port).into();
    spawn_admin(admin_addr, Arc::clone(&state));
    info!(
        %admin_addr,
        "admin HTTP listening (GET /groups/{{id}}/value, GET /metrics/links, \
         POST /admin/shutdown_node/{{id}})"
    );

    tokio::spawn({
        let s = Arc::clone(&state);
        async move { propose_loop(s).await }
    });

    status_loop(state).await
}

async fn run_node(args: Args) -> anyhow::Result<()> {
    validate_counts(&args)?;
    let node_id = args
        .node_id
        .ok_or_else(|| anyhow::anyhow!("--node-id is required for --mode node"))?;
    if node_id < 1 || node_id > args.nodes {
        anyhow::bail!("--node-id must be in 1..={}", args.nodes);
    }

    let peers = peer_addrs(args.base_port, args.nodes);
    let group_ids: Vec<u64> = (0..args.groups).collect();
    let members: Vec<u64> = (1..=args.nodes).collect();

    // Script passes `{root}/node-{id}`; use that path directly.
    std::fs::create_dir_all(&args.data_dir)?;

    let config = ClusterConfig {
        node_id,
        peers,
        data_dir: args.data_dir.clone(),
        heartbeat_interval_ms: 100,
        election_timeout_min_ms: 300,
        election_timeout_max_ms: 600,
    };

    info!(
        node_id,
        nodes = args.nodes,
        groups = args.groups,
        base_port = args.base_port,
        data_dir = %args.data_dir.display(),
        "starting MultiRaft gRPC node"
    );

    let node = MultiRaft::start_grpc(config).await?;

    // Create groups with retries so peers that start later can join initialize.
    let mut last_err = None;
    for attempt in 1..=20 {
        last_err = None;
        for &gid in &group_ids {
            if let Err(e) = node.create_group(gid, &members).await {
                last_err = Some((gid, e));
                break;
            }
        }
        if last_err.is_none() {
            break;
        }
        warn!(
            attempt,
            group = last_err.as_ref().map(|(g, _)| *g),
            error = %last_err.as_ref().unwrap().1,
            "create_group retry"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if let Some((gid, e)) = last_err {
        return Err(anyhow::anyhow!("create_group {gid} failed after retries: {e}"));
    }

    wait_local_sees_leaders(&node, &group_ids, Duration::from_secs(30)).await?;

    let nid = node.node_id();
    node.on_leader_change(move |group, leader| {
        info!(node = nid, group, ?leader, "leader change");
    });

    let state = Arc::new(DemoState {
        nodes: vec![node],
        group_ids: group_ids.clone(),
        idem: AtomicU64::new(1),
    });

    let admin_addr = admin_addr_for_node(args.base_port, node_id);
    spawn_admin(admin_addr, Arc::clone(&state));
    info!(
        %admin_addr,
        node_id,
        "admin HTTP listening (GET /groups/{{id}}/value, GET /metrics/links)"
    );

    tokio::spawn({
        let s = Arc::clone(&state);
        async move { propose_loop(s).await }
    });

    status_loop(state).await
}

fn validate_counts(args: &Args) -> anyhow::Result<()> {
    if args.nodes < 1 {
        anyhow::bail!("--nodes must be >= 1");
    }
    if args.groups < 1 {
        anyhow::bail!("--groups must be >= 1");
    }
    Ok(())
}

/// Wait until this node observes a leader for every group (metrics), with retries.
async fn wait_local_sees_leaders(
    node: &MultiRaft,
    group_ids: &[u64],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    for &gid in group_ids {
        loop {
            if node.leader(gid).is_some() {
                info!(group = gid, leader = ?node.leader(gid), "group ready (local view)");
                break;
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timeout waiting for local node {} to see a leader for group {gid}",
                    node.node_id()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

fn spawn_admin(admin_addr: SocketAddr, state: Arc<DemoState>) {
    tokio::spawn(async move {
        if let Err(e) = serve_admin(admin_addr, state).await {
            warn!(error = %e, "admin HTTP exited");
        }
    });
}

async fn status_loop(state: Arc<DemoState>) -> anyhow::Result<()> {
    let mut last_status = Instant::now() - Duration::from_secs(2);
    loop {
        if last_status.elapsed() >= Duration::from_secs(2) {
            print_status(&state).await;
            last_status = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn propose_loop(state: Arc<DemoState>) {
    loop {
        for &gid in &state.group_ids {
            let mut proposed = false;
            for n in &state.nodes {
                if !n.is_leader(gid) {
                    continue;
                }
                let idem = state.idem.fetch_add(1, Ordering::Relaxed);
                let data = CounterFsm::encode_add(1, idem);
                match n.propose(gid, data).await {
                    Ok(ok) => {
                        tracing::debug!(
                            group = gid,
                            leader = n.node_id(),
                            index = ok.index,
                            term = ok.term,
                            idem,
                            "proposed"
                        );
                        proposed = true;
                    }
                    Err(MultiRaftError::NotLeader { .. }) => {}
                    Err(e) => {
                        warn!(group = gid, error = %e, "propose failed");
                    }
                }
                break;
            }
            if !proposed {
                tracing::debug!(group = gid, "no local leader yet");
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn print_status(state: &DemoState) {
    let links = state
        .nodes
        .first()
        .map(|n| n.unique_peer_links())
        .unwrap_or(0);

    for &gid in &state.group_ids {
        let leader = state.nodes.iter().find_map(|n| n.leader(gid));
        let mut value = 0i64;
        for n in &state.nodes {
            if let Some(v) = n.with_fsm(gid, |fsm| fsm.value(gid)).await {
                value = value.max(v);
            }
        }
        info!(
            group = gid,
            ?leader,
            value,
            unique_peer_links = links,
            "status"
        );
    }
}

async fn serve_admin(addr: SocketAddr, state: Arc<DemoState>) -> anyhow::Result<()> {
    let app = AxumRouter::new()
        .route("/groups/:id/value", get(group_value))
        .route("/metrics/links", get(metrics_links))
        .route("/admin/shutdown_node/:id", post(shutdown_node))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn group_value(
    State(state): State<Arc<DemoState>>,
    Path(id): Path<u64>,
) -> Result<axum::Json<GroupValueResp>, axum::http::StatusCode> {
    if !state.group_ids.contains(&id) {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }
    let leader = state.nodes.iter().find_map(|n| n.leader(id));
    let mut value = 0i64;
    for n in &state.nodes {
        if let Some(v) = n.with_fsm(id, |fsm| fsm.value(id)).await {
            value = value.max(v);
        }
    }
    Ok(axum::Json(GroupValueResp {
        group: id,
        value,
        leader,
    }))
}

async fn metrics_links(State(state): State<Arc<DemoState>>) -> axum::Json<LinksResp> {
    let unique_peer_links = state
        .nodes
        .first()
        .map(|n| n.unique_peer_links())
        .unwrap_or(0);
    axum::Json(LinksResp { unique_peer_links })
}

/// Shut down one local MultiRaft node (in-process leader-loss simulation).
async fn shutdown_node(
    State(state): State<Arc<DemoState>>,
    Path(id): Path<u64>,
) -> Result<axum::Json<ShutdownNodeResp>, axum::http::StatusCode> {
    let Some(node) = state.nodes.iter().find(|n| n.node_id() == id) else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };
    info!(node_id = id, "admin: shutting down MultiRaft node");
    node.shutdown()
        .await
        .map_err(|e| {
            warn!(node_id = id, error = %e, "admin shutdown_node failed");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(axum::Json(ShutdownNodeResp {
        node_id: id,
        ok: true,
    }))
}
