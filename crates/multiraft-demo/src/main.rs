//! Phase-1 MultiRaft demo: 3 logical nodes × N groups in **one process**.
//!
//! True multi-process clustering needs cross-process transport (Task 8 / tonic).
//! Until then, `--mode cluster` (default) uses [`MultiRaft::start_cluster`] with a
//! shared in-process [`Router`]. `--mode node` exits with a clear error.

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
    /// Single OS process, 3 logical nodes via `start_cluster` (phase-1 default).
    #[default]
    Cluster,
    /// Reserved for multi-process; not supported until Task 8 transport.
    Node,
}

#[derive(Debug, Parser)]
#[command(
    name = "multiraft-demo",
    about = "multiraft demo: 3-node × N-group CounterFsm cluster"
)]
struct Args {
    /// `cluster` = one process with N logical nodes; `node` = multi-process (unsupported).
    #[arg(long, value_enum, default_value_t = Mode::Cluster)]
    mode: Mode,

    /// Local node id (required for `--mode node`; ignored in cluster mode).
    #[arg(long)]
    node_id: Option<u64>,

    /// Base port: admin HTTP binds here; peer addrs use base+id-1.
    #[arg(long, default_value_t = 21000)]
    base_port: u16,

    /// Number of Raft groups.
    #[arg(long, default_value_t = 10)]
    groups: u64,

    /// Root data directory; each logical node uses `{data-dir}/node-{id}/`.
    #[arg(long, default_value = ".demo-data")]
    data_dir: PathBuf,

    /// Logical node count for `--mode cluster`.
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
        Mode::Node => {
            anyhow::bail!(
                "multi-process `--mode node` is not supported yet: \
                 phase-1 networking is in-process only (shared Router). \
                 Use `--mode cluster` (default), or wait for Task 8 / tonic transport."
            );
        }
    }
}

async fn run_cluster(args: Args) -> anyhow::Result<()> {
    if args.nodes < 1 {
        anyhow::bail!("--nodes must be >= 1");
    }
    if args.groups < 1 {
        anyhow::bail!("--groups must be >= 1");
    }

    let peer_ids: Vec<u64> = (1..=args.nodes).collect();
    let group_ids: Vec<u64> = (0..args.groups).collect();

    std::fs::create_dir_all(&args.data_dir)?;

    let configs: Vec<ClusterConfig> = peer_ids
        .iter()
        .map(|&id| {
            let peers: Vec<(u64, SocketAddr)> = peer_ids
                .iter()
                .map(|&pid| {
                    let port = args.base_port.saturating_add((pid as u16).saturating_sub(1));
                    let addr: SocketAddr = format!("127.0.0.1:{port}")
                        .parse()
                        .expect("peer addr");
                    (pid, addr)
                })
                .collect();
            ClusterConfig {
                node_id: id,
                peers,
                data_dir: args.data_dir.join(format!("node-{id}")),
                heartbeat_interval_ms: 100,
                election_timeout_min_ms: 300,
                election_timeout_max_ms: 600,
            }
        })
        .collect();

    info!(
        nodes = args.nodes,
        groups = args.groups,
        base_port = args.base_port,
        data_dir = %args.data_dir.display(),
        "starting single-process MultiRaft cluster (phase-1 in-process Router)"
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
    let http_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(e) = serve_admin(admin_addr, http_state).await {
            warn!(error = %e, "admin HTTP exited");
        }
    });
    info!(%admin_addr, "admin HTTP listening (GET /groups/{{id}}/value, GET /metrics/links)");

    let propose_state = Arc::clone(&state);
    tokio::spawn(async move {
        propose_loop(propose_state).await;
    });

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
        .map(|n| n.router().unique_peer_links())
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
        .map(|n| n.router().unique_peer_links())
        .unwrap_or(0);
    axum::Json(LinksResp { unique_peer_links })
}
