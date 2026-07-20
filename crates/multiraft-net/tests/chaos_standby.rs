//! In-process chaos for Standby (Learner offload / recover / promote).
//!
//! Maps to `docs/chaos-checklist.md` (C40–C43).
//!
//! Run: `cargo test -p multiraft-net --test chaos_standby`

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::routing::get;
use multiraft_core::ClusterConfig;
use multiraft_core::NodeRole;
use multiraft_core::RecoverOutcome;
use multiraft_core::SnapshotAdvertisement;
use multiraft_core::SnapshotMode;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::SharedFabric;
use multiraft_net::wait_for_leader;
fn temp_dir(tag: &str, id: u64) -> std::path::PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "multiraft-chaos-sb-{}-{}-{}-{}",
        tag,
        std::process::id(),
        stamp,
        id
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cfg(
    node_id: u64,
    peer_ids: &[u64],
    data_dir: std::path::PathBuf,
    role: NodeRole,
) -> ClusterConfig {
    let mut c = ClusterConfig::for_test(node_id, peer_ids);
    c.data_dir = data_dir;
    c.role = role;
    c.snapshot_mode = SnapshotMode::StandbyOffload;
    c.snapshot_keep = 2;
    if role == NodeRole::Standby {
        c.enable_stale_queries = true;
    }
    c
}

async fn propose_on_leader(nodes: &[MultiRaft], group: u64, data: Vec<u8>) {
    propose_on_leader_skipping(nodes, group, data, &[]).await;
}

async fn propose_on_leader_skipping(
    nodes: &[MultiRaft],
    group: u64,
    data: Vec<u8>,
    skip: &[u64],
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        for n in nodes {
            if skip.contains(&n.node_id()) {
                continue;
            }
            if n.is_leader(group) {
                match n.propose(group, data.clone()).await {
                    Ok(_) => return,
                    Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {}
                    Err(e) => panic!("propose failed: {e:?}"),
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out proposing");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_leader_skipping(
    nodes: &[MultiRaft],
    group: u64,
    skip: &[u64],
    timeout: Duration,
) -> u64 {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for n in nodes {
            if skip.contains(&n.node_id()) {
                continue;
            }
            if n.is_leader(group) {
                if let Some(lid) = n.leader(group) {
                    if !skip.contains(&lid) {
                        return lid;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader among survivors (skip={skip:?})");
}

async fn max_value_skipping(nodes: &[MultiRaft], group: u64, skip: &[u64]) -> i64 {
    let mut best = 0i64;
    for n in nodes {
        if skip.contains(&n.node_id()) {
            continue;
        }
        if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
            best = best.max(v);
        }
    }
    best
}

async fn wait_fsm_ge(n: &MultiRaft, group: u64, min: i64, timeout: Duration) -> i64 {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let v = n.with_fsm(group, |fsm| fsm.value(group)).await.unwrap_or(0);
        if v >= min {
            return v;
        }
        if std::time::Instant::now() >= deadline {
            panic!("node {}: fsm {v} < {min}", n.node_id());
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

async fn max_voter_value(voters: &[MultiRaft], group: u64) -> i64 {
    let mut best = 0i64;
    for n in voters {
        if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
            best = best.max(v);
        }
    }
    best
}

struct SnapServe {
    data: Vec<u8>,
    last_index: u64,
    last_term: u64,
    snapshot_id: String,
    sha256_hex: String,
}

async fn serve_snap(State(s): State<Arc<SnapServe>>) -> (HeaderMap, Vec<u8>) {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-snapshot-index",
        HeaderValue::from_str(&s.last_index.to_string()).unwrap(),
    );
    headers.insert(
        "x-snapshot-term",
        HeaderValue::from_str(&s.last_term.to_string()).unwrap(),
    );
    headers.insert(
        "x-snapshot-id",
        HeaderValue::from_str(&s.snapshot_id).unwrap(),
    );
    headers.insert(
        "x-snapshot-sha256",
        HeaderValue::from_str(&s.sha256_hex).unwrap(),
    );
    (headers, s.data.clone())
}

async fn spawn_snap_server(snap: SnapServe) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/snap", get(serve_snap))
        .with_state(Arc::new(snap));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (addr, handle)
}

/// C40: Kill Standby under load — voters keep writing; restart Standby catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_standby_voters_keep_writing() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();
    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("c40", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(cfg(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .unwrap(),
        );
    }
    let standby = fabric
        .start_node(cfg(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .unwrap();

    for n in &voters {
        n.create_group(group, &members).await.unwrap();
    }
    standby.create_group(group, &members).await.unwrap();

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .unwrap();
    voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .unwrap()
        .add_standby(group, standby_id)
        .await
        .unwrap();

    for i in 0..5u64 {
        propose_on_leader(&voters, group, CounterFsm::encode_add(1, 10 + i)).await;
    }
    wait_fsm_ge(&standby, group, 5, Duration::from_secs(10)).await;

    standby.shutdown().await.unwrap();

    let before = max_voter_value(&voters, group).await;
    for i in 0..5u64 {
        propose_on_leader(&voters, group, CounterFsm::encode_add(1, 100 + i)).await;
    }
    let after = max_voter_value(&voters, group).await;
    assert!(after >= before + 5, "after={after} before={before}");

    let standby2 = fabric
        .start_node(cfg(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .unwrap();
    standby2.create_group(group, &members).await.unwrap();
    // Re-join as learner (membership may still list id 4).
    let lid = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .unwrap();
    let _ = voters
        .iter()
        .find(|n| n.node_id() == lid)
        .unwrap()
        .add_standby(group, standby_id)
        .await;

    wait_fsm_ge(&standby2, group, after, Duration::from_secs(15)).await;
}

/// C41: Kill leader while Standby is present — survivors writable; Standby catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_leader_with_standby_present() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();
    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("c41", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(cfg(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .unwrap(),
        );
    }
    let standby = fabric
        .start_node(cfg(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .unwrap();

    for n in &voters {
        n.create_group(group, &members).await.unwrap();
    }
    standby.create_group(group, &members).await.unwrap();

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .unwrap();
    voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .unwrap()
        .add_standby(group, standby_id)
        .await
        .unwrap();

    propose_on_leader(&voters, group, CounterFsm::encode_add(3, 1)).await;
    wait_fsm_ge(&standby, group, 3, Duration::from_secs(10)).await;
    let before = max_voter_value(&voters, group).await;

    let leader = voters.iter().find(|n| n.node_id() == leader_id).unwrap();
    leader.shutdown().await.unwrap();

    let dead = [leader_id];
    let new_leader =
        wait_for_leader_skipping(&voters, group, &dead, Duration::from_secs(15)).await;
    assert_ne!(new_leader, leader_id);

    propose_on_leader_skipping(&voters, group, CounterFsm::encode_add(2, 2), &dead).await;
    let after = max_value_skipping(&voters, group, &dead).await;
    assert!(after >= before + 2);

    wait_fsm_ge(&standby, group, after, Duration::from_secs(15)).await;
    let stale = standby
        .read_stale(group, |fsm| fsm.value(group))
        .await
        .expect("stale offload");
    assert_eq!(stale.value, after);
    assert!(stale.applied_index > 0);
}

/// C42: Wipe a voter and recover from Standby snapshot ad under continued writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn voter_recover_from_standby_under_load() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();
    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("c42", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(cfg(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .unwrap(),
        );
    }
    let standby = fabric
        .start_node(cfg(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .unwrap();

    for n in &voters {
        n.create_group(group, &members).await.unwrap();
    }
    standby.create_group(group, &members).await.unwrap();

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .unwrap();
    let leader = voters.iter().find(|n| n.node_id() == leader_id).unwrap();
    leader.add_standby(group, standby_id).await.unwrap();

    let mut expected = 0i64;
    for (i, d) in [2i64, 3, 5].into_iter().enumerate() {
        expected += d;
        propose_on_leader(&voters, group, CounterFsm::encode_add(d, 50 + i as u64)).await;
    }
    wait_fsm_ge(&standby, group, expected, Duration::from_secs(10)).await;

    leader.trigger_standby_snapshot(group).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let entry = loop {
        if let Some(e) = standby.latest_catalog_entry(group) {
            break e;
        }
        if std::time::Instant::now() >= deadline {
            panic!("no catalog entry");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let data = std::fs::read(entry.dir.join("data.bin")).unwrap();
    let (addr, _h) = spawn_snap_server(SnapServe {
        data: data.clone(),
        last_index: entry.last_index,
        last_term: entry.last_term,
        snapshot_id: entry.snapshot_id.clone(),
        sha256_hex: entry.sha256_hex.clone(),
    })
    .await;
    let fetch_url = format!("http://{addr}/snap");
    let ad = SnapshotAdvertisement {
        group,
        last_index: entry.last_index,
        last_term: entry.last_term,
        snapshot_id: entry.snapshot_id.clone(),
        size: data.len() as u64,
        sha256_hex: entry.sha256_hex.clone(),
        fetch_url,
    };

    // Kill voter 3 (prefer non-leader).
    let victim_id = voter_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id)
        .unwrap();
    let victim_idx = voters.iter().position(|n| n.node_id() == victim_id).unwrap();
    voters[victim_idx].shutdown().await.unwrap();

    // Continue writes on survivors.
    let dead = [victim_id];
    propose_on_leader_skipping(
        &voters,
        group,
        CounterFsm::encode_add(1, 999),
        &dead,
    )
    .await;
    expected += 1;

    // Wipe victim disk and restart; recover from ad.
    let _ = std::fs::remove_dir_all(&dirs[victim_idx]);
    std::fs::create_dir_all(&dirs[victim_idx]).unwrap();
    voters[victim_idx] = fabric
        .start_node(cfg(
            victim_id,
            &peer_ids,
            dirs[victim_idx].clone(),
            NodeRole::Voter,
        ))
        .await
        .unwrap();
    voters[victim_idx]
        .create_group(group, &members)
        .await
        .unwrap();
    voters[victim_idx].record_snapshot_ad(ad);

    match voters[victim_idx]
        .try_recover_from_standby_ads(group)
        .await
        .unwrap()
    {
        RecoverOutcome::Installed { .. } | RecoverOutcome::SkippedNotNewer { .. } => {}
        other => panic!("unexpected recover: {other:?}"),
    }

    let restored = voters[victim_idx]
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .unwrap_or(0);
    assert!(
        restored >= expected - 1,
        "restored={restored} expected~={expected}"
    );

    // Catch up remaining log.
    wait_fsm_ge(&voters[victim_idx], group, expected, Duration::from_secs(20)).await;
}

/// C43: Promote Standby under load, then kill an old voter — quorum still writable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_standby_then_kill_old_voter() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();
    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("c43", id)).collect();
    let fabric = SharedFabric::new();

    let mut nodes = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        nodes.push(
            fabric
                .start_node(cfg(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .unwrap(),
        );
    }
    nodes.push(
        fabric
            .start_node(cfg(
                standby_id,
                &peer_ids,
                dirs[3].clone(),
                NodeRole::Standby,
            ))
            .await
            .unwrap(),
    );

    for n in &nodes {
        n.create_group(group, &members).await.unwrap();
    }

    let leader_id = wait_for_leader(&nodes[..3], group, Duration::from_secs(10))
        .await
        .unwrap();
    nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .unwrap()
        .add_standby(group, standby_id)
        .await
        .unwrap();

    for i in 0..4u64 {
        propose_on_leader(&nodes[..3], group, CounterFsm::encode_add(1, 200 + i)).await;
    }
    wait_fsm_ge(&nodes[3], group, 4, Duration::from_secs(10)).await;

    nodes
        .iter()
        .find(|n| n.is_leader(group))
        .unwrap()
        .promote_standby(group, standby_id)
        .await
        .unwrap();

    // 4-voter quorum: kill one original voter (not the only leader if possible).
    let kill_id = voter_ids
        .iter()
        .copied()
        .find(|&id| {
            !nodes
                .iter()
                .find(|n| n.node_id() == id)
                .map(|n| n.is_leader(group))
                .unwrap_or(false)
        })
        .unwrap_or(voter_ids[0]);
    let before = max_voter_value(&nodes, group).await;
    nodes
        .iter()
        .find(|n| n.node_id() == kill_id)
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let dead = [kill_id];
    wait_for_leader_skipping(&nodes, group, &dead, Duration::from_secs(15)).await;
    propose_on_leader_skipping(&nodes, group, CounterFsm::encode_add(7, 777), &dead).await;
    let after = max_value_skipping(&nodes, group, &dead).await;
    assert!(after >= before + 7);
}
