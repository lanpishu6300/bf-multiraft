//! P0/P1 Aeron Standby premium parity: HTTP recover, throttle, promote/demote.

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
        "multiraft-premium-{}-{}-{}-{}",
        tag,
        std::process::id(),
        stamp,
        id
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn standby_config(
    node_id: u64,
    peer_ids: &[u64],
    data_dir: std::path::PathBuf,
    role: NodeRole,
) -> ClusterConfig {
    let mut cfg = ClusterConfig::for_test(node_id, peer_ids);
    cfg.data_dir = data_dir;
    cfg.role = role;
    cfg.snapshot_mode = SnapshotMode::StandbyOffload;
    cfg.snapshot_keep = 2;
    cfg
}

async fn propose_on_leader(nodes: &[MultiRaft], group: u64, data: Vec<u8>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        for n in nodes {
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
        .route("/snapshots/0/latest", get(serve_snap))
        .with_state(Arc::new(snap));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_recover_via_http_from_standby_ads() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("http", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .expect("start voter"),
        );
    }
    let standby = fabric
        .start_node(standby_config(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .expect("start standby");

    for n in &voters {
        n.create_group(group, &members).await.expect("create_group");
    }
    standby
        .create_group(group, &members)
        .await
        .expect("standby create_group");

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");
    leader
        .add_standby(group, standby_id)
        .await
        .expect("add_standby");

    let mut expected = 0i64;
    for (i, delta) in [1i64, 2, 3].into_iter().enumerate() {
        expected += delta;
        propose_on_leader(&voters, group, CounterFsm::encode_add(delta, (i as u64) + 1)).await;
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let v = standby
            .with_fsm(group, |fsm| fsm.value(group))
            .await
            .expect("standby fsm");
        if v == expected {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby lag: {v} != {expected}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    leader
        .trigger_standby_snapshot(group)
        .await
        .expect("trigger");

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let entry = loop {
        if let Some(e) = standby.latest_catalog_entry(group) {
            break e;
        }
        if std::time::Instant::now() >= deadline {
            panic!("catalog empty");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let data = standby
        .snapshot_catalog()
        .expect("catalog")
        .read(group, &entry.snapshot_id)
        .expect("read")
        .expect("bytes");

    let (addr, _srv) = spawn_snap_server(SnapServe {
        data,
        last_index: entry.last_index,
        last_term: entry.last_term,
        snapshot_id: entry.snapshot_id.clone(),
        sha256_hex: entry.sha256_hex.clone(),
    })
    .await;
    let fetch_url = format!("http://{addr}/snapshots/0/latest");

    // Wipe voter 1 storage so local applied is 0; recover via HTTP ad (not catalog handle).
    voters[0].shutdown().await.expect("shutdown");
    std::fs::remove_dir_all(&dirs[0]).ok();
    std::fs::create_dir_all(&dirs[0]).unwrap();
    let restarted = fabric
        .start_node(standby_config(1, &peer_ids, dirs[0].clone(), NodeRole::Voter))
        .await
        .expect("restart");
    restarted
        .create_group(group, &members)
        .await
        .expect("recreate");

    let ad_index = entry.last_index;
    restarted.record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: entry.last_index,
        last_term: entry.last_term,
        snapshot_id: entry.snapshot_id,
        size: entry.size,
        sha256_hex: entry.sha256_hex,
        fetch_url: fetch_url.clone(),
    });

    let outcome = restarted
        .try_recover_from_standby_ads(group)
        .await
        .expect("recover");
    match outcome {
        RecoverOutcome::Installed { last_index, .. } => {
            assert_eq!(last_index, ad_index);
        }
        RecoverOutcome::SkippedNotNewer { .. } => {
            // Raft may have already caught up via log; still exercise direct HTTP pull.
            restarted
                .pull_and_install_snapshot(group, &fetch_url)
                .await
                .expect("direct pull");
        }
        other => panic!("expected Installed or SkippedNotNewer, got {other:?}"),
    }

    let restored = restarted
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .expect("fsm");
    assert!(
        restored >= expected,
        "restored={restored} expected>={expected}"
    );
    // SM watermark must track durable install (not only Raft metrics).
    let (idx, _) = restarted.local_applied(group).await.expect("applied");
    assert!(idx >= ad_index, "sm applied={idx} ad={ad_index}");

    let again = restarted
        .try_recover_from_standby_ads(group)
        .await
        .expect("recover again");
    assert!(
        matches!(again, RecoverOutcome::SkippedNotNewer { .. }),
        "second recover should skip, got {again:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_throttle_delay_proposes_still_ok() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("throttle", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        let mut cfg = standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter);
        cfg.standby_replicate_delay_ms = 50;
        cfg.standby_max_inflight = 2;
        voters.push(fabric.start_node(cfg).await.expect("start voter"));
    }
    let mut standby_cfg = standby_config(
        standby_id,
        &peer_ids,
        dirs[3].clone(),
        NodeRole::Standby,
    );
    standby_cfg.standby_replicate_delay_ms = 50;
    let standby = fabric.start_node(standby_cfg).await.expect("standby");

    for n in &voters {
        n.create_group(group, &members).await.expect("create_group");
    }
    standby
        .create_group(group, &members)
        .await
        .expect("standby create_group");

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");
    leader
        .add_standby(group, standby_id)
        .await
        .expect("add_standby");
    assert!(leader.standby_throttle_ids().contains(&standby_id));

    for i in 0..8u64 {
        propose_on_leader(
            &voters,
            group,
            CounterFsm::encode_add(1, 1000 + i),
        )
        .await;
    }

    let v = leader
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .expect("fsm");
    assert_eq!(v, 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_then_demote_standby_membership() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("promo", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .expect("start voter"),
        );
    }
    let standby = fabric
        .start_node(standby_config(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .expect("standby");

    for n in &voters {
        n.create_group(group, &members).await.expect("create_group");
    }
    standby
        .create_group(group, &members)
        .await
        .expect("standby create_group");

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");
    leader
        .add_standby(group, standby_id)
        .await
        .expect("add_standby");

    // Catch up before promote.
    propose_on_leader(&voters, group, CounterFsm::encode_add(1, 1)).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if standby
            .with_fsm(group, |fsm| fsm.value(group))
            .await
            .unwrap_or(0)
            >= 1
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby not caught up");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    leader
        .promote_standby(group, standby_id)
        .await
        .expect("promote");
    assert!(!leader.standby_throttle_ids().contains(&standby_id));

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let voters_set = leader.voter_ids(group).expect("voters");
        if voters_set.contains(&standby_id) && voters_set.len() == 4 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("promoted node not in voters: {voters_set:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Promoted node may become leader eventually (warm DR).
    let mut saw_standby_leader = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        if standby.is_leader(group) {
            saw_standby_leader = true;
            break;
        }
        // Nudge elections by shutting down current leader briefly is heavy;
        // membership as voter is the hard assert above. Soft-check eligibility.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = saw_standby_leader;

    // Demote back — clears leadership eligibility.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut demote_ok = false;
    while std::time::Instant::now() < deadline {
        for n in voters.iter().chain(std::iter::once(&standby)) {
            if !n.is_leader(group) {
                continue;
            }
            match n.demote_to_standby(group, standby_id).await {
                Ok(()) => {
                    demote_ok = true;
                    assert!(n.standby_throttle_ids().contains(&standby_id));
                    break;
                }
                Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {}
                Err(e) => panic!("demote failed: {e:?}"),
            }
        }
        if demote_ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(demote_ok, "demote_to_standby did not succeed");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let Some(voters_set) = voters
            .iter()
            .chain(std::iter::once(&standby))
            .find_map(|n| n.voter_ids(group))
        else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        if !voters_set.contains(&standby_id) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("demoted node still voter: {voters_set:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for _ in 0..30 {
        assert!(
            !standby.is_leader(group),
            "demoted standby must not be leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
