//! P2 Aeron Standby parity: multi-standby ads, daisy-chain snapshot sync, Range fetch.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use multiraft_core::ClusterConfig;
use multiraft_core::NodeRole;
use multiraft_core::RecoverOutcome;
use multiraft_core::SnapshotAdvertisement;
use multiraft_core::SnapshotMode;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::SharedFabric;
use multiraft_net::pull_snapshot_chunked;
use multiraft_net::wait_for_leader;
use sha2::Digest;
use sha2::Sha256;

fn temp_dir(tag: &str, id: u64) -> std::path::PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "multiraft-p2-{}-{}-{}-{}",
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

fn hex_sha256(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

struct RangeSnapServe {
    data: Vec<u8>,
    last_index: u64,
    last_term: u64,
    snapshot_id: String,
    sha256_hex: String,
    /// Fail the first N successful range bodies (for resume test).
    fail_after_bytes: AtomicUsize,
    failed_once: AtomicBool,
}

fn snap_meta_headers(s: &RangeSnapServe) -> HeaderMap {
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
    headers.insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers
}

async fn serve_range_snap(
    State(s): State<Arc<RangeSnapServe>>,
    headers: HeaderMap,
) -> Response {
    let mut out = snap_meta_headers(&s);
    out.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&s.data.len().to_string()).unwrap(),
    );

    if let Some(range) = headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
    {
        let rest = range.strip_prefix("bytes=").unwrap_or("");
        let (start_s, end_s) = rest.split_once('-').unwrap_or(("", ""));
        let start: u64 = start_s.parse().unwrap_or(0);
        let end: u64 = if end_s.is_empty() {
            s.data.len() as u64 - 1
        } else {
            end_s.parse().unwrap_or(s.data.len() as u64 - 1)
        };
        let end = end.min(s.data.len() as u64 - 1);
        if start > end {
            return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
        }
        let body = s.data[start as usize..=end as usize].to_vec();
        let fail_after = s.fail_after_bytes.load(Ordering::SeqCst);
        let would_fail = fail_after > 0
            && !s.failed_once.load(Ordering::SeqCst)
            && (start as usize + body.len()) >= fail_after;
        if would_fail {
            s.failed_once.store(true, Ordering::SeqCst);
            // Simulate mid-download drop: 500 so client keeps partial temp and can resume.
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        let cr = format!("bytes {start}-{end}/{}", s.data.len());
        out.insert(
            axum::http::header::CONTENT_RANGE,
            HeaderValue::from_str(&cr).unwrap(),
        );
        out.insert(
            axum::http::header::CONTENT_LENGTH,
            HeaderValue::from_str(&body.len().to_string()).unwrap(),
        );
        return (StatusCode::PARTIAL_CONTENT, out, body).into_response();
    }

    (StatusCode::OK, out, s.data.clone()).into_response()
}

async fn spawn_range_server(snap: RangeSnapServe) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/snapshots/0/latest", get(serve_range_snap))
        .with_state(Arc::new(snap));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_standby_ads_pick_newest() {
    let peer_ids = [1u64, 2, 3, 4, 5];
    let voter_ids = [1u64, 2, 3];
    let standby_a = 4u64;
    let standby_b = 5u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("multi", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .expect("voter"),
        );
    }
    let sa = fabric
        .start_node(standby_config(
            standby_a,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .expect("standby a");
    let sb = fabric
        .start_node(standby_config(
            standby_b,
            &peer_ids,
            dirs[4].clone(),
            NodeRole::Standby,
        ))
        .await
        .expect("standby b");

    for n in &voters {
        n.create_group(group, &members).await.expect("create");
    }
    sa.create_group(group, &members).await.expect("sa create");
    sb.create_group(group, &members).await.expect("sb create");

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");
    leader.add_standby(group, standby_a).await.expect("add a");
    leader.add_standby(group, standby_b).await.expect("add b");

    for (i, delta) in [1i64, 2, 3].into_iter().enumerate() {
        propose_on_leader(&voters, group, CounterFsm::encode_add(delta, (i as u64) + 1)).await;
    }

    let old_data = b"old-snapshot-payload".to_vec();

    // Wait standby A catch-up, trigger snapshot → newer ad from A.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let v = sa.with_fsm(group, |fsm| fsm.value(group)).await.unwrap_or(0);
        if v >= 6 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby A lag");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    leader
        .trigger_standby_snapshot(group)
        .await
        .expect("trigger");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let entry_a = loop {
        if let Some(e) = sa.latest_catalog_entry(group) {
            break e;
        }
        if std::time::Instant::now() >= deadline {
            panic!("catalog A empty");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let data_a = sa
        .snapshot_catalog()
        .unwrap()
        .read(group, &entry_a.snapshot_id)
        .unwrap()
        .unwrap();

    let (addr_old, _h1) = spawn_range_server(RangeSnapServe {
        data: old_data.clone(),
        last_index: 1,
        last_term: 1,
        snapshot_id: "1-1".into(),
        sha256_hex: hex_sha256(&old_data),
        fail_after_bytes: AtomicUsize::new(0),
        failed_once: AtomicBool::new(false),
    })
    .await;
    let (addr_new, _h2) = spawn_range_server(RangeSnapServe {
        data: data_a.clone(),
        last_index: entry_a.last_index,
        last_term: entry_a.last_term,
        snapshot_id: entry_a.snapshot_id.clone(),
        sha256_hex: entry_a.sha256_hex.clone(),
        fail_after_bytes: AtomicUsize::new(0),
        failed_once: AtomicBool::new(false),
    })
    .await;

    // Both standbys advertise; B's ad is older, A's is newer.
    voters[0].record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: 1,
        last_term: 1,
        snapshot_id: "1-1".into(),
        size: old_data.len() as u64,
        sha256_hex: hex_sha256(&old_data),
        fetch_url: format!("http://{addr_old}/snapshots/0/latest"),
    });
    voters[0].record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: entry_a.last_index,
        last_term: entry_a.last_term,
        snapshot_id: entry_a.snapshot_id.clone(),
        size: entry_a.size,
        sha256_hex: entry_a.sha256_hex.clone(),
        fetch_url: format!("http://{addr_new}/snapshots/0/latest"),
    });

    let best = voters[0].best_snapshot_ad(group).expect("best");
    assert_eq!(best.last_index, entry_a.last_index);
    assert!(best.fetch_url.contains(&addr_new.to_string()));

    // Wipe voter 1 and recover — must pull the newer ad.
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
    restarted.record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: 1,
        last_term: 1,
        snapshot_id: "1-1".into(),
        size: old_data.len() as u64,
        sha256_hex: hex_sha256(&old_data),
        fetch_url: format!("http://{addr_old}/snapshots/0/latest"),
    });
    restarted.record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: entry_a.last_index,
        last_term: entry_a.last_term,
        snapshot_id: entry_a.snapshot_id,
        size: entry_a.size,
        sha256_hex: entry_a.sha256_hex,
        fetch_url: format!("http://{addr_new}/snapshots/0/latest"),
    });

    let outcome = restarted
        .try_recover_from_standby_ads(group)
        .await
        .expect("recover");
    match outcome {
        RecoverOutcome::Installed { last_index, .. } => {
            assert_eq!(last_index, entry_a.last_index);
        }
        RecoverOutcome::SkippedNotNewer { ad_index, .. } => {
            assert_eq!(ad_index, entry_a.last_index);
        }
        other => panic!("unexpected {other:?}"),
    }
    let _ = (sa, sb);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daisy_chain_snapshot_from_upstream() {
    let peer_ids = [1u64, 2, 3, 4, 5];
    let voter_ids = [1u64, 2, 3];
    let standby_a = 4u64;
    let standby_b = 5u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("daisy", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter))
                .await
                .expect("voter"),
        );
    }
    let sa = fabric
        .start_node(standby_config(
            standby_a,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
        ))
        .await
        .expect("standby a");

    for n in &voters {
        n.create_group(group, &members).await.expect("create");
    }
    sa.create_group(group, &members).await.expect("sa create");

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");
    // Only A is add_learner'd.
    leader.add_standby(group, standby_a).await.expect("add a");

    for (i, delta) in [1i64, 2, 3, 4].into_iter().enumerate() {
        propose_on_leader(
            &voters,
            group,
            CounterFsm::encode_add(delta, (i as u64) + 1),
        )
        .await;
    }
    let expected: i64 = 10;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let v = sa.with_fsm(group, |fsm| fsm.value(group)).await.unwrap_or(0);
        if v == expected {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby A lag: {v}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    leader
        .trigger_standby_snapshot(group)
        .await
        .expect("trigger");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let entry_a = loop {
        if let Some(e) = sa.latest_catalog_entry(group) {
            break e;
        }
        if std::time::Instant::now() >= deadline {
            panic!("catalog A empty");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let data_a = sa
        .snapshot_catalog()
        .unwrap()
        .read(group, &entry_a.snapshot_id)
        .unwrap()
        .unwrap();

    let (addr_a, _ha) = spawn_range_server(RangeSnapServe {
        data: data_a.clone(),
        last_index: entry_a.last_index,
        last_term: entry_a.last_term,
        snapshot_id: entry_a.snapshot_id.clone(),
        sha256_hex: entry_a.sha256_hex.clone(),
        fail_after_bytes: AtomicUsize::new(0),
        failed_once: AtomicBool::new(false),
    })
    .await;

    // Standby B: snapshot-only daisy (not add_learner'd).
    let mut cfg_b = standby_config(standby_b, &peer_ids, dirs[4].clone(), NodeRole::Standby);
    cfg_b.daisy_upstream_base = Some(format!("http://{addr_a}"));
    cfg_b.snapshot_fetch_chunk_bytes = 32;

    let sb = fabric.start_node(cfg_b).await.expect("standby b");
    sb.create_group(group, &members).await.expect("sb create");

    let outcome = sb
        .sync_from_daisy_upstream(group)
        .await
        .expect("daisy sync");
    match outcome {
        RecoverOutcome::Installed { last_index, .. } => {
            assert_eq!(last_index, entry_a.last_index);
        }
        other => panic!("expected Installed, got {other:?}"),
    }

    let entry_b = sb.latest_catalog_entry(group).expect("B catalog");
    assert_eq!(entry_b.last_index, entry_a.last_index);
    assert_eq!(entry_b.sha256_hex, entry_a.sha256_hex);
    let data_b = sb
        .snapshot_catalog()
        .unwrap()
        .read(group, &entry_b.snapshot_id)
        .unwrap()
        .unwrap();
    assert_eq!(data_b, data_a);

    let (addr_b, _hb) = spawn_range_server(RangeSnapServe {
        data: data_b,
        last_index: entry_b.last_index,
        last_term: entry_b.last_term,
        snapshot_id: entry_b.snapshot_id.clone(),
        sha256_hex: entry_b.sha256_hex.clone(),
        fail_after_bytes: AtomicUsize::new(0),
        failed_once: AtomicBool::new(false),
    })
    .await;
    let fetch_b = format!("http://{addr_b}/snapshots/0/latest");

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
    restarted.record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: entry_b.last_index,
        last_term: entry_b.last_term,
        snapshot_id: entry_b.snapshot_id,
        size: entry_b.size,
        sha256_hex: entry_b.sha256_hex,
        fetch_url: fetch_b,
    });

    let outcome = restarted
        .try_recover_from_standby_ads(group)
        .await
        .expect("recover from B");
    match outcome {
        RecoverOutcome::Installed { last_index, .. } => {
            assert_eq!(last_index, entry_a.last_index);
        }
        RecoverOutcome::SkippedNotNewer { .. } => {
            // Already caught up via log; force pull from B.
            restarted
                .pull_and_install_snapshot(group, &format!("http://{addr_b}/snapshots/0/latest"))
                .await
                .expect("pull B");
        }
        other => panic!("unexpected {other:?}"),
    }
    let restored = restarted
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .expect("fsm");
    assert!(restored >= expected, "restored={restored}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunked_range_fetch_install() {
    let peer_ids = [1u64, 2, 3];
    let group = 0u64;
    let members = peer_ids.to_vec();
    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("chunk", id)).collect();
    let fabric = SharedFabric::new();

    let mut nodes = Vec::new();
    for (i, &id) in peer_ids.iter().enumerate() {
        let mut cfg = standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter);
        cfg.snapshot_fetch_chunk_bytes = 16;
        nodes.push(fabric.start_node(cfg).await.expect("start"));
    }
    for n in &nodes {
        n.create_group(group, &members).await.expect("create");
    }
    wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");

    for (i, delta) in [5i64, 7].into_iter().enumerate() {
        propose_on_leader(&nodes, group, CounterFsm::encode_add(delta, (i as u64) + 1)).await;
    }
    let expected = 12i64;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_ok = true;
        for n in &nodes {
            let v = n.with_fsm(group, |fsm| fsm.value(group)).await.unwrap_or(0);
            if v != expected {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("nodes not at expected={expected}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let snap_bytes = nodes[0]
        .with_fsm(group, |fsm| {
            use multiraft_fsm::StateMachine;
            fsm.snapshot(group)
        })
        .await
        .expect("snap")
        .expect("snapshot ok");
    let sha = hex_sha256(&snap_bytes);
    let last_index = 10u64;
    let last_term = 1u64;

    let (addr, _h) = spawn_range_server(RangeSnapServe {
        data: snap_bytes.clone(),
        last_index,
        last_term,
        snapshot_id: format!("{last_index}-{last_term}"),
        sha256_hex: sha.clone(),
        fail_after_bytes: AtomicUsize::new(0),
        failed_once: AtomicBool::new(false),
    })
    .await;
    let url = format!("http://{addr}/snapshots/0/latest");

    // Direct chunked helper.
    let tmp = temp_dir("chunk-tmp", 0);
    let fetched = pull_snapshot_chunked(&url, 16, &tmp).await.expect("chunked pull");
    assert_eq!(fetched.data, snap_bytes);
    assert_eq!(fetched.sha256_hex, sha);

    // Install via MultiRaft API.
    nodes[1]
        .pull_and_install_snapshot(group, &url)
        .await
        .expect("install");
    let v = nodes[1]
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .unwrap();
    assert_eq!(v, expected);

    // Resume: pad payload so mid-fail is reachable with small chunks.
    let mut padded = snap_bytes.clone();
    padded.extend(std::iter::repeat(b'X').take(200));
    let sha_pad = hex_sha256(&padded);
    let (addr2, _h2) = spawn_range_server(RangeSnapServe {
        data: padded.clone(),
        last_index,
        last_term,
        snapshot_id: format!("{last_index}-{last_term}"),
        sha256_hex: sha_pad.clone(),
        fail_after_bytes: AtomicUsize::new(40),
        failed_once: AtomicBool::new(false),
    })
    .await;
    let url2 = format!("http://{addr2}/snapshots/0/latest");
    let tmp2 = temp_dir("chunk-resume", 0);
    let first = pull_snapshot_chunked(&url2, 16, &tmp2).await;
    assert!(first.is_err(), "first attempt should fail for resume");
    let partials: Vec<_> = std::fs::read_dir(&tmp2)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .contains("multiraft-snap-")
        })
        .collect();
    assert!(!partials.is_empty(), "expected partial temp after mid-fail");
    let fetched2 = pull_snapshot_chunked(&url2, 16, &tmp2)
        .await
        .expect("resume pull");
    assert_eq!(fetched2.data, padded);
    assert_eq!(fetched2.sha256_hex, sha_pad);
}
