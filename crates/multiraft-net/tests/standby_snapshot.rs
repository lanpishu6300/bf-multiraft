//! Standby async snapshot (Aeron-aligned): Learner offload + voter recovery pull.

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::NodeRole;
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
        "multiraft-standby-{}-{}-{}-{}",
        tag,
        std::process::id(),
        stamp,
        id
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn standby_config(node_id: u64, peer_ids: &[u64], data_dir: std::path::PathBuf, role: NodeRole) -> ClusterConfig {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_async_snapshot_and_voter_recovery() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("main", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        let cfg = standby_config(id, &peer_ids, dirs[i].clone(), NodeRole::Voter);
        voters.push(fabric.start_node(cfg).await.expect("start voter"));
    }
    let standby_cfg = standby_config(
        standby_id,
        &peer_ids,
        dirs[3].clone(),
        NodeRole::Standby,
    );
    let standby = fabric
        .start_node(standby_cfg)
        .await
        .expect("start standby");

    for n in &voters {
        n.create_group(group, &members)
            .await
            .expect("voter create_group");
    }
    standby
        .create_group(group, &members)
        .await
        .expect("standby create_group");

    let leader_id = wait_for_leader(&voters, group, Duration::from_secs(10))
        .await
        .expect("leader elected");
    let leader = voters
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");

    leader
        .add_standby(group, standby_id)
        .await
        .expect("add_standby");

    // Standby must never become leader.
    for _ in 0..20 {
        assert!(
            !standby.is_leader(group),
            "standby must not be leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut expected = 0i64;
    for (i, delta) in [1i64, 2, 3, 4, 5].into_iter().enumerate() {
        expected += delta;
        let data = CounterFsm::encode_add(delta, (i as u64) + 1);
        propose_on_leader(&voters, group, data).await;
    }

    // Wait for standby to catch up before trigger.
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
            panic!("standby did not catch up: got {v}, want {expected}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    standby.set_snapshot_serialize_delay(Some(Duration::from_millis(200)));

    leader
        .trigger_standby_snapshot(group)
        .await
        .expect("trigger_standby_snapshot");

    // During serialize delay, voters continue to propose successfully.
    let continue_data = CounterFsm::encode_add(10, 100);
    expected += 10;
    propose_on_leader(&voters, group, continue_data).await;

    // Poll until standby catalog has an entry.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let entry = loop {
        if let Some(e) = standby.latest_catalog_entry(group) {
            break e;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby catalog empty after trigger");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(entry.last_index > 0);
    assert!(!standby.snapshot_ads().is_empty() || entry.size > 0);

    // Kill voter 1 and restart; pull snapshot from standby catalog.
    let voter1_dir = dirs[0].clone();
    voters[0].shutdown().await.expect("shutdown voter1");

    let restarted = fabric
        .start_node(standby_config(
            1,
            &peer_ids,
            voter1_dir,
            NodeRole::Voter,
        ))
        .await
        .expect("restart voter1");
    restarted
        .create_group(group, &members)
        .await
        .expect("recreate group on voter1");

    let catalog = standby
        .snapshot_catalog()
        .expect("standby catalog");
    restarted
        .try_install_from_standby_catalog(group, catalog.as_ref())
        .await
        .expect("install from standby catalog");

    let restored = restarted
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .expect("fsm after install");
    // Snapshot was taken at trigger time (before +10) or after — either is valid
    // as long as restore succeeded and value is from a consistent freeze.
    assert!(
        restored >= expected - 10 && restored <= expected,
        "restored={restored} expected_range=[{}..{expected}]",
        expected - 10
    );

    assert!(!standby.is_leader(group));
}
