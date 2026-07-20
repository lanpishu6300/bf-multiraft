//! P3 Standby service offload: `read_stale` with applied watermark.

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
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
        "multiraft-p3-{}-{}-{}-{}",
        tag,
        std::process::id(),
        stamp,
        id
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn node_config(
    node_id: u64,
    peer_ids: &[u64],
    data_dir: std::path::PathBuf,
    role: NodeRole,
    enable_stale_queries: bool,
) -> ClusterConfig {
    let mut cfg = ClusterConfig::for_test(node_id, peer_ids);
    cfg.data_dir = data_dir;
    cfg.role = role;
    cfg.snapshot_mode = SnapshotMode::StandbyOffload;
    cfg.snapshot_keep = 2;
    cfg.enable_stale_queries = enable_stale_queries;
    cfg
}

async fn propose_on_leader(nodes: &[MultiRaft], group: u64, data: Vec<u8>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        for n in nodes {
            if n.is_leader(group) {
                match n.propose(group, data.clone()).await {
                    Ok(_) => return,
                    Err(MultiRaftError::NotLeader { .. }) => {}
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
async fn standby_read_stale_offload() {
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4u64;
    let group = 0u64;
    let members = voter_ids.to_vec();

    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("stale", id)).collect();
    let fabric = SharedFabric::new();

    let mut voters = Vec::new();
    for (i, &id) in voter_ids.iter().enumerate() {
        voters.push(
            fabric
                .start_node(node_config(
                    id,
                    &peer_ids,
                    dirs[i].clone(),
                    NodeRole::Voter,
                    false,
                ))
                .await
                .expect("start voter"),
        );
    }
    let standby = fabric
        .start_node(node_config(
            standby_id,
            &peer_ids,
            dirs[3].clone(),
            NodeRole::Standby,
            true,
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

    for i in 0..5u64 {
        propose_on_leader(&voters, group, CounterFsm::encode_add(1, 1000 + i)).await;
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let v = standby
            .with_fsm(group, |fsm| fsm.value(group))
            .await
            .unwrap_or(0);
        if v >= 5 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby did not catch up: {v}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let stale = standby
        .read_stale(group, |fsm| fsm.value(group))
        .await
        .expect("standby read_stale");
    assert_eq!(stale.value, 5);
    assert!(stale.applied_index > 0);
    assert!(standby.stale_queries_enabled());

    let lin = leader
        .read_linearizable(group, |fsm| fsm.value(group))
        .await
        .expect("linearizable");
    assert_eq!(lin, stale.value);

    let denied = voters[0]
        .read_stale(group, |fsm| fsm.value(group))
        .await
        .expect_err("voter must deny");
    assert!(matches!(denied, MultiRaftError::StaleQueriesDisabled));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_stale_queries_on_voter_for_analytics() {
    let peer_ids = [1u64, 2, 3];
    let group = 0u64;
    let members = peer_ids.to_vec();
    let dirs: Vec<_> = peer_ids.iter().map(|&id| temp_dir("analytics", id)).collect();
    let fabric = SharedFabric::new();

    let mut nodes = Vec::new();
    for (i, &id) in peer_ids.iter().enumerate() {
        let enable = id == 3;
        nodes.push(
            fabric
                .start_node(node_config(
                    id,
                    &peer_ids,
                    dirs[i].clone(),
                    NodeRole::Voter,
                    enable,
                ))
                .await
                .expect("start"),
        );
    }
    for n in &nodes {
        n.create_group(group, &members).await.expect("create_group");
    }
    wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");

    propose_on_leader(&nodes, group, CounterFsm::encode_add(3, 42)).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(r) = nodes[2].read_stale(group, |fsm| fsm.value(group)).await {
            if r.value >= 3 {
                assert!(r.applied_index > 0);
                return;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("analytics replica did not see write");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
