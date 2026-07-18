//! In-process chaos: leader/follower loss while MultiRaft stays writable.
//!
//! Maps to `docs/chaos-checklist.md` (C01–C31).
//!
//! Run with: `cargo test -p multiraft-net --test chaos_failover`

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::SharedFabric;
use multiraft_net::wait_for_leader;

fn start_configs(peer_ids: &[u64]) -> Vec<ClusterConfig> {
    peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, peer_ids))
        .collect()
}

fn configs_with_temp_dirs(peer_ids: &[u64], root: &std::path::Path) -> Vec<ClusterConfig> {
    peer_ids
        .iter()
        .map(|&id| {
            let mut cfg = ClusterConfig::for_test(id, peer_ids);
            cfg.data_dir = root.join(format!("node-{id}"));
            std::fs::create_dir_all(&cfg.data_dir).expect("mkdir data_dir");
            cfg
        })
        .collect()
}

async fn start_on_fabric(
    fabric: &SharedFabric,
    configs: &[ClusterConfig],
) -> Vec<MultiRaft> {
    let mut nodes = Vec::with_capacity(configs.len());
    for config in configs {
        nodes.push(
            fabric
                .start_node(config.clone())
                .await
                .unwrap_or_else(|e| panic!("start_node {}: {e:#}", config.node_id)),
        );
    }
    nodes
}

async fn create_groups(nodes: &[MultiRaft], groups: &[u64], members: &[u64]) {
    for &g in groups {
        for n in nodes {
            n.create_group(g, members)
                .await
                .unwrap_or_else(|e| panic!("create_group {g} on {}: {e:?}", n.node_id()));
        }
    }
}

async fn propose_on_leader(nodes: &[MultiRaft], group: u64, data: Vec<u8>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        for n in nodes {
            if n.is_leader(group) {
                match n.propose(group, data.clone()).await {
                    Ok(_) => return,
                    Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {}
                    Err(multiraft_core::MultiRaftError::UnknownGroup(_)) => {}
                    Err(e) => panic!("propose failed: {e:?}"),
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out proposing on group {group}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Returns true if any leader accepted a propose within `timeout`.
async fn propose_succeeds_within(
    nodes: &[MultiRaft],
    group: u64,
    data: Vec<u8>,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for n in nodes {
            if !n.is_leader(group) {
                continue;
            }
            match tokio::time::timeout(Duration::from_millis(500), n.propose(group, data.clone()))
                .await
            {
                Ok(Ok(_)) => return true,
                Ok(Err(_)) | Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn max_fsm_value(nodes: &[MultiRaft], group: u64) -> i64 {
    let mut best = 0i64;
    for n in nodes {
        if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
            best = best.max(v);
        }
    }
    best
}

async fn wait_fsm_at_least(nodes: &[MultiRaft], group: u64, min: i64, timeout: Duration) -> i64 {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let v = max_fsm_value(nodes, group).await;
        if v >= min {
            return v;
        }
        if std::time::Instant::now() >= deadline {
            panic!("group {group}: FSM value {v} < {min} within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn alive_nodes<'a>(nodes: &'a [MultiRaft], dead: &[u64]) -> Vec<&'a MultiRaft> {
    nodes
        .iter()
        .filter(|n| !dead.contains(&n.node_id()))
        .collect()
}

async fn wait_for_leader_among(
    nodes: &[MultiRaft],
    group: u64,
    dead: &[u64],
    timeout: Duration,
) -> u64 {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for n in alive_nodes(nodes, dead) {
            if n.is_leader(group) {
                if let Some(lid) = n.leader(group) {
                    if !dead.contains(&lid) {
                        return lid;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader for group {group} among survivors (dead={dead:?})");
}

async fn restart_node(
    fabric: &SharedFabric,
    nodes: &mut [MultiRaft],
    configs: &[ClusterConfig],
    node_id: u64,
    groups: &[u64],
    members: &[u64],
) {
    let idx = nodes
        .iter()
        .position(|n| n.node_id() == node_id)
        .unwrap_or_else(|| panic!("node {node_id} missing"));
    nodes[idx].shutdown().await.expect("shutdown before restart");
    nodes[idx] = fabric
        .start_node(configs[idx].clone())
        .await
        .expect("start_node restart");
    for &g in groups {
        nodes[idx]
            .create_group(g, members)
            .await
            .unwrap_or_else(|e| panic!("create_group {g} after restart: {e:?}"));
    }
    for &g in groups {
        let _ = nodes[idx]
            .wait_for_recovery(g, Duration::from_secs(15))
            .await;
    }
}

// --- C01–C04 (existing) -------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_follower_cluster_stays_available() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");

    let follower_id = peer_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id)
        .expect("follower");
    let follower = nodes
        .iter()
        .find(|n| n.node_id() == follower_id)
        .expect("follower handle");
    follower.shutdown().await.expect("shutdown follower");

    let before = max_fsm_value(&nodes, group).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(5, 1)).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(7, 2)).await;
    let after = wait_fsm_at_least(&nodes, group, before + 12, Duration::from_secs(10)).await;
    assert!(after >= before + 12, "value must grow after follower kill");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_then_kill_follower_still_writes() {
    // 5 nodes so after leader + one follower kill, majority (3/5) remains.
    let peer_ids = [1u64, 2, 3, 4, 5];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let l1 = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("L1");
    nodes
        .iter()
        .find(|n| n.node_id() == l1)
        .expect("L1 handle")
        .shutdown()
        .await
        .expect("shutdown L1");

    let mut dead = vec![l1];
    let l2 = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(20)).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(3, 1)).await;
    let mid = wait_fsm_at_least(&nodes, group, 3, Duration::from_secs(15)).await;

    let follower = peer_ids
        .iter()
        .copied()
        .find(|&id| id != l2 && !dead.contains(&id))
        .expect("follower to kill");
    nodes
        .iter()
        .find(|n| n.node_id() == follower)
        .expect("follower handle")
        .shutdown()
        .await
        .expect("shutdown follower");
    dead.push(follower);

    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(20)).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(11, 2)).await;
    let after = wait_fsm_at_least(&nodes, group, mid + 11, Duration::from_secs(15)).await;
    assert!(after >= mid + 11);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_propose_during_leader_shutdown() {
    let peer_ids = [1u64, 2, 3];
    let nodes = Arc::new(
        MultiRaft::start_cluster(start_configs(&peer_ids))
            .await
            .expect("start_cluster"),
    );
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let l1 = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");

    let ok_count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..20u64 {
        let nodes = Arc::clone(&nodes);
        let ok_count = Arc::clone(&ok_count);
        handles.push(tokio::spawn(async move {
            let data = CounterFsm::encode_add(1, i + 1);
            let deadline = std::time::Instant::now() + Duration::from_secs(25);
            while std::time::Instant::now() < deadline {
                for n in nodes.iter() {
                    if !n.is_leader(group) {
                        continue;
                    }
                    match n.propose(group, data.clone()).await {
                        Ok(_) => {
                            ok_count.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {}
                        Err(multiraft_core::MultiRaftError::UnknownGroup(_)) => {}
                        Err(_) => {}
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(30)).await;
    nodes
        .iter()
        .find(|n| n.node_id() == l1)
        .expect("leader handle")
        .shutdown()
        .await
        .expect("shutdown leader");

    for h in handles {
        let _ = h.await;
    }

    let dead = [l1];
    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(20)).await;

    // Cluster eventually writable after failover.
    propose_on_leader(&nodes, group, CounterFsm::encode_add(1, 10_000)).await;

    let successes = ok_count.load(Ordering::SeqCst);
    let value = wait_fsm_at_least(&nodes, group, successes as i64, Duration::from_secs(15)).await;
    assert!(
        value >= successes as i64,
        "FSM value {value} < acknowledged proposes {successes}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_group_independent_failover() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let groups: Vec<u64> = (1..=5).collect();
    let members = peer_ids.to_vec();
    create_groups(&nodes, &groups, &members).await;

    let mut leaders = Vec::new();
    for &g in &groups {
        let lid = wait_for_leader(&nodes, g, Duration::from_secs(15))
            .await
            .unwrap_or_else(|| panic!("leader for group {g}"));
        leaders.push((g, lid));
    }

    // Prefer killing a node that leads at least one group, if any split exists.
    let kill_id = leaders
        .iter()
        .map(|(_, lid)| *lid)
        .next()
        .unwrap_or(peer_ids[0]);

    let survivor_led: Vec<u64> = leaders
        .iter()
        .filter(|(_, lid)| *lid != kill_id)
        .map(|(g, _)| *g)
        .collect();

    let mut before_map = std::collections::BTreeMap::new();
    for &g in &groups {
        before_map.insert(g, max_fsm_value(&nodes, g).await);
    }

    nodes
        .iter()
        .find(|n| n.node_id() == kill_id)
        .expect("kill handle")
        .shutdown()
        .await
        .expect("shutdown");

    let dead = [kill_id];

    // Groups that had a different leader should keep accepting writes.
    for &g in &survivor_led {
        propose_on_leader(&nodes, g, CounterFsm::encode_add(2, 1)).await;
        let min = before_map[&g] + 2;
        wait_fsm_at_least(&nodes, g, min, Duration::from_secs(15)).await;
    }

    // All groups eventually have a leader among survivors.
    for &g in &groups {
        let _ = wait_for_leader_among(&nodes, g, &dead, Duration::from_secs(20)).await;
        propose_on_leader(&nodes, g, CounterFsm::encode_add(1, 99)).await;
    }

    for &g in &groups {
        wait_fsm_at_least(&nodes, g, before_map[&g] + 1, Duration::from_secs(15)).await;
    }
}

// --- C05–C08 ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn majority_lost_writes_stall() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let _ = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    propose_on_leader(&nodes, group, CounterFsm::encode_add(1, 1)).await;
    wait_fsm_at_least(&nodes, group, 1, Duration::from_secs(10)).await;

    // Kill 2 of 3 → no majority.
    nodes[0].shutdown().await.expect("kill 0");
    nodes[1].shutdown().await.expect("kill 1");

    let ok = propose_succeeds_within(
        &nodes,
        group,
        CounterFsm::encode_add(5, 2),
        Duration::from_secs(3),
    )
    .await;
    assert!(!ok, "propose must fail/timeout without majority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn majority_loss_then_recover() {
    let peer_ids = [1u64, 2, 3];
    let tmp = tempfile::tempdir().expect("tempdir");
    let configs = configs_with_temp_dirs(&peer_ids, tmp.path());
    let fabric = SharedFabric::new();
    let mut nodes = start_on_fabric(&fabric, &configs).await;
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let _ = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    propose_on_leader(&nodes, group, CounterFsm::encode_add(4, 1)).await;
    let before = wait_fsm_at_least(&nodes, group, 4, Duration::from_secs(15)).await;

    // Lose majority, then bring one killed node back.
    let kill_a = nodes[0].node_id();
    let kill_b = nodes[1].node_id();
    nodes[0].shutdown().await.expect("kill a");
    nodes[1].shutdown().await.expect("kill b");

    assert!(
        !propose_succeeds_within(
            &nodes,
            group,
            CounterFsm::encode_add(1, 99),
            Duration::from_secs(2),
        )
        .await,
        "should stall without majority"
    );

    restart_node(
        &fabric,
        &mut nodes,
        &configs,
        kill_a,
        &[group],
        &members,
    )
    .await;
    // kill_b stays down; majority is survivor + restarted kill_a.
    let dead = [kill_b];
    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(30)).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(6, 2)).await;
    let after = wait_fsm_at_least(&nodes, group, before + 6, Duration::from_secs(20)).await;
    assert!(after >= before + 6);
    assert!(after >= before, "values must not go backwards");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_leader_kill() {
    let peer_ids = [1u64, 2, 3, 4, 5];
    let tmp = tempfile::tempdir().expect("tempdir");
    let configs = configs_with_temp_dirs(&peer_ids, tmp.path());
    let fabric = SharedFabric::new();
    let mut nodes = start_on_fabric(&fabric, &configs).await;
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let mut idem = 1u64;
    let mut baseline = 0i64;
    for round in 0..3 {
        let leader = wait_for_leader(&nodes, group, Duration::from_secs(25))
            .await
            .unwrap_or_else(|| panic!("leader round {round}"));
        nodes
            .iter()
            .find(|n| n.node_id() == leader)
            .expect("leader handle")
            .shutdown()
            .await
            .expect("kill leader");

        let dead = [leader];
        let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(30)).await;
        propose_on_leader(&nodes, group, CounterFsm::encode_add(1, idem)).await;
        idem += 1;
        baseline = wait_fsm_at_least(&nodes, group, baseline + 1, Duration::from_secs(20)).await;

        // Restart previous leader before next kill so majority stays comfortable.
        restart_node(
            &fabric,
            &mut nodes,
            &configs,
            leader,
            &[group],
            &members,
        )
        .await;
        let _ = wait_for_leader(&nodes, group, Duration::from_secs(25))
            .await
            .expect("leader after restart");
    }
    assert!(baseline >= 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_restart_all_nodes() {
    let peer_ids = [1u64, 2, 3];
    let tmp = tempfile::tempdir().expect("tempdir");
    let configs = configs_with_temp_dirs(&peer_ids, tmp.path());
    let fabric = SharedFabric::new();
    let mut nodes = start_on_fabric(&fabric, &configs).await;
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let _ = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    propose_on_leader(&nodes, group, CounterFsm::encode_add(3, 1)).await;
    let mut floor = wait_fsm_at_least(&nodes, group, 3, Duration::from_secs(15)).await;

    let mut idem = 2u64;
    for &id in &peer_ids {
        let snap = max_fsm_value(&nodes, group).await;
        restart_node(&fabric, &mut nodes, &configs, id, &[group], &members).await;
        let _ = wait_for_leader(&nodes, group, Duration::from_secs(25))
            .await
            .expect("leader after rolling restart");
        let after_restart = max_fsm_value(&nodes, group).await;
        assert!(
            after_restart >= snap,
            "node {id}: value went backwards {snap} -> {after_restart}"
        );
        floor = floor.max(after_restart);
        propose_on_leader(&nodes, group, CounterFsm::encode_add(1, idem)).await;
        idem += 1;
        floor = wait_fsm_at_least(&nodes, group, floor + 1, Duration::from_secs(20)).await;
    }
}

// --- C20–C23 ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asymmetric_lag_follower_catchup() {
    let peer_ids = [1u64, 2, 3];
    let tmp = tempfile::tempdir().expect("tempdir");
    let configs = configs_with_temp_dirs(&peer_ids, tmp.path());
    let fabric = SharedFabric::new();
    let mut nodes = start_on_fabric(&fabric, &configs).await;
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    let follower_id = peer_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id)
        .expect("follower");

    nodes
        .iter()
        .find(|n| n.node_id() == follower_id)
        .expect("follower")
        .shutdown()
        .await
        .expect("kill follower");

    for i in 0..20u64 {
        propose_on_leader(&nodes, group, CounterFsm::encode_add(1, i + 1)).await;
    }
    let leader_val = wait_fsm_at_least(&nodes, group, 20, Duration::from_secs(20)).await;

    restart_node(
        &fabric,
        &mut nodes,
        &configs,
        follower_id,
        &[group],
        &members,
    )
    .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let fv = nodes
            .iter()
            .find(|n| n.node_id() == follower_id)
            .unwrap()
            .with_fsm(group, |fsm| fsm.value(group))
            .await
            .unwrap_or(0);
        let lv = max_fsm_value(&nodes, group).await.max(leader_val);
        if fv == lv {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("follower {follower_id} value {fv} != leader/cluster {lv}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_replay_across_failover() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let _ = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    let baseline = max_fsm_value(&nodes, group).await;

    let data = CounterFsm::encode_add(10, 7);
    propose_on_leader(&nodes, group, data.clone()).await;
    wait_fsm_at_least(&nodes, group, baseline + 10, Duration::from_secs(15)).await;

    let l1 = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader to kill");
    nodes
        .iter()
        .find(|n| n.node_id() == l1)
        .expect("leader")
        .shutdown()
        .await
        .expect("kill leader");

    let dead = [l1];
    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(25)).await;
    // Same idem key — must not double-apply.
    propose_on_leader(&nodes, group, data).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut final_v = 0;
    while std::time::Instant::now() < deadline {
        final_v = max_fsm_value(&nodes, group).await;
        if final_v == baseline + 10 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        final_v,
        baseline + 10,
        "idempotent replay must apply once (got {final_v}, want {})",
        baseline + 10
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivor_fsm_converges() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let _ = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    propose_on_leader(&nodes, group, CounterFsm::encode_add(5, 1)).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(7, 2)).await;
    wait_fsm_at_least(&nodes, group, 12, Duration::from_secs(15)).await;

    let kill_id = peer_ids[2];
    nodes
        .iter()
        .find(|n| n.node_id() == kill_id)
        .unwrap()
        .shutdown()
        .await
        .expect("kill");

    let dead = [kill_id];
    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(20)).await;
    propose_on_leader(&nodes, group, CounterFsm::encode_add(3, 3)).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut vals = Vec::new();
        for n in alive_nodes(&nodes, &dead) {
            if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
                vals.push(v);
            }
        }
        if vals.len() >= 2 {
            let min = *vals.iter().min().unwrap();
            let max = *vals.iter().max().unwrap();
            if min == max && min >= 15 {
                return;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("survivors did not converge: {vals:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_propose_survives_leader_kill() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let group = 1u64;
    let members = peer_ids.to_vec();
    create_groups(&nodes, &[group], &members).await;

    let l1 = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader");
    propose_on_leader(&nodes, group, CounterFsm::encode_add(9, 1)).await;
    let recorded = wait_fsm_at_least(&nodes, group, 9, Duration::from_secs(15)).await;

    nodes
        .iter()
        .find(|n| n.node_id() == l1)
        .unwrap()
        .shutdown()
        .await
        .expect("kill leader");

    let dead = [l1];
    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(25)).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut best = 0i64;
        for n in alive_nodes(&nodes, &dead) {
            if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
                best = best.max(v);
            }
        }
        if best >= recorded {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("survivors value {best} < recorded {recorded}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// --- C30–C31 ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_group_storm_under_leader_kill() {
    let peer_ids = [1u64, 2, 3];
    let nodes = Arc::new(
        MultiRaft::start_cluster(start_configs(&peer_ids))
            .await
            .expect("start_cluster"),
    );
    let groups: Vec<u64> = (1..=8).collect();
    let members = peer_ids.to_vec();
    create_groups(&nodes, &groups, &members).await;

    for &g in &groups {
        let _ = wait_for_leader(&nodes, g, Duration::from_secs(20))
            .await
            .unwrap_or_else(|| panic!("leader group {g}"));
    }

    let mut before = std::collections::BTreeMap::new();
    for &g in &groups {
        before.insert(g, max_fsm_value(&nodes, g).await);
    }

    let writes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();
    for &g in &groups {
        let nodes = Arc::clone(&nodes);
        let writes = Arc::clone(&writes);
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let mut idem = 1u64;
            while !stop.load(Ordering::SeqCst) {
                let data = CounterFsm::encode_add(1, (g * 1000) + idem);
                for n in nodes.iter() {
                    if !n.is_leader(g) {
                        continue;
                    }
                    if n.propose(g, data.clone()).await.is_ok() {
                        writes.fetch_add(1, Ordering::SeqCst);
                        idem += 1;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    let kill_id = wait_for_leader(&nodes, groups[0], Duration::from_secs(5))
        .await
        .unwrap_or(peer_ids[0]);
    nodes
        .iter()
        .find(|n| n.node_id() == kill_id)
        .unwrap()
        .shutdown()
        .await
        .expect("kill leader node");

    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    for h in handles {
        let _ = h.await;
    }

    let dead = [kill_id];
    for &g in &groups {
        let _ = wait_for_leader_among(&nodes, g, &dead, Duration::from_secs(30)).await;
        propose_on_leader(&nodes, g, CounterFsm::encode_add(1, 50_000 + g)).await;
        wait_fsm_at_least(&nodes, g, before[&g] + 1, Duration::from_secs(20)).await;
    }
    assert!(
        writes.load(Ordering::SeqCst) > 0,
        "writers should have landed some proposes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_links_remain_o_nodes_under_churn() {
    let peer_ids = [1u64, 2, 3];
    let nodes = MultiRaft::start_cluster(start_configs(&peer_ids))
        .await
        .expect("start_cluster");
    let groups: Vec<u64> = (1..=10).collect();
    let members = peer_ids.to_vec();
    create_groups(&nodes, &groups, &members).await;

    for &g in &groups {
        let _ = wait_for_leader(&nodes, g, Duration::from_secs(20))
            .await
            .unwrap_or_else(|| panic!("leader {g}"));
    }

    let leader = wait_for_leader(&nodes, 1, Duration::from_secs(10))
        .await
        .expect("leader");
    let follower = peer_ids
        .iter()
        .copied()
        .find(|&id| id != leader)
        .expect("follower");
    nodes
        .iter()
        .find(|n| n.node_id() == follower)
        .unwrap()
        .shutdown()
        .await
        .expect("kill follower");

    for n in nodes.iter().filter(|n| n.node_id() != follower) {
        let links = n.unique_peer_links();
        assert!(
            links < 10,
            "node {}: links {links} must not scale with 10 groups",
            n.node_id()
        );
        assert!(
            links <= 6,
            "node {}: expected O(nodes) links (<=6), got {links}",
            n.node_id()
        );
    }
}
