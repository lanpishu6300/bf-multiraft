//! In-process chaos: leader/follower loss while MultiRaft stays writable.
//!
//! Run with: `cargo test -p multiraft-net --test chaos_failover`

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;

fn start_configs(peer_ids: &[u64]) -> Vec<ClusterConfig> {
    peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, peer_ids))
        .collect()
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
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
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
            panic!("timed out proposing on group {group}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
