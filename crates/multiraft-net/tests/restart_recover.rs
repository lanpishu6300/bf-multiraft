//! 3-node MultiRaft restart with shared file-backed data dirs.

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;

fn temp_data_dirs(peer_ids: &[u64]) -> Vec<std::path::PathBuf> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    peer_ids
        .iter()
        .map(|&id| {
            let dir = std::env::temp_dir().join(format!(
                "multiraft-net-restart-{}-{}-{}",
                std::process::id(),
                stamp,
                id
            ));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        })
        .collect()
}

fn configs_with_dirs(peer_ids: &[u64], dirs: &[std::path::PathBuf]) -> Vec<ClusterConfig> {
    peer_ids
        .iter()
        .zip(dirs.iter())
        .map(|(&id, dir)| {
            let mut cfg = ClusterConfig::for_test(id, peer_ids);
            cfg.data_dir = dir.clone();
            cfg
        })
        .collect()
}

async fn propose_on_leader(nodes: &[MultiRaft], group: u64, data: Vec<u8>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
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

#[tokio::test]
async fn restart_replays_committed_state() {
    let peer_ids = [1u64, 2, 3];
    let dirs = temp_data_dirs(&peer_ids);
    let group = 1u64;
    let members = peer_ids.to_vec();
    let expected: i64 = 1 + 2 + 3 + 4 + 5;

    {
        let configs = configs_with_dirs(&peer_ids, &dirs);
        let nodes = MultiRaft::start_cluster(configs)
            .await
            .expect("start_cluster");

        for n in &nodes {
            n.create_group(group, &members)
                .await
                .expect("create_group");
        }

        wait_for_leader(&nodes, group, Duration::from_secs(5))
            .await
            .expect("leader elected");

        for (i, delta) in [1i64, 2, 3, 4, 5].into_iter().enumerate() {
            let data = CounterFsm::encode_add(delta, /*idem=*/ (i as u64) + 1);
            propose_on_leader(&nodes, group, data).await;
        }

        // Spot-check before shutdown.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut saw = false;
        for n in &nodes {
            if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
                if v == expected {
                    saw = true;
                    break;
                }
            }
        }
        assert!(saw, "at least one node should have applied all cmds");

        for n in &nodes {
            n.shutdown().await.expect("shutdown");
        }
    }

    // Restart with the same per-node data_dir paths.
    let configs = configs_with_dirs(&peer_ids, &dirs);
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("restart cluster");

    for n in &nodes {
        n.create_group(group, &members)
            .await
            .expect("create_group after restart");
    }

    wait_for_leader(&nodes, group, Duration::from_secs(5))
        .await
        .expect("leader after restart");

    for n in &nodes {
        n.wait_for_recovery(group, Duration::from_secs(5))
            .await
            .expect("wait_for_recovery");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut recovered = None;
    while std::time::Instant::now() < deadline {
        for n in &nodes {
            if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
                if v == expected {
                    recovered = Some(v);
                    break;
                }
            }
        }
        if recovered.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        recovered,
        Some(expected),
        "FSM value must be restored after restart"
    );

    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
