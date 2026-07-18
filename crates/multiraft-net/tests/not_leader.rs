//! Non-leader propose must return `MultiRaftError::NotLeader`.

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;

#[tokio::test]
async fn propose_on_follower_returns_not_leader() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();

    let nodes = MultiRaft::start_cluster(configs).await.expect("start_cluster");
    let members = peer_ids.to_vec();
    let group = 1u64;

    for n in &nodes {
        n.create_group(group, &members)
            .await
            .expect("create_group");
    }

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(5))
        .await
        .expect("leader elected");

    let follower = nodes
        .iter()
        .find(|n| n.node_id() != leader_id && !n.is_leader(group))
        .expect("follower handle");

    let data = CounterFsm::encode_add(1, /*idem=*/ 1);
    let err = follower
        .propose(group, data)
        .await
        .expect_err("follower propose must fail");

    match err {
        MultiRaftError::NotLeader { hint } => {
            // hint may be Some(leader) or None depending on metrics freshness
            if let Some(h) = hint {
                assert_eq!(h, leader_id);
            }
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }
}
