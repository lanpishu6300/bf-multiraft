//! Connection cardinality: 3 nodes × 10 groups must share O(nodes) links.
//!
//! Bootstrap pattern adapted from openraft
//! `examples/multi-raft-kv/tests/cluster/test_cluster.rs` at tag
//! `v0.10.0-alpha.30`.

use std::collections::BTreeMap;
use std::time::Duration;

use multiraft_core::TypeConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::Router;
use multiraft_net::create_node;
use multiraft_store::Request;
use openraft::BasicNode;
use openraft::async_runtime::WatchReceiver;
use openraft::type_config::TypeConfigExt;

#[tokio::test]
async fn peer_connections_are_o_nodes_not_o_groups() {
    let router = Router::new();
    let group_ids: Vec<u64> = (1..=10).collect();

    // 3 in-process nodes, each hosting all 10 groups; one shared channel per node.
    let node1 = create_node(1, &group_ids, router.clone(), |_| CounterFsm::new()).await;
    let node2 = create_node(2, &group_ids, router.clone(), |_| CounterFsm::new()).await;
    let node3 = create_node(3, &group_ids, router.clone(), |_| CounterFsm::new()).await;

    let node1_rafts: Vec<_> = group_ids
        .iter()
        .map(|&g| node1.get_raft(g).unwrap().clone())
        .collect();
    let node2_rafts: Vec<_> = group_ids
        .iter()
        .map(|&g| node2.get_raft(g).unwrap().clone())
        .collect();
    let node3_rafts: Vec<_> = group_ids
        .iter()
        .map(|&g| node3.get_raft(g).unwrap().clone())
        .collect();

    TypeConfig::spawn(node1.run());
    TypeConfig::spawn(node2.run());
    TypeConfig::spawn(node3.run());

    TypeConfig::sleep(Duration::from_millis(100)).await;

    let all_nodes = {
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, BasicNode { addr: "".to_string() });
        nodes.insert(2u64, BasicNode { addr: "".to_string() });
        nodes.insert(3u64, BasicNode { addr: "".to_string() });
        nodes
    };

    for raft in &node1_rafts {
        raft.initialize(all_nodes.clone()).await.unwrap();
    }

    // Drive heartbeats / replication across all groups.
    TypeConfig::sleep(Duration::from_millis(800)).await;

    for (i, raft) in node1_rafts.iter().enumerate() {
        let req = Request::new(CounterFsm::encode_add(1, /*idem=*/ (i as u64) + 1));
        raft.client_write(req).await.unwrap();
    }

    TypeConfig::sleep(Duration::from_millis(500)).await;

    // Confirm followers applied at least one entry on a sample of groups.
    for raft in node2_rafts.iter().chain(node3_rafts.iter()).take(3) {
        let metrics = raft.metrics().borrow_watched().clone();
        assert!(
            metrics.last_applied.is_some(),
            "followers should have applied logs after multi-group traffic"
        );
    }

    let links = router.unique_peer_links();
    // Directed full mesh among 3 nodes is at most 3*2 = 6; shared router uses
    // one open channel per node (3), never one per group (10+).
    assert!(
        links <= 3 * 2,
        "expected O(nodes) peer links (<=6), got {links}"
    );
    assert!(
        links < 10,
        "peer links must not scale with groups; got {links} for 10 groups"
    );
    assert_eq!(
        links, 3,
        "in-process shared router should open exactly one channel per node"
    );
}
