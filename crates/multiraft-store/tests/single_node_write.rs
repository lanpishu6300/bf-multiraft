//! Single-group, single-node memory cluster smoke test.
//!
//! Bootstrap pattern transcribed from openraft
//! `examples/multi-raft-kv/tests/cluster/test_cluster.rs` at tag `v0.10.0-alpha.30`,
//! reduced to ONE group (`GroupId = u64`) and ONE node.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use multiraft_fsm::CounterFsm;
use multiraft_store::MemLogStore;
use multiraft_store::Raft;
use multiraft_store::Request;
use multiraft_store::StateMachineStore;
use multiraft_store::StubNetworkFactory;
use multiraft_store::TypeConfig as RaftTypeConfig;
use openraft::BasicNode;
use openraft::Config;
use openraft::type_config::TypeConfigExt;

async fn create_single_node(
    node_id: u64,
    group_id: u64,
) -> (Raft<CounterFsm>, StateMachineStore<CounterFsm>) {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let log_store = MemLogStore::default();
    let state_machine_store = StateMachineStore::new(group_id, CounterFsm::new());
    let network = StubNetworkFactory;

    let raft = openraft::Raft::new(
        node_id,
        config,
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    (raft, state_machine_store)
}

#[test]
fn single_node_client_write_applies_to_counter_fsm() {
    RaftTypeConfig::run(async {
        let group_id = 1u64;
        let (raft, sm) = create_single_node(1, group_id).await;

        // Initialize single-node membership (from upstream test_cluster bootstrap).
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, BasicNode { addr: "".to_string() });
        raft.initialize(nodes).await.unwrap();

        RaftTypeConfig::sleep(Duration::from_millis(200)).await;

        let expected_delta = 10i64;
        let req = Request::new(CounterFsm::encode_add(expected_delta, /*idem=*/ 42));
        raft.client_write(req).await.unwrap();

        RaftTypeConfig::sleep(Duration::from_millis(100)).await;

        let value = sm.with_fsm(|fsm| fsm.value(group_id)).await;
        assert_eq!(value, expected_delta);
    });
}
