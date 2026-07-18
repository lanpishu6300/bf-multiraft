//! Single-node file log: propose, shut down, reopen same dir, FSM rebuilt.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use multiraft_fsm::CounterFsm;
use multiraft_store::FileLogStoreOf;
use multiraft_store::Raft;
use multiraft_store::Request;
use multiraft_store::StateMachineStore;
use multiraft_store::StubNetworkFactory;
use multiraft_store::TypeConfig as RaftTypeConfig;
use openraft::BasicNode;
use openraft::Config;
use openraft::type_config::TypeConfigExt;

async fn create_single_node_file(
    node_id: u64,
    group_id: u64,
    data_dir: &std::path::Path,
) -> (Raft<CounterFsm>, StateMachineStore<CounterFsm>) {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let dir = data_dir.join(format!("group-{group_id}"));
    let log_store = FileLogStoreOf::open(&dir).expect("open file log");
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
fn restart_replays_committed_state() {
    RaftTypeConfig::run(async {
        let group_id = 1u64;
        let data_dir = std::env::temp_dir().join(format!(
            "multiraft-store-restart-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&data_dir).unwrap();

        let expected: i64 = 1 + 2 + 3 + 4 + 5;

        {
            let (raft, sm) = create_single_node_file(1, group_id, &data_dir).await;

            let mut nodes = BTreeMap::new();
            nodes.insert(1u64, BasicNode { addr: "".to_string() });
            raft.initialize(nodes).await.unwrap();
            RaftTypeConfig::sleep(Duration::from_millis(200)).await;

            for (i, delta) in [1i64, 2, 3, 4, 5].into_iter().enumerate() {
                let req = Request::new(CounterFsm::encode_add(delta, /*idem=*/ (i as u64) + 1));
                raft.client_write(req).await.unwrap();
            }

            RaftTypeConfig::sleep(Duration::from_millis(100)).await;
            let value = sm.with_fsm(|fsm| fsm.value(group_id)).await;
            assert_eq!(value, expected);

            raft.shutdown().await.unwrap();
        }

        // Fresh FSM + same durable log → replay restores counter.
        let (raft, sm) = create_single_node_file(1, group_id, &data_dir).await;
        raft.wait_for_recovery(Some(Duration::from_secs(5)))
            .await
            .expect("recovery");

        let value = sm.with_fsm(|fsm| fsm.value(group_id)).await;
        assert_eq!(value, expected, "FSM must rebuild from durable log");

        let _ = std::fs::remove_dir_all(&data_dir);
    });
}
