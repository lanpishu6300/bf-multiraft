//! In-process porcupine linearizability check under leader kill (Jepsen-adjacent).
//!
//! Run with:
//! `cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture`

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;
use porcupine_rs::Model;
use porcupine_rs::Operation;

#[derive(Clone, Debug)]
enum CounterOp {
    /// Successful propose(+delta).
    Inc { delta: i64 },
    /// Successful read_linearizable observed value.
    Read(i64),
}

#[derive(Clone, Debug)]
struct CounterModel;

impl Model for CounterModel {
    type State = i64;
    type Op = CounterOp;
    type Metadata = ();

    fn init() -> i64 {
        0
    }

    fn step(state: &i64, op: &CounterOp) -> (bool, i64) {
        match op {
            CounterOp::Inc { delta } => (true, state + delta),
            CounterOp::Read(v) => (*v == *state, *state),
        }
    }
}

fn now_ns(start: Instant) -> i64 {
    start.elapsed().as_nanos() as i64
}

/// Propose +1, retrying across local nodes until success. Records only the
/// successful attempt's call/return window.
async fn propose_inc_ok(
    nodes: &[MultiRaft],
    group: u64,
    idem: u64,
    client_id: u32,
    t0: Instant,
    history: &Mutex<Vec<Operation<CounterModel>>>,
) {
    let data = CounterFsm::encode_add(1, idem);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        // Prefer current leader, then try everyone (NotLeader race / failover).
        let mut order: Vec<&MultiRaft> = Vec::with_capacity(nodes.len());
        for n in nodes {
            if n.is_leader(group) {
                order.push(n);
            }
        }
        for n in nodes {
            if !n.is_leader(group) {
                order.push(n);
            }
        }

        for n in order {
            let call = now_ns(t0);
            match n.propose(group, data.clone()).await {
                Ok(_) => {
                    let ret = now_ns(t0);
                    history.lock().unwrap().push(Operation {
                        client_id: Some(client_id),
                        call_time: call,
                        return_time: ret,
                        op: CounterOp::Inc { delta: 1 },
                        metadata: None,
                    });
                    return;
                }
                Err(MultiRaftError::NotLeader { .. })
                | Err(MultiRaftError::UnknownGroup(_)) => {}
                Err(_) => {}
            }
        }

        if Instant::now() >= deadline {
            panic!("client {client_id}: propose timed out");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Linearizable read, retrying across local nodes until success.
async fn read_ok(
    nodes: &[MultiRaft],
    group: u64,
    client_id: u32,
    t0: Instant,
    history: &Mutex<Vec<Operation<CounterModel>>>,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut order: Vec<&MultiRaft> = Vec::with_capacity(nodes.len());
        for n in nodes {
            if n.is_leader(group) {
                order.push(n);
            }
        }
        for n in nodes {
            if !n.is_leader(group) {
                order.push(n);
            }
        }

        for n in order {
            let call = now_ns(t0);
            match n.read_linearizable(group, |fsm| fsm.value(group)).await {
                Ok(v) => {
                    let ret = now_ns(t0);
                    history.lock().unwrap().push(Operation {
                        client_id: Some(client_id),
                        call_time: call,
                        return_time: ret,
                        op: CounterOp::Read(v),
                        metadata: None,
                    });
                    return;
                }
                Err(MultiRaftError::NotLeader { .. })
                | Err(MultiRaftError::UnknownGroup(_)) => {}
                Err(_) => {}
            }
        }

        if Instant::now() >= deadline {
            panic!("client {client_id}: read_linearizable timed out");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn porcupine_counter_under_leader_kill() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = Arc::new(
        MultiRaft::start_cluster(configs)
            .await
            .expect("start_cluster"),
    );
    let group = 1u64;
    for n in nodes.iter() {
        n.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let _ = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("initial leader");

    let history: Arc<Mutex<Vec<Operation<CounterModel>>>> = Arc::new(Mutex::new(Vec::new()));
    let idem = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let workload = Duration::from_secs(3);
    let n_clients = 8u32;

    let clients: Vec<_> = (0..n_clients)
        .map(|cid| {
            let nodes = Arc::clone(&nodes);
            let history = Arc::clone(&history);
            let idem = Arc::clone(&idem);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut next_write = cid % 2 == 0;
                while !stop.load(Ordering::Relaxed) {
                    if next_write {
                        let id = idem.fetch_add(1, Ordering::Relaxed);
                        propose_inc_ok(&nodes, group, id, cid, t0, &history).await;
                    } else {
                        read_ok(&nodes, group, cid, t0, &history).await;
                    }
                    next_write = !next_write;
                }
            })
        })
        .collect();

    // Mid-test chaos: shut down the current leader once.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut killed = None;
    for n in nodes.iter() {
        if n.is_leader(group) {
            let id = n.node_id();
            eprintln!("porcupine: shutting down leader node {id}");
            n.shutdown().await.expect("shutdown leader");
            killed = Some(id);
            break;
        }
    }
    assert!(killed.is_some(), "expected a leader to kill mid-test");

    tokio::time::sleep(workload.saturating_sub(Duration::from_millis(1500))).await;
    stop.store(true, Ordering::Relaxed);

    for h in clients {
        h.await.expect("client join");
    }

    let ops = history.lock().unwrap().clone();
    let n_inc = ops
        .iter()
        .filter(|o| matches!(o.op, CounterOp::Inc { .. }))
        .count();
    let n_read = ops
        .iter()
        .filter(|o| matches!(o.op, CounterOp::Read(_)))
        .count();
    eprintln!(
        "porcupine: history ops={} (inc={n_inc} read={n_read}) killed_leader={killed:?}",
        ops.len()
    );
    assert!(n_inc > 0, "expected some successful Inc ops");
    assert!(n_read > 0, "expected some successful Read ops");

    assert!(
        porcupine_rs::check_operations::<CounterModel>(&ops),
        "history is not linearizable ({} ops)",
        ops.len()
    );
}
