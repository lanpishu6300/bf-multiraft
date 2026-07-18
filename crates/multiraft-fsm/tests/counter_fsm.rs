use multiraft_fsm::{CounterFsm, StateMachine};

#[test]
fn apply_increments_and_is_idempotent() {
    let mut fsm = CounterFsm::new();
    let g = 1u64;
    let cmd = CounterFsm::encode_add(10, /*idem=*/ 42);
    fsm.apply(g, 1, &cmd).unwrap();
    fsm.apply(g, 2, &cmd).unwrap(); // same idem key
    assert_eq!(fsm.value(g), 10);

    let cmd2 = CounterFsm::encode_add(5, 43);
    fsm.apply(g, 3, &cmd2).unwrap();
    assert_eq!(fsm.value(g), 15);
}

#[test]
fn snapshot_restore_roundtrip() {
    let mut fsm = CounterFsm::new();
    fsm.apply(7, 1, &CounterFsm::encode_add(3, 1)).unwrap();
    let snap = fsm.snapshot(7).unwrap();
    let mut fsm2 = CounterFsm::new();
    fsm2.restore(7, &snap).unwrap();
    assert_eq!(fsm2.value(7), 3);
}
