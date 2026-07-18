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

#[test]
fn multi_group_isolation() {
    let mut fsm = CounterFsm::new();
    fsm.apply(1, 1, &CounterFsm::encode_add(10, 1)).unwrap();
    fsm.apply(2, 1, &CounterFsm::encode_add(7, 1)).unwrap();
    assert_eq!(fsm.value(1), 10);
    assert_eq!(fsm.value(2), 7);

    fsm.apply(1, 2, &CounterFsm::encode_add(5, 2)).unwrap();
    assert_eq!(fsm.value(1), 15);
    assert_eq!(fsm.value(2), 7);
}

#[test]
fn bad_payload_returns_decode_error() {
    let mut fsm = CounterFsm::new();
    let err = fsm
        .apply(1, 1, b"not-json")
        .expect_err("invalid payload must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("decode"),
        "expected decode error, got: {msg}"
    );
}

#[test]
fn snapshot_restore_preserves_idempotency() {
    let mut fsm = CounterFsm::new();
    let g = 9u64;
    let cmd = CounterFsm::encode_add(10, /*idem=*/ 42);
    fsm.apply(g, 1, &cmd).unwrap();
    assert_eq!(fsm.value(g), 10);

    let snap = fsm.snapshot(g).unwrap();
    let mut restored = CounterFsm::new();
    restored.restore(g, &snap).unwrap();
    assert_eq!(restored.value(g), 10);

    // Same idempotency key must not double-apply after restore.
    restored.apply(g, 2, &cmd).unwrap();
    assert_eq!(restored.value(g), 10);
}
