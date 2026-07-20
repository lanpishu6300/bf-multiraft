# multiraft Cross-Process gRPC Transport Plan

**中文：** [2026-07-18-multiraft-grpc.zh-CN.md](./2026-07-18-multiraft-grpc.zh-CN.md)

**Goal:** Replace single-process-only demo networking with tonic/gRPC so 3 OS processes form a Multi-Raft cluster; acceptance kills a real leader PID.

**Architecture:** Keep in-process `Router` for unit tests. Add `GrpcGroupRouter` + tonic server that demuxes by `group_id`. Demo `--mode node` binds Raft RPC + admin HTTP; `run_demo_cluster.sh` starts 3 processes. Acceptance uses `kill $LEADER_PID`.

**Tech Stack:** tonic, prost, tokio, existing openraft-multi `GroupRouter`, openraft `=0.10.0-alpha.30`

**Workdir:** `$REPO_ROOT`

---

## File map

| Path | Responsibility |
|------|----------------|
| `crates/multiraft-net/proto/raft.proto` | Append/Vote/Snapshot RPCs + group_id |
| `crates/multiraft-net/src/grpc/{mod,server,client,router}.rs` | tonic server + shared channel pool |
| `crates/multiraft-net/src/multiraft.rs` | Wire Grpc transport when peers have real addrs |
| `crates/multiraft-demo/src/main.rs` | `--mode node` multi-process |
| `scripts/run_demo_cluster.sh` | Start 3 OS processes |
| `scripts/acceptance.sh` | Kill real PID for failover |

---

### Task 1: Proto + tonic GroupRouter skeleton

- [ ] Add tonic/prost build deps; define proto with `group_id` on each RPC
- [ ] Implement `GrpcGroupRouter` implementing `openraft_multi::GroupRouter`
- [ ] Per-peer channel cache; `unique_peer_links()` counts distinct peer channels
- [ ] Unit test: 2 fake servers, 10 groups, assert link count == 2 (or 1 directed as designed)
- [ ] Commit: `feat(net): add tonic GrpcGroupRouter skeleton`

### Task 2: Wire MultiRaft::start for remote peers

- [ ] `MultiRaft::start(config)` starts local node + tonic server on `peers[self].addr`
- [ ] Outbound `GrpcGroupRouter` to other peer addrs
- [ ] `create_group` / `propose` / leader APIs work across processes
- [ ] Integration test: spawn 3 tokio tasks each with real TCP tonic (or 3 threads), 1 group write
- [ ] Commit: `feat(net): MultiRaft gRPC cross-process start`

### Task 3: Demo multi-process mode

- [ ] `--mode node` works with `--node-id` / `--base-port` / `--groups` / `--data-dir`
- [ ] Admin HTTP per process (value, links, optional inject)
- [ ] `run_demo_cluster.sh` starts 3 processes with distinct PIDs
- [ ] Smoke 30s: 10 groups advance
- [ ] Commit: `feat(demo): multi-process gRPC cluster launcher`

### Task 4: Acceptance kill-PID

- [ ] Update `acceptance.sh`: detect leader node via HTTP; `kill` that PID; wait new leaders; values durable; restart killed node
- [ ] Keep checks 1,2,5,6; criterion 3 = OS kill
- [ ] `./scripts/acceptance.sh` → `ACCEPTANCE OK`
- [ ] Commit: `test: acceptance kills real leader process`
- [ ] Update README + design addendum

---

## Spec coverage

| Item | Task |
|------|------|
| tonic/gRPC | 1–2 |
| Shared channel O(nodes) | 1, 4 |
| 3 OS processes | 3–4 |
| Kill PID failover | 4 |
| Keep in-process tests | 1–2 (feature coexistence) |
