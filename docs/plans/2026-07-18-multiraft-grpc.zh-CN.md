# multiraft 跨进程 gRPC 传输计划

**English：** [2026-07-18-multiraft-grpc.md](./2026-07-18-multiraft-grpc.md)

> **说明：** 英文版为规范来源（canonical）；本中文版供阅读对照。

**目标：** 将仅单进程可用的 Demo 网络替换为 tonic/gRPC，使 3 个 OS 进程组成 Multi-Raft 集群；验收时杀掉真实 Leader PID。

**架构：** 单元测试保留进程内 `Router`。新增 `GrpcGroupRouter` + 按 `group_id` 解复用的 tonic server。Demo `--mode node` 绑定 Raft RPC + Admin HTTP；`run_demo_cluster.sh` 启动 3 个进程。Acceptance 使用 `kill $LEADER_PID`。

**技术栈：** tonic, prost, tokio, 既有 openraft-multi `GroupRouter`, openraft `=0.10.0-alpha.30`

**工作目录：** `$REPO_ROOT`

---

## 文件映射

| Path | Responsibility |
|------|----------------|
| `crates/multiraft-net/proto/raft.proto` | Append/Vote/Snapshot RPCs + group_id |
| `crates/multiraft-net/src/grpc/{mod,server,client,router}.rs` | tonic server + shared channel pool |
| `crates/multiraft-net/src/multiraft.rs` | Wire Grpc transport when peers have real addrs |
| `crates/multiraft-demo/src/main.rs` | `--mode node` multi-process |
| `scripts/run_demo_cluster.sh` | Start 3 OS processes |
| `scripts/acceptance.sh` | Kill real PID for failover |

---

### Task 1: Proto + tonic GroupRouter 骨架

- [ ] Add tonic/prost build deps; define proto with `group_id` on each RPC
- [ ] Implement `GrpcGroupRouter` implementing `openraft_multi::GroupRouter`
- [ ] Per-peer channel cache; `unique_peer_links()` counts distinct peer channels
- [ ] Unit test: 2 fake servers, 10 groups, assert link count == 2 (or 1 directed as designed)
- [ ] Commit: `feat(net): add tonic GrpcGroupRouter skeleton`

### Task 2: 为远程 peers 接线 MultiRaft::start

- [ ] `MultiRaft::start(config)` starts local node + tonic server on `peers[self].addr`
- [ ] Outbound `GrpcGroupRouter` to other peer addrs
- [ ] `create_group` / `propose` / leader APIs work across processes
- [ ] Integration test: spawn 3 tokio tasks each with real TCP tonic (or 3 threads), 1 group write
- [ ] Commit: `feat(net): MultiRaft gRPC cross-process start`

### Task 3: Demo 多进程模式

- [ ] `--mode node` works with `--node-id` / `--base-port` / `--groups` / `--data-dir`
- [ ] Admin HTTP per process (value, links, optional inject)
- [ ] `run_demo_cluster.sh` starts 3 processes with distinct PIDs
- [ ] Smoke 30s: 10 groups advance
- [ ] Commit: `feat(demo): multi-process gRPC cluster launcher`

### Task 4: Acceptance 杀 PID

- [ ] Update `acceptance.sh`: detect leader node via HTTP; `kill` that PID; wait new leaders; values durable; restart killed node
- [ ] Keep checks 1,2,5,6; criterion 3 = OS kill
- [ ] `./scripts/acceptance.sh` → `ACCEPTANCE OK`
- [ ] Commit: `test: acceptance kills real leader process`
- [ ] Update README + design addendum

---

## Spec 覆盖

| Item | Task |
|------|------|
| tonic/gRPC | 1–2 |
| Shared channel O(nodes) | 1, 4 |
| 3 OS processes | 3–4 |
| Kill PID failover | 4 |
| Keep in-process tests | 1–2 (feature coexistence) |
