# multiraft 实现计划

**English：** [2026-07-18-multiraft.md](./2026-07-18-multiraft.md)

> **说明：** 英文版为 agent 执行的规范来源（canonical）；本中文版供人工阅读。  
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 交付独立仓库 `multiraft`：基于 openraft + openraft-multi 的薄 Multi-Raft 运行时，≥10 Group、共享连接、3 节点切主且不丢失已 commit 命令。

**架构：** 每个 `GroupId` 一个 openraft 实例；共享 `GroupRouter` 做 peer RPC；可插拔 `StateMachine` trait；静态 3 节点成员。一期 Demo 本地注入 propose（无 RMQ）。参考实现模式：[openraft `examples/multi-raft-kv`](https://github.com/databendlabs/openraft/tree/main/examples/multi-raft-kv)。

**技术栈：** Rust 2021, Tokio, `openraft` + `openraft-multi` **钉在 `0.10.0-alpha.30`**（必须一致）, serde, tonic 或测试用进程内 router, tracing。

**规格：** [`docs/specs/2026-07-18-multiraft-design.md`](../specs/2026-07-18-multiraft-design.md) · [中文](../specs/2026-07-18-multiraft-design.zh-CN.md)

**工作目录：** `$REPO_ROOT`（新 git 仓库）。始终 `export PATH="$HOME/.cargo/bin:$PATH"`。

**上游参考（只克隆阅读，不要整仓 vendor TiKV）：**
```bash
git clone --depth 1 --branch v0.10.0-alpha.30 \
  https://github.com/databendlabs/openraft.git /tmp/openraft-0.10.0-alpha.30
# Study: /tmp/openraft-0.10.0-alpha.30/examples/multi-raft-kv/
```

---

## 文件映射

| Path | Responsibility |
|------|----------------|
| `Cargo.toml` | Workspace root |
| `README.md` | How to build/run demo + acceptance |
| `docs/specs/2026-07-18-multiraft-design.md` | 已批准设计规格（仓内） |
| `crates/multiraft-fsm/` | `StateMachine` trait + `ApplyOut` |
| `crates/multiraft-store/` | Per-group log + SM storage (start: memory; then file-backed) |
| `crates/multiraft-net/` | Shared router, connection counter, `GroupRouter` impl |
| `crates/multiraft-core/` | `ClusterConfig`, `MultiRaft`, propose / leader APIs |
| `crates/multiraft-demo/` | 3-node × 10-group binary + acceptance scripts |
| `scripts/acceptance.sh` | Kill-leader / restart orchestration |

---

### Task 1: 搭建独立仓库 + workspace

**文件：**
- Create: `$REPO_ROOT/Cargo.toml`
- Create: `$REPO_ROOT/README.md`
- Create: `$REPO_ROOT/.gitignore`
- Create: `$REPO_ROOT/crates/multiraft-fsm/{Cargo.toml,src/lib.rs}`
- Create: `$REPO_ROOT/docs/specs/2026-07-18-multiraft-design.md` （保留在仓内 docs/specs/）

- [ ] **Step 1: 初始化 git 仓库并忽略 target**

```bash
mkdir -p $REPO_ROOT
cd $REPO_ROOT
git init
printf '%s\n' '/target' '**/*.rs.bk' '.DS_Store' > .gitignore
```

- [ ] **Step 2: Workspace Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
  "crates/multiraft-fsm",
  "crates/multiraft-store",
  "crates/multiraft-net",
  "crates/multiraft-core",
  "crates/multiraft-demo",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "MIT OR Apache-2.0"

[workspace.dependencies]
openraft = { version = "=0.10.0-alpha.30", default-features = false, features = ["serde", "type-alias"] }
openraft-multi = { version = "=0.10.0-alpha.30" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
futures = "0.3"
async-trait = "0.1"
anyhow = "1"
```

> 若 crates.io alpha 解析失败，对两个 crate 使用指向 tag `v0.10.0-alpha.30` 的 git 依赖（同一 commit）。

- [ ] **Step 3: 仅 stub `multiraft-fsm`（其余 crate 在后续 Task）**

```toml
# crates/multiraft-fsm/Cargo.toml
[package]
name = "multiraft-fsm"
version.workspace = true
edition.workspace = true

[dependencies]
thiserror = { workspace = true }
```

```rust
// crates/multiraft-fsm/src/lib.rs
//! Pluggable state machine for multiraft.

pub type GroupId = u64;
pub type NodeId = u64;

#[derive(Debug, Clone, Default)]
pub struct ApplyOut {
    pub effects: Vec<u8>,
}

pub trait StateMachine: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(
        &mut self,
        group: GroupId,
        index: u64,
        data: &[u8],
    ) -> Result<ApplyOut, Self::Error>;

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error>;

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error>;
}
```

暂时在 workspace `members` 中注释掉尚未存在的成员，直到 Task 2–5 创建它们，**或**创建带 `pub fn stub() {}` 的空 stub crate，以便 `cargo check` 可通过。

- [ ] **Step 4: 确保仓内规格 + README 骨架**

```bash
mkdir -p docs/specs
# 已批准设计保留在 docs/specs/2026-07-18-multiraft-design.md（仅仓内）
```

```markdown
# multiraft

Thin Multi-Raft runtime for matching-engine HA (openraft + openraft-multi).

See `docs/specs/2026-07-18-multiraft-design.md`.

## Build

```bash
cargo test --workspace
```
```

- [ ] **Step 5: 验证并提交**

```bash
cd $REPO_ROOT
cargo check -p multiraft-fsm
git add -A
git commit -m "$(cat <<'EOF'
chore: scaffold multiraft workspace and multiraft-fsm stub

EOF
)"
```

---

### Task 2: 带幂等的 Demo FSM（TDD）

**文件：**
- Create: `crates/multiraft-fsm/src/counter_fsm.rs`
- Modify: `crates/multiraft-fsm/src/lib.rs`
- Create: `crates/multiraft-fsm/tests/counter_fsm.rs`

- [ ] **Step 1: 编写会失败的测试**

```rust
// crates/multiraft-fsm/tests/counter_fsm.rs
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
```

- [ ] **Step 2: 跑测试 — 期望 FAIL**

```bash
cargo test -p multiraft-fsm --test counter_fsm
```

期望：编译错误 `CounterFsm` not found。

- [ ] **Step 3: 实现 `CounterFsm`**

```rust
// crates/multiraft-fsm/src/counter_fsm.rs
use crate::{ApplyOut, GroupId, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CounterError {
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cmd {
    idem: u64,
    delta: i64,
}

#[derive(Debug, Default)]
pub struct CounterFsm {
    values: HashMap<GroupId, i64>,
    seen: HashMap<GroupId, HashSet<u64>>,
}

impl CounterFsm {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode_add(delta: i64, idem: u64) -> Vec<u8> {
        serde_json::to_vec(&Cmd { idem, delta }).unwrap()
    }

    pub fn value(&self, group: GroupId) -> i64 {
        *self.values.get(&group).unwrap_or(&0)
    }
}

impl StateMachine for CounterFsm {
    type Error = CounterError;

    fn apply(
        &mut self,
        group: GroupId,
        _index: u64,
        data: &[u8],
    ) -> Result<ApplyOut, Self::Error> {
        let cmd: Cmd = serde_json::from_slice(data)
            .map_err(|e| CounterError::Decode(e.to_string()))?;
        let seen = self.seen.entry(group).or_default();
        if seen.insert(cmd.idem) {
            *self.values.entry(group).or_default() += cmd.delta;
        }
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        let v = self.value(group);
        let seen: Vec<u64> = self
            .seen
            .get(&group)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        Ok(serde_json::to_vec(&(v, seen)).unwrap())
    }

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error> {
        let (v, seen): (i64, Vec<u64>) = serde_json::from_slice(snapshot)
            .map_err(|e| CounterError::Decode(e.to_string()))?;
        self.values.insert(group, v);
        self.seen.insert(group, seen.into_iter().collect());
        Ok(())
    }
}
```

在 `lib.rs` 中加入：`mod counter_fsm; pub use counter_fsm::CounterFsm;`，并在 `multiraft-fsm/Cargo.toml` 增加依赖 `serde`、`serde_json`。

- [ ] **Step 4: 跑测试 — 期望 PASS**

```bash
cargo test -p multiraft-fsm --test counter_fsm
```

- [ ] **Step 5: 提交**

```bash
git add crates/multiraft-fsm
git commit -m "$(cat <<'EOF'
feat(fsm): add CounterFsm with idempotent apply and snapshot

EOF
)"
```

---

### Task 3: 从上游示例适配 openraft TypeConfig + 内存 store

**文件：**
- Create: `crates/multiraft-store/` (full crate)
- Create: `crates/multiraft-core/src/type_config.rs` (or under store)

**步骤（不要凭记忆发明 OpenRaft 0.10 trait）：**

- [ ] **Step 1: 在锁定 tag 克隆参考**（见工作目录一节）。

- [ ] **Step 2: 从 `examples/multi-raft-kv` 与共享示例 crates（`log-mem`、`sm-mem`、`types-kv`）复制/适配到 `multiraft-store` / `multiraft-core`：**
  - Type aliases / `RaftTypeConfig` impl → `crates/multiraft-core/src/type_config.rs`
  - In-memory log storage → `crates/multiraft-store/src/log_mem.rs`
  - State machine bridge that calls `multiraft_fsm::StateMachine` → `crates/multiraft-store/src/sm_bridge.rs`

将 Group 从字符串 `"users"` 重命名为 `GroupId: u64`。保持 openraft trait impl 能在 `=0.10.0-alpha.30` 下编译。

- [ ] **Step 3: 单元冒烟 — 单 Group 内存集群（1 节点）client_write**

```rust
// crates/multiraft-store/tests/single_node_write.rs
// Pattern: follow multi-raft-kv test_cluster bootstrap for ONE group, ONE node.
// Assert: after client_write, CounterFsm value == expected.
```

精确 bootstrap 代码必须从上游 `tests/cluster/test_cluster.rs` 转录（API 名随 alpha 变化 — 从锁定 tag 复制后再重命名）。

- [ ] **Step 4: `cargo test -p multiraft-store` PASS**

- [ ] **Step 5: 提交**

```bash
git commit -am "$(cat <<'EOF'
feat(store): adapt openraft TypeConfig and memory log/SM bridge

EOF
)"
```

---

### Task 4: 共享网络 router + 连接计数

**文件：**
- Create: `crates/multiraft-net/src/{lib.rs,router.rs,conn_metrics.rs}`
- Adapt from: upstream `examples/multi-raft-kv/src/{router.rs,network.rs}`

- [ ] **Step 1: 编写连接基数测试**

```rust
// crates/multiraft-net/tests/shared_connections.rs
#[tokio::test]
async fn peer_connections_are_o_nodes_not_o_groups() {
    // Start 3 in-process nodes, create 10 groups, drive heartbeats/writes.
    // Assert: router.unique_peer_links() <= 3 * 2  (or == number of directed edges among 3 nodes)
    // Assert: router.unique_peer_links() < 10  (must NOT be per-group)
}
```

实现 `Router::unique_peer_links()`：统计有打开 channel 的 distinct peer node id（首次连到 peer 时递增，绝不按 Group 递增）。

- [ ] **Step 2: 实现包装共享 channel 的 `GroupRouter`**

按 crate README 使用 `openraft_multi::{GroupRouter, GroupNetworkAdapter, GroupNetworkFactory}`。路由键 = `(target_node_id, group_id)`。

**一期测试**优先用示例的进程内 `Router`（channels），而不是 tonic — CI 更快。可选 tonic 放在 feature `net-tonic` 后面做。

- [ ] **Step 3: 跑测试 — PASS**

```bash
cargo test -p multiraft-net --test shared_connections
```

- [ ] **Step 4: 提交**

```bash
git commit -am "$(cat <<'EOF'
feat(net): shared GroupRouter with O(nodes) connection metric

EOF
)"
```

---

### Task 5: `MultiRaft` 公共 API

**文件：**
- Create: `crates/multiraft-core/src/{lib.rs,config.rs,multiraft.rs,error.rs}`

- [ ] **Step 1: 按规格定义 API 表面**

```rust
// crates/multiraft-core/src/config.rs
use multiraft_fsm::NodeId;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>,
    pub data_dir: PathBuf,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
}
```

```rust
// crates/multiraft-core/src/error.rs
use multiraft_fsm::NodeId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MultiRaftError {
    #[error("not leader; hint={hint:?}")]
    NotLeader { hint: Option<NodeId> },
    #[error("unknown group {0}")]
    UnknownGroup(u64),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct ProposeOk {
    pub index: u64,
    pub term: u64,
}
```

```rust
// crates/multiraft-core/src/multiraft.rs (signatures)
impl MultiRaft {
    pub async fn start(config: ClusterConfig) -> anyhow::Result<Self>;
    pub async fn create_group(&self, group: u64, members: &[u64]) -> Result<(), MultiRaftError>;
    pub async fn propose(&self, group: u64, data: Vec<u8>) -> Result<ProposeOk, MultiRaftError>;
    pub fn is_leader(&self, group: u64) -> bool;
    pub fn leader(&self, group: u64) -> Option<u64>;
    pub fn on_leader_change<F>(&self, cb: F)
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static;
}
```

- [ ] **Step 2: 编写会失败的 NotLeader 集成测试**

```rust
// crates/multiraft-core/tests/not_leader.rs
#[tokio::test]
async fn propose_on_follower_returns_not_leader() {
    // Bootstrap 3 nodes, 1 group; wait until leader known.
    // Call propose on a follower handle → MultiRaftError::NotLeader { .. }
}
```

- [ ] **Step 3: 通过 openraft `Raft::client_write` / 锁定示例的写 API 实现 `propose`；将非 Leader 错误映射为 `NotLeader`。**

- [ ] **Step 4: 通过观察每 Group 的 openraft metrics（`RaftMetrics` / 0.10 等价物）接线 `on_leader_change`。**

- [ ] **Step 5: 测试 PASS + 提交**

```bash
cargo test -p multiraft-core
git commit -am "$(cat <<'EOF'
feat(core): MultiRaft start/create_group/propose/leader APIs

EOF
)"
```

---

### Task 6: 文件持久化 + 重启恢复

**文件：**
- Create: `crates/multiraft-store/src/log_file.rs` (or adapt openraft file example if present)
- Modify: `ClusterConfig.data_dir` usage in `MultiRaft::start`

- [ ] **Step 1: 测试**

```rust
// crates/multiraft-store/tests/restart_recover.rs
#[tokio::test]
async fn restart_replays_committed_state() {
    // node1 single or 3-node: propose 5 cmds to group 1, shut down cleanly,
    // restart with same data_dir, assert CounterFsm value unchanged / caught up.
}
```

- [ ] **Step 2: 在 `{data_dir}/group-{id}/` 下实现持久 log**

最低要求：持久化 raft log entries + hard state，使重启不会清空 FSM。若仅靠 log replay 就能通过测试，本 Task 快照可选。

- [ ] **Step 3: PASS + 提交**

```bash
git commit -am "$(cat <<'EOF'
feat(store): file-backed raft log for restart recovery

EOF
)"
```

---

### Task 7: `multiraft-demo` — 3 节点 × 10 Group

**文件：**
- Create: `crates/multiraft-demo/src/main.rs`
- Create: `crates/multiraft-demo/Cargo.toml`
- Create: `scripts/run_demo_cluster.sh`

- [ ] **Step 1: 二进制接受 `--node-id`、`--base-port`、`--groups 10`**

每个进程：
1. `MultiRaft::start`
2. 对 `0..groups` 调用 `create_group`
3. 循环：若 `is_leader(g)`，用唯一幂等键 `propose` 计数命令
4. 每 2s 打印每 Group 的 value + leader

- [ ] **Step 2: 脚本在端口 `base`、`base+1`、`base+2` 启动 3 个进程**

```bash
# scripts/run_demo_cluster.sh
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-21000}"
DATA="${ROOT}/.demo-data"
rm -rf "$DATA"
mkdir -p "$DATA"
cargo build -p multiraft-demo
for id in 1 2 3; do
  port=$((BASE_PORT + id - 1))
  "$ROOT/target/debug/multiraft-demo" \
    --node-id "$id" --base-port "$BASE_PORT" --groups 10 \
    --data-dir "$DATA/node-$id" >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
done
echo "cluster started; logs under $DATA"
```

- [ ] **Step 3: 手工冒烟 — 跑 30s，日志显示 10 个 Group 有 Leader 且计数器增长**

- [ ] **Step 4: 提交**

```bash
git commit -am "$(cat <<'EOF'
feat(demo): 3-node 10-group multiraft-demo binary and launcher

EOF
)"
```

---

### Task 8: 验收自动化（规格 §5.2）

**文件：**
- Create: `scripts/acceptance.sh`
- Create: `crates/multiraft-demo/tests/acceptance.rs` (or script-driven)

覆盖全部六条标准：

| # | Automation |
|---|------------|
| 1 | Propose to 10 groups; assert FSM values |
| 2 | Assert `unique_peer_links() < 10` and `<= 6` for 3 nodes |
| 3 | `kill $(cat node-L.pid)` where L is current leader node; wait until other nodes report new leaders |
| 4 | Before kill, record committed propose ids; after failover, query FSM (HTTP or admin RPC) — all present |
| 5 | Restart killed node; wait until caught up |
| 6 | Unit/integration `not_leader` from Task 5 |

- [ ] **Step 1: 为 Demo 增加最小 Admin 查询** — 例如本机 HTTP `GET /groups/{id}/value` 与 `GET /metrics/links`，便于脚本断言而无需刮日志。

- [ ] **Step 2: 编写 `scripts/acceptance.sh`，仅当全部检查通过时 exit 0**

```bash
# Key fragments
./scripts/run_demo_cluster.sh
sleep 5
# write workload via demo admin or `--inject` subcommand
# snapshot committed set
kill "$(cat .demo-data/node-$LEADER.pid)"
# wait leaders
# verify values
# restart node
# verify catch-up
```

- [ ] **Step 3: 跑验收 — PASS**

```bash
chmod +x scripts/*.sh
./scripts/acceptance.sh
```

期望：exit code 0；打印 `ACCEPTANCE OK`。

- [ ] **Step 4: 提交**

```bash
git commit -am "$(cat <<'EOF'
test: add acceptance script for 10-group failover and durability

EOF
)"
```

---

### Task 9: 文档润色 + 锁定说明

**文件：**
- Modify: `README.md`
- Modify: 仓内 docs/specs/2026-07-18-multiraft-design.md 状态 → Approved / Implemented-phase1 when done
- Create: `multiraft/docs/upstream.md`

- [ ] **Step 1: 记录锁定版本、如何跑 acceptance、以及下游集成说明（二期不在本计划范围）**

- [ ] **Step 2: 将 `docs/specs/2026-07-18-multiraft-design.md` 状态行更新为 `Approved — 2026-07-18`（实现状态另记）。**

- [ ] **Step 3: 最终 `cargo test --workspace` + `./scripts/acceptance.sh`**

- [ ] **Step 4: 提交**

```bash
git commit -am "$(cat <<'EOF'
docs: README, upstream pin, acceptance runbook

EOF
)"
```

---

## Spec 覆盖检查清单

| Spec item | Task |
|-----------|------|
| Independent repo + crate split | 1, 3–7 |
| openraft + openraft-multi | 3, 4 |
| FSM trait / 不依赖撮合引擎 FSM | 2 |
| RMQ path B (documented; demo injects) | Spec copy; demo Task 7 |
| ≥10 groups shared connection | 4, 7, 8 |
| Kill leader, commit durable | 8 |
| Restart recovery | 6, 8 |
| NotLeader | 5, 8 |
| No dynamic membership / no TiKV fork | enforced by scope |
| Phase-2 撮合进程 / 入站壳 | explicitly out of this plan |

## 占位 / 一致性自检

- 各处版本锁定为 `0.10.0-alpha.30`。
- `GroupId` / `NodeId` = `u64`，与 FSM crate 一致。
- OpenRaft trait 体**改编自锁定上游示例**，不可凭空手写 — Task 3 明确要求从 `/tmp/openraft-0.10.0-alpha.30/examples/multi-raft-kv` 转录。
- 一期任务中无 RMQ 代码（与规格范围说明一致）。
