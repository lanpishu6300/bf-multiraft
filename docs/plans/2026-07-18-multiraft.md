# multiraft Implementation Plan

**中文：** [2026-07-18-multiraft.zh-CN.md](./2026-07-18-multiraft.zh-CN.md)

**Goal:** Deliver independent repo `multiraft`: openraft + openraft-multi thin Multi-Raft runtime with ≥10 groups, shared connections, 3-node failover that does not lose committed commands.

**Architecture:** One openraft instance per `GroupId`; shared `GroupRouter` for peer RPC; pluggable `StateMachine` trait; static 3-node membership. Phase-1 demo injects proposes locally (no RMQ). Reference implementation pattern: [openraft `examples/multi-raft-kv`](https://github.com/databendlabs/openraft/tree/main/examples/multi-raft-kv).

**Tech Stack:** Rust 2021, Tokio, `openraft` + `openraft-multi` **pinned to `0.10.0-alpha.30`** (must match), serde, tonic or in-process router for tests, tracing.

**Spec:** [`docs/specs/2026-07-18-multiraft-design.md`](../specs/2026-07-18-multiraft-design.md) · [中文](../specs/2026-07-18-multiraft-design.zh-CN.md)

**Workdir:** repository root (ensure `cargo` is on `PATH`).

**Upstream reference (clone once for reading, do not vendor whole TiKV):**
```bash
git clone --depth 1 --branch v0.10.0-alpha.30 \
  https://github.com/databendlabs/openraft.git /tmp/openraft-0.10.0-alpha.30
# Study: /tmp/openraft-0.10.0-alpha.30/examples/multi-raft-kv/
```

---

## File map

| Path | Responsibility |
|------|----------------|
| `Cargo.toml` | Workspace root |
| `README.md` | How to build/run demo + acceptance |
| `docs/specs/2026-07-18-multiraft-design.md` | Approved design spec (in-repo) |
| `crates/multiraft-fsm/` | `StateMachine` trait + `ApplyOut` |
| `crates/multiraft-store/` | Per-group log + SM storage (start: memory; then file-backed) |
| `crates/multiraft-net/` | Shared router, connection counter, `GroupRouter` impl |
| `crates/multiraft-core/` | `ClusterConfig`, `MultiRaft`, propose / leader APIs |
| `crates/multiraft-demo/` | 3-node × 10-group binary + acceptance scripts |
| `scripts/acceptance.sh` | Kill-leader / restart orchestration |

---

### Task 1: Scaffold independent repo + workspace

**Files:**
- Create: `$REPO_ROOT/Cargo.toml`
- Create: `$REPO_ROOT/README.md`
- Create: `$REPO_ROOT/.gitignore`
- Create: `$REPO_ROOT/crates/multiraft-fsm/{Cargo.toml,src/lib.rs}`
- Create: `$REPO_ROOT/docs/specs/2026-07-18-multiraft-design.md` (keep in-repo under docs/specs/)

- [ ] **Step 1: Init git repo and ignore target**

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

> If crates.io alpha resolution fails, use git deps pointed at tag `v0.10.0-alpha.30` for both crates (same commit).

- [ ] **Step 3: Stub `multiraft-fsm` only (other crates in later tasks)**

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

Temporarily comment out non-existent members in workspace `members` until Task 2–5 create them, **or** create empty stub crates with `pub fn stub() {}` so `cargo check` works.

- [ ] **Step 4: Ensure in-repo spec + README skeleton**

```bash
mkdir -p docs/specs
# Keep the approved design at docs/specs/2026-07-18-multiraft-design.md (in-repo only)
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

- [ ] **Step 5: Verify + commit**

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

### Task 2: Demo FSM with idempotency (TDD)

**Files:**
- Create: `crates/multiraft-fsm/src/counter_fsm.rs`
- Modify: `crates/multiraft-fsm/src/lib.rs`
- Create: `crates/multiraft-fsm/tests/counter_fsm.rs`

- [ ] **Step 1: Write failing tests**

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

- [ ] **Step 2: Run tests — expect FAIL**

```bash
cargo test -p multiraft-fsm --test counter_fsm
```

Expected: compile error `CounterFsm` not found.

- [ ] **Step 3: Implement `CounterFsm`**

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

Add to `lib.rs`: `mod counter_fsm; pub use counter_fsm::CounterFsm;` and deps `serde`, `serde_json` in `multiraft-fsm/Cargo.toml`.

- [ ] **Step 4: Run tests — expect PASS**

```bash
cargo test -p multiraft-fsm --test counter_fsm
```

- [ ] **Step 5: Commit**

```bash
git add crates/multiraft-fsm
git commit -m "$(cat <<'EOF'
feat(fsm): add CounterFsm with idempotent apply and snapshot

EOF
)"
```

---

### Task 3: Adapt openraft TypeConfig + memory store from upstream example

**Files:**
- Create: `crates/multiraft-store/` (full crate)
- Create: `crates/multiraft-core/src/type_config.rs` (or under store)

**Procedure (follow the pinned openraft tag; do not guess API names):**

- [ ] **Step 1: Clone reference at pinned tag** (see Workdir section).

- [ ] **Step 2: Copy/adapt these files from `examples/multi-raft-kv` + shared example crates (`log-mem`, `sm-mem`, `types-kv`) into `multiraft-store` / `multiraft-core`:**
  - Type aliases / `RaftTypeConfig` impl → `crates/multiraft-core/src/type_config.rs`
  - In-memory log storage → `crates/multiraft-store/src/log_mem.rs`
  - State machine bridge that calls `multiraft_fsm::StateMachine` → `crates/multiraft-store/src/sm_bridge.rs`

Rename groups from string `"users"` to `GroupId: u64`. Keep openraft trait impls compiling against `=0.10.0-alpha.30`.

- [ ] **Step 3: Unit smoke test — single group mem cluster (1 node) client_write**

```rust
// crates/multiraft-store/tests/single_node_write.rs
// Pattern: follow multi-raft-kv test_cluster bootstrap for ONE group, ONE node.
// Assert: after client_write, CounterFsm value == expected.
```

Bootstrap from upstream `tests/cluster/test_cluster.rs` at the pinned tag (API names change between alphas — copy, then rename).

- [ ] **Step 4: `cargo test -p multiraft-store` PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "$(cat <<'EOF'
feat(store): adapt openraft TypeConfig and memory log/SM bridge

EOF
)"
```

---

### Task 4: Shared network router + connection counting

**Files:**
- Create: `crates/multiraft-net/src/{lib.rs,router.rs,conn_metrics.rs}`
- Adapt from: upstream `examples/multi-raft-kv/src/{router.rs,network.rs}`

- [ ] **Step 1: Write test for connection cardinality**

```rust
// crates/multiraft-net/tests/shared_connections.rs
#[tokio::test]
async fn peer_connections_are_o_nodes_not_o_groups() {
    // Start 3 in-process nodes, create 10 groups, drive heartbeats/writes.
    // Assert: router.unique_peer_links() <= 3 * 2  (or == number of directed edges among 3 nodes)
    // Assert: router.unique_peer_links() < 10  (must NOT be per-group)
}
```

Implement `Router::unique_peer_links()` as count of distinct peer node ids with an open channel (increment on first connect to peer, never per group).

- [ ] **Step 2: Implement `GroupRouter` wrapping shared channels**

Use `openraft_multi::{GroupRouter, GroupNetworkAdapter, GroupNetworkFactory}` as in crate README. Route key = `(target_node_id, group_id)`.

For **phase-1 tests**, prefer the example’s in-process `Router` (channels) over tonic — faster CI. Optional tonic behind feature `net-tonic` later.

- [ ] **Step 3: Run test — PASS**

```bash
cargo test -p multiraft-net --test shared_connections
```

- [ ] **Step 4: Commit**

```bash
git commit -am "$(cat <<'EOF'
feat(net): shared GroupRouter with O(nodes) connection metric

EOF
)"
```

---

### Task 5: `MultiRaft` public API

**Files:**
- Create: `crates/multiraft-core/src/{lib.rs,config.rs,multiraft.rs,error.rs}`

- [ ] **Step 1: Define API surface matching the spec**

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

- [ ] **Step 2: Failing integration test for NotLeader**

```rust
// crates/multiraft-core/tests/not_leader.rs
#[tokio::test]
async fn propose_on_follower_returns_not_leader() {
    // Bootstrap 3 nodes, 1 group; wait until leader known.
    // Call propose on a follower handle → MultiRaftError::NotLeader { .. }
}
```

- [ ] **Step 3: Implement `propose` via openraft `Raft::client_write` / write API from pinned example; map non-leader errors to `NotLeader`.**

- [ ] **Step 4: Wire `on_leader_change` by watching openraft metrics (`RaftMetrics` / equivalent in 0.10) per group.**

- [ ] **Step 5: Tests PASS + commit**

```bash
cargo test -p multiraft-core
git commit -am "$(cat <<'EOF'
feat(core): MultiRaft start/create_group/propose/leader APIs

EOF
)"
```

---

### Task 6: File-backed persistence + restart recovery

**Files:**
- Create: `crates/multiraft-store/src/log_file.rs` (or adapt openraft file example if present)
- Modify: `ClusterConfig.data_dir` usage in `MultiRaft::start`

- [ ] **Step 1: Test**

```rust
// crates/multiraft-store/tests/restart_recover.rs
#[tokio::test]
async fn restart_replays_committed_state() {
    // node1 single or 3-node: propose 5 cmds to group 1, shut down cleanly,
    // restart with same data_dir, assert CounterFsm value unchanged / caught up.
}
```

- [ ] **Step 2: Implement durable log under `{data_dir}/group-{id}/`**

Minimum: persist raft log entries + hard state so restart does not empty FSM. Snapshot optional for this task if log replay alone passes the test.

- [ ] **Step 3: PASS + commit**

```bash
git commit -am "$(cat <<'EOF'
feat(store): file-backed raft log for restart recovery

EOF
)"
```

---

### Task 7: `multiraft-demo` — 3 nodes × 10 groups

**Files:**
- Create: `crates/multiraft-demo/src/main.rs`
- Create: `crates/multiraft-demo/Cargo.toml`
- Create: `scripts/run_demo_cluster.sh`

- [ ] **Step 1: Binary accepts `--node-id`, `--base-port`, `--groups 10`**

Each process:
1. `MultiRaft::start`
2. `create_group` for `0..groups`
3. Loop: if `is_leader(g)`, `propose` counter cmds with unique idem keys
4. Every 2s print per-group value + leader

- [ ] **Step 2: Script starts 3 processes on ports `base`, `base+1`, `base+2`**

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

- [ ] **Step 3: Manual smoke — 30s run, logs show 10 groups with leaders and increasing counters**

- [ ] **Step 4: Commit**

```bash
git commit -am "$(cat <<'EOF'
feat(demo): 3-node 10-group multiraft-demo binary and launcher

EOF
)"
```

---

### Task 8: Acceptance automation (spec §5.2)

**Files:**
- Create: `scripts/acceptance.sh`
- Create: `crates/multiraft-demo/tests/acceptance.rs` (or script-driven)

Cover all six criteria:

| # | Automation |
|---|------------|
| 1 | Propose to 10 groups; assert FSM values |
| 2 | Assert `unique_peer_links() < 10` and `<= 6` for 3 nodes |
| 3 | `kill $(cat node-L.pid)` where L is current leader node; wait until other nodes report new leaders |
| 4 | Before kill, record committed propose ids; after failover, query FSM (HTTP or admin RPC) — all present |
| 5 | Restart killed node; wait until caught up |
| 6 | Unit/integration `not_leader` from Task 5 |

- [ ] **Step 1: Add minimal admin query on demo** — e.g. localhost HTTP `GET /groups/{id}/value` and `GET /metrics/links` so scripts can assert without scraping logs.

- [ ] **Step 2: Write `scripts/acceptance.sh` that exits 0 only if all checks pass**

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

- [ ] **Step 3: Run acceptance — PASS**

```bash
chmod +x scripts/*.sh
./scripts/acceptance.sh
```

Expected: exit code 0; prints `ACCEPTANCE OK`.

- [ ] **Step 4: Commit**

```bash
git commit -am "$(cat <<'EOF'
test: add acceptance script for 10-group failover and durability

EOF
)"
```

---

### Task 9: Docs polish + pin note

**Files:**
- Modify: `README.md`
- Modify: in-repo docs/specs/2026-07-18-multiraft-design.md status → Approved / Implemented-phase1 when done
- Create: `multiraft/docs/upstream.md`

- [ ] **Step 1: Document pinned versions, how to run acceptance, and Downstream integration (phase 2 out of scope)**

- [ ] **Step 2: Update `docs/specs/2026-07-18-multiraft-design.md` status line to `Approved — 2026-07-18` (implementation status separate).

- [ ] **Step 3: Final `cargo test --workspace` + `./scripts/acceptance.sh`**

- [ ] **Step 4: Commit**

```bash
git commit -am "$(cat <<'EOF'
docs: README, upstream pin, acceptance runbook

EOF
)"
```

---

## Spec coverage checklist

| Spec item | Task |
|-----------|------|
| Independent repo + crate split | 1, 3–7 |
| openraft + openraft-multi | 3, 4 |
| FSM trait / no matching-engine-FSM dep | 2 |
| RMQ path B (documented; demo injects) | Spec copy; demo Task 7 |
| ≥10 groups shared connection | 4, 7, 8 |
| Kill leader, commit durable | 8 |
| Restart recovery | 6, 8 |
| NotLeader | 5, 8 |
| No dynamic membership / no TiKV fork | enforced by scope |
| Phase-2 matching process / ingress shell | explicitly out of this plan |

## Placeholder / consistency self-review

- Versions locked to `0.10.0-alpha.30` everywhere.
- `GroupId` / `NodeId` = `u64` consistent with FSM crate.
- OpenRaft trait bodies are **adapted from pinned upstream example**, not hand-waved — Task 3 explicitly requires transcription from `/tmp/openraft-0.10.0-alpha.30/examples/multi-raft-kv`.
- No RMQ code in phase-1 tasks (matches spec range note).
