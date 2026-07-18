# multiraft — Thin Multi-Raft for Matching HA

**中文：** [2026-07-18-multiraft-design.zh-CN.md](./2026-07-18-multiraft-design.zh-CN.md)

**Date:** 2026-07-18  
**Status:** Approved — 2026-07-18; Phase-1 + gRPC cross-process (Phase-1.5) implemented in multiraft  

**Related:** [downstream matching engine](https://github.com/lanpishu6300/downstream matching engine) matching-engine design; this repo architecture: [ARCHITECTURE.md](../ARCHITECTURE.md) · [中文](../ARCHITECTURE.zh-CN.md); consistency / Jepsen: [jepsen.md](../jepsen.md) · [中文](../jepsen.zh-CN.md)  
**Code root:** this repository `multiraft`

---

## 0. Decision summary

| Item | Choice |
|------|--------|
| Product shape | Thin Multi-Raft runtime for matching (not a thick TiKV raftstore fork) |
| Consensus | **openraft** + **openraft-multi** (shared links, route by group) |
| Repository | Independent `multiraft`; `downstream matching engine` depends in Phase-2 |
| vs RMQ | **Path B:** RMQ remains ingress; only Leader proposes/replicates; Followers follow state |
| Sharding | `GroupId` ↔ symbol (stable mapping); no Region split/merge |
| Phase-1 acceptance | ≥10 Groups, 3 nodes, shared links, committed commands survive Leader kill |

**Rejected:** Thick fork of TiKV `raftstore` (C); Raft replacing RMQ sequencing (Path A — out of scope this period).

---

## 1. Background and goals

### 1.1 Background

Production / `downstream matching engine` matches one symbol per single thread. Multi-replica HA via SOFAJRaft / TiKV Multi-Raft thick stacks brings Region scheduling, split/merge, PD, and other capabilities matching does not need. There is no SofaJRaft equivalent on the Rust side; we need a **thin** Multi-Raft library for “one Raft Group per trading pair.”

### 1.2 Goals (Phase-1)

1. Ship a runnable Multi-Raft runtime in an independent repo (openraft multi-group).  
2. Reuse peer connections; link count must not grow linearly with Group count.  
3. `multiraft-demo`: ≥10 Groups, 3 nodes; after Leader kill, committed commands are not lost; FSM can be reconciled.  
4. FSM injected via trait; the library itself does not depend on `match-core` / RocketMQ.

### 1.3 Non-goals (Phase-1)

- No real RMQ; do not change the `match-contract` production path.  
- No dynamic membership, PD, Region split/merge, or Hibernate Region.  
- No performance SLO (metrics OK; not a gate).  
- No Follower read-only / LeaseRead.  
- No thick fork of the TiKV `raftstore` tree.

---

## 2. Repo layout and boundaries

```text
multiraft/
├── Cargo.toml
├── README.md
├── docs/specs/          # link to this file or keep a copy
└── crates/
    ├── multiraft-core/  # Group lifecycle, propose, leadership
    ├── multiraft-net/   # openraft-multi adapter, shared links
    ├── multiraft-store/ # log / hard state / snapshot persistence
    ├── multiraft-fsm/   # StateMachine trait
    └── multiraft-demo/  # 3 nodes × ≥10 Groups acceptance
```

| Crate | Does | Does not |
|-------|------|----------|
| `multiraft-core` | Create/destroy Group, propose, leadership query & callbacks | Business fields, RMQ |
| `multiraft-net` | `(node, group_id)` routing, connection reuse | Matching protocol |
| `multiraft-store` | Independent log space per Group | Order book |
| `multiraft-fsm` | `apply` / `snapshot` / `restore` trait | Depend on `match-core` |
| `multiraft-demo` | Fake FSM + process kill for failover | Production deploy |

**With `downstream matching engine` (Phase-2):**

```text
match-contract (RMQ consumer, Leader only)
  → multiraft (propose / leader callbacks)
    → FSM adapter → match-core
```

---

## 3. Data flow (RMQ then replicate)

> **Scope note:** This section describes the target architecture after integrating with `downstream matching engine`. Phase-1 `multiraft-demo` **does not** attach RMQ; it simulates Ingress with local propose injection. After failover, “replay unacked commands” is simulated with scripts for at-least-once.

### 3.1 Roles

| Role | Responsibility |
|------|----------------|
| Ingress (Leader only) | Consume RMQ; validate; symbol → `group_id`; `propose` |
| Raft (three nodes) | Replicate log; majority commit |
| FSM (every node) | Apply committed entries; Leader/Follower share the same apply path |
| Egress (Leader only) | Outbound after apply (Phase-2 via `match-contract`) |

Followers **do not** consume RMQ.

### 3.2 Happy path

```text
RMQ (per-symbol)
  → [Leader] parse/validate
  → propose(group_id, cmd_bytes)
  → openraft replicate to majority → commit
  → each node FSM.apply(cmd)
  → [Leader] egress / business ack (Phase-2)
  → [Leader] RMQ ack (recommended after commit+apply succeeds)
```

### 3.3 Failover and idempotency

```text
Leader down → each Group elects a new leader
  → new Leader continues from committed state and starts consuming RMQ
  → RMQ at-least-once redelivery → cmd carries idempotency key → FSM dedupes
```

| Guaranteed | Not guaranteed |
|------------|----------------|
| Committed commands are not lost; survivor FSM replicas agree | Uncommitted propose (rely on RMQ redelivery + idempotency) |

### 3.4 Multi-Group

N openraft logical groups in one process; `GroupRouter` shares connections; messages carry `group_id`. Only Groups for which this node is Leader propose from RMQ.

---

## 4. Minimal interface

### 4.1 Identifiers

- `NodeId = u64`
- `GroupId = u64` (stable symbol mapping: config table or deterministic hash)
- Idempotency key encoded into command bytes (e.g. business `uniqId`)

### 4.2 FSM (`multiraft-fsm`)

```text
apply(group, index, data) -> ApplyOut
snapshot(group) -> bytes
restore(group, snapshot) -> ()
```

`ApplyOut.effects`: optional, for Leader egress; Followers may discard.  
Phase-1 demo: counter or simple KV + idempotent dedupe.

### 4.3 Runtime (`multiraft-net::MultiRaft`; types in core)

```text
start / start_cluster / start_grpc(ClusterConfig) -> MultiRaft
create_group(group, members)
propose(group, data) -> ProposeOk { index, term }   // after majority commit+apply
read_linearizable(group, f) -> R                    // read FSM after ReadIndex
with_fsm(group, f) -> R                             // local read, may be stale (debug)
is_leader(group) / leader(group)
on_leader_change(callback)   // start/stop RMQ consume for that group on Ingress
```

Non-leader `propose` / `read_linearizable` → `NotLeader { hint }`.  
Phase-1 **static** 3-node membership; no online add/remove.

### 4.3.1 Consistency Contract (per Group)

Aligned with [Jepsen Consistency Models](https://jepsen.io/consistency/models): each `GroupId` (symbol) is **one object**.

| API | Promised model | Notes |
|-----|----------------|-------|
| `propose` Ok | **Linearizable write** | Ok ⇒ write is in majority commit history and applied; failure/timeout ⇒ **indeterminate** — client must retry with the same idempotency key |
| `read_linearizable` | **Linearizable read** | Real-time ordered with cluster write history; non-leader → `NotLeader` |
| `with_fsm` | **No strong guarantee** (local / eventual) | Observation/debug only; must not be used for order ACK, order truth, or settlement |
| Multi-Group | **Per-group** linearizable; **no cross-group transactions** | Cross-symbol atomicity needs separate coordination; Strict Serializable not promised |
| Egress events (Phase-2) | Prefix-consistent ordered stream | Need not be linearizable; carry `index`/`term` |

**RMQ (Phase-2 MUST):** Leader-only consume; **ack only after commit+apply succeeds**; after failover, at-least-once redelivery + FSM idempotency keys.

**Indeterminate writes:** Timeouts / disconnects / proposes in a leadership window must not be treated as “definitely failed” without retry (without idempotency this double-writes).

### 4.4 Network (`multiraft-net`)

Based on `openraft-multi`: `GroupRouter` implements append/vote/snapshot and carries `group_id`.  
Peer link count target: **O(nodes)**, not O(Groups).

### 4.5 Storage (`multiraft-store`)

Satisfy openraft log / state / snapshot persistence; independent space per Group (directory or prefix).  
Phase-1: file or openraft example-level storage (restart recovery). RocksDB etc. deferred to Phase-2.

### 4.6 Config

```text
ClusterConfig {
  node_id, peers: [(NodeId, addr)], data_dir,
  election / heartbeat timeouts
}
```

---

## 5. Phase-1 acceptance

### 5.1 Environment

- 3 nodes (multi-port on one machine is fine)
- ≥10 Groups
- Shared connections
- No real RMQ / `match-core`

### 5.2 Must pass

| # | Scenario | Pass criteria |
|---|----------|---------------|
| 1 | Multi-Group writes | Parallel propose to 10 Groups; each FSM end state matches inputs |
| 2 | Shared connections | Prove peer link count is O(nodes) |
| 3 | Kill Leader | Remaining nodes elect a new Leader |
| 4 | Commit durability | Successfully returned proposes still present in FSM after recovery |
| 5 | Restart recovery | Single node restart catches up and aligns |
| 6 | Non-leader propose | `NotLeader`, no forked state |

### 5.3 Definition of Done

- `cargo test` + one-command demo start for 3 nodes  
- Criteria 1–6 reproducible by script  

---

## 6. Phase-2 (backlog)

1. `downstream matching engine` depends on this library: Leader consumes RMQ → propose  
2. FSM adapts `match-core` + idempotency keys  
3. Harden persistence and snapshot policy  
4. Metrics: propose latency, lag index, Leader switch count  

---

## 7. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| `openraft-multi` is young; APIs may change | Pin versions in Phase-1; isolate net layer; thin custom Router if needed |
| Dual at-least-once (RMQ + Raft) | Mandate command idempotency keys; document ack timing |
| Election storms with many Groups in one process | Jitter election timeouts; consider quieter heartbeats for idle Groups in Phase-2 |
| Matching integration delayed | Split library vs integration; get demo green first |

---

## 8. Why not a thick TiKV raftstore fork (summary)

Matching shards are stable symbols, not an unbounded KV keyspace. Region split/merge, PD scheduling, million-Region Hibernate, and KV apply pipelines are unused. Borrow only the “multi-Peer + shared links + batch ready” pattern; implement on openraft family rather than moving the `raftstore` tree.
