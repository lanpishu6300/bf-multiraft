# Architecture notes

**中文：** [ARCHITECTURE.zh-CN.md](./ARCHITECTURE.zh-CN.md)

Thin Multi-Raft runtime for matching HA. One Raft group per trading symbol
(`GroupId`); shared peer connections; pluggable FSM. Built on
`openraft` + `openraft-multi` (pinned).

## Phase rule

1. **Phase-1 (this repo):** library + multi-process demo + chaos / Jepsen.
   No RocketMQ, no matching engine FSM.
2. **Phase-2 (downstream app):** optional Leader RMQ consume → `propose`;
   pluggable matching engine FSM. Followers do not consume ingress.
3. Do not pull match DTOs / RMQ into `multiraft-*` crates.

## Crate responsibilities

```text
crates/
├── multiraft-core/   # TypeConfig, ClusterConfig, MultiRaftError, ProposeOk
├── multiraft-net/    # Shared GroupRouter / GrpcRouter + MultiRaft facade
├── multiraft-fsm/    # StateMachine trait (apply / snapshot / restore)
├── multiraft-store/  # Per-group file-backed log / state / snapshot
└── multiraft-demo/   # 3-node × N-group CounterFsm + admin HTTP
```

| Crate | Does | Does not |
|-------|------|----------|
| `multiraft-core` | Shared types / errors | Networking, storage |
| `multiraft-net` | `MultiRaft` API, O(nodes) links | Business commands |
| `multiraft-fsm` | Trait + demo `CounterFsm` | Depend on a matching engine FSM |
| `multiraft-store` | Per-group persistence | Order book |
| `multiraft-demo` | Acceptance / Jepsen target | Production deploy |

## Topology

```text
                    ┌──────────────────────────────────────┐
  Admin HTTP        │  OS process = one Raft node          │
  (per node)        │  MultiRaft + N groups (shared gRPC)  │
                    └───────────────┬──────────────────────┘
                                    │ tonic / openraft-multi
                    ┌───────────────┼──────────────────────┐
                    ▼               ▼                      ▼
                 node-1          node-2                 node-3
              groups 0..N-1   groups 0..N-1          groups 0..N-1
```

- `--mode node`: one OS process per Raft node (production-shaped).
- `--mode cluster`: in-process 3-logical-node harness for fast tests.
- Peer links: **O(nodes)**, not O(groups). `unique_peer_links()` exposes this.

## Data flow (phase-2 target)

```text
RMQ (per-symbol)
  → [Leader only] validate → propose(group, cmd)
  → openraft quorum commit → FSM.apply on all replicas
  → [Leader] egress / ack RMQ after commit+apply
```

Phase-1 demo injects `propose` locally (`POST /groups/{id}/inc` or background loop).

## Consistency Contract (per group)

| API | Model |
|-----|--------|
| `propose` → Ok | Linearizable write (committed + applied) |
| `read_linearizable` | Linearizable read (ReadIndex) |
| `with_fsm` | Local / may be stale — debug only |
| Cross-group | No cross-symbol transactions |

Failed / timed-out `propose` is **indeterminate** — retry with the same idempotency key.

Details: [specs/2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) · [中文](./specs/2026-07-18-multiraft-design.zh-CN.md) §4.3.1,
[jepsen.md](./jepsen.md) · [中文](./jepsen.zh-CN.md).

## Downstream integration (phase 2)

```text
matching process / ingress shell (RMQ consumer, Leader only)
  → multiraft::MultiRaft (propose / leader callbacks)
    → FSM adapter → matching engine FSM
```

## Standby async snapshot

Optional `SnapshotMode::StandbyOffload`: voters never sync-dump the FSM in
`build_snapshot`. A **Standby** (openraft Learner) applies a magic trigger log,
freezes the FSM briefly, then `spawn_blocking` serializes into a durable catalog
under `{data_dir}/snapshots/`. Voters pull advertisements on recovery.

Details: [specs/2026-07-20-standby-async-snapshot-design.md](./specs/2026-07-20-standby-async-snapshot-design.md)
· [中文](./specs/2026-07-20-standby-async-snapshot-design.zh-CN.md).

## Upstream pin

| Crate | Version |
|-------|---------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

See [upstream.md](./upstream.md) · [中文](./upstream.zh-CN.md).
