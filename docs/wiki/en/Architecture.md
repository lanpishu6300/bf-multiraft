# Architecture

**中文：** [zh/Architecture.md](../zh/Architecture.md)

Full notes: [docs/ARCHITECTURE.md](../../ARCHITECTURE.md) · [中文](../../ARCHITECTURE.zh-CN.md)

## Positioning

Thin Multi-Raft for matching HA: one Raft group per symbol (`GroupId`),
shared peer connections, pluggable FSM. Stack: **openraft + openraft-multi**
(exact pin). Not a TiKV `raftstore` fork.

## Crate map

```text
multiraft-core   → shared types / errors / ClusterConfig
multiraft-net    → MultiRaft facade + shared gRPC / in-process Router
multiraft-fsm    → StateMachine trait
multiraft-store  → per-group file persistence
multiraft-demo   → 3-node × N-group acceptance + Jepsen target
```

## Topology

```text
Admin HTTP (per node)     Raft gRPC (shared, O(nodes))
        │                         │
   node-1 / node-2 / node-3  ←── many groups per process
```

- `--mode node`: one OS process per Raft node
- `--mode cluster`: in-process 3-logical-node harness

## Consistency (per group)

| API | Guarantee |
|-----|-----------|
| `propose` Ok | Linearizable write |
| `read_linearizable` | Linearizable read (ReadIndex) |
| `with_fsm` | Local / may be stale (debug only) |

No cross-group transactions. See [Consistency & testing](./Consistency.md) · [jepsen.md](../../jepsen.md) · [中文](../../jepsen.zh-CN.md).

## Phase-2 integration (target)

```text
match-contract (Leader-only RMQ)
  → multiraft propose
  → FSM → match-core
```

## Related designs

- [Design spec](../../specs/2026-07-18-multiraft-design.md) · [中文](../../specs/2026-07-18-multiraft-design.zh-CN.md)
- [Implementation plan](../../plans/2026-07-18-multiraft.md) · [中文](../../plans/2026-07-18-multiraft.zh-CN.md)
- [gRPC plan](../../plans/2026-07-18-multiraft-grpc.md) · [中文](../../plans/2026-07-18-multiraft-grpc.zh-CN.md)
