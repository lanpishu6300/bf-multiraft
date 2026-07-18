# 架构说明

**English：** [ARCHITECTURE.md](./ARCHITECTURE.md)

面向撮合高可用的薄 Multi-Raft 运行时。每个交易对一个 Raft Group
（`GroupId`）；节点间连接复用；FSM 可插拔。基于
`openraft` + `openraft-multi`（精确锁版本）。

## 阶段规则

1. **一期（本仓）：** 库 + 多进程 Demo + chaos / Jepsen。
   不接 RocketMQ，不接撮合引擎 FSM。
2. **二期（下游应用）：** 可选 Leader 消费 RMQ → `propose`；可插拔撮合引擎 FSM。
   Follower 不消费入站。
3. 不要把撮合 DTO / RMQ 拉进 `multiraft-*` crates。

## Crate 职责

```text
crates/
├── multiraft-core/   # TypeConfig, ClusterConfig, MultiRaftError, ProposeOk
├── multiraft-net/    # Shared GroupRouter / GrpcRouter + MultiRaft facade
├── multiraft-fsm/    # StateMachine trait (apply / snapshot / restore)
├── multiraft-store/  # Per-group file-backed log / state / snapshot
└── multiraft-demo/   # 3-node × N-group CounterFsm + admin HTTP
```

| Crate | 做 | 不做 |
|-------|------|----------|
| `multiraft-core` | 共享类型 / 错误 | 网络、存储 |
| `multiraft-net` | `MultiRaft` API，O(nodes) 连接 | 业务命令 |
| `multiraft-fsm` | Trait + demo `CounterFsm` | 依赖撮合引擎 FSM |
| `multiraft-store` | 每 Group 持久化 | 订单簿 |
| `multiraft-demo` | 验收 / Jepsen 靶标 | 生产部署 |

## 拓扑

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

- `--mode node`：每个 Raft 节点一个 OS 进程（贴近生产形态）。
- `--mode cluster`：进程内 3 逻辑节点，便于快速测试。
- Peer 连接：**O(nodes)**，非 O(groups)。`unique_peer_links()` 暴露该指标。

## 数据流（二期目标）

```text
RMQ (per-symbol)
  → [Leader only] validate → propose(group, cmd)
  → openraft quorum commit → FSM.apply on all replicas
  → [Leader] egress / ack RMQ after commit+apply
```

一期 Demo 本地注入 `propose`（`POST /groups/{id}/inc` 或后台循环）。

## Consistency Contract（每 Group）

| API | 模型 |
|-----|--------|
| `propose` → Ok | Linearizable 写（已 commit + applied） |
| `read_linearizable` | Linearizable 读（ReadIndex） |
| `with_fsm` | 本地 / 可能 stale — 仅调试 |
| Cross-group | 无跨 symbol 事务 |

失败 / 超时的 `propose` 结果**不确定** — 须用同一幂等键重试。

详情：[specs/2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) · [中文](./specs/2026-07-18-multiraft-design.zh-CN.md) §4.3.1，
[jepsen.md](./jepsen.md) · [中文](./jepsen.zh-CN.md)。

## 下游集成（二期）

```text
撮合进程 / 入站壳（RMQ consumer, Leader only）
  → multiraft::MultiRaft (propose / leader callbacks)
    → FSM 适配器 → 撮合引擎 FSM
```

## 上游锁定

| Crate | Version |
|-------|---------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

见 [upstream.md](./upstream.md) · [中文](./upstream.zh-CN.md)。
