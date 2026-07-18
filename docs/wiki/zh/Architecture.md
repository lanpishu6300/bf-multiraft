# 架构

**English：** [en/Architecture.md](../en/Architecture.md)

完整说明见：[docs/ARCHITECTURE.md](../../ARCHITECTURE.md)

## 定位

撮合专用**薄 Multi-Raft**：每交易对一个 Raft Group（`GroupId` ↔ symbol），节点间连接复用，FSM 可插拔。共识栈为 **openraft + openraft-multi**（精确锁版本）。

不厚 fork TiKV `raftstore`（无 Region split/merge / PD）。

## Crate 地图

```text
multiraft-core   → 共享类型 / 错误 / ClusterConfig
multiraft-net    → MultiRaft 门面 + 共享 gRPC / in-process Router
multiraft-fsm    → StateMachine trait（apply / snapshot / restore）
multiraft-store  → 每 Group 文件持久化
multiraft-demo   → 3 节点 × N Group 验收与 Jepsen 靶
```

## 拓扑

```text
Admin HTTP（每节点）     Raft gRPC（共享连接，O(nodes)）
        │                         │
   node-1 / node-2 / node-3  ←── 多 Group 同进程
```

- `--mode node`：一进程一 Raft 节点（跨进程）
- `--mode cluster`：进程内 3 逻辑节点（快测）

## 一致性（每 Group）

| API | 承诺 |
|-----|------|
| `propose` Ok | Linearizable 写 |
| `read_linearizable` | Linearizable 读（ReadIndex） |
| `with_fsm` | 本地 / 可能 stale（仅调试） |

组间**无事务**。详见 [jepsen.md](../../jepsen.md)。

## 二期集成（目标）

```text
match-contract（仅 Leader 消费 RMQ）
  → multiraft propose
  → FSM → match-core
```

## 相关设计

- [薄 Multi-Raft 设计](../../specs/2026-07-18-multiraft-design.md)
- [实现计划](../../plans/2026-07-18-multiraft.md)
- [gRPC 跨进程计划](../../plans/2026-07-18-multiraft-grpc.md)
