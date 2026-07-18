# multiraft 撮合专用薄 Multi-Raft 设计

**English：** [2026-07-18-multiraft-design.md](./2026-07-18-multiraft-design.md)

**日期：** 2026-07-18  
**状态：** Approved — 2026-07-18; Phase-1 + gRPC cross-process (Phase-1.5) implemented in multiraft  

**关联：** [downstream matching engine](https://github.com/lanpishu6300/downstream matching engine) 撮合引擎设计；本仓架构见 [ARCHITECTURE.md](../ARCHITECTURE.md) · [中文](../ARCHITECTURE.zh-CN.md)；一致性 / Jepsen 见 [jepsen.md](../jepsen.md) · [中文](../jepsen.zh-CN.md)  
**代码根：** 本仓库 `multiraft`

---

## 0. 决策摘要

| 项 | 选择 |
|----|------|
| 产品形态 | 撮合专用薄 Multi-Raft 运行时（非 TiKV raftstore 厚 fork） |
| 共识库 | **openraft** + **openraft-multi**（共享连接、按 group 路由） |
| 仓库 | 独立仓 `multiraft`；`downstream matching engine` 二期再依赖 |
| 与 RMQ | **路径 B**：RMQ 仍入站；仅 Leader propose/复制；Follower 只跟状态 |
| 分片 | `GroupId` ↔ symbol（稳定映射）；无 Region split/merge |
| 一期验收 | ≥10 Group、3 节点、共享连接、杀 Leader 后已 commit 不丢 |

**否决：** 厚 fork TiKV `raftstore`（C）；Raft 替代 RMQ 定序（路径 A，本期不做）。

---

## 1. 背景与目标

### 1.1 背景

现网 / `downstream matching engine` 按 symbol 单线程撮合。多副本高可用若走 SOFAJRaft / TiKV Multi-Raft 厚栈，会带上 Region 调度、split/merge、PD 等撮合用不上的能力。Rust 侧无 SofaJRaft 等价物；需要一个**薄** Multi-Raft 库，专供「每交易对一个 Raft Group」。

### 1.2 目标（一期）

1. 独立仓库交付可运行的 Multi-Raft 运行时（openraft 多 Group）。  
2. 节点间连接复用，不随 Group 数线性增长。  
3. `multiraft-demo`：≥10 Group、3 节点；杀 Leader 后已 commit 命令不丢，FSM 可对账。  
4. FSM 通过 trait 注入；库本身不依赖 `match-core` / RocketMQ。

### 1.3 非目标（一期）

- 不接真实 RMQ、不改 `match-contract` 生产路径。  
- 不做动态 membership、PD、Region split/merge、Hibernate Region。  
- 不设性能 SLO（可打点，不卡门）。  
- 不做 Follower 只读 / LeaseRead。  
- 不厚 fork TiKV `raftstore` 代码树。

---

## 2. 仓结构与边界

```text
multiraft/
├── Cargo.toml
├── README.md
├── docs/specs/          # 可链到本文件或存放副本
└── crates/
    ├── multiraft-core/  # Group 生命周期、propose、领导权
    ├── multiraft-net/   # openraft-multi 适配、共享连接
    ├── multiraft-store/ # log / hard state / snapshot 持久化
    ├── multiraft-fsm/   # StateMachine trait
    └── multiraft-demo/  # 3 节点 × ≥10 Group 验收
```

| Crate | 做 | 不做 |
|-------|----|------|
| `multiraft-core` | 创建/销毁 Group、propose、领导权查询与回调 | 业务字段、RMQ |
| `multiraft-net` | `(node, group_id)` 路由、连接复用 | 撮合协议 |
| `multiraft-store` | 每 Group 独立 log 空间 | 订单簿 |
| `multiraft-fsm` | `apply` / `snapshot` / `restore` trait | 依赖 `match-core` |
| `multiraft-demo` | 假 FSM + 杀进程验切主 | 生产部署 |

**与 `downstream matching engine`（二期）：**

```text
match-contract (RMQ consumer, Leader only)
  → multiraft (propose / leader callbacks)
    → FSM 适配器 → match-core
```

---

## 3. 数据流（RMQ 后复制）

> **范围说明：** 本节描述与 `downstream matching engine` 集成后的目标架构。一期 `multiraft-demo` **不接 RMQ**，用本地注入 propose 模拟 Ingress；切主后「重放未 ack 命令」用脚本模拟 at-least-once。

### 3.1 角色

| 角色 | 职责 |
|------|------|
| Ingress（仅 Leader） | 消费 RMQ；校验；symbol → `group_id`；`propose` |
| Raft（三节点） | 复制 log；多数派 commit |
| FSM（每节点） | apply 已 commit 条目；Leader/Follower 同一套 apply |
| Egress（仅 Leader） | apply 后出站（二期接 `match-contract`） |

Follower **不消费 RMQ**。

### 3.2 主路径

```text
RMQ (per-symbol)
  → [Leader] parse/validate
  → propose(group_id, cmd_bytes)
  → openraft 复制至多数派 → commit
  → 各节点 FSM.apply(cmd)
  → [Leader] 出站 / 业务 ack（二期）
  → [Leader] RMQ ack（建议 commit+apply 成功后）
```

### 3.3 切主与幂等

```text
Leader 宕机 → 各 Group 选新主
  → 新 Leader 以已 commit 为准继续，并开始消费 RMQ
  → RMQ at-least-once 重投 → cmd 带幂等键 → FSM 去重
```

| 保证 | 不保证 |
|------|--------|
| 已 commit 的指令不丢；存活副本 FSM 一致 | 未 commit 的 propose（靠 RMQ 重投 + 幂等） |

### 3.4 多 Group

同进程内 N 个 openraft 逻辑组；`GroupRouter` 共享连接，消息带 `group_id`。仅当前为 Leader 的 Group 才从 RMQ propose。

---

## 4. 最小接口

### 4.1 标识

- `NodeId = u64`
- `GroupId = u64`（symbol 稳定映射：配置表或确定性 hash）
- 幂等键编入命令字节（如业务 `uniqId`）

### 4.2 FSM（`multiraft-fsm`）

```text
apply(group, index, data) -> ApplyOut
snapshot(group) -> bytes
restore(group, snapshot) -> ()
```

`ApplyOut.effects`：可选，供 Leader 出站；Follower 可丢弃。  
一期 demo：计数器或简易 KV + 幂等去重。

### 4.3 运行时（`multiraft-net::MultiRaft`；类型在 core）

```text
start / start_cluster / start_grpc(ClusterConfig) -> MultiRaft
create_group(group, members)
propose(group, data) -> ProposeOk { index, term }   // 多数派 commit+apply 后返回
read_linearizable(group, f) -> R                    // ReadIndex 后读 FSM
with_fsm(group, f) -> R                             // 本地读，可能 stale（调试用）
is_leader(group) / leader(group)
on_leader_change(callback)   // Ingress 启停该 group 的 RMQ 消费
```

非 Leader `propose` / `read_linearizable` → `NotLeader { hint }`。  
一期 **静态** 3 节点成员，不做在线加减节点。

### 4.3.1 Consistency Contract（per Group）

对标 [Jepsen Consistency Models](https://jepsen.io/consistency/models)：每个 `GroupId`（symbol）是**一个对象**。

| API | 承诺模型 | 说明 |
|-----|----------|------|
| `propose` Ok | **Linearizable 写** | Ok ⇒ 写已进入多数派提交历史且已 apply；失败/超时 ⇒ **不确定**，客户端须同幂等键重试 |
| `read_linearizable` | **Linearizable 读** | 与集群写历史实时序一致；非 Leader → `NotLeader` |
| `with_fsm` | **无强一致承诺**（local / eventual） | 仅观测/调试；禁止作下单 ACK、查单真值、清算依据 |
| 多 Group | **组内** linearizable；**组间无事务** | 跨 symbol 原子需另建协调，不承诺 Strict Serializable |
| 出站事件（二期） | 前缀一致有序流 | 不必 linearizable；带 `index`/`term` |

**RMQ（二期 MUST）：** 仅 Leader 消费；**commit+apply 成功后再 ack**；切主后 at-least-once 重投 + FSM 幂等键。

**不确定写：** 超时 / 连接断开 / 切主窗口的 propose 不得当作「确定失败」而不重试（无幂等时会双写）。

### 4.4 网络（`multiraft-net`）

基于 `openraft-multi`：`GroupRouter` 实现 append/vote/snapshot 且携带 `group_id`。  
节点间连接数目标：**O(节点数)**，非 O(Group)。

### 4.5 存储（`multiraft-store`）

满足 openraft 的 log / state / snapshot 持久化；每 Group 独立空间（目录或前缀）。  
一期：文件或 openraft 示例级存储（可重启恢复）。二期再升 RocksDB 等。

### 4.6 配置

```text
ClusterConfig {
  node_id, peers: [(NodeId, addr)], data_dir,
  election / heartbeat timeouts
}
```

---

## 5. 一期验收

### 5.1 环境

- 3 节点（同机多端口即可）
- ≥10 Group
- 共享连接
- 不接真实 RMQ / `match-core`

### 5.2 必须通过

| # | 场景 | 通过标准 |
|---|------|----------|
| 1 | 多 Group 写入 | 10 Group 并行 propose；各 FSM 终态与输入一致 |
| 2 | 共享连接 | 可证明 peer 连接数为 O(节点) |
| 3 | 杀 Leader | 剩余节点选出新 Leader |
| 4 | commit 不丢 | 已成功返回的 propose 在恢复后仍存在于 FSM |
| 5 | 重启恢复 | 单节点重启后追上并对齐 |
| 6 | 非 Leader propose | `NotLeader`，无分叉状态 |

### 5.3 Done 定义

- `cargo test` + demo 一键起 3 节点  
- 上表 1–6 可脚本复现  

---

## 6. 二期（挂号）

1. `downstream matching engine` 依赖本库：Leader 消费 RMQ → propose  
2. FSM 适配 `match-core` + 幂等键  
3. 持久化与 snapshot 策略加固  
4. 指标：propose 延迟、落后 index、Leader 切换次数  

---

## 7. 风险与缓解

| 风险 | 缓解 |
|------|------|
| `openraft-multi` 偏新、API 可能变 | 一期锁版本；net 层隔离，必要时自研薄 Router |
| RMQ 与 Raft 双层 at-least-once | 命令幂等键强制；文档写清 ack 时机 |
| 多 Group 同进程选举风暴 | 错开选举抖动；二期再考虑闲 Group 降心跳 |
| 与撮合集成延迟超预期 | 一期库与集成拆开；demo 先绿 |

---

## 8. 为何不厚叉 TiKV raftstore（摘要）

撮合分片是稳定 symbol，非无限 KV keyspace。用不上 Region split/merge、PD 调度、百万 Region 级 Hibernate 与 KV apply 管道。只借鉴「多 Peer + 共享连接 + batch ready」模式；实现选 openraft 族而非搬 `raftstore` 代码树。
