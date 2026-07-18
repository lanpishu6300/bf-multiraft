# 常见问题

**English：** [en/FAQ.md](../en/FAQ.md)

### 为什么不直接用 SofaJRaft / TiKV raftstore？

Rust 无官方 SofaJRaft；TiKV `raftstore` 厚在 Region / PD / split，撮合按稳定 symbol 分片用不上。本库只做薄 Multi-Raft（共享连接 + 多 Group）。

### `propose` 超时算失败吗？

不算确定失败。超时 / 断连 / 切主窗口为**不确定写**，须用同一幂等键重试。见 Consistency Contract。

### `with_fsm` 能当查单真值吗？

不能。可能 stale；生产读用 `read_linearizable`。

### Jepsen 报告在哪？

跑完后在 `jepsen/multiraft/store/latest/`（已 gitignore）。用例源码在 `jepsen/multiraft/src/`。更多见 [一致性与测试](./Consistency.md)。

### 和 downstream matching engine 什么关系？

本仓一期独立交付运行时。二期由 `downstream matching engine` 的 `match-contract` 依赖本库做 Leader propose。
