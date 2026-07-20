# 路线图

**English：** [en/Roadmap.md](../en/Roadmap.md)

## 一期（本仓，已落地）

- [x] openraft + openraft-multi 薄 Multi-Raft
- [x] 文件持久化、重启恢复
- [x] 多进程 gRPC Demo（≥10 Group）
- [x] `acceptance.sh` / `chaos.sh` / porcupine
- [x] 本地 Jepsen（counter + kill nemesis）
- [x] Consistency Contract + `read_linearizable`

## 一期加固 / 库能力

- [x] Standby 异步快照（对齐 Aeron 的 Learner 卸载）— 见 [设计](../../specs/2026-07-20-standby-async-snapshot-design.zh-CN.md)
- [x] Aeron Standby Premium 对等 **P0**：从 ad HTTP 拉取 + standby 复制限速 — [对等设计](../../specs/2026-07-20-aeron-standby-parity-design.zh-CN.md)
- [x] Aeron Standby Premium 对等 **P1**：`promote_standby` / `demote_to_standby` 切换
- [ ] Aeron Standby Premium 对等 **P2**：daisy-chain / 多 standby / 流式拉取
- [ ] Aeron Standby Premium 对等 **P3**：Standby 只读服务卸载

## 二期（下游应用）

- [ ] 可选 Leader 消费 RMQ → `propose`
- [ ] 可插拔撮合引擎 FSM + 幂等键
- [ ] 生产指标（propose 延迟、落后 index、切主次数）
- [x] 持久化 / snapshot 策略加固（StandbyOffload catalog；更多 hardening 待续）

## 明确不做（近期）

- Region split/merge、PD、动态 membership
- 用 Raft 替代 RMQ 定序（路径 A）
- Follower LeaseRead 作生产读默认
