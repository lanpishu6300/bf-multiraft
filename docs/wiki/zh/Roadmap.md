# 路线图

**English：** [en/Roadmap.md](../en/Roadmap.md)

## 一期（本仓，已落地）

- [x] openraft + openraft-multi 薄 Multi-Raft
- [x] 文件持久化、重启恢复
- [x] 多进程 gRPC Demo（≥10 Group）
- [x] `acceptance.sh` / `chaos.sh` / porcupine
- [x] 本地 Jepsen（counter + kill nemesis）
- [x] Consistency Contract + `read_linearizable`

## 二期（`downstream matching engine`）

- [ ] Leader 消费 RMQ → `propose`
- [ ] FSM 适配 `match-core` + 幂等键
- [ ] 生产指标（propose 延迟、落后 index、切主次数）
- [ ] 持久化 / snapshot 策略加固

## 明确不做（近期）

- Region split/merge、PD、动态 membership
- 用 Raft 替代 RMQ 定序（路径 A）
- Follower LeaseRead 作生产读默认
