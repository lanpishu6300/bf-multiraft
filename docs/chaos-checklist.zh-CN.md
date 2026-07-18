# multiraft Chaos Engineering Checklist

**English：** [chaos-checklist.md](./chaos-checklist.md)

**目标：** 在故障注入下验证多数派可用、已 commit 不丢、无分叉、多 Group 独立、恢复后追上。

**图例：** ✅ 已自动化 · 🔶 脚本覆盖 · ⬜ 待做

---

## 1. 进程 / 节点故障

| ID | 场景 | 期望 | 状态 | 实现 |
|----|------|------|------|------|
| C01 | 杀单个 Follower | 集群继续写入，值单调增 | ✅ | `chaos_failover::kill_follower_cluster_stays_available` |
| C02 | 杀 Leader → 切主 → 再杀 Follower | 多数派仍可写 | ✅ | `failover_then_kill_follower_still_writes`（5 节点） |
| C03 | 切主窗口并发 propose | 不 panic；成功数 ≤ FSM；切主后可写 | ✅ | `concurrent_propose_during_leader_shutdown` |
| C04 | 杀「带多个 Group Leader」的节点 | 各 Group 在存活节点重选主并继续写 | ✅ | `multi_group_independent_failover` |
| C05 | 丢失多数派（3 中杀 2） | 写入停滞 / 超时失败 | ✅ | `majority_lost_writes_stall` |
| C06 | 丢失多数派后恢复一个节点 | 恢复多数后可写，值不回退 | ✅ | `majority_loss_then_recover`（SharedFabric 重启） |
| C07 | 连续杀主（rolling leader kill） | 每次切主后仍可写 | ✅ | `rolling_leader_kill`（5 节点） |
| C08 | 滚动重启全部节点 | 逐个 shutdown+同盘重启；值不回退 | ✅ | `rolling_restart_all_nodes` |
| C09 | 多进程 `kill -9` 随机节点 | 切主、值不回退、重启追上 | 🔶 | `scripts/chaos.sh` `SCENARIO=random` |
| C10 | 多进程定点杀 Leader | 同上 | 🔶 | `scripts/chaos.sh` `SCENARIO=kill_leader` |
| C11 | 多进程定点杀 Follower | 写入不中断，值不回退 | 🔶 | `scripts/chaos.sh` `SCENARIO=kill_follower` |
| C12 | 多进程滚动重启 | 逐节点 kill+start | 🔶 | `scripts/chaos.sh` `SCENARIO=rolling` |
| C13 | 多进程双杀（串行） | 始终保留多数；值不回退 | 🔶 | `scripts/chaos.sh` `SCENARIO=double_kill` |

## 2. 数据 / 一致性

| ID | 场景 | 期望 | 状态 | 实现 |
|----|------|------|------|------|
| C20 | Follower 落后后追上 | 停 follower → 多笔 propose → 重启 → 值对齐 | ✅ | `asymmetric_lag_follower_catchup` |
| C21 | 切主后同幂等键重放 | FSM 只加一次 | ✅ | `idempotent_replay_across_failover` |
| C22 | 存活副本值一致 | 故障后存活节点 max/min 值差为 0（已 apply） | ✅ | `survivor_fsm_converges` |
| C23 | 已成功 propose 在杀主后仍在 | 记录 ProposeOk 后杀主，值 ≥ 已成功增量 | ✅ | `committed_propose_survives_leader_kill` |

## 3. 多 Group / 负载

| ID | 场景 | 期望 | 状态 | 实现 |
|----|------|------|------|------|
| C30 | 多 Group + 杀主风暴 | ≥5 Group 并发写时杀主，最终均可写 | ✅ | `multi_group_storm_under_leader_kill` |
| C31 | 连接数不随 Group 膨胀 | 故障后 `unique_peer_links` 仍 O(nodes) | ✅ | `peer_links_remain_o_nodes_under_churn` |

## 4. 明确不做（本期）

| ID | 场景 | 原因 |
|----|------|------|
| C90 | 真实网络分区（iptables/tc） | 需 root/容器；用 shutdown 近似 |
| C91 | 时钟跳变 / 磁盘满 | 环境依赖重 |
| C92 | Byzantine 恶意节点 | 超出 Raft 威胁模型 |

---

## 如何跑

```bash
# 清单内 in-process 自动化
cargo test -p multiraft-net --test chaos_failover -- --nocapture

# 多进程脚本（可组合）
./scripts/chaos.sh                          # SCENARIO=random ROUNDS=5
SCENARIO=kill_leader ROUNDS=3 ./scripts/chaos.sh
SCENARIO=kill_follower ROUNDS=3 ./scripts/chaos.sh
SCENARIO=rolling ROUNDS=1 ./scripts/chaos.sh
SCENARIO=double_kill ROUNDS=3 ./scripts/chaos.sh
SCENARIO=all ROUNDS=2 ./scripts/chaos.sh    # 依次跑完脚本场景

# 一键
./scripts/test_all.sh
CHAOS=1 ./scripts/test_all.sh
```
