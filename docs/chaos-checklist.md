# multiraft Chaos Engineering Checklist

**中文：** [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md)

**Goal:** Under fault injection, verify majority availability, no loss of committed entries, no fork, multi-group independence, and catch-up after recovery.

**Legend:** ✅ Automated · 🔶 Script coverage · ⬜ TODO

---

## 1. Process / node failures

| ID | Scenario | Expectation | Status | Implementation |
|----|----------|-------------|--------|----------------|
| C01 | Kill a single Follower | Cluster keeps writing; values increase monotonically | ✅ | `chaos_failover::kill_follower_cluster_stays_available` |
| C02 | Kill Leader → failover → kill a Follower | Majority still writable | ✅ | `failover_then_kill_follower_still_writes` (5 nodes) |
| C03 | Concurrent propose during leader transition | No panic; successes ≤ FSM; writable after failover | ✅ | `concurrent_propose_during_leader_shutdown` |
| C04 | Kill a node that leads multiple Groups | Each Group re-elects on survivors and keeps writing | ✅ | `multi_group_independent_failover` |
| C05 | Lose majority (kill 2 of 3) | Writes stall / time out | ✅ | `majority_lost_writes_stall` |
| C06 | Recover one node after majority loss | Writable after majority restored; values do not go backward | ✅ | `majority_loss_then_recover` (SharedFabric restart) |
| C07 | Rolling leader kill | Writable after each failover | ✅ | `rolling_leader_kill` (5 nodes) |
| C08 | Rolling restart of all nodes | Shutdown + restart on same disk one by one; values do not go backward | ✅ | `rolling_restart_all_nodes` |
| C09 | Multi-process `kill -9` random node | Failover; values do not go backward; restart catches up | 🔶 | `scripts/chaos.sh` `SCENARIO=random` |
| C10 | Multi-process targeted Leader kill | Same as above | 🔶 | `scripts/chaos.sh` `SCENARIO=kill_leader` |
| C11 | Multi-process targeted Follower kill | Writes continue; values do not go backward | 🔶 | `scripts/chaos.sh` `SCENARIO=kill_follower` |
| C12 | Multi-process rolling restart | Kill+start per node | 🔶 | `scripts/chaos.sh` `SCENARIO=rolling` |
| C13 | Multi-process double kill (serial) | Majority always retained; values do not go backward | 🔶 | `scripts/chaos.sh` `SCENARIO=double_kill` |

## 2. Data / consistency

| ID | Scenario | Expectation | Status | Implementation |
|----|----------|-------------|--------|----------------|
| C20 | Follower lag then catch-up | Stop follower → many proposes → restart → values align | ✅ | `asymmetric_lag_follower_catchup` |
| C21 | Replay same idempotency key after failover | FSM increments once only | ✅ | `idempotent_replay_across_failover` |
| C22 | Survivor replicas agree | After fault, max/min value delta among survivors is 0 (applied) | ✅ | `survivor_fsm_converges` |
| C23 | Successful propose survives leader kill | Record ProposeOk then kill leader; value ≥ successful deltas | ✅ | `committed_propose_survives_leader_kill` |

## 3. Multi-Group / load

| ID | Scenario | Expectation | Status | Implementation |
|----|----------|-------------|--------|----------------|
| C30 | Multi-Group + leader-kill storm | ≥5 Groups concurrent writes under kill; all writable eventually | ✅ | `multi_group_storm_under_leader_kill` |
| C31 | Link count does not grow with Groups | After churn, `unique_peer_links` still O(nodes) | ✅ | `peer_links_remain_o_nodes_under_churn` |

## 4. Explicitly out of scope (this period)

| ID | Scenario | Reason |
|----|----------|--------|
| C90 | Real network partition (iptables/tc) | Needs root/containers; approximate with shutdown |
| C91 | Clock jumps / disk full | Heavy environment dependency |
| C92 | Byzantine malicious nodes | Outside Raft threat model |

---

## How to run

```bash
# In-process automation from this checklist
cargo test -p multiraft-net --test chaos_failover -- --nocapture

# Multi-process scripts (composable)
./scripts/chaos.sh                          # SCENARIO=random ROUNDS=5
SCENARIO=kill_leader ROUNDS=3 ./scripts/chaos.sh
SCENARIO=kill_follower ROUNDS=3 ./scripts/chaos.sh
SCENARIO=rolling ROUNDS=1 ./scripts/chaos.sh
SCENARIO=double_kill ROUNDS=3 ./scripts/chaos.sh
SCENARIO=all ROUNDS=2 ./scripts/chaos.sh    # run all script scenarios in order

# One-shot
./scripts/test_all.sh
CHAOS=1 ./scripts/test_all.sh
```
