# 一致性与测试

**English：** [en/Consistency.md](../en/Consistency.md)

完整说明：[docs/jepsen.md](../../jepsen.md) · Chaos：[docs/chaos-checklist.md](../../chaos-checklist.md)

## Consistency Contract（每 Group）

| API | 承诺 |
|-----|------|
| `propose` → Ok | **Linearizable 写**（多数派 commit + apply） |
| `read_linearizable` | **Linearizable 读**（ReadIndex） |
| `with_fsm` | 本地 / 可能 stale — 仅调试 |
| 跨 Group | 无跨 symbol 事务 |

超时 / 断连的 `propose` 为**不确定写** — 须用同一幂等键重试。

## Porcupine（CI 门禁）

进程内线性一致性检查（含一次杀主）：

```bash
cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
```

## 本地 Jepsen

Clojure Jepsen 通过 Admin HTTP 打多进程 Demo（`checker/counter` + kill/restart nemesis）：

```bash
./scripts/run_jepsen.sh
```

报告目录：`jepsen/multiraft/store/latest/`（`results.edn`、`history.edn`、`timeline.html`）— 已 gitignore。

## Chaos

| 层 | 方式 |
|----|------|
| 进程内 | `cargo test -p multiraft-net --test chaos_failover` |
| 多进程 | `./scripts/chaos.sh`（`SCENARIO=kill_leader\|rolling\|…`） |
| 验收 | `./scripts/acceptance.sh`（≥10 Group，杀 Leader PID） |

完整场景矩阵见 [chaos-checklist.md](../../chaos-checklist.md)（C01…）。
