# Jepsen 与 porcupine

**English：** [jepsen.md](./jepsen.md)

本文说明 multiraft Group 的 **Consistency Contract**、进程内 porcupine
线性一致性测试，以及 `jepsen/multiraft/` 下的**本地 Jepsen** 套件。

## Consistency Contract（每 Group）

| API | 保证 |
| --- | --- |
| `propose(group, data) -> ProposeOk` | **Linearizable 写。** Ok 返回前，命令已进入 Raft log 多数派提交并已 apply 到 FSM。 |
| `read_linearizable(group, f)` | **Linearizable 读**（ReadIndex）：确认领导权后，对本地 FSM 执行 `f`。 |
| 非 Leader 的 `propose` / `read_linearizable` | 返回 `MultiRaftError::NotLeader { hint }`。调用方须在 Leader（或切主后其他节点）重试。 |
| `with_fsm(group, f)` | **本地 / 可能 stale。** 仅调试、指标或末级 Admin 回退 — 非业务真值。 |
| `read_stale(group, f)` | **Standby 卸载 / 本地。** 需 `enable_stale_queries`；带 applied 水位，**非** linearizable — Jepsen 拒绝 `"consistency":"stale"`。 |

Demo Admin：

- `GET /groups/{id}/value` — 优先 `read_linearizable`；JSON 含 `"consistency": "linearizable"` 或 `"local"` / `"stale"`（Standby 卸载），或 HTTP 503。
- `GET /groups/{id}/stale` — 显式 Standby 卸载读。
- `POST /groups/{id}/inc` — body `{"delta":1,"idem":null}`；在本地 Leader 上 propose `CounterFsm::encode_add`（跨本地节点 NotLeader 重试）。配合 `--no-auto-propose`，避免后台 propose 循环与 Jepsen 客户端竞态。

## Porcupine 测试（CI 门禁）

进程内检查，使用 [porcupine-rs](https://crates.io/crates/porcupine-rs)：

```bash
cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
```

**模型：** 每 Group 一个计数器/寄存器。测试中途杀一次 Leader。
仅成功的操作进入历史。

相关：

```bash
cargo test -p multiraft-net --test linearizable_read
```

## 真实 Jepsen（本地，无 SSH VM）

Clojure Jepsen 0.3.9 套件驱动**多进程 gRPC Demo** 的 Admin HTTP，
目标为 `127.0.0.1`。节点名 `"1"` / `"2"` / `"3"` → Admin
`http://127.0.0.1:(BASE_PORT+100+id-1)`。

### 运行（推荐）

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
./scripts/run_jepsen.sh
```

包装脚本会：

1. 记录磁盘空闲（`df`）
2. `cargo build -p multiraft-demo`
3. 以 `JEPSEN=1` / `NO_AUTO_PROPOSE=1`、`GROUPS=1`、`BASE_PORT=23000` 启动 `./scripts/run_demo_cluster.sh`，数据在 `.jepsen-data/`
4. `cd jepsen/multiraft && lein run test -- --time-limit 30 --concurrency 6`
5. 退出时停止 Demo 进程

环境变量：`BASE_PORT`、`GROUPS`、`NODES`、`DATA_DIR`、`JEPSEN_TIME_LIMIT`、`JEPSEN_CONCURRENCY`、`JAVA_HOME`。

### 负载

| Op | Client |
| --- | --- |
| `:add` | `POST /groups/0/inc` `{"delta":1}` |
| `:read` | `GET /groups/0/value` — 除非 `"consistency":"linearizable"` 否则 fail/retry |

- **Checker：** `jepsen.checker/counter`（+ timeline/stats）
- **Nemesis：** 对 `.jepsen-data/node-$id.pid` 本地 `kill -9`，用绝对路径 `target/debug/multiraft-demo --no-auto-propose` 重启
- **SSH：** `{:dummy? true}` — 无远程 VM

项目布局：[jepsen/multiraft/README.md](../jepsen/multiraft/README.md) · [中文](../jepsen/multiraft/README.zh-CN.md)。

报告落在 `jepsen/multiraft/store/latest/`（`results.edn`、`history.edn`、
`timeline.html`），已 gitignore — 重新跑才会生成。

### Java / Leiningen

- Leiningen：若缺失，将 `lein` 装到 `$HOME/bin`（见仓内脚本 / Leiningen 文档）。
- 推荐 **Java 17+**；本工作区 Java 22 可与 Jepsen 0.3.9 配合。若 22 上依赖解析失败，将 `JAVA_HOME` 指向 JDK 17（`/usr/libexec/java_home -v 17`）。

### 外部客户端用 Demo 标志

```bash
NO_AUTO_PROPOSE=1 GROUPS=1 ./scripts/run_demo_cluster.sh
# or
JEPSEN=1 GROUPS=1 ./scripts/run_demo_cluster.sh
```

等价 CLI：`multiraft-demo --no-auto-propose ...`。

## 已验证 / 未验证

| 已验证 | 未验证 |
| --- | --- |
| 进程内 propose + `read_linearizable` 的 Porcupine（一次杀主） | 网络分区 / netem |
| 本地 Jepsen counter + 进程 kill/restart（经 `run_jepsen.sh` 冒烟） | 多小时 / 跨机房 Jepsen |
| Chaos 切主脚本（`chaos.sh`） | 磁盘满、Byzantine 故障 |
| Demo Admin 优先 linearizable 读；`/inc` 供客户端驱动计数 | 每个 Admin 调用方都尊重 `"stale": true` |

Porcupine 仍是快速 CI 门禁；本地 Jepsen 锻炼真实多进程 Demo 与 kill nemesis。
二者都不能替代带网络分区的完整远程 Jepsen 部署。
