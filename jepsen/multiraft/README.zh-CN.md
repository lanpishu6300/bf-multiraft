# Jepsen: multiraft（本地）

**English：** [README.md](./README.md)

针对多进程 `multiraft-demo` Admin HTTP API 的本地 Jepsen 套件。
无远程 SSH VM — 节点是 `127.0.0.1` 上的 OS 进程。

## 前置条件

- Java **17+**（此处 Java 22 可与 Jepsen 0.3.9 配合；若依赖失败，改用 Java 17）
- [Leiningen](https://leiningen.org/)（`lein` 在 `PATH` 上）
- 已构建 Demo：`cargo build -p multiraft-demo`

## 快速运行

在 multiraft 仓库根目录：

```bash
./scripts/run_jepsen.sh
```

会构建 Demo，以 `NO_AUTO_PROPOSE=1` / `GROUPS=1` 启动 3 节点集群，
跑约 30s 的 counter 测试（含 kill/restart nemesis），然后拆除集群。

可选 Standby（Learner，不在 Jepsen `:nodes` 中）：

```bash
STANDBY=1 ./scripts/run_jepsen.sh
```

客户端只接受 `"consistency":"linearizable"`；`"stale"` / `"local"` 记为失败。
nemesis 只杀/启 voter `1..NODES`。

## 手动

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
export BASE_PORT=23000 GROUPS=1 NODES=3
export DATA_DIR="$PWD/.jepsen-data"
export DEMO_BIN="$PWD/target/debug/multiraft-demo"
export MULTIRAFT_ROOT="$PWD"
export JEPSEN=1 NO_AUTO_PROPOSE=1

./scripts/run_demo_cluster.sh
# wait until admins respond, then:
cd jepsen/multiraft
lein run test -- --time-limit 30 --concurrency 6
```

## 负载

| Op | HTTP |
| --- | --- |
| `:add` | `POST /groups/0/inc` `{"delta":1}` |
| `:read` | `GET /groups/0/value` — 仅接受 `"consistency":"linearizable"` |

Checker：`jepsen.checker/counter`。

Nemesis：对 `$DATA/node-$id.pid` 本地 `kill -9`，经 `multiraft-demo --no-auto-propose` 重启。

## Admin 端口

`BASE_PORT=23000` 时：

| Node | Admin |
| --- | --- |
| `"1"` | `http://127.0.0.1:23100` |
| `"2"` | `http://127.0.0.1:23101` |
| `"3"` | `http://127.0.0.1:23102` |
