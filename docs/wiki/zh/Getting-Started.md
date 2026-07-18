# 快速开始

**English：** [en/Getting-Started.md](../en/Getting-Started.md)

## 前置

- Rust（建议与姊妹仓对齐的较新 stable；见 `rustc --version`）
- macOS / Linux
- Demo / Jepsen：可选 Java **17+**、[Leiningen](https://leiningen.org/)（`lein`）

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
```

## 克隆与测试

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
cargo test --workspace
```

## 多进程 Demo

```bash
./scripts/run_demo_cluster.sh
```

默认 `--base-port 21000`：

| Node | Raft gRPC | Admin HTTP |
|------|-----------|------------|
| 1 | `127.0.0.1:21000` | `http://127.0.0.1:21100` |
| 2 | `127.0.0.1:21001` | `http://127.0.0.1:21101` |
| 3 | `127.0.0.1:21002` | `http://127.0.0.1:21102` |

```bash
curl -s http://127.0.0.1:21100/groups/0/value
curl -s -X POST http://127.0.0.1:21100/groups/0/inc \
  -H 'content-type: application/json' -d '{"delta":1}'
```

## 验收 / Chaos / Jepsen

```bash
./scripts/acceptance.sh
./scripts/chaos.sh
./scripts/run_jepsen.sh          # ~30s smoke
./scripts/test_all.sh            # CHAOS=1 含 chaos.sh
```

大重建前建议清理 `target/` 以释放磁盘。

更多：[README.zh-CN.md](../../../README.zh-CN.md) · [jepsen.md](../../jepsen.md)
