# multiraft

面向 matching 撮合高可用的 **薄 Multi-Raft** 库：每交易对一个 Raft Group、节点间连接复用、FSM 可插拔。

基于 [openraft](https://github.com/databendlabs/openraft) + `openraft-multi`（精确锁版本）。姊妹仓为 [downstream matching engine](https://github.com/lanpishu6300/downstream matching engine)；本仓一期交付运行时与 Demo，RMQ Leader propose / 真实撮合 FSM 在二期接入该仓。

**许可：** [Apache License 2.0](LICENSE)  
**English：** [README.md](README.md)  
**Wiki：** [中文](docs/wiki/zh/Home.md) · [English](docs/wiki/en/Home.md)

---

## 特性

- 同进程多 Group；peer 连接 **O(节点)**，非 O(Group)
- `MultiRaft`：`propose` / `read_linearizable` / 领导权回调
- 每 Group 文件持久化（可重启恢复）
- 多进程 gRPC Demo + Admin HTTP（验收 / Jepsen）
- acceptance、chaos、porcupine、本地 Jepsen

---

## 架构

```text
multiraft-demo → multiraft-net (MultiRaft + 共享 gRPC)
                    ├── multiraft-core
                    ├── multiraft-fsm
                    └── multiraft-store
```

完整说明：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · Wiki [架构](docs/wiki/zh/Architecture.md)

### 依赖锁定

| Crate | 版本 |
|-------|------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

见 [docs/upstream.md](docs/upstream.md)。

---

## 快速开始

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
cargo test --workspace
./scripts/run_demo_cluster.sh
```

端口与 Admin API 见 [快速开始](docs/wiki/zh/Getting-Started.md)。

### 一致性（每 Group）

| API | 模型 |
|-----|------|
| `propose` Ok | Linearizable 写 |
| `read_linearizable` | Linearizable 读 |
| `with_fsm` | 本地 / 可能 stale（仅调试） |

详见 [docs/jepsen.md](docs/jepsen.md)。

---

## 验证

```bash
./scripts/acceptance.sh
./scripts/chaos.sh
./scripts/run_jepsen.sh
./scripts/test_all.sh
```

大重建前建议清理 `target/`。

---

## 与 downstream matching engine

```text
一期（本仓）     → 运行时 + Demo + 一致性测试
二期（downstream matching engine） → RMQ Leader propose → FSM → match-core
```

---

## 文档

| 文档 | 说明 |
|------|------|
| [docs/README.md](docs/README.md) | 索引 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Crate 边界 |
| [设计规格](docs/specs/2026-07-18-multiraft-design.md) | 设计 |
| [Wiki 首页](docs/wiki/zh/Home.md) | Wiki |
