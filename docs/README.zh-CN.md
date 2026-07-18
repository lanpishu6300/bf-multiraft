# 文档索引

**English：** [README.md](./README.md)

**multiraft** GitHub 仓库的自包含文档（导航不必依赖仓外链接）。下表每份文档均有英文 + 中文（`.zh-CN.md`）成对版本。

## 从这里开始

| Doc (EN) | 中文 | 读者 |
|----------|------|------|
| [wiki/en/Home.md](./wiki/en/Home.md) | [wiki/zh/Home.md](./wiki/zh/Home.md) | Wiki（入门 / FAQ / 路线图） |
| [../README.md](../README.md) | [../README.zh-CN.md](../README.zh-CN.md) | 仓库 README |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | [ARCHITECTURE.zh-CN.md](./ARCHITECTURE.zh-CN.md) | 贡献者 — crate 边界与契约 |
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | Consistency Contract、porcupine、Jepsen |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | Chaos / 切主覆盖 |
| [upstream.md](./upstream.md) | [upstream.zh-CN.md](./upstream.zh-CN.md) | openraft 锁定与升版说明 |

## 设计（specs）

| Spec (EN) | 中文 | 主题 |
|-----------|------|------|
| [2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) | [2026-07-18-multiraft-design.zh-CN.md](./specs/2026-07-18-multiraft-design.zh-CN.md) | 撮合高可用薄 Multi-Raft |

## 计划（plans）

| Plan (EN) | 中文 | 主题 |
|-----------|------|------|
| [2026-07-18-multiraft.md](./plans/2026-07-18-multiraft.md) | [2026-07-18-multiraft.zh-CN.md](./plans/2026-07-18-multiraft.zh-CN.md) | 一期库 + Demo |
| [2026-07-18-multiraft-grpc.md](./plans/2026-07-18-multiraft-grpc.md) | [2026-07-18-multiraft-grpc.zh-CN.md](./plans/2026-07-18-multiraft-grpc.zh-CN.md) | 跨进程 gRPC（Phase-1.5） |

## 运维

| Doc (EN) | 中文 | 主题 |
|----------|------|------|
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | 本地 Jepsen + porcupine CI 门禁 |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | 杀主 / 滚动 / 双杀 |
| [../jepsen/multiraft/README.md](../jepsen/multiraft/README.md) | [../jepsen/multiraft/README.zh-CN.md](../jepsen/multiraft/README.zh-CN.md) | Jepsen 套件布局 |
| [../scripts/](../scripts/) | — | `acceptance.sh`, `chaos.sh`, `run_jepsen.sh`, `test_all.sh` |

## 元文档

| Doc (EN) | 中文 |
|----------|------|
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | [../CONTRIBUTING.zh-CN.md](../CONTRIBUTING.zh-CN.md) |
| [../SUPPORT.md](../SUPPORT.md) | [../SUPPORT.zh-CN.md](../SUPPORT.zh-CN.md) |
| [../SECURITY.md](../SECURITY.md) | [../SECURITY.zh-CN.md](../SECURITY.zh-CN.md) |
