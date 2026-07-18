# Documentation index

Self-contained docs for the **multiraft** GitHub repository (no links outside this tree required to navigate).

## Start here

| Doc | Audience |
|-----|----------|
| [wiki/zh/Home.md](./wiki/zh/Home.md) | 中文 Wiki（入门 / FAQ / 路线图） |
| [wiki/en/Home.md](./wiki/en/Home.md) | English Wiki |
| [../README.zh-CN.md](../README.zh-CN.md) | 中文 README |
| [../README.md](../README.md) | English README |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Contributors — crate boundaries & contract |
| [jepsen.md](./jepsen.md) | Consistency Contract, porcupine, Jepsen |
| [chaos-checklist.md](./chaos-checklist.md) | Chaos / failover coverage |
| [upstream.md](./upstream.md) | openraft pin & upgrade notes |

## Designs (specs)

| Spec | Topic |
|------|-------|
| [2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) | Thin Multi-Raft for matching HA |

## Plans

| Plan | Topic |
|------|-------|
| [2026-07-18-multiraft.md](./plans/2026-07-18-multiraft.md) | Phase-1 library + demo |
| [2026-07-18-multiraft-grpc.md](./plans/2026-07-18-multiraft-grpc.md) | Cross-process gRPC (Phase-1.5) |

## Operations

| Doc | Topic |
|-----|-------|
| [jepsen.md](./jepsen.md) | Local Jepsen + porcupine CI gate |
| [chaos-checklist.md](./chaos-checklist.md) | Kill leader / rolling / double-kill |
| [../scripts/](../scripts/) | `acceptance.sh`, `chaos.sh`, `run_jepsen.sh`, `test_all.sh` |
