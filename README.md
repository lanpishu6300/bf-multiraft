# multiraft

Thin Multi-Raft library for matching HA, built on [openraft](https://github.com/datafuselabs/openraft) and `openraft-multi`.

This is an independent Rust workspace (not part of `downstream matching engine`). Early tasks scaffold the crate layout; openraft wiring comes later.

## Workspace

| Crate | Role |
| --- | --- |
| `multiraft-fsm` | Pluggable state machine trait |
| `multiraft-store` | Raft log / state storage (stub) |
| `multiraft-net` | Shared GroupRouter + `MultiRaft` facade (`use multiraft_net::MultiRaft`) |
| `multiraft-core` | TypeConfig, `ClusterConfig`, `MultiRaftError` / `ProposeOk` |
| `multiraft-demo` | Minimal demo binary (stub) |

## Build & test

```bash
cargo test --workspace
```

Check a single crate:

```bash
cargo check -p multiraft-fsm
```

## Design

See [docs/specs/2026-07-18-multiraft-design.md](docs/specs/2026-07-18-multiraft-design.md).
