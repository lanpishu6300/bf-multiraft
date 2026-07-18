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
| `multiraft-demo` | 3-node × 10-group CounterFsm demo |

## Demo (Task 7)

Phase-1 networking is **in-process only** (shared `Router`). The demo defaults to
`--mode cluster`: one OS process hosting 3 logical nodes and N Raft groups via
`MultiRaft::start_cluster`. Multi-process `--mode node` is stubbed until Task 8
(tonic / TCP transport).

```bash
# Build + launch (writes logs under .demo-data/)
./scripts/run_demo_cluster.sh

# Or run directly:
cargo run -p multiraft-demo -- \
  --mode cluster --base-port 21000 --groups 10 --data-dir .demo-data
```

CLI flags: `--node-id`, `--base-port`, `--groups`, `--data-dir`, `--mode`, `--nodes`.

Every ~2s the process logs per-group `leader` + CounterFsm `value` (increasing).
Localhost admin (for Task 8 acceptance scripts):

- `GET http://127.0.0.1:<base-port>/groups/{id}/value`
- `GET http://127.0.0.1:<base-port>/metrics/links`

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
