# multiraft

Thin Multi-Raft library for matching HA, built on [openraft](https://github.com/datafuselabs/openraft) and `openraft-multi`.

Independent Rust workspace (not part of `downstream matching engine`). Phase-1 delivers a single-process demo and acceptance suite; integrating RMQ Leader propose into matching is phase 2 and out of scope here.

## Dependency pin

| Crate | Version |
| --- | --- |
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

Exact pins (`=`) keep the alpha Multi-Raft API stable. See [docs/upstream.md](docs/upstream.md).

## Architecture

| Crate | Role |
| --- | --- |
| `multiraft-core` | `TypeConfig`, `ClusterConfig`, `MultiRaftError` / `ProposeOk`, shared types |
| `multiraft-net` | Shared `GroupRouter` + **`MultiRaft` facade** (`use multiraft_net::MultiRaft`) |
| `multiraft-fsm` | Pluggable state machine trait |
| `multiraft-store` | File-backed Raft log / state for restart recovery |
| `multiraft-demo` | 3 logical nodes × N groups `CounterFsm` demo + localhost admin |

**Phase-1 topology:** one OS process hosts **3 logical nodes** and N Raft groups over an in-process shared `Router`. Multi-OS-process clustering needs a future TCP / tonic transport (not phase-1).

## Demo & acceptance

```bash
# Build + launch (logs under .demo-data/)
./scripts/run_demo_cluster.sh

# Or run directly:
cargo run -p multiraft-demo -- \
  --mode cluster --base-port 21000 --groups 10 --data-dir .demo-data
```

CLI flags: `--node-id`, `--base-port`, `--groups`, `--data-dir`, `--mode`, `--nodes`.

Every ~2s the process logs per-group `leader` + CounterFsm `value`. Admin HTTP:

- `GET http://127.0.0.1:<base-port>/groups/{id}/value`
- `GET http://127.0.0.1:<base-port>/metrics/links`
- `POST http://127.0.0.1:<base-port>/admin/shutdown_node/{id}` (acceptance failover)

Phase-1 acceptance (10 groups, simulate Leader loss, durability check):

```bash
./scripts/acceptance.sh
```

Optional env: `BASE_PORT` (default `21000`), `GROUPS` (default `10`), `ACCEPTANCE_DATA`.

## Build & test

```bash
cargo test --workspace
```

Check a single crate:

```bash
cargo check -p multiraft-fsm
```

## Relation to downstream matching engine

Phase-1 stops at the Multi-Raft runtime + demo. **Phase 2** (RMQ inbound → Leader propose / Follower follow, real match FSM) lives in `downstream matching engine` and is out of scope for this repo.

## Design

See [docs/specs/2026-07-18-multiraft-design.md](docs/specs/2026-07-18-multiraft-design.md).
