# multiraft

Thin Multi-Raft library for matching HA, built on [openraft](https://github.com/datafuselabs/openraft) and `openraft-multi`.

Independent Rust workspace (not part of `downstream matching engine`). Phase-1 delivers a multi-process gRPC demo and acceptance suite; integrating RMQ Leader propose into matching is phase 2 and out of scope here.

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
| `multiraft-net` | Shared `GroupRouter` / `GrpcRouter` + **`MultiRaft` facade** (`use multiraft_net::MultiRaft`) |
| `multiraft-fsm` | Pluggable state machine trait |
| `multiraft-store` | File-backed Raft log / state for restart recovery |
| `multiraft-demo` | 3-node × N groups `CounterFsm` demo + per-node admin HTTP |

**Topology:** `--mode node` runs one OS process per Raft node over tonic gRPC. `--mode cluster` keeps an in-process 3-logical-node harness for quick regression.

## Demo & acceptance

```bash
# Build + launch 3 OS processes (logs / pids under .demo-data/)
./scripts/run_demo_cluster.sh

# Or run one node directly:
cargo run -p multiraft-demo -- \
  --mode node --node-id 1 --nodes 3 --base-port 21000 \
  --groups 10 --data-dir .demo-data/node-1
```

With `--base-port 21000`:

| Node | Raft gRPC | Admin HTTP |
| --- | --- | --- |
| 1 | `127.0.0.1:21000` | `http://127.0.0.1:21100` |
| 2 | `127.0.0.1:21001` | `http://127.0.0.1:21101` |
| 3 | `127.0.0.1:21002` | `http://127.0.0.1:21102` |

CLI flags: `--mode`, `--node-id`, `--nodes`, `--base-port`, `--groups`, `--data-dir`.

Admin endpoints (per process in `--mode node`):

- `GET /groups/{id}/value` — local FSM value + leader
- `GET /metrics/links` — `unique_peer_links()`

In-process regression (`--mode cluster`) still exposes `POST /admin/shutdown_node/{id}`.

Acceptance (10 groups, kill real leader PID, durability + catch-up):

```bash
./scripts/acceptance.sh
```

Optional env: `BASE_PORT` (default `21000`), `GROUPS` (default `10`), `ACCEPTANCE_DATA`, `NODES`.

## Consistency (per group)

| API | Model |
| --- | --- |
| `propose` Ok | Linearizable write |
| `read_linearizable` | Linearizable read (ReadIndex) |
| `with_fsm` | Local / may be stale — debug only |

See design §4.3.1. Jepsen (Knossos) can target this register via `propose` + `read_linearizable`.

## Build & test

```bash
cargo test --workspace
cargo test -p multiraft-net --test linearizable_read
cargo test -p multiraft-net --test chaos_failover
./scripts/chaos.sh   # optional multi-process chaos
```

Chaos coverage checklist: [docs/chaos-checklist.md](docs/chaos-checklist.md).

`scripts/chaos.sh` env:

| Env | Default | Notes |
| --- | --- | --- |
| `SCENARIO` | `random` | `random` · `kill_leader` · `kill_follower` · `rolling` · `double_kill` · `all` |
| `ROUNDS` | `5` | Per-scenario rounds (`rolling` = one full pass over nodes per round) |
| `NODES` / `GROUPS` / `BASE_PORT` | `3` / `5` / `22000` | Same as acceptance, different default port |

```bash
SCENARIO=kill_leader ROUNDS=3 ./scripts/chaos.sh
SCENARIO=all ROUNDS=1 ./scripts/chaos.sh
```

Or run the bundled suite (unit + chaos_failover + acceptance; set `CHAOS=1` for `chaos.sh`):

```bash
./scripts/test_all.sh
```

Check a single crate:

```bash
cargo check -p multiraft-fsm
```

## Relation to downstream matching engine

Phase-1 stops at the Multi-Raft runtime + demo. **Phase 2** (RMQ inbound → Leader propose / Follower follow, real match FSM) lives in `downstream matching engine` and is out of scope for this repo.

## Design

See [docs/specs/2026-07-18-multiraft-design.md](docs/specs/2026-07-18-multiraft-design.md).
