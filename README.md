# multiraft

Thin **Multi-Raft** library for matching matching HA — one Raft group per trading
symbol, shared peer connections, pluggable FSM.

Built on [openraft](https://github.com/databendlabs/openraft) + `openraft-multi`
(exact pin). Companion to [downstream matching engine](https://github.com/lanpishu6300/downstream matching engine);
Phase-1 stops at the runtime + demo. RMQ Leader propose / real match FSM is Phase-2
in that repo.

**License:** [Apache License 2.0](LICENSE)  
**中文：** [README.zh-CN.md](README.zh-CN.md)  
**Wiki：** [English](docs/wiki/en/Home.md) · [中文](docs/wiki/zh/Home.md)

---

## Features

- Multi-group Raft in one process; peer links **O(nodes)**, not O(groups)
- `MultiRaft` facade: `propose`, `read_linearizable`, leader callbacks
- File-backed log / state / snapshot per group (restart recovery)
- Multi-process gRPC demo (`multiraft-demo`) + admin HTTP for ops / Jepsen
- Acceptance, chaos scripts, porcupine linearizability test, local Jepsen suite

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│ multiraft-demo (3 OS processes × N groups)                  │
│  Admin HTTP  ·  CounterFsm  ·  kill/restart scripts         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
                       multiraft-net
                    (MultiRaft + shared gRPC)
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   multiraft-core      multiraft-fsm       multiraft-store
   (types/errors)      (StateMachine)     (per-group files)
```

| Crate | Role |
|-------|------|
| `multiraft-core` | `TypeConfig`, `ClusterConfig`, errors / `ProposeOk` |
| `multiraft-net` | Shared router + **`MultiRaft`** facade |
| `multiraft-fsm` | Pluggable state machine trait |
| `multiraft-store` | File-backed Raft storage |
| `multiraft-demo` | 3-node × N-group acceptance target |

Full notes: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Dependency pin

| Crate | Version |
|-------|---------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

See [docs/upstream.md](docs/upstream.md).

---

## Quick Start

### Prerequisites

- Rust (recent stable)
- macOS / Linux
- Optional for Jepsen: Java **17+**, `lein`

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
cargo test --workspace
```

### Multi-process demo

```bash
./scripts/run_demo_cluster.sh
```

With `--base-port 21000`:

| Node | Raft gRPC | Admin HTTP |
|------|-----------|------------|
| 1 | `127.0.0.1:21000` | `http://127.0.0.1:21100` |
| 2 | `127.0.0.1:21001` | `http://127.0.0.1:21101` |
| 3 | `127.0.0.1:21002` | `http://127.0.0.1:21102` |

```bash
# Linearizable read / propose (for external clients / Jepsen)
curl -s http://127.0.0.1:21100/groups/0/value
curl -s -X POST http://127.0.0.1:21100/groups/0/inc \
  -H 'content-type: application/json' -d '{"delta":1}'
```

CLI: `--mode`, `--node-id`, `--nodes`, `--base-port`, `--groups`, `--data-dir`,
`--no-auto-propose`. Set `JEPSEN=1` or `NO_AUTO_PROPOSE=1` on
`run_demo_cluster.sh` to disable the background propose loop.

### Consistency (per group)

| API | Model |
|-----|--------|
| `propose` Ok | Linearizable write |
| `read_linearizable` | Linearizable read (ReadIndex) |
| `with_fsm` | Local / may be stale — debug only |

See [docs/jepsen.md](docs/jepsen.md).

---

## Verification

```bash
./scripts/acceptance.sh          # ≥10 groups, kill real leader PID
./scripts/chaos.sh               # SCENARIO=kill_leader|rolling|...
./scripts/run_jepsen.sh          # local Clojure Jepsen (~30s)
./scripts/test_all.sh            # unit + chaos_failover + acceptance

cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
cargo test -p multiraft-net --test chaos_failover
```

Chaos checklist: [docs/chaos-checklist.md](docs/chaos-checklist.md).  
Clean `target/` before large rebuilds if disk is tight.

---

## Relation to downstream matching engine

```text
Phase-1 (this repo)     → runtime + demo + consistency tests
Phase-2 (downstream matching engine) → RMQ Leader propose → FSM → match-core
```

---

## Documentation

| Doc | Topic |
|-----|-------|
| [docs/README.md](docs/README.md) | Index |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Crate boundaries |
| [docs/specs/2026-07-18-multiraft-design.md](docs/specs/2026-07-18-multiraft-design.md) | Design |
| [docs/wiki/en/Home.md](docs/wiki/en/Home.md) | Wiki |
