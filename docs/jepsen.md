# Jepsen & porcupine

**中文：** [jepsen.zh-CN.md](./jepsen.zh-CN.md)

This note describes the **Consistency Contract** for multiraft groups, the
in-process porcupine linearizability test, and the **local Jepsen** suite under
`jepsen/multiraft/`.

## Consistency Contract (per group)

| API | Guarantee |
| --- | --- |
| `propose(group, data) -> ProposeOk` | **Linearizable write.** The command is committed in the Raft log and applied to the FSM before Ok returns. |
| `read_linearizable(group, f)` | **Linearizable read** via ReadIndex: confirms leadership, then applies `f` to the local FSM. |
| Non-leader `propose` / `read_linearizable` | Returns `MultiRaftError::NotLeader { hint }`. Callers must retry on the leader (or another node after failover). |
| `with_fsm(group, f)` | **Local / may be stale.** Debug, metrics, or last-resort admin fallback only — not application truth. |

Demo admin:

- `GET /groups/{id}/value` — prefer `read_linearizable`; JSON includes `"consistency": "linearizable"` or `"local"` / `"stale": true`, or HTTP 503.
- `POST /groups/{id}/inc` — body `{"delta":1,"idem":null}`; proposes `CounterFsm::encode_add` on a local leader (NotLeader retry across local nodes). Use with `--no-auto-propose` so the background propose loop does not race Jepsen clients.

## Porcupine test (CI gate)

In-process check using [porcupine-rs](https://crates.io/crates/porcupine-rs):

```bash
cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
```

**Model:** a single counter/register per group. Mid-test leader shutdown once.
Only successful ops enter the history.

Related:

```bash
cargo test -p multiraft-net --test linearizable_read
```

## Real Jepsen (local, no SSH VMs)

Clojure Jepsen 0.3.9 suite drives the **multi-process gRPC demo** over admin HTTP
on `127.0.0.1`. Nodes are named `"1"` / `"2"` / `"3"` → admin
`http://127.0.0.1:(BASE_PORT+100+id-1)`.

### Run (recommended)

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
./scripts/run_jepsen.sh
```

What the wrapper does:

1. Notes free disk (`df`)
2. `cargo build -p multiraft-demo`
3. Starts `./scripts/run_demo_cluster.sh` with `JEPSEN=1` / `NO_AUTO_PROPOSE=1`, `GROUPS=1`, `BASE_PORT=23000`, data under `.jepsen-data/`
4. `cd jepsen/multiraft && lein run test -- --time-limit 30 --concurrency 6`
5. Stops demo processes on exit

Env knobs: `BASE_PORT`, `GROUPS`, `NODES`, `DATA_DIR`, `JEPSEN_TIME_LIMIT`, `JEPSEN_CONCURRENCY`, `JAVA_HOME`.

### Workload

| Op | Client |
| --- | --- |
| `:add` | `POST /groups/0/inc` `{"delta":1}` |
| `:read` | `GET /groups/0/value` — fail/retry unless `"consistency":"linearizable"` |

- **Checker:** `jepsen.checker/counter` (+ timeline/stats)
- **Nemesis:** local `kill -9` of `.jepsen-data/node-$id.pid`, restart via absolute `target/debug/multiraft-demo --no-auto-propose`
- **SSH:** `{:dummy? true}` — no remote VMs

Project layout: [jepsen/multiraft/README.md](../jepsen/multiraft/README.md) · [中文](../jepsen/multiraft/README.zh-CN.md).

Reports land under `jepsen/multiraft/store/latest/` (`results.edn`, `history.edn`,
`timeline.html`) and are gitignored — re-run to regenerate.

### Java / Leiningen

- Leiningen: install `lein` to `$HOME/bin` if missing (see repo scripts / Leiningen docs).
- **Java 17+** preferred; Java 22 works with Jepsen 0.3.9 in this workspace. If dependency resolution fails on 22, set `JAVA_HOME` to a JDK 17 install (`/usr/libexec/java_home -v 17`).

### Demo flags for external clients

```bash
NO_AUTO_PROPOSE=1 GROUPS=1 ./scripts/run_demo_cluster.sh
# or
JEPSEN=1 GROUPS=1 ./scripts/run_demo_cluster.sh
```

Equivalent CLI: `multiraft-demo --no-auto-propose ...`.

## What is / isn’t verified

| Verified | Not verified |
| --- | --- |
| Porcupine on in-process propose + `read_linearizable` under one leader kill | Network partitions / netem |
| Local Jepsen counter + process kill/restart (smoke via `run_jepsen.sh`) | Multi-hour / multi-datacenter Jepsen |
| Chaos failover scripts (`chaos.sh`) | Disk-full, Byzantine faults |
| Demo admin prefers linearizable reads; `/inc` for client-driven counters | That every admin caller honors `"stale": true` |

Porcupine remains the fast CI gate; local Jepsen exercises the real multi-process
demo and kill nemesis. Neither replaces a full remote Jepsen deployment with
network partitions.
