# Jepsen readiness & porcupine checker

This note describes the **Consistency Contract** for multiraft groups, the
in-process porcupine linearizability test we run today, and what a full Jepsen
suite would add later.

## Consistency Contract (per group)

| API | Guarantee |
| --- | --- |
| `propose(group, data) -> ProposeOk` | **Linearizable write.** The command is committed in the Raft log and applied to the FSM before Ok returns. |
| `read_linearizable(group, f)` | **Linearizable read** via ReadIndex: confirms leadership, then applies `f` to the local FSM. |
| Non-leader `propose` / `read_linearizable` | Returns `MultiRaftError::NotLeader { hint }`. Callers must retry on the leader (or another node after failover). |
| `with_fsm(group, f)` | **Local / may be stale.** Debug, metrics, or last-resort admin fallback only — not application truth. |

Demo admin `GET /groups/{id}/value` prefers `read_linearizable` on a local
leader (then any local node). Only if every linearizable attempt fails does it
fall back to `with_fsm` with `"consistency": "local"` and `"stale": true`, or
HTTP 503 if no FSM value is available.

## Porcupine test (today)

In-process, Jepsen-adjacent check using [porcupine-rs](https://crates.io/crates/porcupine-rs)
(a Rust port of [Porcupine](https://github.com/anishathalye/porcupine)):

```bash
cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
```

**Model:** a single counter/register per group.

| Op | Call window | Model step |
| --- | --- | --- |
| `Inc { delta }` | start of successful `propose` → `ProposeOk` | `state += delta` |
| `Read(value)` | start of successful `read_linearizable` → observed `i64` | accept iff `value == state` |

**Workload:**

- 3-node `MultiRaft::start_cluster`, one group
- 8 concurrent clients, mixed propose(+1) and `read_linearizable` for ~3s
- Mid-test: shut down the current leader once (chaos)
- Only **successful** ops are recorded (failed / `NotLeader` attempts are retried; the successful attempt’s call/return times go into the history)
- Assert: `porcupine_rs::check_operations::<CounterModel>(&history)`

Related unit tests (no porcupine):

```bash
cargo test -p multiraft-net --test linearizable_read
```

## Future: full Jepsen

A production-style Jepsen suite would typically:

1. **Client** — Clojure (or other) process driving the cluster over gRPC / admin HTTP: `propose` + `read_linearizable` against the real multi-process demo.
2. **Checker** — [Knossos](https://github.com/jepsen-io/knossos) or [Elle](https://github.com/jepsen-io/elle) on the recorded history (register / counter / list-append as appropriate).
3. **Nemesis** — process kill, pause, network partition, clock skew; map to our chaos scenarios in [docs/chaos-checklist.md](chaos-checklist.md).
4. **Background** — [jepsen.io/consistency](https://jepsen.io/consistency) for the consistency model taxonomy.

That work is **out of scope** for the current in-process porcupine test.

## What is / isn’t verified today

| Verified | Not verified |
| --- | --- |
| Successful propose + `read_linearizable` histories are linearizable under one in-process leader kill | Multi-process / gRPC Jepsen with network partitions |
| Follower `read_linearizable` returns `NotLeader` (unit test) | Disk-full, split-brain, Byzantine faults |
| Chaos failover keeps the cluster writable (`chaos_failover`, `chaos.sh`) | Knossos/Elle on long multi-hour histories |
| Demo admin prefers linearizable reads | That every admin caller honors `"stale": true` |

Porcupine today is a **fast CI gate** for the Consistency Contract, not a
substitute for a full Jepsen run against a deployed topology.
