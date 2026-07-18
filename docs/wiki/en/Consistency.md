# Consistency & testing

**中文：** [zh/Consistency.md](../zh/Consistency.md)

Full runbook: [docs/jepsen.md](../../jepsen.md) · Chaos: [docs/chaos-checklist.md](../../chaos-checklist.md)

## Consistency Contract (per group)

| API | Guarantee |
|-----|-----------|
| `propose` → Ok | **Linearizable write** (quorum commit + apply) |
| `read_linearizable` | **Linearizable read** (ReadIndex) |
| `with_fsm` | Local / may be stale — debug only |
| Cross-group | No cross-symbol transactions |

Timed-out / disconnected `propose` is **indeterminate** — retry with the same idempotency key.

## Porcupine (CI gate)

In-process linearizability check under one leader kill:

```bash
cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
```

## Local Jepsen

Clojure Jepsen drives the multi-process demo over admin HTTP (`checker/counter` + kill/restart nemesis):

```bash
./scripts/run_jepsen.sh
```

Reports: `jepsen/multiraft/store/latest/` (`results.edn`, `history.edn`, `timeline.html`) — gitignored.

## Chaos

| Layer | How |
|-------|-----|
| In-process | `cargo test -p multiraft-net --test chaos_failover` |
| Multi-process | `./scripts/chaos.sh` (`SCENARIO=kill_leader\|rolling\|…`) |
| Acceptance | `./scripts/acceptance.sh` (≥10 groups, kill leader PID) |

See [chaos-checklist.md](../../chaos-checklist.md) for the full ID matrix (C01…).
