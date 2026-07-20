# Standby Async Snapshot (Aeron-aligned)

**中文：** [2026-07-20-standby-async-snapshot-design.zh-CN.md](./2026-07-20-standby-async-snapshot-design.zh-CN.md)

**Date:** 2026-07-20  
**Status:** MVP implemented in `multiraft`  
**Related:** [ARCHITECTURE.md](../ARCHITECTURE.md) · openraft `=0.10.0-alpha.30`  
**Premium gaps / roadmap:** [Aeron Standby parity](./2026-07-20-aeron-standby-parity-design.md)

---

## Goal

Offload FSM snapshot serialization from Raft **voters** to a **Standby** (openraft Learner), so Leaders never block the hot path on a sync FSM dump. Pattern inspired by Aeron Cluster Standby snapshots.

This document is the **MVP**. Full commercial parity (auto pull, throttle, transition, daisy-chain) lives in the [parity design](./2026-07-20-aeron-standby-parity-design.md).

## Architecture

```text
  Voters (quorum)                    Standby (Learner)
  ───────────────                    ─────────────────
  propose(app) ───────────────────►  apply(app)
  propose(TRIGGER) ───────────────►  apply(TRIGGER)
                                         │
                                         ├─ brief lock: freeze_for_snapshot
                                         ├─ unlock
                                         └─ spawn_blocking: catalog.write
                                              │
                                              ▼
                                         advertise → voters
                                              │
  voter recovery ◄──── pull fetch_url / catalog bytes
```

| Piece | Choice |
|-------|--------|
| Standby role | openraft Learner via `Raft::add_learner` |
| Trigger | Magic log `STANDBY_SNAPSHOT_TRIGGER` proposed by Leader |
| Async path | `freeze_for_snapshot` under lock → `spawn_blocking` serialize/fsync |
| Durable store | `{data_dir}/snapshots/{group}/{index}-{term}/` (`meta.json`, `data.bin`, `sha256`) |
| Voter `build_snapshot` | In `SnapshotMode::StandbyOffload`: catalog / installed only (never hot-dump) |

## Config

```rust
enum NodeRole { Voter, Standby }
enum SnapshotMode { Disabled, StandbyOffload }

ClusterConfig {
    role, snapshot_mode, snapshot_keep, // default keep=2
    admin_advertise_addr,               // fetch URL base for ads
    // ... existing fields
}
```

- `SnapshotMode::Disabled` (default): previous sync `build_snapshot` behavior.
- `StandbyOffload`: openraft `SnapshotPolicy::Never`; Standby builds on trigger.

## APIs

| API | Who | Behavior |
|-----|-----|----------|
| `add_standby(group, id)` | Leader | `add_learner(..., blocking=true)` |
| `trigger_standby_snapshot(group)` | Leader | `propose(STANDBY_SNAPSHOT_TRIGGER)` |
| `record_snapshot_ad` / `snapshot_ads` | Any | In-memory (+ `{data_dir}/snapshot_ads.json`) |
| `try_install_from_standby_catalog` | Voter recovery | Copy bytes → `install_durable_snapshot` |
| `latest_catalog_entry` | Standby | Poll durable catalog |

## Demo

- `--role voter|standby`; `STANDBY=1` or `--role standby` enables `StandbyOffload`.
- Admin: `POST /admin/standby_snapshot/:id`, `POST|GET /admin/snapshot_ads`, `GET /snapshots/:id/latest`, `POST /admin/add_standby/:group/:standby_id`.
- `scripts/run_demo_cluster.sh` with `STANDBY=1` starts node 4 as Standby and curls the leader to `add_standby`.

## Tests

- `cargo test -p multiraft-store --lib` — catalog write/read/prune.
- `cargo test -p multiraft-net --test standby_snapshot` — 3 voters + 1 standby, async delay while voters continue, voter pull-restore.
