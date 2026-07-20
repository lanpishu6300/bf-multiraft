# Aeron Cluster Standby Premium Parity

**中文：** [2026-07-20-aeron-standby-parity-design.zh-CN.md](./2026-07-20-aeron-standby-parity-design.zh-CN.md)

**Date:** 2026-07-20  
**Status:** Approved for phased implementation  
**Related:** [standby-async-snapshot](./2026-07-20-standby-async-snapshot-design.md) · [ARCHITECTURE.md](../ARCHITECTURE.md)  
**Upstream reference:** [Aeron Cluster Standby (Premium)](https://aeron.io/premium-docs/aeron-cluster-standby/standby-overview.html)

---

## 0. Purpose

Map **Aeron Cluster Standby (commercial)** capabilities onto `multiraft`, record what MVP already covers, and specify the remaining phases so matching HA can approach warm-DR / offload semantics without forking a full Aeron stack.

Constraints:

- Consensus remains **openraft `=0.10.0-alpha.30`** + thin Multi-Raft.
- No Media Driver / Aeron Archive; we approximate Archive with **durable SnapshotCatalog + HTTP/gRPC fetch**.
- Standby is modeled as an openraft **Learner** (non-voter), not a separate consensus implementation.

---

## 1. Aeron Premium capability matrix

| # | Aeron Premium capability | MVP (shipped) | Target in multiraft | Phase |
|---|--------------------------|---------------|---------------------|-------|
| A1 | Non-voting standby follows log | Learner via `add_standby` | Keep | done |
| A2 | Standby snapshot without stopping voters | `StandbyOffload` + async freeze/catalog | Keep | done |
| A3 | Trigger standby snapshot from leader | `trigger_standby_snapshot` magic log | Keep | done |
| A4 | Notify cluster of snapshot location | `SnapshotAdvertisement` + local `snapshot_ads.json` | Persist + fan-out | done / harden |
| A5 | Lazy pull on voter restart | Manual / test catalog copy | **Auto pull from `fetch_url` when ad newer** | **P0** |
| A6 | On-demand replicate tool | Demo curl only | Admin `POST /admin/replicate_standby_snapshot` | P0 |
| A7 | Standby must not back-pressure leader | Not controlled | **Throttle append/replication toward standby peers** | **P0** |
| A8 | Daisy-chain standby←standby | — | Optional log source peer for Standby | P2 |
| A9 | Warm DR / TransitionModule | — | **`promote_standby` Learner→Voter** (+ demote) | **P1** |
| A10 | Multi-standby / selective services | Single standby | Multi learner + per-group enable flags | P2 |
| A11 | Archive recording semantics | Directory catalog | Streaming fetch, resume, checksum verify (have sha256) | P1/P2 |
| A12 | Backup query / auth / PremiumClusterTool | — | Ops CLI / richer admin; auth out of scope | P2 |
| A13 | Clustered services on standby (slow query) | — | Pluggable read-only FSM hooks | P3 |

---

## 2. Target architecture (full)

```text
                    Voting quorum (3)
                 ┌────────────────────┐
   clients ───►  │ L / F / F          │  propose / vote / commit
                 └─────────┬──────────┘
                           │ replicate (throttled to standby)
                           ▼
                 ┌────────────────────┐
                 │ Standby Learner(s) │  apply · async snapshot
                 │ SnapshotCatalog    │
                 └─────────┬──────────┘
                           │ SnapshotAdvertisement
                           ▼
                 voters persist ads; on restart / on-demand:
                 HTTP GET fetch_url → install_durable_snapshot
                           │
            (P1 Transition) promote_standby → change_membership
```

Daisy-chain (P2):

```text
Leader ──► Standby-A ──► Standby-B (log follow from A, not Leader)
```

---

## 3. Phase P0 — Recovery pull + non-blocking standby

### 3.1 Auto recover from advertisements

**API**

```text
MultiRaft::pull_and_install_snapshot(group, fetch_url) -> Result<()>
MultiRaft::try_recover_from_standby_ads(group) -> Result<RecoverOutcome>
  RecoverOutcome = { Installed { ad }, SkippedNoAd, SkippedNotNewer, FetchFailed(err) }
```

**Behavior**

1. Load ads for `group` (memory + `snapshot_ads.json`).
2. Pick ad with max `(last_term, last_index)` (and valid `fetch_url`).
3. Compare to local applied index (FSM / raft metrics). If ad not newer → skip.
4. HTTP GET `fetch_url` (expect body = snapshot bytes; headers or sibling JSON for meta: `X-Snapshot-Index`, `X-Snapshot-Term`, `X-Snapshot-Id`, `X-Snapshot-Sha256`).
5. Verify sha256; `install_durable_snapshot`; fail closed → log replay only.

**When**

- Explicit call after `create_group` (library).
- Demo voter restart path calls `try_recover_from_standby_ads`.
- Admin: `POST /admin/replicate_standby_snapshot/{group}` with optional `{ "fetch_url": "..." }`.

### 3.2 Standby replication throttle

**Config**

```rust
ClusterConfig {
  /// Max outstanding AppendEntries toward Standby node ids (soft).
  standby_max_inflight: u32,          // default 8
  /// Artificial delay before sending RPC to standby peers (ms).
  standby_replicate_delay_ms: u64,    // default 0; tests use >0
  /// Node ids treated as standby for throttling (filled when add_standby succeeds).
  standby_node_ids: Vec<NodeId>,      // runtime + optional seed
}
```

**Behavior**

- In-process `Network` / gRPC `GrpcRouter` client path: if `target` ∈ `standby_node_ids`, apply delay and/or inflight gate **before** send.
- Does not change quorum replication among voters.
- Goal: approximate Aeron “standby must not apply back-pressure to the log on the main cluster”.

### 3.3 Acceptance (P0)

1. After standby snapshot + ad recorded on voter, kill voter, restart, `try_recover_from_standby_ads` restores value ≥ watermark without needing in-process catalog handle.
2. With `standby_replicate_delay_ms = 50`, continuous proposes on leader still succeed; standby may lag.
3. Bad sha256 / unreachable fetch_url → recover returns `FetchFailed` / skip; cluster still serves via log replay.

---

## 4. Phase P1 — Transition (warm promote)

### 4.1 Promote / demote

```text
MultiRaft::promote_standby(group, node_id) -> Result<()>
  // Leader: change_membership(voters ∪ {node_id}, retain=true)
  // Pre: node already learner (add_standby)

MultiRaft::demote_to_standby(group, node_id) -> Result<()>
  // Leader: change_membership(voters \ {node_id}, retain=true) → becomes learner
```

Safety:

- Refuse promote if `node_id` not in current membership as learner (or not caught up when `blocking` path available).
- After promote, clear throttle treatment for that id (no longer standby).
- Demo admin: `POST /admin/promote_standby/{group}/{id}`, `POST /admin/demote_standby/{group}/{id}`.

### 4.2 Acceptance (P1)

1. 3 voters + 1 standby → promote standby → 4-voter quorum; kill one old voter; cluster still elects.
2. Demote back to learner → node no longer wins leadership.

---

## 5. Phase P2 — Daisy-chain & multi-standby (design only until scheduled)

| Item | Design sketch |
|------|----------------|
| Daisy-chain | Standby config `log_source: Leader | Peer(NodeId)`; follow appends only from source (may require app-level log shipper if openraft cannot redirect). Prefer: secondary standby as learner of a **second** group or external shipper — **spike before commit**. |
| Multi-standby | `add_standby` N times; ads pick newest among all fetch_urls. |
| Streaming fetch | Chunked GET / range requests; resume token in catalog meta. |

---

## 6. Phase P3 — Service offload

Allow Standby to register **read-only** query handlers against local FSM (`with_fsm`) with explicit stale semantics — not linearizable. Out of scope for P0/P1 code.

---

## 7. Non-goals

- Replacing openraft with Aeron Consensus Module.
- Full Aeron Archive / Media Driver.
- Cross-DC automated failover without operator (Aeron Transition still operator-gated).
- Byzantine standby trust (ads/fetch must be from configured peers; auth later).

---

## 8. Doc / code map

| Artifact | Role |
|----------|------|
| This spec | Premium parity roadmap |
| [standby-async-snapshot](./2026-07-20-standby-async-snapshot-design.md) | MVP mechanics (keep; link here for gaps) |
| `ClusterConfig` standby throttle fields | P0 |
| `pull_and_install_snapshot` / `try_recover_from_standby_ads` | P0 |
| `promote_standby` / `demote_to_standby` | P1 |
| Tests `standby_snapshot` + `standby_premium` | P0/P1 |

---

## 9. Success criteria (end of P1)

- Design docs (EN+zh-CN) describe Aeron matrix and phases.
- P0+P1 APIs implemented and covered by automated tests.
- Demo can: `STANDBY=1` → trigger → restart voter → auto recover from ad URL → optional promote.
