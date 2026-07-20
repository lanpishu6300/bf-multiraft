# Roadmap

**中文：** [zh/Roadmap.md](../zh/Roadmap.md)

## Phase-1 (this repo — done)

- [x] Thin Multi-Raft on openraft + openraft-multi
- [x] File persistence + restart recovery
- [x] Multi-process gRPC demo (≥10 groups)
- [x] `acceptance.sh` / `chaos.sh` / porcupine
- [x] Local Jepsen (counter + kill nemesis)
- [x] Consistency Contract + `read_linearizable`

## Phase-1.5 / library hardening

- [x] Standby async snapshot (Aeron-aligned Learner offload) — see [spec](../../specs/2026-07-20-standby-async-snapshot-design.md)

## Phase-2 (downstream app)

- [ ] Optional Leader RMQ consume → `propose`
- [ ] Pluggable matching engine FSM + idempotency keys
- [ ] Production metrics (propose latency, lag, leadership changes)
- [x] Stronger persistence / snapshot policy (StandbyOffload catalog; more hardening TBD)

## Explicit non-goals (near term)

- Region split/merge, PD, dynamic membership
- Replacing RMQ sequencing with Raft (path A)
- Follower LeaseRead as the default production read
