# FAQ

**中文：** [zh/FAQ.md](../zh/FAQ.md)

### Why not SofaJRaft / TiKV raftstore?

No official Rust SofaJRaft. TiKV `raftstore` is thick (Region / PD / split);
matching shards on stable symbols do not need that. This library is thin
Multi-Raft (shared links + many groups).

### Is a timed-out `propose` a definite failure?

No. Timeout / disconnect / failover windows are **indeterminate** — retry with
the same idempotency key. See the Consistency Contract.

### Can `with_fsm` be used as source of truth?

No. It may be stale. Use `read_linearizable` for production reads.

### Where are Jepsen reports?

After a run: `jepsen/multiraft/store/latest/` (gitignored). Case source lives
under `jepsen/multiraft/src/`. More: [Consistency & testing](./Consistency.md).

### Downstream integration (phase 2)?

Phase-1 is this independent runtime. Phase-2 (optional, in a downstream app):
a matching process / ingress shell can depend on this crate for Leader-side
propose, with a pluggable matching engine FSM.
