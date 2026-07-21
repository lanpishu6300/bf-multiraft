# Performance notes

**中文：** [perf.zh-CN.md](./perf.zh-CN.md)

## Theoretical ceilings (local release phase breakdown)

| Phase | TPS | p50 | Meaning |
|-------|-----|-----|---------|
| A FSM-only | ~9M | &lt;1µs | Soft ceiling without consensus |
| B 1-node mem | ~55–57k | ~12–14µs | **Raft soft ceiling without replication** |
| C 3-node mem | ~16–17k | ~56–58µs | Current in-process quorum path |
| D codec | ~1.4M | &lt;1µs | bincode no longer dominant |
| File 3-node | ~2.1k | ~420µs | Quorum + per-node `log.bin` append |

**Takeaways:**

- 3-node mem is still ~3.4× slower than 1-node — dominated by **quorum wait + two in-process RPC/scheduling hops**, not FSM or codec.
- For sequential single-group load, C is near the soft cap of “1-node + 2× channel/schedule” (~18–25k TPS ideal).
- File remains ~8× slower than mem — **one disk write per quorum member** (even without fsync).

## Harness

```bash
cargo build -p multiraft-demo --release

RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 5000

RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 2000 \
  --bench-file-log --data-dir /tmp/multiraft-bench

cargo test -p multiraft-net --test bench_ceiling --release -- --nocapture
cargo test -p multiraft-store --test bench_file_log_micro --release -- --nocapture
cargo test -p multiraft-net --test bench_codec_micro --release -- --nocapture
```

## Bottlenecks addressed

| Round | Hotspot | Change | Effect |
|-------|---------|--------|--------|
| R1 | Full `log.json` rewrite | NDJSON append | micro 4×+ |
| R2 | wipe/restart log reversion | `allow_log_reversion` | stable chaos |
| R3 | hot-path log noise | trace / byte sizes | lower overhead |
| R4 | NDJSON JSON + JSON RPC | `log.bin` + bincode RPC/commands | file **897→2130 TPS**; 3-node mem **14k→16.5k** |

## Remaining bottlenecks

1. **3-node mem:** per-op quorum RTT of `client_write`; batching / multi-group concurrency raises wall TPS (conc=4 already ~65k).
2. **File:** one `write` per replica; optional fsync policy, group commit, or userspace/io_uring IO.
3. **Cross-process gRPC:** separate harness (default bench is in-process).
