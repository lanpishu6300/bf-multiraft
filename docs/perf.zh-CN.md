# 性能说明

**English：** [perf.md](./perf.md)

## 理论上限（本机 release 实测拆分）

| 阶段 | TPS | p50 | 含义 |
|------|-----|-----|------|
| A FSM-only | ~9M | &lt;1µs | 无共识软上限（无关 Raft） |
| B 1-node mem | ~55–57k | ~12–14µs | **无复制 Raft 软上限** |
| C 3-node mem | ~16–17k | ~56–58µs | 当前 in-process 多数派路径 |
| D 编解码 | ~1.4M | &lt;1µs | bincode 已不再是主因 |
| File 3-node | ~2.1k | ~420µs | 多数派 + 每节点 `log.bin` append |

**结论：**

- 3-node mem 相对 1-node 仍慢 ~3.4×，主因是 **quorum 等待 + 两次 in-process RPC/调度**，不是 FSM、也不是编解码。
- 顺序单 Group 下，C 已接近「1-node + 2× 通道/调度」的软天花板（理想约 18–25k TPS）。
- File 相对 mem 仍慢 ~8×，主因是 **每个 quorum 成员一次磁盘 append**（无 fsync 时仍受 write 系统调用与序列化影响）。

## 压测入口

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

## 已处理瓶颈

| 轮次 | 热点 | 改动 | 效果 |
|------|------|------|------|
| R1 | 全量重写 `log.json` | `log.ndjson` 追加 | 微基准 4×+ |
| R2 | wipe/restart log reversion | `allow_log_reversion` | chaos 稳定 |
| R3 | 热路径日志噪音 | trace / 只打字节数 | 降开销 |
| R4 | NDJSON JSON 行写 + JSON RPC | `log.bin` 长度前缀 bincode；RPC/命令 bincode | file **897→2130 TPS**；3-node mem **14k→16.5k** |

## 当前瓶颈（继续优化方向）

1. **3-node mem**：openraft 单条 `client_write` 的 quorum 往返；批量 propose / 多 Group 并行可抬高墙钟 TPS（conc=4 已到 ~65k）。
2. **File**：每副本一次 `write`；可选 `fsync` 策略、组提交、或 io_uring/用户态盘路径。
3. **跨进程 gRPC**：额外序列化+内核网络；需单独压测（本仓库 bench 默认 in-process）。
