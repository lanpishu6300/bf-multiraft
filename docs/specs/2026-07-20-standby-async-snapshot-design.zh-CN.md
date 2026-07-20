# Standby 异步快照（对齐 Aeron）

**English：** [2026-07-20-standby-async-snapshot-design.md](./2026-07-20-standby-async-snapshot-design.md)

**日期：** 2026-07-20  
**状态：** MVP 已在 `multiraft` 落地  
**相关：** [ARCHITECTURE.zh-CN.md](../ARCHITECTURE.zh-CN.md) · openraft `=0.10.0-alpha.30`  
**商业版差距 / 路线图：** [Aeron Standby 对标](./2026-07-20-aeron-standby-parity-design.zh-CN.md)

---

## 目标

把 FSM 快照序列化从 Raft **Voter** 卸载到 **Standby**（openraft Learner），避免 Leader 热路径被同步 dump 阻塞。思路对齐 Aeron Cluster Standby 快照。

本文为 **MVP**。完整商业版对标（自动拉取、限速、Transition、Daisy-chain）见 [对标设计](./2026-07-20-aeron-standby-parity-design.zh-CN.md)。

## 架构

```text
  Voters（法定人数）                 Standby（Learner）
  ─────────────────                 ─────────────────
  propose(业务) ──────────────────►  apply(业务)
  propose(TRIGGER) ───────────────►  apply(TRIGGER)
                                         │
                                         ├─ 短锁：freeze_for_snapshot
                                         ├─ 放锁
                                         └─ spawn_blocking：catalog.write
                                              │
                                              ▼
                                         广告 → voters
                                              │
  voter 恢复 ◄──── 拉取 fetch_url / catalog 字节
```

| 组件 | 选择 |
|------|------|
| Standby 角色 | openraft Learner（`Raft::add_learner`） |
| 触发 | Leader 提出魔术日志 `STANDBY_SNAPSHOT_TRIGGER` |
| 异步路径 | 持锁 `freeze_for_snapshot` → `spawn_blocking` 序列化/fsync |
| 持久目录 | `{data_dir}/snapshots/{group}/{index}-{term}/`（`meta.json`、`data.bin`、`sha256`） |
| Voter `build_snapshot` | `SnapshotMode::StandbyOffload` 下仅服务 catalog / 已安装快照（禁止热 dump） |

## 配置

```rust
enum NodeRole { Voter, Standby }
enum SnapshotMode { Disabled, StandbyOffload }

ClusterConfig {
    role, snapshot_mode, snapshot_keep, // keep 默认 2
    admin_advertise_addr,               // 广告中的 fetch URL 基址
    // ... 既有字段
}
```

- `SnapshotMode::Disabled`（默认）：保持原同步 `build_snapshot`。
- `StandbyOffload`：openraft `SnapshotPolicy::Never`；Standby 在 trigger 时构建。

## API

| API | 调用方 | 行为 |
|-----|--------|------|
| `add_standby(group, id)` | Leader | `add_learner(..., blocking=true)` |
| `trigger_standby_snapshot(group)` | Leader | `propose(STANDBY_SNAPSHOT_TRIGGER)` |
| `record_snapshot_ad` / `snapshot_ads` | 任意 | 内存（+ `{data_dir}/snapshot_ads.json`） |
| `try_install_from_standby_catalog` | Voter 恢复 | 拷贝字节 → `install_durable_snapshot` |
| `latest_catalog_entry` | Standby | 轮询持久 catalog |

## Demo

- `--role voter|standby`；`STANDBY=1` 或 `--role standby` 启用 `StandbyOffload`。
- Admin：`POST /admin/standby_snapshot/:id`、`POST|GET /admin/snapshot_ads`、`GET /snapshots/:id/latest`、`POST /admin/add_standby/:group/:standby_id`。
- `scripts/run_demo_cluster.sh` 在 `STANDBY=1` 时启动 node 4 为 Standby，并 curl Leader `add_standby`。

## 测试

- `cargo test -p multiraft-store --lib` — catalog 写/读/裁剪。
- `cargo test -p multiraft-net --test standby_snapshot` — 3 voter + 1 standby、异步延时下 voter 继续写、voter 拉取恢复。
