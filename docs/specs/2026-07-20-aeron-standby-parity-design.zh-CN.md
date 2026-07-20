# Aeron Cluster Standby 商业版对标

**English：** [2026-07-20-aeron-standby-parity-design.md](./2026-07-20-aeron-standby-parity-design.md)

**日期：** 2026-07-20  
**状态：** 已批准分阶段实现  
**关联：** [standby-async-snapshot](./2026-07-20-standby-async-snapshot-design.zh-CN.md) · [ARCHITECTURE.zh-CN.md](../ARCHITECTURE.zh-CN.md)  
**上游参考：** [Aeron Cluster Standby (Premium)](https://aeron.io/premium-docs/aeron-cluster-standby/standby-overview.html)

---

## 0. 目的

将 **Aeron Cluster Standby（商业版）** 能力映射到 `multiraft`：标明 MVP 已覆盖项，并规定后续阶段，使撮合 HA 接近热备 / 卸载语义，而不整叉 Aeron 栈。

约束：

- 共识仍为 **openraft `=0.10.0-alpha.30`** + 薄 Multi-Raft。
- 无 Media Driver / Aeron Archive；用 **持久 SnapshotCatalog + HTTP/gRPC 拉取** 近似 Archive。
- Standby 建模为 openraft **Learner**（非投票），而非另一套共识实现。

---

## 1. 商业版能力矩阵

| # | Aeron Premium 能力 | MVP（已交付） | multiraft 目标 | 阶段 |
|---|-------------------|---------------|----------------|------|
| A1 | 非投票 Standby 跟 log | Learner / `add_standby` | 保持 | done |
| A2 | Standby 打快照、主集群不停 | `StandbyOffload` + 异步 freeze/catalog | 保持 | done |
| A3 | Leader 触发 standby snapshot | `trigger_standby_snapshot` | 保持 | done |
| A4 | 通知集群快照位置 | `SnapshotAdvertisement` + `snapshot_ads.json` | 持久化 + 分发 | done / 加固 |
| A5 | Voter 重启懒拉取 | 测试内 catalog 拷贝 | **ad 更新时自动 HTTP 拉 `fetch_url`** | **P0** |
| A6 | 按需 replicate 工具 | Demo curl | Admin `POST /admin/replicate_standby_snapshot` | P0 |
| A7 | Standby 不反压 Leader | 未控制 | **对 standby peer 限速/限并发复制** | **P0** |
| A8 | Daisy-chain | — | **快照 daisy**：`daisy_upstream_base`（非完整 openraft log 重定向） | **P2** |
| A9 | Warm DR / TransitionModule | — | **`promote_standby` Learner→Voter**（+ demote） | **P1** |
| A10 | 多 Standby / 选择性服务 | 单 Standby | 多 learner + `best_snapshot_ad` 选最新 | **P2** |
| A11 | Archive 语义 | 目录 catalog | **HTTP Range** 分块拉取 + 断点续传 + sha256 | **P2** |
| A12 | Backup query / 鉴权 / Tool | — | 更丰富 admin；鉴权后期 | P2 |
| A13 | Standby 上跑慢查询服务 | — | `read_stale` + `enable_stale_queries` | **P3** |

---

## 2. 目标架构（完整）

```text
                    投票多数派 (3)
                 ┌────────────────────┐
   客户端 ────►  │ L / F / F          │  propose / vote / commit
                 └─────────┬──────────┘
                           │ 复制（对 standby 限速）
                           ▼
                 ┌────────────────────┐
                 │ Standby Learner(s) │  apply · 异步快照
                 │ SnapshotCatalog    │
                 └─────────┬──────────┘
                           │ SnapshotAdvertisement
                           ▼
                 voter 持久化 ad；重启 / 按需：
                 HTTP GET fetch_url → install_durable_snapshot
                           │
            (P1 Transition) promote_standby → change_membership
```

---

## 3. 阶段 P0 — 自动恢复 + Standby 不挡主

### 3.1 按广告自动恢复

**API**

```text
MultiRaft::pull_and_install_snapshot(group, fetch_url) -> Result<()>
MultiRaft::try_recover_from_standby_ads(group) -> Result<RecoverOutcome>
```

**行为：** 选最新 ad → 与本地 applied 比较 → HTTP GET → 校验 sha256 → `install_durable_snapshot`；失败则仅依赖 log replay。

### 3.2 Standby 复制限速

**配置：** `standby_max_inflight`、`standby_replicate_delay_ms`、`standby_node_ids`。  
对目标为 standby 的 RPC 施加延迟 / 飞行窗口，**不影响** voter 之间多数派复制。

### 3.3 验收（P0）

1. 有 ad 时 voter 重启可经 `fetch_url` 恢复，无需进程内 catalog 句柄。  
2. `standby_replicate_delay_ms > 0` 时 Leader 仍可持续 propose。  
3. 坏校验 / 不可达 URL → 跳过拉取，集群仍可用。

---

## 4. 阶段 P1 — Transition（热升格）

```text
promote_standby(group, id)  → change_membership(voters ∪ {id})
demote_to_standby(group, id) → change_membership(voters \ {id}, retain learner)
```

验收：升格后 4 节点多数派可用；降级后该节点不再当选 Leader。

---

## 5. 阶段 P2 — Daisy-chain / 多 Standby / 流式拉取（**已规定并实现**）

### 5.1 多 Standby

- Leader 可多次 `add_standby`。
- `try_recover_from_standby_ads` / `best_snapshot_ad(group)` 按 `(last_term, last_index)` 取最新。

### 5.2 Daisy-chain（快照带宽近似）

```rust
pub daisy_upstream_base: Option<String>, // e.g. "http://127.0.0.1:23103"
pub daisy_sync_interval_ms: u64,         // default 2000
```

```text
sync_from_daisy_upstream(group) -> RecoverOutcome
spawn_daisy_sync_loop(groups)   // daisy_upstream_base 已设置时后台同步
```

Standby B 可仅为快照节点（测试中不必 `add_learner`）：从 A 的 `{base}/snapshots/{group}/latest` 拉取 → 写入本地 catalog → 安装 FSM → `record_snapshot_ad`（`admin_advertise_addr`）。

**限制：** 这是**快照链**，不是 Aeron 式 log daisy / openraft AppendEntries 重定向。

### 5.3 流式 / Range 续传

- `GET /snapshots/:id/latest` 支持 `Range` → `206` + `Content-Range`。
- `pull_and_install_snapshot` 经 `pull_snapshot_chunked`（`snapshot_fetch_chunk_bytes`，默认 64KiB）写入临时文件并可续传，校验 sha256 后安装。

验收与测试：`standby_p2.rs`；Demo：`STANDBY=2` / `DAISY=1` / `DAISY_UPSTREAM=...`。

## 6. P3 — 服务卸载（已实现）

Standby（或打开开关的节点）对本地 FSM 提供只读查询，并返回 applied 水位；**非** linearizable。

```text
enable_stale_queries: bool
read_stale(group, f) -> StaleRead { value, applied_index, applied_term }
```

Demo：`GET /groups/{id}/stale`；测试：`standby_p3.rs`。细节见英文版。

## 7. 非目标

不做 Aeron 整栈替换 / Media Driver / 无人值守跨 DC 切换。见英文版。

---

## 8–9. 成功标准（P3 结束）

- 双语设计文档含商业版矩阵与阶段（至 P3）。  
- P0–P3 API 与自动化测试就绪。  
- Demo：`STANDBY=1` 恢复；可选 promote / daisy；Standby `GET /groups/{id}/stale`。
