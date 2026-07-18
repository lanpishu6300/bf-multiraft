# 上游版本锁定说明

**English：** [upstream.md](./upstream.md)

## 锁定版本

Workspace `Cargo.toml` 锁定：

```toml
openraft = { version = "=0.10.0-alpha.30", default-features = false, features = ["serde", "type-alias", "tokio-rt"] }
openraft-multi = { version = "=0.10.0-alpha.30" }
```

两个 crate 均使用**精确**版本要求（`=`）。未经刻意升版并复测 Demo + `./scripts/acceptance.sh`，不要放宽为 `^` / `~`。

## 为何锁定

`openraft-multi` 以及 `multiraft-net` 所用的 Multi-Raft API（`MultiGroup`、共享网络 / router 模式）仍在 **0.10.0-alpha** 线上。Patch alpha 可能改动 type alias、feature flags 与示例布局。将二者钉在同一 alpha 修订可避免：

- Cargo 意外解析到更新的不兼容 alpha
- 同一 workspace 内 `openraft` / `openraft-multi` 版本漂移
- 一期进程内 `GroupRouter` + Demo 验收被静默破坏

## 参考示例

上游多 Group KV 示例（同一发布列车）：

- [openraft `examples/multi-raft-kv`](https://github.com/datafuselabs/openraft/tree/v0.10.0-alpha.30/examples/multi-raft-kv)

升版锁定时，先从该 tag 的 `Cargo.toml` / README 入手，然后重跑：

```bash
cargo test --workspace
./scripts/acceptance.sh
```

## 升版检查清单

1. 将两个 workspace 依赖更新为相同的新 `=x.y.z`（或匹配的 alpha）。
2. 对照该 tag 的上游 multi-raft-kv 示例做 diff。
3. 修复 `multiraft-core` 与 `multiraft-net` 的编译 / API 破坏。
4. 通过 workspace 测试与 acceptance。
