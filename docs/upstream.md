# Upstream pin note

## Locked versions

Workspace `Cargo.toml` pins:

```toml
openraft = { version = "=0.10.0-alpha.30", default-features = false, features = ["serde", "type-alias", "tokio-rt"] }
openraft-multi = { version = "=0.10.0-alpha.30" }
```

Both crates use an **exact** version requirement (`=`). Do not widen to `^` / `~` without a deliberate bump and retest of the demo + `./scripts/acceptance.sh`.

## Why lock

`openraft-multi` and the Multi-Raft APIs used by `multiraft-net` (`MultiGroup`, shared network / router patterns) are still on the **0.10.0-alpha** line. Patch alphas can change type aliases, feature flags, and example layouts. Pinning both crates to the same alpha revision avoids:

- accidental Cargo resolution to a newer incompatible alpha
- `openraft` / `openraft-multi` version skew within one workspace
- silent breakage of phase-1 in-process `GroupRouter` + demo acceptance

## Reference example

Upstream multi-group KV example (same release train):

- [openraft `examples/multi-raft-kv`](https://github.com/datafuselabs/openraft/tree/v0.10.0-alpha.30/examples/multi-raft-kv)

When bumping the pin, start from that tree’s `Cargo.toml` / README for the target tag, then re-run:

```bash
cargo test --workspace
./scripts/acceptance.sh
```

## Bump checklist

1. Update both workspace deps to the same new `=x.y.z` (or matching alpha).
2. Diff against the upstream multi-raft-kv example for that tag.
3. Fix compile / API breaks in `multiraft-core` and `multiraft-net`.
4. Pass workspace tests and acceptance.
