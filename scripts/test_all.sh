#!/usr/bin/env bash
# CI-friendly suite: unit tests + in-process chaos + acceptance.
# Set CHAOS=1 to also run multi-process ./scripts/chaos.sh.
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "[test_all] cargo test --workspace"
cargo test --workspace --manifest-path "$ROOT/Cargo.toml"

echo "[test_all] cargo test -p multiraft-net --test chaos_failover"
cargo test -p multiraft-net --test chaos_failover --manifest-path "$ROOT/Cargo.toml"

echo "[test_all] ./scripts/acceptance.sh"
"$ROOT/scripts/acceptance.sh"

if [[ "${CHAOS:-0}" == "1" ]]; then
  echo "[test_all] ./scripts/chaos.sh (CHAOS=1)"
  "$ROOT/scripts/chaos.sh"
else
  echo "[test_all] skipping ./scripts/chaos.sh (set CHAOS=1 to enable)"
fi

echo "[test_all] OK"
