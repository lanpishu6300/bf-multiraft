#!/usr/bin/env bash
# Launch the phase-1 multiraft demo.
#
# Networking today is in-process only (shared Router). This script therefore
# starts **one** `multiraft-demo --mode cluster` process that hosts 3 logical
# nodes × 10 groups. True 3-OS-process clustering needs Task 8 / tonic.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-21000}"
GROUPS="${GROUPS:-10}"
DATA="${ROOT}/.demo-data"

export PATH="${HOME}/.cargo/bin:${PATH}"

rm -rf "$DATA"
mkdir -p "$DATA"

cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"

"$ROOT/target/debug/multiraft-demo" \
  --mode cluster \
  --base-port "$BASE_PORT" \
  --groups "$GROUPS" \
  --data-dir "$DATA" \
  >"$DATA/cluster.log" 2>&1 &
echo $! >"$DATA/cluster.pid"

# Compatibility stubs for scripts that expect per-node pid files (Task 8).
# All point at the single cluster process for now.
for id in 1 2 3; do
  cp "$DATA/cluster.pid" "$DATA/node-$id.pid"
  ln -sfn cluster.log "$DATA/node-$id.log"
done

cat <<EOF
cluster started (single-process --mode cluster; 3 logical nodes × ${GROUPS} groups)
  pid:  $(cat "$DATA/cluster.pid")
  log:  $DATA/cluster.log
  admin: http://127.0.0.1:${BASE_PORT}/groups/0/value
         http://127.0.0.1:${BASE_PORT}/metrics/links
EOF
