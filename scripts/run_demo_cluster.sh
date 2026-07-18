#!/usr/bin/env bash
# Launch a 3-process MultiRaft demo over gRPC (`--mode node`).
#
# Each OS process is one Raft node. Admin HTTP per node:
#   node N → http://127.0.0.1:(BASE_PORT + 100 + N - 1)
# Raft gRPC:
#   node N → 127.0.0.1:(BASE_PORT + N - 1)
#
# Compatible with macOS Bash 3.2.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-21000}"
GROUPS="${GROUPS:-10}"
NODES="${NODES:-3}"
DATA="${DATA_DIR:-$ROOT/.demo-data}"

export PATH="${HOME}/.cargo/bin:${PATH}"

rm -rf "$DATA"
mkdir -p "$DATA"

cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"

BIN="$ROOT/target/debug/multiraft-demo"

id=1
while [[ "$id" -le "$NODES" ]]; do
  NODE_DATA="$DATA/node-$id"
  mkdir -p "$NODE_DATA"
  "$BIN" \
    --mode node \
    --node-id "$id" \
    --nodes "$NODES" \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$NODE_DATA" \
    >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
  # Stagger binds so peers come up cleanly.
  sleep 0.4
  id=$((id + 1))
done

echo "cluster started (${NODES} OS processes × ${GROUPS} groups, gRPC)"
echo "  data: $DATA"
id=1
while [[ "$id" -le "$NODES" ]]; do
  admin_port=$((BASE_PORT + 100 + id - 1))
  raft_port=$((BASE_PORT + id - 1))
  echo "  node ${id}: pid=$(cat "$DATA/node-$id.pid") raft=127.0.0.1:${raft_port} admin=http://127.0.0.1:${admin_port}/groups/0/value log=$DATA/node-$id.log"
  id=$((id + 1))
done
echo "  metrics: http://127.0.0.1:$((BASE_PORT + 100))/metrics/links"
