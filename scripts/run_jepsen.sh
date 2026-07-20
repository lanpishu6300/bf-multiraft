#!/usr/bin/env bash
# Build demo, start 3-node cluster with NO_AUTO_PROPOSE, run local Jepsen smoke.
# Compatible with macOS Bash 3.2.
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${HOME}/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASE_PORT="${BASE_PORT:-23000}"
# Prefer JEPSEN_GROUPS so ambient GROUPS from chaos/acceptance does not leak in.
GROUPS="${JEPSEN_GROUPS:-1}"
NODES="${JEPSEN_NODES:-${NODES:-3}}"
# Optional Learner Standby (node NODES+1). Clients/nemesis still use voters 1..NODES.
STANDBY="${STANDBY:-0}"
DATA="${DATA_DIR:-$ROOT/.jepsen-data}"
TIME_LIMIT="${JEPSEN_TIME_LIMIT:-30}"
CONCURRENCY="${JEPSEN_CONCURRENCY:-6}"

# Prefer Java 17 for Jepsen if present (Java 22 also OK when deps resolve).
if [[ -z "${JAVA_HOME:-}" ]]; then
  if [[ -d "/Users/lan/Library/Java/JavaVirtualMachines/graalvm-jdk-17.0.10/Contents/Home" ]]; then
    export JAVA_HOME="/Users/lan/Library/Java/JavaVirtualMachines/graalvm-jdk-17.0.10/Contents/Home"
  elif command -v /usr/libexec/java_home >/dev/null 2>&1; then
    if JH="$(/usr/libexec/java_home -v 17 2>/dev/null)"; then
      export JAVA_HOME="$JH"
    fi
  fi
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

log() { printf '[jepsen] %s\n' "$*"; }
fail() { printf '[jepsen] FAIL: %s\n' "$*" >&2; exit 1; }

log "disk:"
df -h "$ROOT" | tail -1 || df -h . | tail -1 || true

command -v lein >/dev/null 2>&1 || fail "lein not found; install to \$HOME/bin/lein"
command -v java >/dev/null 2>&1 || fail "java not found"
log "java: $(java -version 2>&1 | head -1)"
log "lein: $(lein version 2>&1 | head -1)"

stop_cluster() {
  local id pid max
  max=$((NODES + 2))
  id=1
  while [[ "$id" -le "$max" ]]; do
    if [[ -f "$DATA/node-$id.pid" ]]; then
      pid="$(cat "$DATA/node-$id.pid" 2>/dev/null || true)"
      if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
      fi
    fi
    id=$((id + 1))
  done
}

cleanup() {
  log "stopping cluster"
  stop_cluster
}
trap cleanup EXIT

log "building multiraft-demo"
cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"

DEMO_BIN="$ROOT/target/debug/multiraft-demo"
[[ -x "$DEMO_BIN" ]] || fail "missing binary $DEMO_BIN"

stop_cluster
rm -rf "$DATA"

log "starting cluster BASE_PORT=${BASE_PORT} GROUPS=${GROUPS} STANDBY=${STANDBY} DATA=${DATA}"
export DATA_DIR="$DATA"
export BASE_PORT GROUPS NODES STANDBY
export JEPSEN=1
export NO_AUTO_PROPOSE=1
export DEMO_BIN
export MULTIRAFT_ROOT="$ROOT"
"$ROOT/scripts/run_demo_cluster.sh"

admin_ready() {
  local id=1 port
  while [[ "$id" -le "$NODES" ]]; do
    port=$((BASE_PORT + 100 + id - 1))
    if curl -fsS --max-time 2 "http://127.0.0.1:${port}/groups/0/value" >/dev/null 2>&1; then
      return 0
    fi
    id=$((id + 1))
  done
  return 1
}

log "waiting for admin HTTP"
deadline=$((SECONDS + 90))
while ! admin_ready; do
  if (( SECONDS >= deadline )); then
    fail "admins not ready; see ${DATA}/node-*.log"
  fi
  sleep 0.5
done
log "admins ready"

log "running Jepsen (time-limit=${TIME_LIMIT}s concurrency=${CONCURRENCY})"
cd "$ROOT/jepsen/multiraft"
# Fresh store each smoke run.
rm -rf store
set +e
lein run test -- \
  --no-ssh \
  --nodes "1,2,3" \
  --time-limit "$TIME_LIMIT" \
  --concurrency "$CONCURRENCY"
LEIN_EXIT=$?
set -e

if [[ "$LEIN_EXIT" -eq 0 ]]; then
  log "JEPSEN OK"
  printf 'JEPSEN OK\n'
  exit 0
fi

log "JEPSEN FAILED (exit=${LEIN_EXIT}); latest store:"
ls -lt store 2>/dev/null | head -5 || true
if [[ -d store ]]; then
  find store -name 'jepsen.log' -o -name 'history.edn' 2>/dev/null | head -10 || true
fi
fail "Jepsen test failed"
