#!/usr/bin/env bash
# Multi-process gRPC chaos: randomly kill -9 a live node, wait for leaders,
# ensure values never go backwards, restart the killed node, wait for catch-up.
#
# Compatible with macOS Bash 3.2 (no mapfile / namerefs).
#
# Env:
#   ROUNDS       (default 5)
#   BASE_PORT    (default 22000 — avoid clashing with acceptance)
#   GROUPS       (default 5)
#   NODES        (default 3)
#   CHAOS_DATA   (default $ROOT/.chaos-data)
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-22000}"
GROUPS="${GROUPS:-5}"
NODES="${NODES:-3}"
ROUNDS="${ROUNDS:-5}"
DATA="${CHAOS_DATA:-$ROOT/.chaos-data}"
WORKDIR=""

log() { printf '[chaos] %s\n' "$*"; }
fail() { printf '[chaos] FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
  stop_all_nodes
  if [[ -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    rm -rf "${WORKDIR}"
  fi
}
trap cleanup EXIT

admin_url() {
  local id="$1"
  local port=$((BASE_PORT + 100 + id - 1))
  echo "http://127.0.0.1:${port}"
}

http_get() {
  curl -fsS --max-time 3 "$1"
}

stop_all_nodes() {
  local id pid
  if [[ ! -d "$DATA" ]]; then
    return 0
  fi
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    if [[ -f "$DATA/node-$id.pid" ]]; then
      pid="$(cat "$DATA/node-$id.pid" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
      fi
    fi
    id=$((id + 1))
  done
}

start_cluster() {
  local wipe="${1:-wipe}"
  if [[ "$wipe" == "wipe" ]]; then
    stop_all_nodes
    rm -rf "$DATA"
  fi
  mkdir -p "$DATA"
  DATA_DIR="$DATA" BASE_PORT="$BASE_PORT" GROUPS="$GROUPS" NODES="$NODES" \
    "$ROOT/scripts/run_demo_cluster.sh" >/dev/null
  log "cluster started data_dir=${DATA}"
}

start_one_node() {
  local id="$1"
  local node_data="$DATA/node-$id"
  mkdir -p "$node_data"
  "$ROOT/target/debug/multiraft-demo" \
    --mode node \
    --node-id "$id" \
    --nodes "$NODES" \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$node_data" \
    >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
  log "restarted node ${id} pid=$(cat "$DATA/node-$id.pid")"
}

find_live_admin() {
  local id url
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    url="$(admin_url "$id")"
    if http_get "${url}/groups/0/value" >/dev/null 2>&1; then
      echo "$url"
      return 0
    fi
    id=$((id + 1))
  done
  return 1
}

wait_any_admin() {
  local deadline=$((SECONDS + 60))
  local url
  while (( SECONDS < deadline )); do
    if url="$(find_live_admin)"; then
      log "admin ready: ${url}"
      return 0
    fi
    sleep 0.3
  done
  fail "no admin HTTP ready within 60s; see ${DATA}/node-*.log"
}

fetch_group_agg() {
  local g="$1"
  local id url json best_value best_leader v leader
  best_value=""
  best_leader=""
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    url="$(admin_url "$id")"
    if json="$(http_get "${url}/groups/${g}/value" 2>/dev/null)"; then
      v="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["value"])' "$json")"
      leader="$(python3 -c '
import json,sys
d=json.loads(sys.argv[1])
print("" if d.get("leader") is None else str(d["leader"]))
' "$json")"
      if [[ -z "$best_value" ]] || [[ "$v" -gt "$best_value" ]]; then
        best_value="$v"
      fi
      if [[ -z "$best_leader" && -n "$leader" ]]; then
        best_leader="$leader"
      fi
    fi
    id=$((id + 1))
  done
  [[ -n "$best_value" ]] || return 1
  echo "${g} ${best_value} ${best_leader}"
}

fetch_all_groups() {
  local out="$1"
  local g line
  : >"$out"
  g=0
  while [[ "$g" -lt "$GROUPS" ]]; do
    line="$(fetch_group_agg "$g")" || return 1
    echo "$line" >>"$out"
    g=$((g + 1))
  done
}

assert_groups_healthy() {
  local label="$1"
  local file="$2"
  local line g value leader
  local count=0
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    g=$(echo "$line" | awk '{print $1}')
    value=$(echo "$line" | awk '{print $2}')
    leader=$(echo "$line" | awk '{print $3}')
    [[ -n "$leader" ]] || fail "${label}: group ${g} has no leader"
    [[ "$value" =~ ^-?[0-9]+$ ]] || fail "${label}: group ${g} bad value '${value}'"
    count=$((count + 1))
  done <"$file"
  [[ "$count" -eq "$GROUPS" ]] || fail "${label}: expected ${GROUPS} groups, got ${count}"
}

values_file() {
  awk '{print $2}'
}

# List node ids whose pid file points at a live process (space-separated).
live_node_ids() {
  local id pid out=""
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    if [[ -f "$DATA/node-$id.pid" ]]; then
      pid="$(cat "$DATA/node-$id.pid" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        if [[ -z "$out" ]]; then
          out="$id"
        else
          out="$out $id"
        fi
      fi
    fi
    id=$((id + 1))
  done
  echo "$out"
}

pick_random_live_node() {
  local live ids count idx
  live="$(live_node_ids)"
  [[ -n "$live" ]] || return 1
  # shellcheck disable=SC2206
  ids=($live)
  count=${#ids[@]}
  [[ "$count" -gt 0 ]] || return 1
  idx=$((RANDOM % count))
  echo "${ids[$idx]}"
}

assert_values_not_backwards() {
  local pre="$1"
  local post="$2"
  local label="$3"
  python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post), "length mismatch"
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit("group %d went backwards (%d -> %d)" % (i, a, b))
' "$pre" "$post" || fail "${label}: values went backwards"
}

# --- prep ---
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/multiraft-chaos.XXXXXX")"
SNAPSHOT="${WORKDIR}/snap.txt"
AFTER="${WORKDIR}/after.txt"
VALUES_PRE="${WORKDIR}/v_pre.txt"
VALUES_POST="${WORKDIR}/v_post.txt"

log "building + starting multi-process demo (ROUNDS=${ROUNDS})"
export DATA_DIR="$DATA"
export BASE_PORT GROUPS NODES
start_cluster wipe
wait_any_admin

# Warm up: wait until all groups have leaders and some progress.
WARM_DEADLINE=$((SECONDS + 60))
while true; do
  if fetch_all_groups "$SNAPSHOT" 2>/dev/null; then
    if python3 -c '
import sys
ok = True
for line in open(sys.argv[1]):
    parts = line.split()
    if len(parts) < 3 or not parts[2]:
        ok = False
        break
sys.exit(0 if ok else 1)
' "$SNAPSHOT"; then
      break
    fi
  fi
  if (( SECONDS >= WARM_DEADLINE )); then
    fail "groups not healthy within 60s at start"
  fi
  sleep 0.5
done
assert_groups_healthy "warmup" "$SNAPSHOT"
log "warmup OK"

round=1
while [[ "$round" -le "$ROUNDS" ]]; do
  log "=== round ${round}/${ROUNDS} ==="
  fetch_all_groups "$SNAPSHOT" || fail "round ${round}: fetch before kill"
  assert_groups_healthy "round${round}-pre" "$SNAPSHOT"
  values_file <"$SNAPSHOT" >"$VALUES_PRE"

  KILL_NODE="$(pick_random_live_node)" || fail "round ${round}: no live nodes"
  KILL_PID="$(cat "$DATA/node-${KILL_NODE}.pid")"
  [[ -n "$KILL_PID" ]] || fail "round ${round}: missing pid for node ${KILL_NODE}"
  kill -0 "$KILL_PID" 2>/dev/null || fail "round ${round}: pid ${KILL_PID} not running"
  log "kill -9 node ${KILL_NODE} pid=${KILL_PID}"
  kill -9 "$KILL_PID"
  wait "$KILL_PID" 2>/dev/null || true

  # Wait for remaining admins to show leaders for all groups.
  FAILOVER_DEADLINE=$((SECONDS + 60))
  while true; do
    if fetch_all_groups "$AFTER" 2>/dev/null; then
      if python3 -c '
import sys
ok = True
for line in open(sys.argv[1]):
    parts = line.split()
    if len(parts) < 3 or not parts[2]:
        ok = False
        break
sys.exit(0 if ok else 1)
' "$AFTER"; then
        break
      fi
    fi
    if (( SECONDS >= FAILOVER_DEADLINE )); then
      fail "round ${round}: leaders not restored within 60s after killing node ${KILL_NODE}"
    fi
    sleep 0.5
  done
  assert_groups_healthy "round${round}-postkill" "$AFTER"
  values_file <"$AFTER" >"$VALUES_POST"
  assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "round${round}-postkill"
  log "round ${round}: leaders restored; values non-decreasing"

  # Restart killed node; wait catch-up (values >= post-kill snapshot).
  cp "$VALUES_POST" "$VALUES_PRE"
  start_one_node "$KILL_NODE"

  CATCHUP_DEADLINE=$((SECONDS + 60))
  while true; do
    if http_get "$(admin_url "$KILL_NODE")/groups/0/value" >/dev/null 2>&1 \
      && fetch_all_groups "$AFTER" 2>/dev/null; then
      values_file <"$AFTER" >"$VALUES_POST"
      if python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for a, b in zip(pre, post):
    if b < a:
        raise SystemExit(1)
sys.exit(0)
' "$VALUES_PRE" "$VALUES_POST"; then
        break
      fi
    fi
    if (( SECONDS >= CATCHUP_DEADLINE )); then
      fail "round ${round}: restarted node ${KILL_NODE} did not catch up within 60s"
    fi
    sleep 0.5
  done
  assert_groups_healthy "round${round}-restart" "$AFTER"
  assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "round${round}-restart"
  log "round ${round} OK"

  round=$((round + 1))
done

log "CHAOS OK"
printf 'CHAOS OK\n'
