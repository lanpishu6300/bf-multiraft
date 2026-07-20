#!/usr/bin/env bash
# Multi-process gRPC chaos: kill nodes under several scenarios, wait for leaders,
# ensure values never go backwards, restart killed nodes, wait for catch-up.
#
# Compatible with macOS Bash 3.2 (no mapfile / namerefs / assoc arrays).
#
# Env:
#   SCENARIO     random|kill_leader|kill_follower|rolling|double_kill|standby|all
#                (default: random)
#   ROUNDS       (default 5) — per-scenario rounds; rolling = full node pass per round
#   BASE_PORT    (default 22000 — avoid clashing with acceptance)
#   GROUPS       (default 5)
#   NODES        (default 3)
#   STANDBY      0|1 — start Learner Standby (node NODES+1); forced on for SCENARIO=standby
#   CHAOS_DATA   (default $ROOT/.chaos-data)
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-22000}"
GROUPS="${GROUPS:-5}"
NODES="${NODES:-3}"
ROUNDS="${ROUNDS:-5}"
SCENARIO="${SCENARIO:-random}"
STANDBY="${STANDBY:-0}"
if [[ "$SCENARIO" == "standby" ]]; then
  STANDBY=1
fi
DATA="${CHAOS_DATA:-$ROOT/.chaos-data}"
WORKDIR=""
# Voter count for majority; Standby is NODES+1 when STANDBY=1.
MAX_NODE_ID="$NODES"
if [[ "$STANDBY" == "1" ]]; then
  MAX_NODE_ID=$((NODES + 1))
fi

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
  local id pid max
  if [[ ! -d "$DATA" ]]; then
    return 0
  fi
  max=$((NODES + 2))
  id=1
  while [[ "$id" -le "$max" ]]; do
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
    STANDBY="$STANDBY" \
    "$ROOT/scripts/run_demo_cluster.sh" >/dev/null
  log "cluster started data_dir=${DATA} STANDBY=${STANDBY}"
}

start_one_node() {
  local id="$1"
  local node_data="$DATA/node-$id"
  # Strings avoid Bash 3.2 `set -u` unbound empty-array expand.
  local extra=""
  local role_flag="--role"
  local role_val="voter"
  mkdir -p "$node_data"
  if [[ "${JEPSEN:-0}" == "1" || "${NO_AUTO_PROPOSE:-0}" == "1" ]]; then
    extra="--no-auto-propose"
  fi
  if [[ "$STANDBY" == "1" && "$id" -gt "$NODES" ]]; then
    role_val="standby"
    extra="--no-auto-propose"
  fi
  # shellcheck disable=SC2086
  "$ROOT/target/debug/multiraft-demo" \
    --mode node \
    --node-id "$id" \
    --nodes "$NODES" \
    "$role_flag" "$role_val" \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$node_data" \
    $extra \
    >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
  log "restarted node ${id} --role ${role_val} pid=$(cat "$DATA/node-$id.pid")"
}

readd_standby() {
  local standby_id="$1"
  local id admin_port attempt=0
  while [[ "$attempt" -lt 30 ]]; do
    id=1
    while [[ "$id" -le "$NODES" ]]; do
      admin_port=$((BASE_PORT + 100 + id - 1))
      if curl -sf -X POST \
        "http://127.0.0.1:${admin_port}/admin/add_standby/0/${standby_id}" \
        >/dev/null 2>&1; then
        log "add_standby ${standby_id} ok via node ${id}"
        return 0
      fi
      id=$((id + 1))
    done
    attempt=$((attempt + 1))
    sleep 0.3
  done
  log "warning: add_standby ${standby_id} did not succeed"
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
  local deadline=$((SECONDS + 90))
  local url
  while (( SECONDS < deadline )); do
    if url="$(find_live_admin)"; then
      log "admin ready: ${url}"
      return 0
    fi
    sleep 0.3
  done
  fail "no admin HTTP ready within 90s; see ${DATA}/node-*.log"
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

count_words() {
  # shellcheck disable=SC2206
  local arr=($1)
  echo "${#arr[@]}"
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

# Leader of group 0 from any live admin JSON (empty if unknown).
group0_leader() {
  local id url json leader
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    url="$(admin_url "$id")"
    if json="$(http_get "${url}/groups/0/value" 2>/dev/null)"; then
      leader="$(python3 -c '
import json,sys
d=json.loads(sys.argv[1])
print("" if d.get("leader") is None else str(d["leader"]))
' "$json")"
      if [[ -n "$leader" ]]; then
        echo "$leader"
        return 0
      fi
    fi
    id=$((id + 1))
  done
  return 1
}

# Pick a live node that is not the reported group-0 leader.
pick_live_follower() {
  local leader live id
  leader="$(group0_leader)" || return 1
  live="$(live_node_ids)"
  for id in $live; do
    if [[ "$id" != "$leader" ]]; then
      echo "$id"
      return 0
    fi
  done
  return 1
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

wait_all_groups_healthy() {
  local label="$1"
  local out="$2"
  local deadline=$((SECONDS + 90))
  while true; do
    if fetch_all_groups "$out" 2>/dev/null; then
      if python3 -c '
import sys
ok = True
for line in open(sys.argv[1]):
    parts = line.split()
    if len(parts) < 3 or not parts[2]:
        ok = False
        break
sys.exit(0 if ok else 1)
' "$out"; then
        assert_groups_healthy "$label" "$out"
        return 0
      fi
    fi
    if (( SECONDS >= deadline )); then
      fail "${label}: leaders not restored within 90s"
    fi
    sleep 0.5
  done
}

wait_node_catchup() {
  local node_id="$1"
  local values_pre="$2"
  local after_file="$3"
  local values_post="$4"
  local label="$5"
  local deadline=$((SECONDS + 90))
  while true; do
    if http_get "$(admin_url "$node_id")/groups/0/value" >/dev/null 2>&1 \
      && fetch_all_groups "$after_file" 2>/dev/null; then
      values_file <"$after_file" >"$values_post"
      if python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for a, b in zip(pre, post):
    if b < a:
        raise SystemExit(1)
sys.exit(0)
' "$values_pre" "$values_post"; then
        assert_groups_healthy "${label}-restart" "$after_file"
        assert_values_not_backwards "$values_pre" "$values_post" "${label}-restart"
        return 0
      fi
    fi
    if (( SECONDS >= deadline )); then
      fail "${label}: restarted node ${node_id} did not catch up within 90s"
    fi
    sleep 0.5
  done
}

# Kill one live node, wait healthy + non-decreasing, restart, catch-up.
kill_restart_one() {
  local kill_node="$1"
  local label="$2"
  local kill_pid

  fetch_all_groups "$SNAPSHOT" || fail "${label}: fetch before kill"
  assert_groups_healthy "${label}-pre" "$SNAPSHOT"
  values_file <"$SNAPSHOT" >"$VALUES_PRE"

  [[ -n "$kill_node" ]] || fail "${label}: empty kill target"
  kill_pid="$(cat "$DATA/node-${kill_node}.pid" 2>/dev/null || true)"
  [[ -n "$kill_pid" ]] || fail "${label}: missing pid for node ${kill_node}"
  kill -0 "$kill_pid" 2>/dev/null || fail "${label}: pid ${kill_pid} not running"

  # Never drop below majority (ceil(NODES/2)+1 live after kill).
  local live_before live_count majority
  live_before="$(live_node_ids)"
  live_count="$(count_words "$live_before")"
  majority=$(( NODES / 2 + 1 ))
  if [[ "$((live_count - 1))" -lt "$majority" ]]; then
    fail "${label}: refusing kill of ${kill_node}: would leave $((live_count - 1)) < majority ${majority}"
  fi

  log "kill -9 node ${kill_node} pid=${kill_pid}"
  kill -9 "$kill_pid"
  wait "$kill_pid" 2>/dev/null || true

  wait_all_groups_healthy "${label}-postkill" "$AFTER"
  values_file <"$AFTER" >"$VALUES_POST"
  assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "${label}-postkill"
  log "${label}: leaders restored; values non-decreasing"

  cp "$VALUES_POST" "$VALUES_PRE"
  start_one_node "$kill_node"
  wait_node_catchup "$kill_node" "$VALUES_PRE" "$AFTER" "$VALUES_POST" "$label"
  log "${label}: node ${kill_node} catch-up OK"
}

run_random_rounds() {
  local round=1 kill_node
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== random round ${round}/${ROUNDS} ==="
    kill_node="$(pick_random_live_node)" || fail "random round ${round}: no live nodes"
    kill_restart_one "$kill_node" "random-r${round}"
    round=$((round + 1))
  done
}

run_kill_leader_rounds() {
  local round=1 kill_node
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== kill_leader round ${round}/${ROUNDS} ==="
    kill_node="$(group0_leader)" || fail "kill_leader round ${round}: no group-0 leader"
    # Ensure target is live.
    if ! kill -0 "$(cat "$DATA/node-${kill_node}.pid" 2>/dev/null || echo)" 2>/dev/null; then
      fail "kill_leader round ${round}: leader ${kill_node} not live"
    fi
    kill_restart_one "$kill_node" "kill_leader-r${round}"
    round=$((round + 1))
  done
}

run_kill_follower_rounds() {
  local round=1 kill_node
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== kill_follower round ${round}/${ROUNDS} ==="
    kill_node="$(pick_live_follower)" || fail "kill_follower round ${round}: no live follower"
    kill_restart_one "$kill_node" "kill_follower-r${round}"
    round=$((round + 1))
  done
}

run_rolling_rounds() {
  local round=1 id
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== rolling round ${round}/${ROUNDS} (nodes 1..${NODES}) ==="
    id=1
    while [[ "$id" -le "$NODES" ]]; do
      kill_restart_one "$id" "rolling-r${round}-n${id}"
      id=$((id + 1))
    done
    round=$((round + 1))
  done
}

run_double_kill_rounds() {
  local round=1 first second
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== double_kill round ${round}/${ROUNDS} ==="
    first="$(pick_random_live_node)" || fail "double_kill round ${round}: no live nodes"
    kill_restart_one "$first" "double_kill-r${round}-a"
    # After restart, pick another (prefer different) live node.
    second="$(pick_random_live_node)" || fail "double_kill round ${round}: no live after restart"
    if [[ "$second" == "$first" ]]; then
      second="$(pick_live_follower 2>/dev/null || true)"
      if [[ -z "${second:-}" ]]; then
        second="$(pick_random_live_node)" || fail "double_kill round ${round}: no second target"
      fi
    fi
    kill_restart_one "$second" "double_kill-r${round}-b"
    round=$((round + 1))
  done
}

# Kill/restart Standby (does not affect voter majority). Then kill leader once.
run_standby_rounds() {
  local round=1 standby_id kill_pid
  standby_id=$((NODES + 1))
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== standby kill/restart round ${round}/${ROUNDS} (node ${standby_id}) ==="
    fetch_all_groups "$SNAPSHOT" || fail "standby-r${round}: fetch before kill"
    assert_groups_healthy "standby-r${round}-pre" "$SNAPSHOT"
    values_file <"$SNAPSHOT" >"$VALUES_PRE"

    kill_pid="$(cat "$DATA/node-${standby_id}.pid" 2>/dev/null || true)"
    [[ -n "$kill_pid" ]] || fail "standby-r${round}: missing pid for standby ${standby_id}"
    log "kill -9 standby ${standby_id} pid=${kill_pid}"
    kill -9 "$kill_pid"
    wait "$kill_pid" 2>/dev/null || true

    wait_all_groups_healthy "standby-r${round}-postkill" "$AFTER"
    values_file <"$AFTER" >"$VALUES_POST"
    assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "standby-r${round}-postkill"

    cp "$VALUES_POST" "$VALUES_PRE"
    start_one_node "$standby_id"
    readd_standby "$standby_id"
    # Voters must remain healthy; standby catch-up is best-effort (admin may be stale).
    wait_all_groups_healthy "standby-r${round}-restart" "$AFTER"
    values_file <"$AFTER" >"$VALUES_POST"
    assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "standby-r${round}-restart"
    log "standby-r${round}: OK"
    round=$((round + 1))
  done
  log "=== standby: kill_leader with Standby present ==="
  run_kill_leader_rounds
}

run_one_scenario() {
  local name="$1"
  log "SCENARIO=${name} ROUNDS=${ROUNDS} NODES=${NODES} GROUPS=${GROUPS} STANDBY=${STANDBY}"
  case "$name" in
    random) run_random_rounds ;;
    kill_leader) run_kill_leader_rounds ;;
    kill_follower) run_kill_follower_rounds ;;
    rolling) run_rolling_rounds ;;
    double_kill) run_double_kill_rounds ;;
    standby) run_standby_rounds ;;
    *) fail "unknown SCENARIO='${name}' (want random|kill_leader|kill_follower|rolling|double_kill|standby|all)" ;;
  esac
  log "SCENARIO=${name} OK"
}

# --- prep ---
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/multiraft-chaos.XXXXXX")"
SNAPSHOT="${WORKDIR}/snap.txt"
AFTER="${WORKDIR}/after.txt"
VALUES_PRE="${WORKDIR}/v_pre.txt"
VALUES_POST="${WORKDIR}/v_post.txt"

case "$SCENARIO" in
  random|kill_leader|kill_follower|rolling|double_kill|standby|all) ;;
  *) fail "unknown SCENARIO='${SCENARIO}'" ;;
esac

log "building + starting multi-process demo (SCENARIO=${SCENARIO} ROUNDS=${ROUNDS} STANDBY=${STANDBY})"
export DATA_DIR="$DATA"
export BASE_PORT GROUPS NODES STANDBY
start_cluster wipe
wait_any_admin

wait_all_groups_healthy "warmup" "$SNAPSHOT"
log "warmup OK"

if [[ "$SCENARIO" == "all" ]]; then
  for name in random kill_leader kill_follower rolling double_kill standby; do
    # standby scenario needs its own cluster with STANDBY=1
    if [[ "$name" == "standby" ]]; then
      STANDBY=1
      MAX_NODE_ID=$((NODES + 1))
      export STANDBY
      start_cluster wipe
      wait_any_admin
      wait_all_groups_healthy "standby-warmup" "$SNAPSHOT"
    fi
    run_one_scenario "$name"
  done
else
  run_one_scenario "$SCENARIO"
fi

log "CHAOS OK"
printf 'CHAOS OK\n'
