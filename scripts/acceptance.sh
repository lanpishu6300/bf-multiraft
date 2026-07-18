#!/usr/bin/env bash
# Phase-1 acceptance for multiraft (design §5.2, adapted to single-process demo).
#
# Demo is one OS process with 3 logical MultiRaft nodes (shared in-process Router).
# Leader loss is simulated via POST /admin/shutdown_node/{id}.
#
# Compatible with macOS Bash 3.2 (no mapfile / namerefs).
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-21000}"
GROUPS="${GROUPS:-10}"
DATA="${ACCEPTANCE_DATA:-$ROOT/.acceptance-data}"
ADMIN="http://127.0.0.1:${BASE_PORT}"
DEMO_PID=""
WORKDIR=""

log() { printf '[acceptance] %s\n' "$*"; }
fail() { printf '[acceptance] FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [[ -n "${DEMO_PID}" ]] && kill -0 "${DEMO_PID}" 2>/dev/null; then
    kill "${DEMO_PID}" 2>/dev/null || true
    wait "${DEMO_PID}" 2>/dev/null || true
  fi
  if [[ -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    rm -rf "${WORKDIR}"
  fi
}
trap cleanup EXIT

http_get() {
  curl -fsS --max-time 3 "$1"
}

http_post() {
  curl -fsS --max-time 5 -X POST "$1"
}

start_demo() {
  local wipe="${1:-wipe}"
  if [[ "$wipe" == "wipe" ]]; then
    rm -rf "$DATA"
  fi
  mkdir -p "$DATA"

  "$ROOT/target/debug/multiraft-demo" \
    --mode cluster \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$DATA" \
    >"$DATA/cluster.log" 2>&1 &
  DEMO_PID=$!
  echo "$DEMO_PID" >"$DATA/cluster.pid"
  log "demo started pid=${DEMO_PID} data_dir=${DATA}"
}

wait_admin() {
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if http_get "${ADMIN}/groups/0/value" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "${DEMO_PID}" ]] && ! kill -0 "${DEMO_PID}" 2>/dev/null; then
      fail "demo exited early; see ${DATA}/cluster.log"
    fi
    sleep 0.2
  done
  fail "admin HTTP not ready within 30s; see ${DATA}/cluster.log"
}

# Write lines "group value leader" to $1 (one per group).
fetch_all_groups() {
  local out="$1"
  local g json
  : >"$out"
  g=0
  while [[ "$g" -lt "$GROUPS" ]]; do
    json="$(http_get "${ADMIN}/groups/${g}/value")"
    python3 -c '
import json, sys
d = json.loads(sys.argv[1])
leader = d.get("leader")
leader_s = "" if leader is None else str(leader)
print("%s %s %s" % (d["group"], d["value"], leader_s))
' "$json" >>"$out"
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
  log "${label}: ${GROUPS} groups have leaders + values"
}

values_file() {
  # stdin: group value leader lines → stdout: values only
  awk '{print $2}'
}

# --- prep ---
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/multiraft-acceptance.XXXXXX")"
F1="${WORKDIR}/g1.txt"
F2="${WORKDIR}/g2.txt"
F3="${WORKDIR}/g3.txt"
F4="${WORKDIR}/g4.txt"
F5="${WORKDIR}/g5.txt"

# --- build ---
log "building multiraft-demo"
cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"

# --- 1: start demo; values increase / leaders present ---
log "check 1: multi-group writes (leaders + increasing values)"
start_demo wipe
wait_admin
fetch_all_groups "$F1"
assert_groups_healthy "check1-initial" "$F1"

sleep 1.5
fetch_all_groups "$F2"
assert_groups_healthy "check1-later" "$F2"

values_file <"$F1" >"${WORKDIR}/v_before.txt"
values_file <"$F2" >"${WORKDIR}/v_after.txt"
python3 -c '
import sys
b = [int(x) for x in open(sys.argv[1]).read().split()]
a = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(a) == len(b) and len(a) > 0
for i, (x, y) in enumerate(zip(b, a)):
    if y <= x:
        raise SystemExit("group %d did not increase (%s -> %s)" % (i, x, y))
' "${WORKDIR}/v_before.txt" "${WORKDIR}/v_after.txt" \
  || fail "check1: not all group values increased"
log "check 1 OK: all ${GROUPS} group values increased"

# --- 2: shared connections ---
log "check 2: unique_peer_links O(nodes)"
LINKS_JSON="$(http_get "${ADMIN}/metrics/links")"
LINKS="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["unique_peer_links"])' "$LINKS_JSON")"
[[ "$LINKS" -lt 10 ]] || fail "check2: unique_peer_links=${LINKS} not < 10"
[[ "$LINKS" -le 6 ]] || fail "check2: unique_peer_links=${LINKS} not <= 6"
if [[ "$LINKS" -ne 3 ]]; then
  log "check2 note: expected 3 links, got ${LINKS} (still within bound)"
fi
log "check 2 OK: unique_peer_links=${LINKS}"

# --- 3+4: logical node shutdown (leader loss) + commit durability ---
log "check 3/4: shutdown one logical node; failover + values not lost"
fetch_all_groups "$F1"
values_file <"$F1" >"${WORKDIR}/pre_failover_values.txt"
KILL_NODE="$(python3 -c '
import json,sys
d=json.loads(sys.argv[1])
print(d["leader"] if d.get("leader") is not None else 1)
' "$(http_get "${ADMIN}/groups/0/value")")"
log "shutting down logical node ${KILL_NODE}"
SHUT_JSON="$(http_post "${ADMIN}/admin/shutdown_node/${KILL_NODE}")"
python3 -c '
import json,sys
d=json.loads(sys.argv[1])
assert d.get("ok") is True, d
assert int(d["node_id"]) == int(sys.argv[2]), d
' "$SHUT_JSON" "$KILL_NODE"

FAILOVER_DEADLINE=$((SECONDS + 20))
while true; do
  if fetch_all_groups "$F2" 2>/dev/null; then
    if python3 -c '
import sys
kill = sys.argv[1]
ok = True
for line in open(sys.argv[2]):
    parts = line.split()
    if len(parts) < 3 or parts[2] == kill:
        ok = False
        break
sys.exit(0 if ok else 1)
' "$KILL_NODE" "$F2"; then
      break
    fi
  fi
  if (( SECONDS >= FAILOVER_DEADLINE )); then
    fail "check3: groups did not failover away from node ${KILL_NODE} within 20s"
  fi
  sleep 0.3
done
assert_groups_healthy "check3-after-failover" "$F2"
log "check 3 OK: leaders are among remaining nodes (not ${KILL_NODE})"

values_file <"$F2" >"${WORKDIR}/post_failover_values.txt"
python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit("group %d lost committed value (%d -> %d)" % (i, a, b))
' "${WORKDIR}/pre_failover_values.txt" "${WORKDIR}/post_failover_values.txt" \
  || fail "check4: committed values lost after failover"

sleep 1.5
fetch_all_groups "$F3"
values_file <"$F3" >"${WORKDIR}/post_grow_values.txt"
python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
mid = [int(x) for x in open(sys.argv[2]).read().split()]
late = [int(x) for x in open(sys.argv[3]).read().split()]
assert len(pre) == len(mid) == len(late)
grew = 0
for i, (a, b, c) in enumerate(zip(pre, mid, late)):
    if c < a:
        raise SystemExit("group %d value regressed (%d -> %d)" % (i, a, c))
    if c > b:
        grew += 1
if grew < 1:
    raise SystemExit("no group advanced after failover")
print(grew)
' "${WORKDIR}/pre_failover_values.txt" \
  "${WORKDIR}/post_failover_values.txt" \
  "${WORKDIR}/post_grow_values.txt" >"${WORKDIR}/grew.txt" \
  || fail "check4: values not retained / not advancing after failover"
GREW="$(cat "${WORKDIR}/grew.txt")"
log "check 4 OK: committed values retained; ${GREW}/${GROUPS} groups still advancing"

cp "${WORKDIR}/post_grow_values.txt" "${WORKDIR}/pre_restart_values.txt"

# --- 5: restart whole demo process with same data_dir ---
log "check 5: process restart with same data_dir"
kill "${DEMO_PID}" 2>/dev/null || true
wait "${DEMO_PID}" 2>/dev/null || true
DEMO_PID=""
sleep 0.5
start_demo keep
wait_admin
fetch_all_groups "$F4"
assert_groups_healthy "check5-after-restart" "$F4"

values_file <"$F4" >"${WORKDIR}/restart_values.txt"
python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit("group %d lost value across restart (%d -> %d)" % (i, a, b))
' "${WORKDIR}/pre_restart_values.txt" "${WORKDIR}/restart_values.txt" \
  || fail "check5: values not restored after restart"
log "check 5 OK: FSM values restored/caught-up after restart"

log "check 5b: cargo restart_recover tests"
cargo test -p multiraft-store --test restart_recover --manifest-path "$ROOT/Cargo.toml"
cargo test -p multiraft-net --test restart_recover --manifest-path "$ROOT/Cargo.toml"
log "check 5b OK"

# --- 6: NotLeader ---
log "check 6: not_leader test"
cargo test -p multiraft-net --test not_leader --manifest-path "$ROOT/Cargo.toml"
log "check 6 OK"

log "ACCEPTANCE OK"
printf 'ACCEPTANCE OK\n'
