#!/bin/sh
# Run on Aero Lens host (115) while server.py is up and fetch_clusters.json is configured (ansible mode).
# Usage: BASE=http://127.0.0.1:8787 DATE=2026-11-15 CLUSTER=az-prod-ci-upgrade sh analysis/test_fetch_api.sh

BASE="${BASE:-http://127.0.0.1:8787}"
DATE="${DATE:-2026-11-15}"
CLUSTER="${CLUSTER:-az-prod-ci-upgrade}"
TOKEN="${AERO_LENS_TOKEN:-}"

set -e

HDR=(-H 'Content-Type: application/json')
if [ -n "$TOKEN" ]; then
  HDR+=(-H "X-Aero-Lens-Token: $TOKEN")
fi

echo "=== health ==="
curl -sS "$BASE/api/health" | python3 -m json.tool

echo ""
echo "=== fetch clusters ==="
curl -sS "$BASE/api/fetch/clusters" | python3 -m json.tool

echo ""
echo "=== POST /api/fetch (async ansible) ==="
RESP=$(curl -sS -X POST "$BASE/api/fetch" "${HDR[@]}" \
  -d "{\"cluster\": \"$CLUSTER\", \"date\": \"$DATE\"}")
echo "$RESP" | python3 -m json.tool

JOB=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))")
SESSION=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('session',''))")
POLL=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('poll',''))")

if [ -z "$JOB" ]; then
  echo "No job_id — check fetch_mode and ansible SSH from this host."
  exit 1
fi

echo ""
echo "Polling $POLL (up to 10 min)..."
TRIES=60
while [ "$TRIES" -gt 0 ]; do
  J=$(curl -sS "$BASE/api/fetch/jobs/$JOB")
  STATUS=$(echo "$J" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))")
  echo "  status=$STATUS"
  if [ "$STATUS" = "completed" ]; then
  echo "$J" | python3 -m json.tool
    break
  fi
  if [ "$STATUS" = "failed" ]; then
  echo "$J" | python3 -m json.tool
    exit 1
  fi
  TRIES=$((TRIES - 1))
  sleep 10
done

if [ "$STATUS" != "completed" ]; then
  echo "Timed out; check /tmp/aero_lens_${JOB}.log on Ansible host."
  exit 1
fi

LATENCY=$(echo "$J" | python3 -c "import sys,json; print(json.load(sys.stdin).get('latency_logs',0))" 2>/dev/null || echo 0)
if [ -n "$SESSION" ] && [ "$LATENCY" -gt 0 ]; then
  echo ""
  echo "=== POST /api/analyze smoke ==="
  curl -sS -X POST "$BASE/api/analyze" \
    -H 'Content-Type: application/json' \
    -d "{\"session\": \"$SESSION\", \"window_mode\": \"full\", \"slice_seconds\": 60}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('results', len(d.get('results',[])), 'error', d.get('error'))"
fi

echo "Done."