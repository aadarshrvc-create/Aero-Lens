#!/usr/bin/env bash
# Run on Ansible controller (dbeansible0). Args: one JSON blob from Aero Lens trigger.
set -euo pipefail

PAYLOAD_FILE="${1:-}"
if [ -z "$PAYLOAD_FILE" ] || [ ! -f "$PAYLOAD_FILE" ]; then
  echo "usage: $0 /path/to/payload.json" >&2
  exit 2
fi
PAYLOAD="$(cat "$PAYLOAD_FILE")"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYBOOK="${SCRIPT_DIR}/aero_lens_fetch.yml"
INVENTORY="${AERO_LENS_INVENTORY:-${SCRIPT_DIR}/inventory/production}"

JOB_ID="$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['job_id'])" "$PAYLOAD")"
CLUSTER="$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['cluster'])" "$PAYLOAD")"
GROUP="$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['ansible_group'])" "$PAYLOAD")"
DATE="$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['date'])" "$PAYLOAD")"
LOG_DIR="$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('log_dir','/var/log/aerospike'))" "$PAYLOAD")"
BASENAMES="$(python3 -c "import json,sys; print(','.join(json.loads(sys.argv[1])['log_basenames']))" "$PAYLOAD")"
TOOL_HOST="$(python3 -c "import json,sys; d=json.loads(sys.argv[1])['delivery']; print(d.get('tool_host',''))" "$PAYLOAD")"
TOOL_USER="$(python3 -c "import json,sys; d=json.loads(sys.argv[1])['delivery']; print(d.get('tool_user','dbops'))" "$PAYLOAD")"
INCOMING="$(python3 -c "import json,sys; d=json.loads(sys.argv[1])['delivery']; print(d.get('incoming_dir','/var/spool/aero-lens/incoming'))" "$PAYLOAD")"
BUNDLE="$(python3 -c "import json,sys; d=json.loads(sys.argv[1])['delivery']; print(d.get('bundle_name',''))" "$PAYLOAD")"
CALLBACK="$(python3 -c "import json,sys; d=json.loads(sys.argv[1])['delivery']; print(d.get('callback_url',''))" "$PAYLOAD")"
TOKEN="$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('fetch_token',''))" "$PAYLOAD")"

STAGING="${SCRIPT_DIR}/.aero_lens_staging"
mkdir -p "$STAGING"

IFS=',' read -r -a LOG_BASE_ARRAY <<< "$BASENAMES"

EXTRA=()
EXTRA+=(-e "aero_job_id=${JOB_ID}")
EXTRA+=(-e "aero_cluster_id=${CLUSTER}")
EXTRA+=(-e "aero_cluster_group=${GROUP}")
EXTRA+=(-e "aero_incident_date=${DATE}")
EXTRA+=(-e "aero_log_dir=${LOG_DIR}")
EXTRA+=(-e "aero_staging=${STAGING}")
BASENAMES_JSON="$(python3 -c "import json,sys; print(json.dumps(json.loads(sys.argv[1])['log_basenames']))" "$PAYLOAD")"
EXTRA+=(-e "aero_log_basenames=${BASENAMES_JSON}")

ansible-playbook -i "$INVENTORY" "$PLAYBOOK" "${EXTRA[@]}"

ARCHIVE="${STAGING}/${JOB_ID}.tar.gz"
if [ ! -f "$ARCHIVE" ]; then
  echo "archive missing: $ARCHIVE" >&2
  exit 1
fi

# Ensure incoming exists on tool host
ssh -o BatchMode=yes "${TOOL_USER}@${TOOL_HOST}" "mkdir -p '${INCOMING}'"

scp -q -o BatchMode=yes "${ARCHIVE}" "${TOOL_USER}@${TOOL_HOST}:${INCOMING}/${BUNDLE}"

if [ -n "$CALLBACK" ]; then
  CURL_HDR=(-H "Content-Type: application/json")
  if [ -n "$TOKEN" ]; then
    CURL_HDR+=(-H "X-Aero-Lens-Token: ${TOKEN}")
  fi
  curl -sS -X POST "$CALLBACK" "${CURL_HDR[@]}" \
    -d "$(python3 -c "import json; print(json.dumps({'job_id':'${JOB_ID}','bundle':'${BUNDLE}'}))")"
fi

echo "delivered ${BUNDLE} to ${TOOL_USER}@${TOOL_HOST}:${INCOMING}/"