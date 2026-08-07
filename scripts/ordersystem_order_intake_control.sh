#!/usr/bin/env bash
set -Eeuo pipefail

BASE_URL="${BASE_URL:-http://localhost:8081}"
BOT_NAME="${1:-${BOT_NAME:-}}"
ACTION="${2:-${ACTION:-block}}"
REASON="${REASON:-manual order intake block}"

usage() {
  cat <<EOF
Usage:
  $0 <bot-name> [block|resume|status]

Examples:
  $0 haemabot
  $0 firebot block
  $0 titanbot block
  $0 fibobot block
  $0 fibobot status
  $0 fibobot resume
  $0 meanbot block
  $0 meanbot resume
  $0 haemabot status
  $0 haemabot resume

Overrides:
  BASE_URL=http://localhost:8081
  REASON='manual pause before event'
EOF
}

if [[ "${BOT_NAME}" == "-h" || "${BOT_NAME}" == "--help" || -z "${BOT_NAME}" ]]; then
  usage
  if [[ -z "${BOT_NAME}" ]]; then
    exit 1
  fi
  exit 0
fi

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/ }"
  value="${value//$'\r'/ }"
  printf '%s' "$value"
}

case "${ACTION}" in
  block)
    escaped_reason="$(json_escape "${REASON}")"
    curl -sS -X POST "${BASE_URL}/v1/bots/${BOT_NAME}/block-orders" \
      -H 'Content-Type: application/json' \
      -d "{\"reason\":\"${escaped_reason}\"}"
    echo
    ;;
  resume)
    curl -sS -X POST "${BASE_URL}/v1/bots/${BOT_NAME}/resume" \
      -H 'Content-Type: application/json' \
      -d "{\"reason\":\"resume order intake\"}"
    echo
    ;;
  status)
    curl -sS "${BASE_URL}/v1/bots/${BOT_NAME}/block-orders"
    echo
    ;;
  *)
    echo "Unknown action: ${ACTION}" >&2
    usage >&2
    exit 1
    ;;
esac
