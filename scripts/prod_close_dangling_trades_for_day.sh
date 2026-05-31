#!/usr/bin/env bash
set -Eeuo pipefail

# End-of-day OMS cleanup for all bot strategies.
#
# Purpose:
#   Close all dangling/open OMS trades for one trading day after market hours.
#
# What it does:
#   - Uses DATABASE_URL from the live ordersystem pod, unless DATABASE_URL is set.
#   - Lists all OPEN/PLACED/ENTRY_PLACED/blank-status trades for TARGET_DATE.
#   - Patches bot local order_log.csv files, if reachable.
#   - Scales bot workloads down to clear in-memory state.
#   - Marks matching OMS trades as EOD_SQUARE_OFF and disables trailing.
#   - Marks linked SL order rows with exit_time.
#   - Scales bot workloads back to their original replica counts.
#
# What it does not do:
#   - It does not call Upstox.
#   - It does not cancel broker orders.
#   - It does not delete trade/order history.
#
# Dry run:
#   bash scripts/prod_close_dangling_trades_for_day.sh
#
# Close today's dangling trades after market:
#   CONFIRM_CLOSE=1 bash scripts/prod_close_dangling_trades_for_day.sh
#
# Close a specific day:
#   TARGET_DATE=18-05-2026 CONFIRM_CLOSE=1 bash scripts/prod_close_dangling_trades_for_day.sh

NAMESPACE="${NAMESPACE:-botspace}"
TARGET_DATE="${TARGET_DATE:-$(TZ=Asia/Kolkata date +%d-%m-%Y)}"
CLOSE_STATUS="${CLOSE_STATUS:-EOD_SQUARE_OFF}"
CLOSE_REASON="${CLOSE_REASON:-EOD dangling trade cleanup}"
CLOSE_TIME="${CLOSE_TIME:-$(TZ=Asia/Kolkata date --iso-8601=seconds)}"
CONFIRM_CLOSE="${CONFIRM_CLOSE:-0}"
AFTER_HOURS_GUARD="${AFTER_HOURS_GUARD:-1}"
MARKET_CLOSE_HHMM="${MARKET_CLOSE_HHMM:-1530}"
FORCE_BEFORE_MARKET_CLOSE="${FORCE_BEFORE_MARKET_CLOSE:-0}"

BOT_WORKLOAD_PATTERN="${BOT_WORKLOAD_PATTERN:-solobot|trendobot|haemabot|hemabot|firebot}"
BOT_POD_PATTERN="${BOT_POD_PATTERN:-solobot|trendobot|haemabot|hemabot|firebot}"
ORDERSYSTEM_POD="${ORDERSYSTEM_POD:-}"
PSQL_IMAGE="${PSQL_IMAGE:-postgres:16-alpine}"
PSQL_POD="${PSQL_POD:-oms-eod-close-${USER:-user}-$$}"
BACKUP_DIR="${BACKUP_DIR:-/tmp/oms-eod-dangling-trade-close}"

PATCH_LOCAL_LEDGER="${PATCH_LOCAL_LEDGER:-1}"
SCALE_BOTS="${SCALE_BOTS:-1}"
RESUME_BOTS="${RESUME_BOTS:-1}"
ALLOW_NO_BOT_WORKLOAD="${ALLOW_NO_BOT_WORKLOAD:-0}"

usage() {
  cat <<EOF
Usage:
  bash $0

Dry run:
  bash $0

Close all dangling trades for TARGET_DATE:
  CONFIRM_CLOSE=1 bash $0

Close a specific day:
  TARGET_DATE=18-05-2026 CONFIRM_CLOSE=1 bash $0

Useful overrides:
  NAMESPACE=botspace
  TARGET_DATE=DD-MM-YYYY
  CLOSE_STATUS=EOD_SQUARE_OFF
  BOT_WORKLOAD_PATTERN='solobot|trendobot|haemabot|hemabot|firebot'
  BOT_POD_PATTERN='solobot|trendobot|haemabot|hemabot|firebot'
  ORDERSYSTEM_POD=<ordersystem-pod-name>
  DATABASE_URL='postgresql://...'
  FORCE_BEFORE_MARKET_CLOSE=1
  RESUME_BOTS=0
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

need_cmd kubectl
need_cmd awk
need_cmd grep
need_cmd sed
need_cmd date
need_cmd mkdir

log() {
  printf '\n==> %s\n' "$*"
}

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

if [[ ! "$TARGET_DATE" =~ ^[0-9]{2}-[0-9]{2}-[0-9]{4}$ ]]; then
  echo "TARGET_DATE must be DD-MM-YYYY. Got: ${TARGET_DATE}" >&2
  exit 1
fi

if [[ "$AFTER_HOURS_GUARD" == "1" && "$FORCE_BEFORE_MARKET_CLOSE" != "1" ]]; then
  today_ist="$(TZ=Asia/Kolkata date +%d-%m-%Y)"
  now_hhmm="$(TZ=Asia/Kolkata date +%H%M)"
  if [[ "$TARGET_DATE" == "$today_ist" && "$now_hhmm" < "$MARKET_CLOSE_HHMM" ]]; then
    cat >&2 <<EOF
Refusing to close today's trades before market close.
TARGET_DATE=${TARGET_DATE}, IST time=${now_hhmm}, MARKET_CLOSE_HHMM=${MARKET_CLOSE_HHMM}

Override only if you are sure:
  FORCE_BEFORE_MARKET_CLOSE=1 CONFIRM_CLOSE=1 bash $0
EOF
    exit 1
  fi
fi

find_pod_by_name() {
  local pattern="$1"
  kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null \
    | awk -v pat="$pattern" '$1 ~ pat && $3 == "Running" {print $1; exit}' \
    || true
}

list_pods_by_name() {
  local pattern="$1"
  kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null \
    | awk -v pat="$pattern" '$1 ~ pat && $3 == "Running" {print $1}' \
    || true
}

list_workloads_by_name() {
  local pattern="$1"
  kubectl get deploy,statefulset -n "$NAMESPACE" -o name 2>/dev/null \
    | grep -E "$pattern" \
    || true
}

jsonpath_or_empty() {
  local resource="$1"
  local path="$2"
  kubectl get "$resource" -n "$NAMESPACE" -o "jsonpath=${path}" 2>/dev/null || true
}

ORDERSYSTEM_POD="${ORDERSYSTEM_POD:-$(find_pod_by_name 'ordersystem' || true)}"
mapfile -t BOT_PODS < <(list_pods_by_name "$BOT_POD_PATTERN")
mapfile -t BOT_WORKLOADS < <(list_workloads_by_name "$BOT_WORKLOAD_PATTERN")

log "Resolved Kubernetes targets"
echo "Namespace:          ${NAMESPACE}"
echo "Target date:        ${TARGET_DATE}"
echo "Close status:       ${CLOSE_STATUS}"
echo "Close time:         ${CLOSE_TIME}"
echo "Ordersystem pod:    ${ORDERSYSTEM_POD:-<not found>}"
echo "Bot pods:           ${BOT_PODS[*]:-<none>}"
echo "Bot workloads:      ${BOT_WORKLOADS[*]:-<none>}"

if [[ -z "${ORDERSYSTEM_POD}" && -z "${DATABASE_URL:-}" ]]; then
  echo "Could not find ordersystem pod and DATABASE_URL was not set." >&2
  exit 1
fi

if [[ ${#BOT_WORKLOADS[@]} -eq 0 && "$ALLOW_NO_BOT_WORKLOAD" != "1" ]]; then
  cat >&2 <<EOF
No bot workloads matched BOT_WORKLOAD_PATTERN=${BOT_WORKLOAD_PATTERN}.
This script normally scales bot workloads down before closing OMS rows.

Set BOT_WORKLOAD_PATTERN correctly, or override:
  ALLOW_NO_BOT_WORKLOAD=1 CONFIRM_CLOSE=1 bash $0
EOF
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  DATABASE_URL="$(kubectl exec -n "$NAMESPACE" "$ORDERSYSTEM_POD" -- printenv DATABASE_URL 2>/dev/null | tr -d '\r' || true)"
fi
if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "Could not read DATABASE_URL from ${ORDERSYSTEM_POD}. Set DATABASE_URL manually." >&2
  exit 1
fi

log "Using DATABASE_URL from live ordersystem"
echo "$DATABASE_URL" | sed -E 's#(postgres(ql)?://[^:/?]+:)[^@?]+@#\1****@#'

cleanup_psql_pod() {
  kubectl delete pod -n "$NAMESPACE" "$PSQL_POD" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
}
trap cleanup_psql_pod EXIT

start_psql_pod() {
  log "Starting temporary psql client pod ${PSQL_POD}"
  cleanup_psql_pod
  kubectl run "$PSQL_POD" \
    -n "$NAMESPACE" \
    --image="$PSQL_IMAGE" \
    --restart=Never \
    --env="DATABASE_URL=${DATABASE_URL}" \
    --env="TARGET_DATE=${TARGET_DATE}" \
    --env="CLOSE_STATUS=${CLOSE_STATUS}" \
    --env="CLOSE_TIME=${CLOSE_TIME}" \
    --env="CLOSE_REASON=${CLOSE_REASON}" \
    --command -- sleep 3600 >/dev/null
  kubectl wait -n "$NAMESPACE" --for=condition=Ready "pod/${PSQL_POD}" --timeout=90s >/dev/null
}

psql_exec() {
  kubectl exec -i -n "$NAMESPACE" "$PSQL_POD" -- sh -lc \
    'psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -v target_date="$TARGET_DATE" -v close_status="$CLOSE_STATUS" -v close_time="$CLOSE_TIME" -v close_reason="$CLOSE_REASON"'
}

psql_scalar() {
  kubectl exec -i -n "$NAMESPACE" "$PSQL_POD" -- sh -lc \
    'psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -tA -v target_date="$TARGET_DATE" -v close_status="$CLOSE_STATUS" -v close_time="$CLOSE_TIME" -v close_reason="$CLOSE_REASON"'
}

list_dangling_trades() {
  log "Dangling/open OMS trades for ${TARGET_DATE}"
  psql_exec <<'SQL'
\pset pager off
WITH target AS (
  SELECT
    t.id,
    COALESCE(a.botname, '') AS botname,
    COALESCE(a.curr_date, '') AS curr_date,
    COALESCE(t.status, '') AS status,
    COALESCE(t.tsl_active, false) AS tsl_active,
    COALESCE(t.symbol, '') AS symbol,
    COALESCE(t.side, '') AS side,
    COALESCE(t.qty, 0) AS qty,
    t."timestamp",
    COUNT(o.id) AS order_count,
    STRING_AGG(NULLIF(o.order_id, ''), ', ' ORDER BY o.order_type, o.id) AS broker_order_ids
  FROM trades AS t
  JOIN accounts AS a ON a.id = t.acct_id
  LEFT JOIN orders AS o ON o.trade_id = t.id
  WHERE COALESCE(a.curr_date, '') = :'target_date'
    AND UPPER(COALESCE(t.status, '')) IN ('', 'OPEN', 'PLACED', 'ENTRY_PLACED')
  GROUP BY t.id, a.botname, a.curr_date
)
SELECT *
FROM target
ORDER BY botname, "timestamp", id;

\echo
\echo Count:
SELECT COUNT(*)
FROM trades AS t
JOIN accounts AS a ON a.id = t.acct_id
WHERE COALESCE(a.curr_date, '') = :'target_date'
  AND UPPER(COALESCE(t.status, '')) IN ('', 'OPEN', 'PLACED', 'ENTRY_PLACED');
SQL
}

dangling_trade_ids_csv() {
  psql_scalar <<'SQL'
SELECT COALESCE(STRING_AGG(t.id::text, ',' ORDER BY a.botname, t."timestamp", t.id), '')
FROM trades AS t
JOIN accounts AS a ON a.id = t.acct_id
WHERE COALESCE(a.curr_date, '') = :'target_date'
  AND UPPER(COALESCE(t.status, '')) IN ('', 'OPEN', 'PLACED', 'ENTRY_PLACED');
SQL
}

backup_rows() {
  local stamp backup_file
  stamp="$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$BACKUP_DIR"
  backup_file="${BACKUP_DIR}/dangling_trades_${TARGET_DATE}_${stamp}.json"
  psql_scalar <<'SQL' >"$backup_file"
WITH target AS (
  SELECT
    t.id,
    jsonb_build_object(
      'account', to_jsonb(a),
      'trade', to_jsonb(t),
      'orders', COALESCE((
        SELECT jsonb_agg(to_jsonb(o) ORDER BY o.order_type, o.id)
        FROM orders AS o
        WHERE o.trade_id = t.id
      ), '[]'::jsonb)
    ) AS row_data
  FROM trades AS t
  JOIN accounts AS a ON a.id = t.acct_id
  WHERE COALESCE(a.curr_date, '') = :'target_date'
    AND UPPER(COALESCE(t.status, '')) IN ('', 'OPEN', 'PLACED', 'ENTRY_PLACED')
)
SELECT COALESCE(jsonb_pretty(jsonb_agg(row_data ORDER BY id)), '[]')
FROM target;
SQL
  echo "DB backup written: ${backup_file}"
}

patch_local_ledgers() {
  local ids_csv="$1"
  if [[ "$PATCH_LOCAL_LEDGER" != "1" || -z "$ids_csv" ]]; then
    return
  fi
  if [[ ${#BOT_PODS[@]} -eq 0 ]]; then
    warn "No running bot pods matched BOT_POD_PATTERN=${BOT_POD_PATTERN}; skipping local CSV patch."
    return
  fi

  for pod in "${BOT_PODS[@]}"; do
    log "Patching local order_log.csv files in ${pod}"
    kubectl exec -i -n "$NAMESPACE" "$pod" -- sh -s -- "$ids_csv" "$CLOSE_STATUS" "$CLOSE_TIME" <<'SH' || true
set -eu
ids_csv="$1"
close_status="$2"
close_time="$3"
IDS_CSV="$ids_csv" CLOSE_STATUS="$close_status" CLOSE_TIME="$close_time" python - <<'PY'
import csv
import glob
import os
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

target_ids = {value.strip() for value in os.environ["IDS_CSV"].split(",") if value.strip()}
close_status = os.environ["CLOSE_STATUS"]
close_time = os.environ["CLOSE_TIME"]
stamp = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y%m%d_%H%M%S")

files_dirs = [
    os.environ.get("SOLOBOT_FILES_DIR", "").strip(),
    os.environ.get("TRENDOBOT_FILES_DIR", "").strip(),
    os.environ.get("HAEMABOT_FILES_DIR", "").strip(),
    os.environ.get("HEMABOT_FILES_DIR", "").strip(),
    os.environ.get("FIREBOT_FILES_DIR", "").strip(),
    os.environ.get("BOT_FILES_DIR", "").strip(),
    "files",
    "/app/files",
]

candidates = []
for name in ("ORDERSYSTEM_ORDERS_CSV", "ORDER_SYSTEM_ORDERS_CSV", "OMS_ORDERS_CSV"):
    value = os.environ.get(name, "").strip()
    if value:
        candidates.append(value)
for files_dir in files_dirs:
    if not files_dir:
        continue
    candidates.extend([
        f"{files_dir}/execution_results/prod/order_log.csv",
        f"{files_dir}/execution_results/production/order_log.csv",
        f"{files_dir}/execution_results/sandbox/order_log.csv",
        f"{files_dir}/order_log.csv",
    ])
candidates.extend(glob.glob("files/**/order_log.csv", recursive=True))
candidates.extend(glob.glob("/app/files/**/order_log.csv", recursive=True))

seen = set()
paths = []
for raw in candidates:
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    key = str(path)
    if key not in seen:
        seen.add(key)
        paths.append(path)

matched = 0
for path in paths:
    if not path.exists():
        continue
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)
    except Exception as exc:
        print(f"WARN: could not read {path}: {exc}")
        continue
    if not fieldnames:
        continue

    changed = False
    for row in rows:
        trade_id = str(row.get("id") or row.get("trade_id") or "").strip()
        if trade_id not in target_ids:
            continue
        matched += 1
        changed = True
        for column in ("status", "exit_time", "tsl_active", "description"):
            if column not in fieldnames:
                fieldnames.append(column)
        row["status"] = close_status
        row["tsl_active"] = "False"
        if not str(row.get("exit_time") or "").strip():
            row["exit_time"] = close_time
        existing = str(row.get("description") or "")
        suffix = f"closed by EOD dangling cleanup {stamp}"
        row["description"] = f"{existing} | {suffix}" if existing else suffix

    if not changed:
        continue

    backup = path.with_name(f"{path.name}.bak-eod-close-{stamp}")
    shutil.copy2(path, backup)
    tmp = path.with_name(f".{path.name}.tmp-eod-close-{stamp}")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)
    print(f"patched {path}; backup {backup}")

if matched == 0:
    print("no matching local CSV rows found")
else:
    print(f"patched {matched} local CSV row(s)")
PY
SH
  done
}

SCALE_FILE=""
scale_bots_down() {
  if [[ "$SCALE_BOTS" != "1" ]]; then
    return
  fi
  if [[ ${#BOT_WORKLOADS[@]} -eq 0 ]]; then
    warn "No bot workloads to scale down."
    return
  fi

  SCALE_FILE="$(mktemp /tmp/oms-eod-bot-scales.XXXXXX)"
  log "Scaling bot workloads down"
  for workload in "${BOT_WORKLOADS[@]}"; do
    replicas="$(jsonpath_or_empty "$workload" '{.spec.replicas}')"
    if [[ -z "$replicas" ]]; then
      replicas="1"
    fi
    printf '%s %s\n' "$workload" "$replicas" >>"$SCALE_FILE"
    echo "Scaling ${workload} from ${replicas} to 0"
    kubectl scale -n "$NAMESPACE" "$workload" --replicas=0
  done
  for workload in "${BOT_WORKLOADS[@]}"; do
    kubectl rollout status -n "$NAMESPACE" "$workload" --timeout=180s || true
  done
}

scale_bots_up() {
  if [[ "$SCALE_BOTS" != "1" || "$RESUME_BOTS" != "1" ]]; then
    return
  fi
  if [[ -z "$SCALE_FILE" || ! -f "$SCALE_FILE" ]]; then
    return
  fi

  log "Scaling bot workloads back"
  while read -r workload replicas; do
    [[ -z "$workload" ]] && continue
    if [[ -z "$replicas" || "$replicas" == "0" ]]; then
      replicas="1"
    fi
    echo "Scaling ${workload} back to ${replicas}"
    kubectl scale -n "$NAMESPACE" "$workload" --replicas="$replicas"
  done <"$SCALE_FILE"
  while read -r workload _replicas; do
    [[ -z "$workload" ]] && continue
    kubectl rollout status -n "$NAMESPACE" "$workload" --timeout=240s || true
  done <"$SCALE_FILE"
}

close_trades_in_db() {
  log "Closing dangling OMS trades in database"
  psql_exec <<'SQL'
BEGIN;

WITH target AS (
  SELECT t.id
  FROM trades AS t
  JOIN accounts AS a ON a.id = t.acct_id
  WHERE COALESCE(a.curr_date, '') = :'target_date'
    AND UPPER(COALESCE(t.status, '')) IN ('', 'OPEN', 'PLACED', 'ENTRY_PLACED')
),
updated_trades AS (
  UPDATE trades AS t
  SET
    status = :'close_status',
    tsl_active = false,
    description = CASE
      WHEN NULLIF(BTRIM(COALESCE(t.description, '')), '') IS NULL
        THEN :'close_reason'
      WHEN POSITION(:'close_reason' IN t.description) > 0
        THEN t.description
      ELSE t.description || ' | ' || :'close_reason'
    END
  FROM target
  WHERE t.id = target.id
  RETURNING t.id, t.status, t.symbol, t.side, t.qty, t."timestamp"
),
updated_sl_orders AS (
  UPDATE orders AS o
  SET exit_time = COALESCE(o.exit_time, :'close_time'::timestamptz)
  FROM target
  WHERE o.trade_id = target.id
    AND lower(COALESCE(o.order_type, '')) = 'sl'
    AND o.exit_time IS NULL
  RETURNING o.id, o.trade_id, o.order_id, o.order_type
)
SELECT 'trades_closed' AS result, COUNT(*) FROM updated_trades
UNION ALL
SELECT 'sl_orders_marked_exit_time' AS result, COUNT(*) FROM updated_sl_orders;

COMMIT;
SQL
}

start_psql_pod
list_dangling_trades
ids_csv="$(dangling_trade_ids_csv)"

if [[ -z "$ids_csv" ]]; then
  log "No dangling/open OMS trades found for ${TARGET_DATE}."
  exit 0
fi

if [[ "$CONFIRM_CLOSE" != "1" ]]; then
  cat <<EOF

Dry run only. No changes were made.

This script does not call Upstox. Before closing rows, confirm broker-side
positions/orders are already flat after market hours.

To close all listed dangling trades:
  CONFIRM_CLOSE=1 bash $0

For a specific date:
  TARGET_DATE=${TARGET_DATE} CONFIRM_CLOSE=1 bash $0
EOF
  exit 0
fi

backup_rows
patch_local_ledgers "$ids_csv"
scale_bots_down
close_trades_in_db
list_dangling_trades
scale_bots_up

log "EOD dangling trade cleanup complete for ${TARGET_DATE}"
