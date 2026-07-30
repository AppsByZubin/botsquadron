#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_NAME="$(basename -- "$0")"
readonly DEFAULT_NAMESPACE="botspace"
readonly DEFAULT_RELEASE="botsquadron"
readonly DEFAULT_RETENTION_DAYS=30

readonly -a BOT_NAMES=(
    solobot
    firebot
    titanbot
    fibobot
    trendobot
    haemabot
)

readonly -a BOT_MOUNT_ROOTS=(
    /data/botstrategies
    /data/firebot
    /data/titanbot
    /data/fibobot
    /data/trendobot
    /data/haemabot
)

namespace="${KUBE_NAMESPACE:-$DEFAULT_NAMESPACE}"
release="${HELM_RELEASE:-$DEFAULT_RELEASE}"
kube_context=""
retention_days="$DEFAULT_RETENTION_DAYS"
retention_was_set=false
remove_all=false
apply=false
kubectl_binary="${KUBECTL_BIN:-kubectl}"

usage() {
    cat <<EOF
Usage: $SCRIPT_NAME [OPTIONS]

Clean generated order event logs and order CSVs directly from every bot PVC.
The script discovers a bot pod, verifies its /data mount is backed by a
PersistentVolumeClaim, and launches a temporary cleanup pod on the same node.

The default is a dry run selecting files at least $DEFAULT_RETENTION_DAYS days old.

Options:
  --apply                  Delete selected files. Otherwise only list them.
  --all                    Select files of every age.
  --older-than-days DAYS   Select files at least DAYS old (default: $DEFAULT_RETENTION_DAYS).
  -n, --namespace NAME     Kubernetes namespace (default: $DEFAULT_NAMESPACE).
  --release NAME           Helm release label (default: $DEFAULT_RELEASE).
  --context NAME           kubectl context to use.
  -h, --help               Show this help text.

Examples:
  $SCRIPT_NAME
  $SCRIPT_NAME --older-than-days 60 --apply
  $SCRIPT_NAME --all --apply
  $SCRIPT_NAME --context production --namespace botspace --apply

Only these files are selected from each PVC's files/execution_results tree and
from the legacy files directory:
  order_event_log*.json, order_event_log*.jsonl, order_event*.log(s),
  order_log*.csv, order_status_log*.csv, and orders*.csv

Unrelated runtime state and daily_pnl.csv are preserved.
EOF
}

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 2
}

while (($# > 0)); do
    case "$1" in
        --apply)
            apply=true
            shift
            ;;
        --all)
            remove_all=true
            shift
            ;;
        --older-than-days)
            (($# >= 2)) || fail "--older-than-days requires a value"
            retention_days="$2"
            retention_was_set=true
            shift 2
            ;;
        -n|--namespace)
            (($# >= 2)) || fail "$1 requires a value"
            namespace="$2"
            shift 2
            ;;
        --release)
            (($# >= 2)) || fail "--release requires a value"
            release="$2"
            shift 2
            ;;
        --context)
            (($# >= 2)) || fail "--context requires a value"
            kube_context="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown option: $1"
            ;;
    esac
done

if [[ "$remove_all" == true && "$retention_was_set" == true ]]; then
    fail "--all and --older-than-days cannot be used together"
fi

if [[ ! "$retention_days" =~ ^[0-9]+$ ]]; then
    fail "DAYS must be a non-negative integer: $retention_days"
fi

[[ -n "$namespace" ]] || fail "namespace cannot be empty"
[[ -n "$release" ]] || fail "release cannot be empty"
retention_days="$((10#$retention_days))"

command -v "$kubectl_binary" >/dev/null 2>&1 || fail "kubectl is not installed: $kubectl_binary"
command -v base64 >/dev/null 2>&1 || fail "base64 is not installed"
command -v tr >/dev/null 2>&1 || fail "tr is not installed"

kubectl_cmd=("$kubectl_binary")
if [[ -n "$kube_context" ]]; then
    kubectl_cmd+=(--context "$kube_context")
fi
kubectl_cmd+=(-n "$namespace")

remote_cleanup_python="$(cat <<'PYTHON'
import fnmatch
import os
import sys
import time
from pathlib import Path

files_dir = Path(sys.argv[1])
retention_days = int(sys.argv[2])
remove_all = sys.argv[3] == "true"
apply_changes = sys.argv[4] == "true"

patterns = (
    "order_event_log*.json",
    "order_event_log*.jsonl",
    "order_event*.log",
    "order_event*.logs",
    "order_log*.csv",
    "order_status_log*.csv",
    "orders*.csv",
)


def is_artifact(path):
    return (
        path.is_file()
        and not path.is_symlink()
        and any(fnmatch.fnmatchcase(path.name, pattern) for pattern in patterns)
    )


if not files_dir.is_dir():
    print(f"Error: PVC files directory does not exist: {files_dir}", file=sys.stderr)
    raise SystemExit(2)

candidates = set()
execution_dir = files_dir / "execution_results"
if execution_dir.is_dir() and not execution_dir.is_symlink():
    for root, dir_names, file_names in os.walk(execution_dir, followlinks=False):
        root_path = Path(root)
        dir_names[:] = [
            name for name in dir_names if not (root_path / name).is_symlink()
        ]
        for file_name in file_names:
            path = root_path / file_name
            if is_artifact(path):
                candidates.add(path)

for path in files_dir.iterdir():
    if is_artifact(path):
        candidates.add(path)

cutoff = time.time() - retention_days * 86400
selected = []
for path in sorted(candidates, key=lambda item: str(item)):
    try:
        stat_result = path.stat()
    except OSError as exc:
        print(f"Warning: cannot inspect {path}: {exc}", file=sys.stderr)
        continue
    if remove_all or stat_result.st_mtime <= cutoff:
        selected.append((path, stat_result.st_size))

selected_bytes = sum(size for _, size in selected)
failed = 0
for path, _ in selected:
    if apply_changes:
        try:
            path.unlink()
            print(f"Deleted: {path}")
        except OSError as exc:
            failed += 1
            print(f"Failed:  {path}: {exc}", file=sys.stderr)
    else:
        print(f"Would delete: {path}")

print(f"Candidates found: {len(candidates)}")
print(f"Files selected:   {len(selected)}")
print(f"Selected bytes:   {selected_bytes}")

raise SystemExit(1 if failed else 0)
PYTHON
)"

remote_cleanup_base64="$(printf '%s' "$remote_cleanup_python" | base64 | tr -d '\n')"
python_wrapper="import base64;exec(base64.b64decode('$remote_cleanup_base64'))"
active_cleanup_pod=""

remove_active_cleanup_pod() {
    if [[ -n "$active_cleanup_pod" ]]; then
        "${kubectl_cmd[@]}" delete pod "$active_cleanup_pod" \
            --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
        active_cleanup_pod=""
    fi
}

trap remove_active_cleanup_pod EXIT

if [[ "$apply" == true ]]; then
    printf 'Mode: delete from PVCs\n'
else
    printf 'Mode: PVC dry run (pass --apply to delete)\n'
fi
printf 'Namespace: %s\n' "$namespace"
printf 'Helm release: %s\n' "$release"
if [[ -n "$kube_context" ]]; then
    printf 'kubectl context: %s\n' "$kube_context"
else
    printf 'kubectl context: %s\n' "$("$kubectl_binary" config current-context)"
fi
if [[ "$remove_all" == true ]]; then
    printf 'Age filter: all matching files\n'
else
    printf 'Age filter: at least %s day(s) old\n' "$retention_days"
fi

failed_bots=0
cleaned_pvcs=0

for index in "${!BOT_NAMES[@]}"; do
    bot="${BOT_NAMES[$index]}"
    mount_root="${BOT_MOUNT_ROOTS[$index]}"
    files_dir="$mount_root/files"
    selector="app.kubernetes.io/name=$bot,app.kubernetes.io/instance=$release"

    printf '\n[%s]\n' "$bot"
    if ! pod_output="$(
        "${kubectl_cmd[@]}" get pods \
            --selector "$selector" \
            --field-selector status.phase=Running \
            -o name
    )"; then
        printf 'Error: failed to discover a running %s pod\n' "$bot" >&2
        ((failed_bots += 1))
        continue
    fi

    pod=""
    while IFS= read -r resource_name; do
        if [[ "$resource_name" == pod/* ]]; then
            pod="${resource_name#pod/}"
            break
        fi
    done <<< "$pod_output"

    if [[ -z "$pod" ]]; then
        printf 'Error: no running pod found with selector %s\n' "$selector" >&2
        ((failed_bots += 1))
        continue
    fi

    mount_query="{.spec.containers[?(@.name==\"$bot\")].volumeMounts[?(@.mountPath==\"$mount_root\")].name}"
    if ! volume_name="$("${kubectl_cmd[@]}" get pod "$pod" -o "jsonpath=$mount_query")"; then
        printf 'Error: failed to inspect volume mounts on pod %s\n' "$pod" >&2
        ((failed_bots += 1))
        continue
    fi
    if [[ -z "$volume_name" ]]; then
        printf 'Error: %s is not mounted in container %s on pod %s\n' \
            "$mount_root" "$bot" "$pod" >&2
        ((failed_bots += 1))
        continue
    fi

    image_query="{.spec.containers[?(@.name==\"$bot\")].image}"
    if ! bot_image="$("${kubectl_cmd[@]}" get pod "$pod" -o "jsonpath=$image_query")"; then
        printf 'Error: failed to resolve the container image on pod %s\n' "$pod" >&2
        ((failed_bots += 1))
        continue
    fi
    if [[ -z "$bot_image" || "$bot_image" == *'"'* || "$bot_image" == *'\\'* ]]; then
        printf 'Error: invalid container image resolved from pod %s\n' "$pod" >&2
        ((failed_bots += 1))
        continue
    fi

    node_name="$("${kubectl_cmd[@]}" get pod "$pod" -o jsonpath='{.spec.nodeName}')"
    if [[ -z "$node_name" || "$node_name" == *'"'* || "$node_name" == *'\\'* ]]; then
        printf 'Error: invalid node name resolved from pod %s\n' "$pod" >&2
        ((failed_bots += 1))
        continue
    fi

    fs_group="$("${kubectl_cmd[@]}" get pod "$pod" -o jsonpath='{.spec.securityContext.fsGroup}')"
    if [[ ! "$fs_group" =~ ^[0-9]+$ ]]; then
        fs_group=2000
    fi

    claim_query="{.spec.volumes[?(@.name==\"$volume_name\")].persistentVolumeClaim.claimName}"
    if ! claim_name="$("${kubectl_cmd[@]}" get pod "$pod" -o "jsonpath=$claim_query")"; then
        printf 'Error: failed to resolve volume %s on pod %s\n' "$volume_name" "$pod" >&2
        ((failed_bots += 1))
        continue
    fi
    if [[ -z "$claim_name" ]]; then
        printf 'Error: volume %s on pod %s is not a PVC; refusing cleanup\n' \
            "$volume_name" "$pod" >&2
        ((failed_bots += 1))
        continue
    fi
    if ! "${kubectl_cmd[@]}" get pvc "$claim_name" -o name >/dev/null; then
        printf 'Error: PVC %s was not found\n' "$claim_name" >&2
        ((failed_bots += 1))
        continue
    fi

    printf 'Pod: %s\n' "$pod"
    printf 'PVC: %s\n' "$claim_name"
    printf 'PVC path: %s\n' "$files_dir"

    cleanup_pod="pvc-cleanup-$bot-$(date +%s)-$$"
    cleanup_overrides="$(printf \
        '{"spec":{"restartPolicy":"Never","nodeName":"%s","securityContext":{"fsGroup":%s,"fsGroupChangePolicy":"OnRootMismatch"},"containers":[{"name":"pvc-cleaner","image":"%s","imagePullPolicy":"IfNotPresent","command":["python","-c"],"args":["%s","/pvc/files","%s","%s","%s"],"volumeMounts":[{"name":"target-pvc","mountPath":"/pvc"}]}],"volumes":[{"name":"target-pvc","persistentVolumeClaim":{"claimName":"%s"}}]}}' \
        "$node_name" "$fs_group" "$bot_image" "$python_wrapper" \
        "$retention_days" "$remove_all" "$apply" "$claim_name"
    )"

    active_cleanup_pod="$cleanup_pod"
    if ! "${kubectl_cmd[@]}" run "$cleanup_pod" \
        --image "$bot_image" \
        --restart Never \
        --overrides "$cleanup_overrides" >/dev/null; then
        printf 'Error: failed to create cleanup pod for %s PVC %s\n' \
            "$bot" "$claim_name" >&2
        ((failed_bots += 1))
        remove_active_cleanup_pod
        continue
    fi

    phase=""
    deadline=$((SECONDS + 300))
    while ((SECONDS < deadline)); do
        if ! phase="$("${kubectl_cmd[@]}" get pod "$cleanup_pod" \
            -o jsonpath='{.status.phase}' 2>/dev/null)"; then
            phase="Unknown"
        fi
        case "$phase" in
            Succeeded|Failed)
                break
                ;;
        esac
        sleep 2
    done

    log_status=0
    "${kubectl_cmd[@]}" logs "$cleanup_pod" -c pvc-cleaner || log_status=$?

    if [[ "$phase" == "Succeeded" && "$log_status" -eq 0 ]]; then
        ((cleaned_pvcs += 1))
    else
        printf 'Error: cleanup pod %s ended in phase %s for %s PVC %s\n' \
            "$cleanup_pod" "$phase" "$bot" "$claim_name" >&2
        ((failed_bots += 1))
    fi

    remove_active_cleanup_pod
done

printf '\nPVCs processed successfully: %s\n' "$cleaned_pvcs"
printf 'Bots failed or skipped:      %s\n' "$failed_bots"

if ((failed_bots > 0)); then
    exit 1
fi

if [[ "$apply" != true ]]; then
    printf '\nDry run only; rerun with --apply to delete the selected PVC files.\n'
fi
