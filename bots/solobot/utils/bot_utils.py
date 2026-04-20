#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import datetime
import os
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import yaml

from common import constants
from logger import create_logger

logger = create_logger("botutilsLogger")
IST = ZoneInfo("Asia/Kolkata")

K8S_PARAM_ENV_KEYS = (
    "SOLOBOT_PARAM_YAML",
    "SOLOBOT_PARAMS_YAML",
    "SOLOBOT_PARAM_DATA",
    "SOLOBOT_PARAMS",
    "PARAM_YAML",
    "PARAM_DATA",
)

K8S_PARAM_FILE_ENV_KEYS = (
    "SOLOBOT_PARAM_FILE",
    "SOLOBOT_PARAM_PATH",
    "PARAM_FILE",
    "PARAM_PATH",
)


def stable_bot_id(bot_name: Optional[str], mode: Optional[str], date_value: Optional[str] = None) -> str:
    parts = [
        bot_name or "solobot",
        _bot_id_mode(mode),
        _bot_id_date(date_value),
    ]
    return _normalize_bot_id("_".join(str(part or "").strip() for part in parts if str(part or "").strip()))


def _bot_id_mode(mode: Optional[str]) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized in {"production", "prod"}:
        return "prod"
    if normalized in {"sandbox", "mock"}:
        return normalized
    return normalized or "mock"


def _bot_id_date(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    for layout in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d%m%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, layout).strftime("%d%m%Y")
        except ValueError:
            pass

    digits_only = "".join(ch for ch in raw if ch.isdigit())
    if len(digits_only) == 8:
        if digits_only.startswith(("19", "20")):
            try:
                return datetime.strptime(digits_only, "%Y%m%d").strftime("%d%m%Y")
            except ValueError:
                pass
        return digits_only

    return datetime.now(IST).strftime("%d%m%Y")


def _normalize_bot_id(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    normalized = "_".join(part for part in normalized.split("_") if part)
    return normalized or "solobot"


def _deep_merge_dict(defaults: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge dicts recursively.
    - `defaults` provides fallback values.
    - `overrides` wins where values are present.
    """
    merged = dict(defaults)
    for key, value in overrides.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge_dict(merged[key], value)
        elif value is not None:
            merged[key] = value
    return merged


def _load_yaml_dict_from_file(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file) or {}
    except Exception as exc:
        logger.warning(f"Unable to parse param yaml file {path}: {exc}")
        return None

    if not isinstance(data, dict):
        logger.warning(f"Ignoring non-dict params from file: {path}")
        return None
    return data


def _load_k8s_param_data() -> Dict[str, Any]:
    # 1) ConfigMap/secret mounted file path via env
    for env_key in K8S_PARAM_FILE_ENV_KEYS:
        file_path = os.getenv(env_key, "").strip()
        if not file_path:
            continue
        data = _load_yaml_dict_from_file(Path(file_path))
        if data:
            logger.info(f"Loaded params from kubernetes file env: {env_key}={file_path}")
            return data

    # 2) Full YAML/JSON payload via env value
    for env_key in K8S_PARAM_ENV_KEYS:
        raw_payload = os.getenv(env_key, "").strip()
        if not raw_payload:
            continue
        try:
            data = yaml.safe_load(raw_payload) or {}
        except Exception as exc:
            logger.warning(f"Failed to parse params from env {env_key}: {exc}")
            continue

        if isinstance(data, dict) and data:
            logger.info(f"Loaded params from kubernetes env payload: {env_key}")
            return data

        logger.warning(f"Ignoring empty/non-dict params from env: {env_key}")

    return {}


def _candidate_param_files(mode: Optional[str]):
    solobot_root = Path(__file__).resolve().parents[1]  # .../bots/solobot
    configured_files_dir = Path(constants.SOLOBOT_FILES_DIR).resolve(strict=False)
    default_files_dir = (solobot_root / "files").resolve(strict=False)
    candidates = []

    if configured_files_dir != default_files_dir:
        if mode in {constants.MOCK, constants.SANDBOX, constants.PRODUCTION}:
            candidates.append(configured_files_dir / "execution_results" / mode / "param.yaml")
        candidates.append(configured_files_dir / "param.yaml")

    if mode in {constants.MOCK, constants.SANDBOX, constants.PRODUCTION}:
        candidates.append(solobot_root / "files" / "execution_results" / mode / "param.yaml")

    # Preferred static location under files/
    candidates.append(solobot_root / "files" / "param.yaml")

    # Backward-compatible configured path
    candidates.append(Path(constants.PARAM_PATH))

    # Hard fallback for local/default usage
    candidates.append(solobot_root / "files" / "execution_results" / constants.MOCK / "param.yaml")

    seen = set()
    unique_candidates = []
    for p in candidates:
        rp = p.resolve(strict=False)
        key = str(rp)
        if key in seen:
            continue
        seen.add(key)
        unique_candidates.append(rp)
    return unique_candidates


def _load_fallback_param_data(mode: Optional[str]) -> Dict[str, Any]:
    for path in _candidate_param_files(mode):
        data = _load_yaml_dict_from_file(path)
        if data:
            logger.info(f"Loaded fallback params from file: {path}")
            return data
    return {}


def load_param_data(mode: Optional[str]) -> Optional[Dict[str, Any]]:
    k8s_param_data = _load_k8s_param_data()
    file_param_data = _load_fallback_param_data(mode)

    if k8s_param_data and file_param_data:
        # Kubernetes params override; missing keys fall back to local param.yaml
        return _deep_merge_dict(file_param_data, k8s_param_data)
    if k8s_param_data:
        return k8s_param_data
    if file_param_data:
        return file_param_data
    return None
