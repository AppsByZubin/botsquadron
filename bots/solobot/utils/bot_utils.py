#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import os
from typing import Any, Dict, Optional

import yaml

from common import constants
from logger import create_logger

logger = create_logger("botutilsLogger")

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
    candidates = []

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

