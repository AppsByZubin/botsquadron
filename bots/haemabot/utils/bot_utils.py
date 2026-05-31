#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
General utility helpers for haemabot.
"""

from datetime import date, datetime
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from zoneinfo import ZoneInfo

import yaml

from common import constants
from logger import create_logger

logger = create_logger("BotUtilsLogger")
IST = ZoneInfo("Asia/Kolkata")

K8S_PARAM_ENV_KEYS = (
    "HAEMABOT_PARAM_YAML",
    "HAEMABOT_PARAMS_YAML",
    "HAEMABOT_PARAM_DATA",
    "HAEMABOT_PARAMS",
    "HEMABOT_PARAM_YAML",
    "HEMABOT_PARAMS_YAML",
    "HEMABOT_PARAM_DATA",
    "HEMABOT_PARAMS",
    "BOT_PARAM_YAML",
    "BOT_PARAMS_YAML",
    "PARAM_YAML",
    "PARAM_DATA",
)

K8S_PARAM_FILE_ENV_KEYS = (
    "HAEMABOT_PARAM_FILE",
    "HAEMABOT_PARAM_PATH",
    "HEMABOT_PARAM_FILE",
    "HEMABOT_PARAM_PATH",
    "BOT_PARAM_FILE",
    "BOT_PARAM_PATH",
    "PARAM_FILE",
    "PARAM_PATH",
)

SKIP_EXECUTION_KEYS = ("skip-execution", "skip_execution")
SKIP_EXECUTION_DATE_KEYS = (
    "skip-execution-dates",
    "skip_execution_dates",
    "skip-trading-dates",
    "skip_trading_dates",
    "no-trade-dates",
    "no_trade_dates",
)
DATE_FORMATS = ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%m%Y", "%Y%m%d")


def stable_bot_id(bot_name: Optional[str], mode: Optional[str], date_value: Optional[str] = None) -> str:
    parts = [
        bot_name or "haemabot",
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
    return normalized or "haemabot"


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
    for env_key in K8S_PARAM_FILE_ENV_KEYS:
        file_path = os.getenv(env_key, "").strip()
        if not file_path:
            continue
        data = _load_yaml_dict_from_file(Path(file_path))
        if data:
            logger.info(f"Loaded params from env file: {env_key}={file_path}")
            return data

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
            logger.info(f"Loaded params from env payload: {env_key}")
            return data

        logger.warning(f"Ignoring empty/non-dict params from env: {env_key}")

    return {}


def load_param_data(_mode: Optional[str]) -> Optional[Dict[str, Any]]:
    k8s_param_data = _load_k8s_param_data()
    if k8s_param_data:
        return k8s_param_data

    file_param_data = _load_yaml_dict_from_file(Path(constants.PARAM_PATH))
    if file_param_data:
        logger.info(f"Loaded params from local param file: {constants.PARAM_PATH}")
        return file_param_data

    return None


def _ensure_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable) and not isinstance(value, Mapping):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _ensure_param_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if str(item or "").strip()]
    return [value]


def _parse_date_iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    raw = str(value or "").strip()
    if not raw:
        return None

    for layout in DATE_FORMATS:
        try:
            return datetime.strptime(raw, layout).date().isoformat()
        except ValueError:
            pass

    digits_only = "".join(ch for ch in raw if ch.isdigit())
    if len(digits_only) == 8:
        for layout in ("%Y%m%d", "%d%m%Y"):
            try:
                return datetime.strptime(digits_only, layout).date().isoformat()
            except ValueError:
                pass

    return None


def _param_sources(param_data: Optional[Dict[str, Any]]) -> list:
    if not isinstance(param_data, dict):
        return []

    sources = []
    sp = param_data.get("strategy-parameters") or param_data.get("strategy_parameters")
    if isinstance(sp, dict):
        sources.append(sp)
    sources.append(param_data)
    return sources


def should_skip_strategy_execution(
    param_data: Optional[Dict[str, Any]],
    strategy: Optional[str],
    current_date: Optional[str] = None,
) -> tuple:
    today_iso = _parse_date_iso(current_date) or datetime.now(IST).date().isoformat()

    for source in _param_sources(param_data):
        for key in SKIP_EXECUTION_KEYS:
            if _coerce_bool(source.get(key)):
                return True, f"{key}=true"

        for key in SKIP_EXECUTION_DATE_KEYS:
            for configured_date in _ensure_param_list(source.get(key)):
                raw_date = str(configured_date or "").strip().lower()
                if raw_date in {"*", "all"}:
                    return True, f"{key} contains {raw_date}"
                if raw_date == "today":
                    return True, f"{key} contains today"

                configured_iso = _parse_date_iso(configured_date)
                if configured_iso == today_iso:
                    return True, f"{key} contains {today_iso}"
                if configured_iso is None:
                    logger.warning(
                        f"Ignoring invalid {key} date for strategy {strategy}: {configured_date}"
                    )

    return False, ""


def _pick_nested(mapping: Mapping[str, Any], *path: str) -> Any:
    current: Any = mapping
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return None
        current = current[key]
    return current


def extract_instrument_keys(param_data: Mapping[str, Any], instrument_group: str) -> List[str]:
    if not isinstance(param_data, Mapping):
        return []

    candidates = [
        param_data.get("instrument_keys"),
        param_data.get("instrumentKeys"),
        _pick_nested(param_data, "marketfeeder", "instrument_keys"),
        _pick_nested(param_data, "marketfeeder", "instrumentKeys"),
        _pick_nested(param_data, "market-data", "instrument_keys"),
        _pick_nested(param_data, "market_data", "instrument_keys"),
        _pick_nested(param_data, instrument_group, "instrument_keys"),
        _pick_nested(param_data, "instruments", instrument_group),
    ]

    for candidate in candidates:
        keys = _ensure_list(candidate)
        if keys:
            return keys

    return []
