#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities for uploading end-of-day trading artifacts to CloudPE S3.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Set, Tuple
from urllib.parse import urlparse, urlunparse
from zoneinfo import ZoneInfo

from common import constants
from common.artifacts import archive_directory, artifact_run_lock, cleanup_allowed, migrate_legacy_ledgers
from logger import create_logger

logger = create_logger("S3UploadUtilsLogger")
IST = ZoneInfo("Asia/Kolkata")


def normalize_s3_key(bucket_name: str, key: str) -> str:
    """
    Normalize an S3 object key by removing a leading slash and an accidental
    '<bucket_name>/' prefix.
    """
    normalized_key = key.lstrip("/")
    bucket_prefix = f"{bucket_name}/"
    if bucket_name and normalized_key.startswith(bucket_prefix):
        normalized_key = normalized_key[len(bucket_prefix):]
        logger.warning(
            f"Removed bucket prefix from object key. bucket={bucket_name}, key={normalized_key}"
        )
    return normalized_key


def normalize_endpoint_url(endpoint_url: str, bucket_name: str) -> str:
    """
    Boto3 expects a service endpoint such as https://s3.in-west2.purestore.io.
    A bucket-scoped endpoint causes boto3 to compose invalid upload URLs once
    the Bucket argument is also supplied.
    """
    endpoint_url = (endpoint_url or "").strip()
    if not endpoint_url:
        return endpoint_url

    endpoint_with_scheme = (
        endpoint_url if "://" in endpoint_url else f"https://{endpoint_url}"
    )
    parsed = urlparse(endpoint_with_scheme)
    bucket_host_prefix = f"{bucket_name}." if bucket_name else ""

    if bucket_host_prefix and parsed.netloc.startswith(bucket_host_prefix):
        service_host = parsed.netloc[len(bucket_host_prefix):]
        if service_host:
            normalized = urlunparse(
                parsed._replace(
                    netloc=service_host,
                    path="",
                    params="",
                    query="",
                    fragment="",
                )
            )
            logger.warning(
                f"CLOUDPE_S3_ENDPOINT_URL includes bucket host '{parsed.netloc}'. "
                f"Using service endpoint '{normalized}' for bucket '{bucket_name}'."
            )
            return normalized

    return endpoint_with_scheme


def cloudpe_endpoint_url(endpoint_url: str, bucket_name: str) -> str:
    endpoint = normalize_endpoint_url(endpoint_url, bucket_name)
    host = (urlparse(endpoint).hostname or "").lower().rstrip(".")
    if host == "digitaloceanspaces.com" or host.endswith(".digitaloceanspaces.com"):
        raise ValueError(
            "CLOUDPE_S3_ENDPOINT_URL points to DigitalOcean Spaces. "
            "Configure the CloudPE service endpoint and matching CLOUDPE_S3_* credentials."
        )
    return endpoint


def normalize_upload_prefix(bucket_name: str, prefix: str) -> str:
    prefix = normalize_s3_key(bucket_name, prefix).strip("/")
    if not prefix or prefix == "index-bucket-holder/trades":
        return constants.CLOUDPE_S3_DEFAULT_PREFIX
    return prefix


def _order_sources_for_mode(execution_mode: str) -> Tuple[Path, Path]:
    mode = (execution_mode or "").strip().lower()
    if mode == constants.MOCK:
        return (
            Path(constants.ORDER_MOCK_LOG),
            Path(constants.ORDER_MOCK_EVENT_LOG),
        )
    if mode == constants.SANDBOX:
        return (
            Path(constants.ORDER_SANDBOX_LOG),
            Path(constants.ORDER_SANDBOX_EVENT_LOG),
        )
    if mode == constants.PRODUCTION:
        return (
            Path(constants.ORDER_PROD_LOG),
            Path(constants.ORDER_PROD_EVENT_LOG),
        )
    raise ValueError(
        f"Unsupported execution mode for S3 upload: {execution_mode!r}. "
        f"Expected one of: {constants.MOCK}, {constants.SANDBOX}, {constants.PRODUCTION}"
    )


def _candidate_log_paths(bot_name: str, log_file_name: str) -> Iterable[Path]:
    env_names = [f"{bot_name.upper()}_LOG_DIR", "BOT_LOG_DIR", "LOG_DIR"]
    seen: Set[Path] = set()
    for env_name in env_names:
        configured_dir = os.getenv(env_name, "").strip()
        if not configured_dir:
            continue
        candidate = Path(configured_dir).expanduser() / log_file_name
        if candidate not in seen:
            seen.add(candidate)
            yield candidate

    for candidate in (Path.cwd() / "logs" / log_file_name, Path("logs") / log_file_name):
        if candidate not in seen:
            seen.add(candidate)
            yield candidate


def _upload_key(bucket_name: str, *parts: str) -> str:
    key = "/".join(str(part).strip("/") for part in parts if part)
    return normalize_s3_key(bucket_name, key)


def upload_trade_artifacts_to_s3(bot_name: str, execution_mode: str) -> None:
    endpoint_url = os.getenv(constants.CLOUDPE_S3_ENDPOINT_URL, "").strip()
    region = os.getenv(constants.CLOUDPE_S3_REGION, "").strip()
    access_key_id = os.getenv(constants.CLOUDPE_S3_ACCESS_KEY_ID, "").strip()
    secret_access_key = os.getenv(constants.CLOUDPE_S3_SECRET_ACCESS_KEY, "").strip()
    configured_bucket_name = os.getenv(constants.CLOUDPE_S3_BUCKET_NAME, "").strip()
    bucket_name = configured_bucket_name or constants.CLOUDPE_S3_REQUIRED_BUCKET_NAME
    raw_prefix = os.getenv(
        constants.CLOUDPE_S3_PREFIX,
        constants.CLOUDPE_S3_DEFAULT_PREFIX,
    ).strip()

    missing = [
        name
        for name, value in (
            (constants.CLOUDPE_S3_ENDPOINT_URL, endpoint_url),
            (constants.CLOUDPE_S3_REGION, region),
            (constants.CLOUDPE_S3_ACCESS_KEY_ID, access_key_id),
            (constants.CLOUDPE_S3_SECRET_ACCESS_KEY, secret_access_key),
            (constants.CLOUDPE_S3_BUCKET_NAME, bucket_name),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing S3 configuration: {', '.join(missing)}")

    now = datetime.now(IST)
    upload_prefix = normalize_upload_prefix(bucket_name, raw_prefix)
    endpoint_url = cloudpe_endpoint_url(endpoint_url, bucket_name)
    mode = (execution_mode or "").strip().lower()

    ledger_path, _ = _order_sources_for_mode(mode)
    mode_dir = ledger_path.parent.parent
    if mode_dir.name not in {"mock", "sandbox", "prod"} or mode_dir.parent.name != "execution_results":
        raise ValueError(f"Expected dated artifact directory, got {ledger_path.parent}")
    pnl_constant = {
        constants.MOCK: "DAILY_MOCK_PNL",
        constants.SANDBOX: "DAILY_SANDBOX_PNL",
        constants.PRODUCTION: "DAILY_PROD_PNL",
    }[mode]
    accounting_path = Path(getattr(constants, pnl_constant))
    migrate_legacy_ledgers(str(ledger_path), str(accounting_path))
    custom_sources = _custom_artifact_sources(mode)
    cleanup = cleanup_allowed(now) and not custom_sources
    if custom_sources:
        logger.warning("Custom artifact paths configured; uploading copies and retaining local files")

    import boto3
    from botocore.config import Config

    s3_client_kwargs = {
        "region_name": region,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
    }
    s3_client_kwargs["config"] = Config(s3={"addressing_style": "path"})

    logger.info("Archiving %s %s artifacts to s3://%s/%s via CloudPE endpoint %s",
                bot_name, mode, bucket_name, upload_prefix, endpoint_url)
    s3 = boto3.client("s3", **s3_client_kwargs)
    # Retry all retained days for this mode, using their original trading dates.
    candidates = []
    if mode_dir.exists():
        for directory in sorted(mode_dir.iterdir()):
            if not directory.is_dir():
                continue
            try:
                day = date.fromisoformat(directory.name)
            except ValueError:
                continue
            if day <= now.date():
                candidates.append((directory, day, True))
        if any(path.is_file() for path in mode_dir.iterdir()):
            candidates.append((mode_dir, None, False))

    if not candidates and custom_sources:
        # Custom-only installations may have no managed daily files at all.
        with tempfile.TemporaryDirectory(prefix="bot-custom-archive-") as temporary:
            archive_directory(
                s3, bucket_name,
                _upload_key(bucket_name, upload_prefix, bot_name, now.strftime("%Y%m%d"), mode),
                Path(temporary), extras=custom_sources, cleanup=False,
            )

    for directory, day, recursive in candidates:
        extras = dict(custom_sources)
        if accounting_path.exists():
            extras["accounting/daily_pnl.csv"] = accounting_path
        log_day = day or now.date()
        log_name = f"{log_day.strftime('%d-%m-%y')}_{bot_name}.log"
        log_path = next((path for path in _candidate_log_paths(bot_name, log_name) if path.is_file()), None)
        if log_path:
            extras[f"logs/{log_name}"] = log_path
        date_folder = day.strftime("%Y%m%d") if day else "legacy"
        archive_directory(
            s3, bucket_name,
            _upload_key(bucket_name, upload_prefix, bot_name, date_folder, mode),
            directory, extras=extras, cleanup=cleanup, recursive=recursive,
        )


def _custom_artifact_sources(mode: str) -> dict[str, Path]:
    """Preserve explicitly configured paths; only managed daily folders are cleaned."""
    from utils.bot_utils import load_param_data

    params = load_param_data(mode) or {}
    cfg = next((params[key] for key in (
        "oms", "ordersystem", "order_system", "order-system", "order-system-client"
    ) if isinstance(params.get(key), dict)), {})
    definitions = (
        ("order_log.csv", ("orders_csv", "orders-csv", "local_orders_csv", "local-orders-csv"),
         ("ORDERSYSTEM_ORDERS_CSV", "ORDER_SYSTEM_ORDERS_CSV", "OMS_ORDERS_CSV")),
        ("daily_pnl.csv", ("daily_csv", "daily-csv", "daily_pnl_csv", "daily-pnl-csv", "local_daily_csv", "local-daily-csv"),
         ("ORDERSYSTEM_DAILY_PNL_CSV", "ORDER_SYSTEM_DAILY_PNL_CSV", "OMS_DAILY_PNL_CSV")),
        ("order_event_log.json", ("events_json", "events-json", "events_json_path", "events-json-path", "order_event_log", "order-event-log", "local_events_json", "local-events-json"),
         ("ORDERSYSTEM_EVENTS_JSON", "ORDER_SYSTEM_EVENTS_JSON", "OMS_EVENTS_JSON")),
    )
    sources = {}
    for name, keys, env_keys in definitions:
        value = next((cfg[key] for key in keys if cfg.get(key)), None)
        value = value or next((os.environ[key] for key in env_keys if os.getenv(key, "").strip()), None)
        if value:
            path = Path(value)
            if not path.is_file():
                raise FileNotFoundError(f"Configured artifact source not found: {path}")
            sources[f"custom/{name}"] = path
    return sources


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Retry artifact archival while the bot is stopped.")
    parser.add_argument("--bot-name", required=True)
    parser.add_argument("--mode", required=True, choices=constants.TRADING_EXECUTION_MODES)
    args = parser.parse_args()
    ledger, _ = _order_sources_for_mode(args.mode)
    files_dir = ledger.parent.parent.parent.parent
    with artifact_run_lock(files_dir):
        upload_trade_artifacts_to_s3(args.bot_name, args.mode)
