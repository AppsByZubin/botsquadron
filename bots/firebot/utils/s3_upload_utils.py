#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities for uploading end-of-day trading artifacts to DigitalOcean Spaces.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse
from zoneinfo import ZoneInfo

from common import constants
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


def normalize_do_spaces_endpoint_url(endpoint_url: str, region: str, bucket_name: str) -> str:
    """
    Boto3 expects a DigitalOcean Spaces region endpoint such as
    https://sgp1.digitaloceanspaces.com. A bucket-scoped endpoint causes boto3
    to compose invalid upload URLs once the Bucket argument is also supplied.
    """
    endpoint_url = (endpoint_url or "").strip()
    if not endpoint_url:
        return endpoint_url

    endpoint_with_scheme = (
        endpoint_url if "://" in endpoint_url else f"https://{endpoint_url}"
    )
    parsed = urlparse(endpoint_with_scheme)
    expected_host = f"{region}.digitaloceanspaces.com" if region else ""
    bucket_host_suffix = f".{expected_host}" if expected_host else ""

    if parsed.netloc and bucket_host_suffix and parsed.netloc.endswith(bucket_host_suffix):
        bucket_from_endpoint = parsed.netloc[: -len(bucket_host_suffix)]
        if bucket_from_endpoint:
            normalized = urlunparse(
                parsed._replace(
                    netloc=expected_host,
                    path="",
                    params="",
                    query="",
                    fragment="",
                )
            )
            logger.warning(
                f"DO_S3_ENDPOINT_URL includes bucket host '{parsed.netloc}'. "
                f"Using region endpoint '{normalized}' for bucket '{bucket_name}'."
            )
            return normalized

    return endpoint_with_scheme


def _constant_path(name: str) -> Optional[Path]:
    value = getattr(constants, name, None)
    if not value:
        return None
    return Path(value)


def _select_existing_source(*constant_names: str) -> Path:
    candidates = [
        candidate
        for candidate in (_constant_path(name) for name in constant_names)
        if candidate is not None
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    if not candidates:
        raise ValueError(f"No constants configured for source names: {constant_names}")
    return candidates[0]


def _order_sources_for_mode(execution_mode: str) -> Tuple[Path, Path]:
    mode = (execution_mode or "").strip().lower()
    if mode == constants.MOCK:
        return (
            _select_existing_source("ORDER_MOCK_LOG", "ORDER_LOG"),
            _select_existing_source("ORDER_MOCK_EVENT_LOG", "ORDER_EVENT_LOG"),
        )
    if mode == constants.SANDBOX:
        return (
            _select_existing_source("ORDER_SANDBOX_LOG"),
            _select_existing_source("ORDER_SANDBOX_EVENT_LOG"),
        )
    if mode == constants.PRODUCTION:
        return (
            _select_existing_source("ORDER_PROD_LOG"),
            _select_existing_source("ORDER_PROD_EVENT_LOG"),
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


def _first_existing(paths: Iterable[Path], description: str) -> Path:
    candidates = list(paths)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    checked = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"{description} not found. Checked: {checked}")


def _upload_key(bucket_name: str, *parts: str) -> str:
    key = "/".join(str(part).strip("/") for part in parts if part)
    return normalize_s3_key(bucket_name, key)


def upload_trade_artifacts_to_s3(bot_name: str, execution_mode: str) -> None:
    endpoint_url = os.getenv(constants.DO_S3_ENDPOINT_URL, "").strip()
    region = os.getenv(constants.DO_S3_REGION, "").strip()
    access_key_id = os.getenv(constants.DO_S3_ACCESS_KEY_ID, "").strip()
    secret_access_key = os.getenv(constants.DO_S3_SECRET_ACCESS_KEY, "").strip()
    configured_bucket_name = os.getenv(constants.DO_S3_BUCKET_NAME, "").strip()
    bucket_name = configured_bucket_name or constants.DO_S3_REQUIRED_BUCKET_NAME
    raw_prefix = os.getenv(
        constants.DO_S3_SPACES_PREFIX,
        constants.DO_S3_DEFAULT_PREFIX,
    ).strip()

    missing = [
        name
        for name, value in (
            (constants.DO_S3_ENDPOINT_URL, endpoint_url),
            (constants.DO_S3_REGION, region),
            (constants.DO_S3_ACCESS_KEY_ID, access_key_id),
            (constants.DO_S3_SECRET_ACCESS_KEY, secret_access_key),
            (constants.DO_S3_BUCKET_NAME, bucket_name),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing S3 configuration: {', '.join(missing)}")

    now = datetime.now(IST)
    date_folder = now.strftime("%d%m%y")
    log_file_name = f"{now.strftime('%d-%m-%y')}_{bot_name}.log"
    upload_prefix = normalize_s3_key(bucket_name, raw_prefix).strip("/")
    endpoint_url = normalize_do_spaces_endpoint_url(endpoint_url, region, bucket_name)
    mode = (execution_mode or "").strip().lower()

    log_path = _first_existing(
        _candidate_log_paths(bot_name, log_file_name),
        "today's log file",
    )
    ledger_path, events_path = _order_sources_for_mode(mode)

    uploads: List[Tuple[Path, str]] = [
        (
            log_path,
            _upload_key(bucket_name, upload_prefix, bot_name, date_folder, log_file_name),
        ),
        (
            ledger_path,
            _upload_key(bucket_name, upload_prefix, bot_name, date_folder, mode, "orders", "order_log.csv"),
        ),
        (
            events_path,
            _upload_key(bucket_name, upload_prefix, bot_name, date_folder, mode, "orders", "order_events.json"),
        ),
    ]

    missing_sources = [str(local_path) for local_path, _ in uploads if not local_path.exists()]
    if missing_sources:
        raise FileNotFoundError(
            "S3 upload source file(s) not found: " + ", ".join(missing_sources)
        )

    import boto3
    from botocore.config import Config

    s3_client_kwargs = {
        "region_name": region,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
    }
    if "digitaloceanspaces.com" in endpoint_url:
        s3_client_kwargs["config"] = Config(s3={"addressing_style": "virtual"})

    s3 = boto3.client("s3", **s3_client_kwargs)
    for local_path, destination_key in uploads:
        file_size = local_path.stat().st_size
        logger.info(
            f"Uploading {local_path} ({file_size} bytes) to "
            f"s3://{bucket_name}/{destination_key} via {endpoint_url}"
        )
        s3.upload_file(str(local_path), bucket_name, destination_key)
        logger.info(f"Uploaded file to s3://{bucket_name}/{destination_key}")
