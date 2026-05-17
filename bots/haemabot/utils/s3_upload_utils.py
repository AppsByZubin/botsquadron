#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Optional S3 upload hooks for haemabot trade artifacts.
"""

import os
from pathlib import Path

from common import constants
from logger import create_logger

logger = create_logger("S3UploadUtilsLogger")


def _to_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _order_sources_for_mode(execution_mode: str):
    mode = (execution_mode or "").strip().lower()
    if mode == constants.MOCK:
        return Path(constants.ORDER_MOCK_LOG), Path(constants.ORDER_MOCK_EVENT_LOG)
    if mode == constants.SANDBOX:
        return Path(constants.ORDER_SANDBOX_LOG), Path(constants.ORDER_SANDBOX_EVENT_LOG)
    if mode == constants.PRODUCTION:
        return Path(constants.ORDER_PROD_LOG), Path(constants.ORDER_PROD_EVENT_LOG)
    raise ValueError(f"Unsupported execution mode for S3 upload: {execution_mode!r}")


def maybe_upload_trade_artifacts_to_s3(bot_name: str, execution_mode: str) -> None:
    if not _to_bool(os.getenv("HAEMABOT_UPLOAD_ARTIFACTS", os.getenv("HEMABOT_UPLOAD_ARTIFACTS", ""))):
        logger.info("Skipping S3 upload. Set HAEMABOT_UPLOAD_ARTIFACTS=true to enable it.")
        return

    upload_trade_artifacts_to_s3(bot_name, execution_mode)


def upload_trade_artifacts_to_s3(bot_name: str, execution_mode: str) -> None:
    endpoint_url = os.getenv(constants.DO_S3_ENDPOINT_URL, "").strip()
    region = os.getenv(constants.DO_S3_REGION, "").strip()
    access_key_id = os.getenv(constants.DO_S3_ACCESS_KEY_ID, "").strip()
    secret_access_key = os.getenv(constants.DO_S3_SECRET_ACCESS_KEY, "").strip()
    bucket_name = os.getenv(constants.DO_S3_BUCKET_NAME, "").strip() or constants.DO_S3_REQUIRED_BUCKET_NAME
    prefix = os.getenv(constants.DO_S3_SPACES_PREFIX, constants.DO_S3_DEFAULT_PREFIX).strip("/")

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

    orders_path, events_path = _order_sources_for_mode(execution_mode)
    uploads = {
        orders_path: f"{prefix}/{bot_name}/{execution_mode}/orders/order_log.csv",
        events_path: f"{prefix}/{bot_name}/{execution_mode}/orders/order_events.json",
    }

    missing_sources = [str(path) for path in uploads if not path.exists()]
    if missing_sources:
        raise FileNotFoundError("S3 upload source file(s) not found: " + ", ".join(missing_sources))

    import boto3

    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    for local_path, key in uploads.items():
        logger.info(f"Uploading {local_path} to s3://{bucket_name}/{key}")
        s3.upload_file(str(local_path), bucket_name, key)
