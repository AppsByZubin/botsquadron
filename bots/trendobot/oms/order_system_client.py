#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTTP client for the Go ordersystem service.

This module keeps trendobot's strategy-facing order_manager shape while sending
trade/account operations to ordersystem over HTTP.
"""

from __future__ import annotations

import csv
import json as jsonlib
import math
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urljoin

import requests
from requests import Response
from zoneinfo import ZoneInfo

from common import constants
from logger import create_logger

IST = ZoneInfo("Asia/Kolkata")
log = create_logger("OrderSystemClientLogger")

ORDER_COLUMNS = [
    "id",
    "symbol",
    "instrument_token",
    "side",
    "qty",
    "product",
    "validity",
    "entry_order_ids",
    "sl_order_ids",
    "target_order_id",
    "entry_price",
    "target",
    "stoploss",
    "_sl_limit",
    "tsl_active",
    "start_trail_after",
    "entry_spot",
    "spot_ltp",
    "_spot_trail_anchor",
    "_trail_points",
    "status",
    "timestamp",
    "exit_price",
    "pnl",
    "exit_time",
    "tag_entry",
    "tag_sl",
    "description",
    "sl_order_qty_map",
]

JSON_COLUMNS = {"entry_order_ids", "sl_order_ids", "sl_order_qty_map"}

DAILY_PNL_COLUMNS = [
    "date",
    "daily_pnl",
    "num_trades",
    "win_rate",
    "cash",
    "equity",
    "peak_equity",
    "drawdown",
    "drawdown_pct",
]


class OrderSystemError(RuntimeError):
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        payload: Any = None,
        retry_after: Optional[float] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload
        self.retry_after = retry_after


class OrderSystemClient:
    DEFAULT_BASE_URL = "http://localhost:8081"
    CLOSED_STATUSES = {
        constants.TARGET_HIT.upper(),
        constants.STOPLOSS_HIT.upper(),
        constants.MANUAL_EXIT.upper(),
        constants.EOD_SQUARE_OFF.upper(),
    }

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
        bot_name: Optional[str] = None,
        mode: Optional[str] = None,
        init_cash: Optional[float] = None,
        curr_date: Optional[str] = None,
        month_year: Optional[str] = None,
        product: str = "D",
        validity: str = "DAY",
        tick_size: float = 0.05,
        sl_limit_gap: float = 1.0,
        orders_csv: Optional[str] = None,
        daily_csv: Optional[str] = None,
        events_json_path: Optional[str] = None,
        local_copy_enabled: bool = True,
        session: Optional[requests.Session] = None,
    ):
        self.base_url = _clean_base_url(
            base_url
            or _first_env("ORDERSYSTEM_BASE_URL", "ORDER_SYSTEM_BASE_URL", "OMS_BASE_URL")
            or self.DEFAULT_BASE_URL
        )
        self.timeout = float(
            timeout
            if timeout is not None
            else (_first_env("ORDERSYSTEM_TIMEOUT_SEC", "ORDER_SYSTEM_TIMEOUT_SEC", "OMS_TIMEOUT_SEC") or 15)
        )
        self.bot_name = str(
            bot_name
            or os.getenv("TRENDOBOT_BOT_NAME")
            or os.getenv("SOLOBOT_BOT_NAME")
            or "trendobot"
        ).strip()
        self.mode = constants.resolve_execution_mode(mode)
        self.initial_cash = _to_float(init_cash, _to_float(os.getenv("ACCOUNT_INITIAL_CASH"), 0.0))
        self.curr_date = (
            curr_date
            or os.getenv("TRENDOBOT_CURR_DATE")
            or os.getenv("SOLOBOT_CURR_DATE")
            or _now_ist().strftime("%d-%m-%Y")
        )
        self.month_year = (
            month_year
            or os.getenv("TRENDOBOT_MONTH_YEAR")
            or os.getenv("SOLOBOT_MONTH_YEAR")
            or _now_ist().strftime("%m%Y")
        )
        self.product = str(product or "D").upper()
        self.validity = str(validity or "DAY").upper()
        self.tick_size = float(tick_size or 0.05)
        self.sl_limit_gap = float(sl_limit_gap or 1.0)
        self.account_id: Optional[str] = None
        self.local_copy_enabled = _to_bool(local_copy_enabled, True)
        self.orders_csv = str(
            orders_csv
            or _first_env("ORDERSYSTEM_ORDERS_CSV", "ORDER_SYSTEM_ORDERS_CSV", "OMS_ORDERS_CSV")
            or _default_orders_csv(self.mode)
        )
        self.daily_csv = str(
            daily_csv
            or _first_env("ORDERSYSTEM_DAILY_PNL_CSV", "ORDER_SYSTEM_DAILY_PNL_CSV", "OMS_DAILY_PNL_CSV")
            or _default_daily_csv(self.mode)
        )
        self.events_json_path = str(
            events_json_path
            or _first_env("ORDERSYSTEM_EVENTS_JSON", "ORDER_SYSTEM_EVENTS_JSON", "OMS_EVENTS_JSON")
            or _default_events_json(self.mode)
        )
        self.session = session or requests.Session()
        self.modify_min_interval_sec = max(
            0.0,
            float(_to_float(_first_env(
                "ORDERSYSTEM_MODIFY_MIN_INTERVAL_SEC",
                "ORDER_SYSTEM_MODIFY_MIN_INTERVAL_SEC",
                "OMS_MODIFY_MIN_INTERVAL_SEC",
            ), 1.0) or 0.0),
        )
        self.rate_limit_max_retries = max(
            0,
            int(_to_float(_first_env(
                "ORDERSYSTEM_RATE_LIMIT_MAX_RETRIES",
                "ORDER_SYSTEM_RATE_LIMIT_MAX_RETRIES",
                "OMS_RATE_LIMIT_MAX_RETRIES",
            ), 2) or 0),
        )
        self.rate_limit_base_sleep_sec = max(
            0.1,
            float(_to_float(_first_env(
                "ORDERSYSTEM_RATE_LIMIT_BASE_SLEEP_SEC",
                "ORDER_SYSTEM_RATE_LIMIT_BASE_SLEEP_SEC",
                "OMS_RATE_LIMIT_BASE_SLEEP_SEC",
            ), max(self.modify_min_interval_sec, 1.0)) or 1.0),
        )
        self.rate_limit_max_sleep_sec = max(
            self.rate_limit_base_sleep_sec,
            float(_to_float(_first_env(
                "ORDERSYSTEM_RATE_LIMIT_MAX_SLEEP_SEC",
                "ORDER_SYSTEM_RATE_LIMIT_MAX_SLEEP_SEC",
                "OMS_RATE_LIMIT_MAX_SLEEP_SEC",
            ), 15.0) or 15.0),
        )
        self._last_trade_id: Optional[str] = None
        self._active_trade_by_symbol: Dict[str, str] = {}
        self._trade_cache: Dict[str, Dict[str, Any]] = {}
        self._request_next_ok: Dict[str, float] = {}
        self._request_backoff: Dict[str, float] = {}
        if self.local_copy_enabled:
            self._init_local_ledger()

    @classmethod
    def from_params(
        cls,
        strategy_name: str,
        mode: Optional[str],
        params: Optional[Mapping[str, Any]],
        instrument: str = constants.NIFTY50,
    ) -> "OrderSystemClient":
        resolved_mode = constants.resolve_execution_mode(mode)
        if cls is OrderSystemClient and resolved_mode == constants.MOCK:
            from oms.mock_order_system_client import MockOrderSystemClient

            return MockOrderSystemClient.from_params(
                strategy_name=strategy_name,
                mode=resolved_mode,
                params=params,
                instrument=instrument,
            )

        params = params if isinstance(params, Mapping) else {}
        sp = params.get("strategy-parameters") if isinstance(params, Mapping) else {}
        sp = sp if isinstance(sp, Mapping) else {}
        oms_cfg = _first_mapping(
            params,
            "oms",
            "ordersystem",
            "order_system",
            "order-system",
            "order-system-client",
        )

        bot_name = _first_value(
            oms_cfg.get("bot_name"),
            oms_cfg.get("bot-name"),
            os.getenv("TRENDOBOT_BOT_NAME"),
            os.getenv("SOLOBOT_BOT_NAME"),
            f"{instrument}_{str(strategy_name or '').strip().lower()}",
        )
        init_cash = _first_value(
            oms_cfg.get("init_cash"),
            oms_cfg.get("initial_cash"),
            oms_cfg.get("init-cash"),
            oms_cfg.get("initial-cash"),
            sp.get("init_cash"),
            sp.get("initial_cash"),
            os.getenv("ACCOUNT_INITIAL_CASH"),
        )

        return cls(
            base_url=_first_value(oms_cfg.get("base_url"), oms_cfg.get("base-url"), oms_cfg.get("url")),
            timeout=_to_float(_first_value(oms_cfg.get("timeout_sec"), oms_cfg.get("timeout"), oms_cfg.get("timeout-sec")), None),
            bot_name=str(bot_name),
            mode=resolved_mode,
            init_cash=_to_float(init_cash, 0.0),
            curr_date=_first_value(oms_cfg.get("curr_date"), oms_cfg.get("curr-date")),
            month_year=_first_value(oms_cfg.get("month_year"), oms_cfg.get("month-year")),
            product=str(_first_value(oms_cfg.get("product"), sp.get("product"), "D")).upper(),
            validity=str(_first_value(oms_cfg.get("validity"), sp.get("validity"), "DAY")).upper(),
            tick_size=_to_float(_first_value(oms_cfg.get("tick_size"), oms_cfg.get("tick-size"), sp.get("tick-size"), sp.get("tick_size")), 0.05),
            sl_limit_gap=_to_float(_first_value(oms_cfg.get("sl_limit_gap"), oms_cfg.get("sl-limit-gap"), sp.get("sl-limit-gap"), sp.get("sl_limit_gap")), 1.0),
            orders_csv=_first_value(
                oms_cfg.get("orders_csv"),
                oms_cfg.get("orders-csv"),
                oms_cfg.get("local_orders_csv"),
                oms_cfg.get("local-orders-csv"),
            ),
            daily_csv=_first_value(
                oms_cfg.get("daily_csv"),
                oms_cfg.get("daily-csv"),
                oms_cfg.get("daily_pnl_csv"),
                oms_cfg.get("daily-pnl-csv"),
                oms_cfg.get("local_daily_csv"),
                oms_cfg.get("local-daily-csv"),
            ),
            events_json_path=_first_value(
                oms_cfg.get("events_json"),
                oms_cfg.get("events-json"),
                oms_cfg.get("events_json_path"),
                oms_cfg.get("events-json-path"),
                oms_cfg.get("order_event_log"),
                oms_cfg.get("order-event-log"),
                oms_cfg.get("local_events_json"),
                oms_cfg.get("local-events-json"),
            ),
            local_copy_enabled=_to_bool(
                _first_value(oms_cfg.get("local_copy_enabled"), oms_cfg.get("local-copy-enabled"), True),
                True,
            ),
        )

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/healthz")

    def create_account(
        self,
        bot_name: Optional[str] = None,
        curr_date: Optional[str] = None,
        month_year: Optional[str] = None,
        init_cash: Optional[float] = None,
    ) -> Dict[str, Any]:
        payload = _without_none({
            "bot_name": bot_name or self.bot_name,
            "curr_date": curr_date or self.curr_date,
            "month_year": month_year or self.month_year,
            "init_cash": _to_float(init_cash, self.initial_cash),
        })
        account = self._request("POST", "/v1/accounts", json=payload)
        self._sync_account_state(account)
        return account

    def get_account_details(
        self,
        bot_name: Optional[str] = None,
        curr_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        account = self._request(
            "GET",
            "/v1/accounts",
            params={
                "bot_name": bot_name or self.bot_name,
                "curr_date": curr_date or self.curr_date,
            },
        )
        self._sync_account_state(account)
        self._sync_account_trades(account)
        return account

    def create_trade(
        self,
        symbol: str,
        instrument_token: str,
        qty: int,
        side: str = constants.BUY,
        entry_price: Optional[float] = None,
        target: Optional[float] = None,
        sl_trigger: Optional[float] = None,
        sl_limit: Optional[float] = None,
        trail_points: Optional[float] = None,
        start_trail_after: Optional[float] = None,
        description: Optional[str] = None,
        product: Optional[str] = None,
        validity: Optional[str] = None,
        mode: Optional[str] = None,
        tag_entry: Optional[str] = None,
        tag_sl: Optional[str] = None,
        entry_spot: Optional[float] = None,
        spot_ltp: Optional[float] = None,
        spot_trail_anchor: Optional[float] = None,
        total_brokerage: Optional[float] = None,
        is_amo: bool = False,
        slice: Optional[bool] = None,
        ts: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        payload = self._build_create_trade_payload(
            symbol=symbol,
            instrument_token=instrument_token,
            qty=qty,
            side=side,
            entry_price=entry_price,
            target=target,
            sl_trigger=sl_trigger,
            sl_limit=sl_limit,
            trail_points=trail_points,
            start_trail_after=start_trail_after,
            description=description,
            product=product,
            validity=validity,
            mode=mode,
            tag_entry=tag_entry,
            tag_sl=tag_sl,
            entry_spot=entry_spot,
            spot_ltp=spot_ltp,
            spot_trail_anchor=spot_trail_anchor,
            total_brokerage=total_brokerage,
            is_amo=is_amo,
            slice=slice,
        )
        response = self._request("POST", "/v1/trades", json=payload)
        trade_id = str(response.get("trade_id") or "").strip()
        if trade_id:
            local_row = self._local_row_from_create(payload, response, ts=ts)
            self._last_trade_id = trade_id
            self._active_trade_by_symbol[str(symbol)] = trade_id
            self._remember_trade(local_row)
            self._upsert_local_trade(local_row)
            self._log_local_event("CREATE_TRADE", local_row, extra={"response": dict(response)})
        return response

    def _build_create_trade_payload(
        self,
        symbol: str,
        instrument_token: str,
        qty: int,
        side: str = constants.BUY,
        entry_price: Optional[float] = None,
        target: Optional[float] = None,
        sl_trigger: Optional[float] = None,
        sl_limit: Optional[float] = None,
        trail_points: Optional[float] = None,
        start_trail_after: Optional[float] = None,
        description: Optional[str] = None,
        product: Optional[str] = None,
        validity: Optional[str] = None,
        mode: Optional[str] = None,
        tag_entry: Optional[str] = None,
        tag_sl: Optional[str] = None,
        entry_spot: Optional[float] = None,
        spot_ltp: Optional[float] = None,
        spot_trail_anchor: Optional[float] = None,
        total_brokerage: Optional[float] = None,
        is_amo: bool = False,
        slice: Optional[bool] = None,
    ) -> Dict[str, Any]:
        return _without_none({
            "bot_name": self.bot_name,
            "init_cash": self.initial_cash,
            "curr_date": self.curr_date,
            "month_year": self.month_year,
            "mode": mode or self.mode,
            "symbol": symbol,
            "instrument_token": instrument_token,
            "side": str(side or constants.BUY).upper(),
            "qty": int(qty),
            "product": str(product or self.product).upper(),
            "validity": str(validity or self.validity).upper(),
            "entry_price": _to_float(entry_price, None),
            "target": _to_float(target, None),
            "sl_trigger": _to_float(sl_trigger, None),
            "sl_limit": _to_float(sl_limit, None),
            "tsl_active": bool(trail_points is not None and _to_float(trail_points, 0.0) > 0),
            "start_trail_after": _to_float(start_trail_after, None),
            "entry_spot": _to_float(entry_spot, None),
            "spot_ltp": _to_float(spot_ltp, None),
            "spot_trail_anchor": _to_float(spot_trail_anchor, entry_price),
            "trail_points": _to_float(trail_points, None),
            "total_brokerage": _to_float(total_brokerage, None),
            "tag_entry": tag_entry or f"{self.bot_name}-entry",
            "tag_sl": tag_sl or f"{self.bot_name}-sl",
            "description": description,
            "is_amo": bool(is_amo),
            "slice": slice,
        })

    def buy(
        self,
        symbol: str,
        instrument_token: str,
        qty: int,
        entry_price: Optional[float] = None,
        sl_trigger: Optional[float] = None,
        sl_limit: Optional[float] = None,
        target: Optional[float] = None,
        trail_points: Optional[float] = None,
        start_trail_after: Optional[float] = None,
        description: Optional[str] = None,
        ts: Optional[datetime] = None,
        **kwargs: Any,
    ) -> Optional[str]:
        response = self.create_trade(
            symbol=symbol,
            instrument_token=instrument_token,
            qty=qty,
            side=constants.BUY,
            entry_price=entry_price,
            sl_trigger=sl_trigger,
            sl_limit=sl_limit,
            target=target,
            trail_points=trail_points,
            start_trail_after=start_trail_after,
            description=description,
            ts=ts,
            **kwargs,
        )
        return response.get("trade_id")

    def modify_trade(
        self,
        trade_id: str,
        stoploss: Optional[float] = None,
        sl_limit: Optional[float] = None,
        spot_trail_anchor: Optional[float] = None,
        order_type: str = constants.SL,
        disclosed_quantity: int = 0,
        validity: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = _without_none({
            "mode": mode or self.mode,
            "validity": str(validity or self.validity).upper(),
            "order_type": str(order_type or constants.SL).upper(),
            "disclosed_quantity": int(disclosed_quantity or 0),
            "stoploss": _to_float(stoploss, None),
            "sl_limit": _to_float(sl_limit, None),
            "spot_trail_anchor": _to_float(spot_trail_anchor, None),
        })
        response = self._request("POST", f"/v1/trades/{trade_id}/modify", json=payload)
        updates = {
            "stoploss": payload.get("stoploss"),
            "sl_limit": payload.get("sl_limit"),
            "_sl_limit": payload.get("sl_limit"),
            "spot_trail_anchor": payload.get("spot_trail_anchor"),
            "_spot_trail_anchor": payload.get("spot_trail_anchor"),
        }
        self._patch_cached_trade(trade_id, updates)
        self._patch_local_trade(trade_id, updates)
        self._log_local_event("MODIFY_TRADE", self._get_cached_trade(trade_id) or {"id": trade_id}, extra={"request": payload, "response": dict(response)})
        return response

    def modify_sl_order(
        self,
        trade_id: str,
        ltp_now: Optional[float] = None,
        new_trigger: Optional[float] = None,
        new_limit: Optional[float] = None,
        ts: Optional[datetime] = None,
    ) -> bool:
        try:
            self.modify_trade(
                trade_id=trade_id,
                stoploss=new_trigger,
                sl_limit=new_limit,
                spot_trail_anchor=ltp_now,
            )
            trade = self._get_cached_trade(trade_id) or {"id": trade_id}
            self._log_local_event(
                "MODIFY_SL_ORDER",
                trade,
                ts=ts,
                extra={
                    "ltp_now": _to_float(ltp_now, None),
                    "new_trigger": _to_float(new_trigger, None),
                    "new_limit": _to_float(new_limit, None),
                },
            )
            return True
        except Exception as exc:
            log.warning("modify_sl_order failed trade_id=%s: %s", trade_id, exc)
            return False

    def get_trade_by_id(self, trade_id: str) -> Dict[str, Any]:
        trade = self._request("GET", f"/v1/trades/{trade_id}")
        self._upsert_local_trade(self._local_row_from_trade(trade))
        return self._remember_trade(trade)

    def refresh_trade(self, trade_id: str, ts: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        trade = self.get_trade_by_id(trade_id)
        status = str(trade.get("status") or "").strip().upper()
        if status in self.CLOSED_STATUSES:
            self._log_local_event("SYNC_CLOSED_TRADE", trade, ts=ts)
            self._forget_trade(trade)
        return trade

    def refresh_trade_status(self, trade_id: str, ts: Optional[datetime] = None) -> Optional[str]:
        trade = self.refresh_trade(trade_id, ts=ts)
        if not trade:
            return None
        status = str(trade.get("status") or "").strip()
        return status or None

    def on_tick(
        self,
        symbol: str,
        o: Optional[float] = None,
        h: Optional[float] = None,
        l: Optional[float] = None,
        c: Optional[float] = None,
        ts: Optional[datetime] = None,
        trade_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        price = _first_float(c, h, l, o)
        if price is None or price <= 0:
            return None

        resolved_trade_id = str(
            trade_id
            or self._active_trade_by_symbol.get(str(symbol))
            or self._last_trade_id
            or ""
        ).strip()
        if not resolved_trade_id:
            return None

        trade = self._get_cached_trade(resolved_trade_id)
        if trade is None:
            log.debug("Skipping OMS tick for trade_id=%s; no cached trade snapshot.", resolved_trade_id)
            return None

        status = str(trade.get("status") or "").strip().upper()
        if status in self.CLOSED_STATUSES:
            self._forget_trade(trade)
            return trade
        if status and status != constants.OPEN:
            return trade

        if self.mode == constants.SANDBOX:
            exit_signal = self._local_exit_signal(trade, o=o, h=h, l=l, c=c, include_target=False)
            if exit_signal and exit_signal.get("reason") == constants.STOPLOSS_HIT:
                log.info(
                    "Sandbox SL hit detected locally trade_id=%s symbol=%s price=%.2f stoploss=%.2f",
                    resolved_trade_id,
                    symbol,
                    price,
                    _to_float(trade.get("stoploss"), 0.0) or 0.0,
                )
                return self.square_off_trade(
                    trade_id=resolved_trade_id,
                    exit_price=exit_signal.get("exit_price") or price,
                    ts=ts,
                    reason=constants.STOPLOSS_HIT,
                )

        if not _to_bool(trade.get("tsl_active"), False):
            return trade

        update = self._build_trailing_update(trade, price)
        if update is None:
            return trade

        log.info(
            "Trailing SL update trade_id=%s symbol=%s price=%.2f stoploss=%.2f sl_limit=%.2f",
            resolved_trade_id,
            symbol,
            price,
            update["stoploss"],
            update.get("sl_limit", 0.0),
        )
        try:
            self.modify_trade(
                resolved_trade_id,
                stoploss=update["stoploss"],
                sl_limit=update.get("sl_limit"),
                spot_trail_anchor=price,
            )
        except OrderSystemError as exc:
            if _is_rate_limit_error(exc):
                log.warning("Trailing SL modify deferred by rate limit trade_id=%s: %s", resolved_trade_id, exc)
                return trade
            raise
        trade.update({
            "stoploss": update["stoploss"],
            "sl_limit": update.get("sl_limit"),
            "_sl_limit": update.get("sl_limit"),
            "spot_trail_anchor": price,
            "_spot_trail_anchor": price,
        })
        return self._remember_trade(trade)

    def square_off_trade(
        self,
        trade_id: str,
        exit_price: Optional[float] = None,
        ts: Optional[datetime] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = _without_none({
            "mode": self.mode,
            "exit_price": _to_float(exit_price, None),
            "exit_time": _normalize_timestamp(ts),
            "reason": reason or constants.EOD_SQUARE_OFF,
            "validity": self.validity,
        })
        response = self._request("POST", f"/v1/trades/{trade_id}/square-off", json=payload)
        updates = {
            "status": response.get("status") or payload.get("reason"),
            "exit_price": response.get("exit_price") or payload.get("exit_price"),
            "exit_time": _normalize_timestamp(response.get("exit_time") or payload.get("exit_time")),
        }
        self._patch_cached_trade(trade_id, updates)
        self._patch_local_trade(trade_id, updates)

        try:
            trade = self.get_trade_by_id(trade_id)
            self._log_local_event("SQUARE_OFF_TRADE", trade, extra={"request": payload, "response": dict(response)})
            return trade
        except OrderSystemError:
            self._log_local_event(
                "SQUARE_OFF_TRADE",
                self._get_cached_trade(trade_id) or {"id": trade_id},
                extra={"request": payload, "response": dict(response)},
            )
            return response

    def _init_local_ledger(self) -> None:
        _ensure_csv_file(self.orders_csv, ORDER_COLUMNS)
        _ensure_csv_file(self.daily_csv, DAILY_PNL_COLUMNS)
        _ensure_json_file(self.events_json_path, {"events": []})

    def _sync_account_trades(self, account: Mapping[str, Any]) -> None:
        trades = account.get("trades")
        if not isinstance(trades, list):
            return
        for trade in trades:
            if isinstance(trade, Mapping):
                self._remember_trade(trade)
                self._upsert_local_trade(self._local_row_from_trade(trade))

    def _local_row_from_create(
        self,
        payload: Mapping[str, Any],
        response: Mapping[str, Any],
        ts: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now = _normalize_timestamp(ts) or _now_ist().isoformat()
        return {
            "id": response.get("trade_id"),
            "symbol": payload.get("symbol"),
            "instrument_token": payload.get("instrument_token"),
            "side": payload.get("side"),
            "qty": payload.get("qty"),
            "product": payload.get("product"),
            "validity": payload.get("validity"),
            "entry_order_ids": response.get("entry_order_ids") or [],
            "sl_order_ids": response.get("sl_order_ids") or [],
            "target_order_id": None,
            "entry_price": payload.get("entry_price"),
            "target": payload.get("target"),
            "stoploss": payload.get("sl_trigger"),
            "_sl_limit": payload.get("sl_limit"),
            "tsl_active": payload.get("tsl_active"),
            "start_trail_after": payload.get("start_trail_after"),
            "entry_spot": payload.get("entry_spot"),
            "spot_ltp": payload.get("spot_ltp"),
            "_spot_trail_anchor": payload.get("spot_trail_anchor"),
            "_trail_points": payload.get("trail_points"),
            "status": response.get("status") or constants.OPEN,
            "timestamp": now,
            "exit_price": None,
            "pnl": None,
            "exit_time": None,
            "tag_entry": payload.get("tag_entry"),
            "tag_sl": payload.get("tag_sl"),
            "description": payload.get("description"),
            "sl_order_qty_map": {},
        }

    def _local_row_from_trade(self, trade: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "id": trade.get("id") or trade.get("trade_id"),
            "symbol": trade.get("symbol"),
            "instrument_token": trade.get("instrument_token"),
            "side": trade.get("side"),
            "qty": trade.get("qty"),
            "product": trade.get("product"),
            "validity": trade.get("validity"),
            "entry_order_ids": _trade_order_ids(trade, "entry"),
            "sl_order_ids": _trade_order_ids(trade, "sl"),
            "target_order_id": trade.get("target_order_id"),
            "entry_price": trade.get("entry_price"),
            "target": trade.get("target"),
            "stoploss": trade.get("stoploss") or trade.get("sl_trigger"),
            "_sl_limit": trade.get("sl_limit"),
            "tsl_active": trade.get("tsl_active"),
            "start_trail_after": trade.get("start_trail_after"),
            "entry_spot": trade.get("entry_spot"),
            "spot_ltp": trade.get("spot_ltp"),
            "_spot_trail_anchor": trade.get("spot_trail_anchor"),
            "_trail_points": trade.get("trail_points"),
            "status": trade.get("status"),
            "timestamp": _normalize_timestamp(trade.get("timestamp")),
            "exit_price": trade.get("exit_price"),
            "pnl": trade.get("pnl"),
            "exit_time": _normalize_timestamp(trade.get("exit_time")),
            "tag_entry": trade.get("tag_entry"),
            "tag_sl": trade.get("tag_sl"),
            "description": trade.get("description"),
            "sl_order_qty_map": {},
        }

    def _normalize_trade_snapshot(self, trade: Mapping[str, Any]) -> Dict[str, Any]:
        snapshot = dict(trade)
        trade_id = str(_first_value(snapshot.get("id"), snapshot.get("trade_id")) or "").strip()
        if trade_id:
            snapshot["id"] = trade_id
            snapshot["trade_id"] = trade_id

        alias_pairs = (
            ("sl_limit", "_sl_limit"),
            ("spot_trail_anchor", "_spot_trail_anchor"),
            ("trail_points", "_trail_points"),
        )
        for public_key, local_key in alias_pairs:
            public_value = snapshot.get(public_key)
            local_value = snapshot.get(local_key)
            if (public_value is None or public_value == "") and local_value not in (None, ""):
                snapshot[public_key] = local_value
            if (local_value is None or local_value == "") and public_value not in (None, ""):
                snapshot[local_key] = public_value

        if "tsl_active" in snapshot:
            snapshot["tsl_active"] = _to_bool(snapshot.get("tsl_active"), False)
        return snapshot

    def _remember_trade(self, trade: Mapping[str, Any]) -> Dict[str, Any]:
        snapshot = self._normalize_trade_snapshot(trade)
        trade_id = str(snapshot.get("id") or snapshot.get("trade_id") or "").strip()
        if not trade_id:
            return snapshot

        self._trade_cache[trade_id] = snapshot
        status = str(snapshot.get("status") or "").strip().upper()
        symbol = str(snapshot.get("symbol") or "").strip()

        if status in self.CLOSED_STATUSES:
            if self._last_trade_id == trade_id:
                self._last_trade_id = None
            if symbol and self._active_trade_by_symbol.get(symbol) == trade_id:
                self._active_trade_by_symbol.pop(symbol, None)
        else:
            self._last_trade_id = trade_id
            if symbol:
                self._active_trade_by_symbol[symbol] = trade_id

        return dict(snapshot)

    def _get_cached_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        trade_id = str(trade_id or "").strip()
        if not trade_id:
            return None

        trade = self._trade_cache.get(trade_id)
        if trade is not None:
            return dict(trade)

        row = self._read_local_trade(trade_id)
        if row is None:
            return None
        return self._remember_trade(row)

    def _read_local_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        if not self.local_copy_enabled:
            return None

        trade_id = str(trade_id or "").strip()
        if not trade_id:
            return None

        for row in self._read_local_rows():
            if not self._row_belongs_to_active_day(row):
                continue
            if str(row.get("id") or "").strip() == trade_id:
                return row
        return None

    def _patch_cached_trade(self, trade_id: str, updates: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        trade_id = str(trade_id or "").strip()
        if not trade_id:
            return None

        trade = self._get_cached_trade(trade_id) or {"id": trade_id, "trade_id": trade_id}
        for key, value in updates.items():
            if value is not None:
                trade[key] = value
        return self._remember_trade(trade)

    def _upsert_local_trade(self, row: Mapping[str, Any]) -> None:
        if not self.local_copy_enabled:
            return

        trade_id = str(row.get("id") or "").strip()
        if not trade_id:
            return

        rows = self._read_local_rows()
        by_id = {str(r.get("id") or "").strip(): i for i, r in enumerate(rows)}

        if trade_id in by_id:
            existing = rows[by_id[trade_id]]
            for key in ORDER_COLUMNS:
                value = row.get(key)
                if value is not None and value != "":
                    existing[key] = _local_csv_value(key, value)
            self._enrich_closed_row(existing)
        else:
            new_row = {key: _local_csv_value(key, row.get(key)) for key in ORDER_COLUMNS}
            self._enrich_closed_row(new_row)
            rows.append(new_row)

        self._write_local_rows(rows)
        saved_row = next((r for r in rows if str(r.get("id") or "").strip() == trade_id), row)
        self._update_daily_pnl_for_trade(saved_row)

    def _patch_local_trade(self, trade_id: str, updates: Mapping[str, Any]) -> None:
        if not self.local_copy_enabled:
            return

        trade_id = str(trade_id or "").strip()
        if not trade_id:
            return

        rows = self._read_local_rows()
        patched = False
        patched_row = None
        for row in rows:
            if str(row.get("id") or "").strip() != trade_id:
                continue
            for key, value in updates.items():
                if key in ORDER_COLUMNS and value is not None:
                    row[key] = _local_csv_value(key, value)
            self._enrich_closed_row(row)
            patched_row = row
            patched = True
            break

        if patched:
            self._write_local_rows(rows)
            self._update_daily_pnl_for_trade(patched_row or {"id": trade_id})

    def _read_local_rows(self) -> List[Dict[str, Any]]:
        try:
            with open(self.orders_csv, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)
                rows = []
                for row in reader:
                    normalized = {key: row.get(key, "") for key in ORDER_COLUMNS}
                    rows.append(normalized)
                return rows
        except FileNotFoundError:
            return []
        except Exception as exc:
            log.warning("Failed reading local OMS ledger %s: %s", self.orders_csv, exc)
            return []

    def _write_local_rows(self, rows: List[Mapping[str, Any]]) -> None:
        path = Path(self.orders_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = ""
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=str(path.parent),
            )
            os.close(fd)
            with open(tmp_path, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=ORDER_COLUMNS)
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: _local_csv_value(key, row.get(key)) for key in ORDER_COLUMNS})
            os.replace(tmp_path, path)
        except Exception as exc:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            log.warning("Failed writing local OMS ledger %s: %s", self.orders_csv, exc)

    def _read_daily_rows(self) -> List[Dict[str, Any]]:
        try:
            with open(self.daily_csv, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)
                return [{key: row.get(key, "") for key in DAILY_PNL_COLUMNS} for row in reader]
        except FileNotFoundError:
            return []
        except Exception as exc:
            log.warning("Failed reading local daily pnl %s: %s", self.daily_csv, exc)
            return []

    def _write_daily_rows(self, rows: List[Mapping[str, Any]]) -> None:
        path = Path(self.daily_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = ""
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=str(path.parent),
            )
            os.close(fd)
            with open(tmp_path, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=DAILY_PNL_COLUMNS)
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: row.get(key, "") for key in DAILY_PNL_COLUMNS})
            os.replace(tmp_path, path)
        except Exception as exc:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            log.warning("Failed writing local daily pnl %s: %s", self.daily_csv, exc)

    def _load_events_payload(self) -> Dict[str, Any]:
        try:
            with open(self.events_json_path, "r", encoding="utf-8") as file:
                payload = jsonlib.load(file)
        except FileNotFoundError:
            return {"events": []}
        except Exception as exc:
            log.warning("Failed reading local event log %s: %s", self.events_json_path, exc)
            return {"events": []}

        if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            return {"events": []}
        return payload

    def _write_events_payload(self, payload: Mapping[str, Any]) -> None:
        path = Path(self.events_json_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = ""
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=str(path.parent),
            )
            os.close(fd)
            with open(tmp_path, "w", encoding="utf-8") as file:
                jsonlib.dump(payload, file, indent=2)
            os.replace(tmp_path, path)
        except Exception as exc:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            log.warning("Failed writing local event log %s: %s", self.events_json_path, exc)

    def _remaining_cash_for_event(self, trade: Mapping[str, Any]) -> float:
        initial_cash = _to_float(self.initial_cash, 0.0) or 0.0
        active_month = _month_year_key(self.month_year) or _month_year_key(self.curr_date)
        current_trade_id = str(trade.get("id") or trade.get("trade_id") or "").strip()
        current_trade_counted = False
        realized_pnl = 0.0

        for row in self._read_local_rows():
            status = str(row.get("status") or "").strip().upper()
            if status not in self.CLOSED_STATUSES:
                continue

            row_trade_id = str(row.get("id") or row.get("trade_id") or "").strip()
            row_month = _month_year_key(row.get("exit_time")) or _month_year_key(row.get("timestamp"))
            if active_month and row_month and row_month != active_month:
                continue
            if active_month and not row_month and row_trade_id != current_trade_id:
                continue

            pnl = _to_float(row.get("pnl"), None)
            if pnl is None:
                continue

            realized_pnl += float(pnl)
            if current_trade_id and row_trade_id == current_trade_id:
                current_trade_counted = True

        trade_status = str(trade.get("status") or "").strip().upper()
        if trade_status in self.CLOSED_STATUSES and not current_trade_counted:
            pnl = _to_float(trade.get("pnl"), None)
            if pnl is not None:
                realized_pnl += float(pnl)

        return float(initial_cash) + float(realized_pnl)

    def _log_local_event(
        self,
        event_type: str,
        trade: Mapping[str, Any],
        ts: Optional[datetime] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not self.local_copy_enabled:
            return

        payload = self._load_events_payload()
        event = {
            "ts": _normalize_timestamp(ts) or _now_ist().isoformat(),
            "event_type": event_type,
            "trade_id": trade.get("id") or trade.get("trade_id"),
            "symbol": trade.get("symbol"),
            "instrument_token": trade.get("instrument_token"),
            "side": trade.get("side"),
            "qty": trade.get("qty"),
            "status": trade.get("status"),
            "entry_order_ids": _json_list(trade.get("entry_order_ids")),
            "sl_order_ids": _json_list(trade.get("sl_order_ids")),
            "target_order_id": trade.get("target_order_id"),
            "entry_price": _to_float(trade.get("entry_price"), None),
            "target": _to_float(trade.get("target"), None),
            "stoploss": _to_float(trade.get("stoploss"), None),
            "exit_price": _to_float(trade.get("exit_price"), None),
            "pnl": _to_float(trade.get("pnl"), None),
            "remaining_cash": self._remaining_cash_for_event(trade),
            "exit_time": trade.get("exit_time"),
        }
        if extra:
            event.update(dict(extra))

        payload["events"].append(event)
        self._write_events_payload(payload)

    def _enrich_closed_row(self, row: Dict[str, Any]) -> None:
        status = str(row.get("status") or "").strip().upper()
        if status not in self.CLOSED_STATUSES:
            return

        exit_price = _to_float(row.get("exit_price"), None)
        if exit_price is None or exit_price <= 0:
            return

        if not str(row.get("exit_time") or "").strip():
            row["exit_time"] = _now_ist().isoformat()

        if str(row.get("pnl") or "").strip():
            return

        entry_price = _to_float(row.get("entry_price"), None)
        qty = _to_float(row.get("qty"), None)
        if entry_price is None or qty is None:
            return

        side = str(row.get("side") or constants.BUY).upper()
        pnl = (exit_price - entry_price) * qty
        if side == constants.SELL:
            pnl = (entry_price - exit_price) * qty
        row["pnl"] = round(float(pnl), 2)

    def _update_daily_pnl_for_trade(self, trade: Optional[Mapping[str, Any]]) -> None:
        if not self.local_copy_enabled or not trade:
            return

        status = str(trade.get("status") or "").strip().upper()
        if status not in self.CLOSED_STATUSES:
            return

        day = _timestamp_day_iso(trade.get("exit_time"))
        if not day:
            return

        order_rows = self._read_local_rows()
        closed_rows = []
        for row in order_rows:
            row_status = str(row.get("status") or "").strip().upper()
            if row_status not in self.CLOSED_STATUSES:
                continue
            if _timestamp_day_iso(row.get("exit_time")) != day:
                continue
            closed_rows.append(row)

        daily_pnl = sum(_to_float(row.get("pnl"), 0.0) or 0.0 for row in closed_rows)
        num_trades = len(closed_rows)
        wins = sum(1 for row in closed_rows if (_to_float(row.get("pnl"), 0.0) or 0.0) > 0)
        win_rate = (wins / num_trades) * 100.0 if num_trades else 0.0

        rows = self._read_daily_rows()
        by_date = {str(row.get("date") or ""): i for i, row in enumerate(rows)}
        row = {
            "date": day,
            "daily_pnl": round(float(daily_pnl), 2),
            "num_trades": num_trades,
            "win_rate": round(float(win_rate), 2),
        }
        if day in by_date:
            rows[by_date[day]].update(row)
        else:
            rows.append(row)

        rows = sorted(rows, key=lambda item: str(item.get("date") or ""))
        running_pnl = 0.0
        peak_equity = float(self.initial_cash)
        for daily_row in rows:
            pnl = _to_float(daily_row.get("daily_pnl"), 0.0) or 0.0
            running_pnl += pnl
            equity = float(self.initial_cash) + running_pnl
            peak_equity = max(peak_equity, equity)
            drawdown = equity - peak_equity
            drawdown_pct = (drawdown / peak_equity) if peak_equity else 0.0
            daily_row["cash"] = round(equity, 2)
            daily_row["equity"] = round(equity, 2)
            daily_row["peak_equity"] = round(peak_equity, 2)
            daily_row["drawdown"] = round(drawdown, 2)
            daily_row["drawdown_pct"] = round(drawdown_pct, 6)

        self._write_daily_rows(rows)

    def _row_belongs_to_active_day(self, row: Mapping[str, Any]) -> bool:
        active_day = _curr_date_iso(self.curr_date)
        row_day = _timestamp_day_iso(row.get("timestamp")) or _timestamp_day_iso(row.get("exit_time"))
        return row_day is None or row_day == active_day

    def _local_exit_signal(
        self,
        trade: Mapping[str, Any],
        o: Optional[float] = None,
        h: Optional[float] = None,
        l: Optional[float] = None,
        c: Optional[float] = None,
        include_target: bool = False,
    ) -> Optional[Dict[str, Any]]:
        side = str(trade.get("side") or constants.BUY).upper()
        high = _first_float(h, c, o, l)
        low = _first_float(l, c, o, h)
        close = _first_float(c, h, l, o)
        if high is None or low is None or close is None:
            return None

        stoploss = _to_float(trade.get("stoploss"), None)
        target = _to_float(trade.get("target"), None)

        if side == constants.SELL:
            sl_hit = stoploss is not None and high >= stoploss
            target_hit = target is not None and low <= target
        else:
            sl_hit = stoploss is not None and low <= stoploss
            target_hit = target is not None and high >= target

        # If a candle spans both levels, choose the conservative close.
        if sl_hit:
            return {"reason": constants.STOPLOSS_HIT, "exit_price": float(stoploss)}
        if include_target and target_hit:
            return {"reason": constants.TARGET_HIT, "exit_price": float(target)}
        return None

    def _build_trailing_update(self, trade: Mapping[str, Any], price: float) -> Optional[Dict[str, float]]:
        entry = _to_float(trade.get("entry_price"), 0.0)
        trail_points = _to_float(_first_value(trade.get("trail_points"), trade.get("_trail_points")), 0.0)
        current_sl = _to_float(trade.get("stoploss"), 0.0)
        current_limit = _to_float(_first_value(trade.get("sl_limit"), trade.get("_sl_limit")), 0.0)
        start_after = _to_float(trade.get("start_trail_after"), 0.0)
        side = str(trade.get("side") or constants.BUY).upper()

        if entry <= 0 or trail_points <= 0:
            return None

        if side == constants.SELL:
            start_price = entry - _start_after_points(entry, start_after)
            if price > start_price:
                return None
            new_sl = self._round_price(price + trail_points, "FLOOR")
            if current_sl > 0 and new_sl >= current_sl - self.tick_size:
                return None
            gap = _positive_gap(current_limit - current_sl, self.sl_limit_gap)
            return {
                "stoploss": new_sl,
                "sl_limit": self._round_price(new_sl + gap, "CEIL"),
            }

        start_price = entry + _start_after_points(entry, start_after)
        if price < start_price:
            return None
        new_sl = self._round_price(price - trail_points, "CEIL")
        if current_sl > 0 and new_sl <= current_sl + self.tick_size:
            return None
        gap = _positive_gap(current_sl - current_limit, self.sl_limit_gap)
        sl_limit = self._round_price(new_sl - gap, "FLOOR")
        if sl_limit >= new_sl:
            sl_limit = self._round_price(new_sl - self.tick_size, "FLOOR")
        return {"stoploss": new_sl, "sl_limit": sl_limit}

    def _round_price(self, value: float, mode: str) -> float:
        tick = self.tick_size if self.tick_size > 0 else 0.05
        n = float(value) / tick
        if mode == "CEIL":
            return round(math.ceil(n) * tick, 2)
        if mode == "FLOOR":
            return round(math.floor(n) * tick, 2)
        return round(round(n) * tick, 2)

    def _wait_for_request_slot(self, request_key: str) -> None:
        next_ok = self._request_next_ok.get(request_key, 0.0)
        wait = next_ok - time.monotonic()
        if wait > 0:
            log.debug("Waiting %.2fs before %s", wait, request_key)
            time.sleep(wait)

    def _remember_successful_request(self, request_key: str) -> None:
        self._request_backoff.pop(request_key, None)
        if request_key == _MODIFY_REQUEST_KEY and self.modify_min_interval_sec > 0:
            self._request_next_ok[request_key] = time.monotonic() + self.modify_min_interval_sec
            return
        self._request_next_ok.pop(request_key, None)

    def _remember_rate_limited_request(self, request_key: str, response: Response) -> float:
        retry_after = _retry_after_seconds(response)
        if retry_after is None:
            previous = self._request_backoff.get(request_key, 0.0)
            retry_after = self.rate_limit_base_sleep_sec if previous <= 0 else min(previous * 2.0, self.rate_limit_max_sleep_sec)
        self._request_backoff[request_key] = retry_after
        self._request_next_ok[request_key] = time.monotonic() + retry_after
        return retry_after

    def _request(
        self,
        method: str,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = urljoin(self.base_url + "/", path.lstrip("/"))
        request_key = _request_rate_limit_key(method, path)
        max_attempts = self.rate_limit_max_retries + 1

        for attempt in range(max_attempts):
            self._wait_for_request_slot(request_key)
            try:
                response = self.session.request(
                    method,
                    url,
                    json=json,
                    params=_without_none(params or {}),
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                raise OrderSystemError(f"{method} {url} failed: {exc}") from exc

            payload = _decode_json(response)
            if _is_rate_limited_response(response, payload):
                wait = self._remember_rate_limited_request(request_key, response)
                if attempt < self.rate_limit_max_retries:
                    log.warning(
                        "%s %s rate-limited with status %s; retrying in %.2fs",
                        method,
                        path,
                        response.status_code,
                        wait,
                    )
                    continue

            if not 200 <= response.status_code < 300:
                message = _response_error_message(response, payload)
                raise OrderSystemError(
                    f"{method} {url} failed with status {response.status_code}: {message}",
                    status_code=response.status_code,
                    payload=payload,
                    retry_after=_retry_after_seconds(response),
                )

            self._remember_successful_request(request_key)
            if isinstance(payload, dict):
                return payload
            return {"data": payload}

        raise OrderSystemError(f"{method} {url} failed after rate-limit retries")

    def _sync_account_state(self, payload: Mapping[str, Any]) -> None:
        account_id = str(
            _first_value(payload.get("account_id"), payload.get("acct_id"), payload.get("id"), "") or ""
        ).strip()
        if account_id:
            self.account_id = account_id
        init_cash = _to_float(payload.get("init_cash"), None)
        if init_cash is not None:
            self.initial_cash = init_cash
        curr_date = str(payload.get("curr_date") or "").strip()
        if curr_date:
            self.curr_date = curr_date
        month_year = str(payload.get("month_year") or "").strip()
        if month_year:
            self.month_year = month_year

    def _forget_trade(self, trade: Mapping[str, Any]) -> None:
        trade_id = str(trade.get("id") or trade.get("trade_id") or self._last_trade_id or "").strip()
        symbol = str(trade.get("symbol") or "").strip()
        if trade_id:
            self._trade_cache.pop(trade_id, None)
        if trade_id and self._last_trade_id == trade_id:
            self._last_trade_id = None
        if symbol and self._active_trade_by_symbol.get(symbol) == trade_id:
            self._active_trade_by_symbol.pop(symbol, None)


def _clean_base_url(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return OrderSystemClient.DEFAULT_BASE_URL
    return value.rstrip("/")


def _default_orders_csv(mode: str) -> str:
    mode_key = str(mode or constants.MOCK).strip().lower()
    order_log_by_mode = {
        constants.PRODUCTION: constants.ORDER_PROD_LOG,
        constants.SANDBOX: constants.ORDER_SANDBOX_LOG,
        constants.MOCK: constants.ORDER_MOCK_LOG,
    }
    if mode_key in order_log_by_mode:
        return order_log_by_mode[mode_key]
    return str(Path(constants.TRENDOBOT_EXECUTION_RESULTS_DIR) / (mode_key or constants.MOCK) / "order_log.csv")


def _default_daily_csv(mode: str) -> str:
    mode_key = str(mode or constants.MOCK).strip().lower()
    daily_by_mode = {
        constants.PRODUCTION: constants.DAILY_PROD_PNL,
        constants.SANDBOX: constants.DAILY_SANDBOX_PNL,
        constants.MOCK: constants.DAILY_MOCK_PNL,
    }
    if mode_key in daily_by_mode:
        return daily_by_mode[mode_key]
    return str(Path(constants.TRENDOBOT_EXECUTION_RESULTS_DIR) / (mode_key or constants.MOCK) / "daily_pnl.csv")


def _default_events_json(mode: str) -> str:
    mode_key = str(mode or constants.MOCK).strip().lower()
    events_by_mode = {
        constants.PRODUCTION: constants.ORDER_PROD_EVENT_LOG,
        constants.SANDBOX: constants.ORDER_SANDBOX_EVENT_LOG,
        constants.MOCK: constants.ORDER_MOCK_EVENT_LOG,
    }
    if mode_key in events_by_mode:
        return events_by_mode[mode_key]
    return str(Path(constants.TRENDOBOT_EXECUTION_RESULTS_DIR) / (mode_key or constants.MOCK) / "order_event_log.json")


def initialize_local_ledgers_for_modes(modes: Optional[List[str]] = None) -> Dict[str, Dict[str, str]]:
    initialized: Dict[str, Dict[str, str]] = {}
    for raw_mode in modes or list(constants.EXECUTION_MODES):
        mode = constants.normalize_execution_mode(raw_mode)
        if mode not in constants.EXECUTION_MODES:
            raise ValueError(f"unsupported execution mode for ledger initialization: {raw_mode}")

        paths = {
            "orders_csv": _default_orders_csv(mode),
            "daily_csv": _default_daily_csv(mode),
            "events_json": _default_events_json(mode),
        }
        _ensure_csv_file(paths["orders_csv"], ORDER_COLUMNS)
        _ensure_csv_file(paths["daily_csv"], DAILY_PNL_COLUMNS)
        _ensure_json_file(paths["events_json"], {"events": []})
        initialized[mode] = paths
    return initialized


def _ensure_csv_file(path_value: str, fieldnames: List[str]) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return

    tmp_path = ""
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
        )
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
        os.replace(tmp_path, path)
    except Exception as exc:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        log.warning("Failed initializing local CSV ledger %s: %s", path, exc)


def _ensure_json_file(path_value: str, payload: Mapping[str, Any]) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return

    tmp_path = ""
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
        )
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as file:
            jsonlib.dump(payload, file, indent=2)
        os.replace(tmp_path, path)
    except Exception as exc:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        log.warning("Failed initializing local JSON ledger %s: %s", path, exc)


def _json_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    if isinstance(value, str) and value.strip():
        try:
            parsed = jsonlib.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item or "").strip()]
        except ValueError:
            pass
        return [value]
    return []


def _trade_order_ids(trade: Mapping[str, Any], order_type: str) -> List[str]:
    key = "entry_order_ids" if order_type == "entry" else "sl_order_ids"
    values = trade.get(key)
    if isinstance(values, list):
        return [str(value) for value in values if str(value or "").strip()]
    if isinstance(values, str) and values.strip():
        try:
            parsed = jsonlib.loads(values)
            if isinstance(parsed, list):
                return [str(value) for value in parsed if str(value or "").strip()]
        except ValueError:
            return [values]

    orders = trade.get("orders")
    if not isinstance(orders, list):
        return []

    out = []
    for order in orders:
        if not isinstance(order, Mapping):
            continue
        if str(order.get("order_type") or "").strip().lower() != order_type:
            continue
        order_id = str(order.get("order_id") or "").strip()
        if order_id:
            out.append(order_id)
    return out


def _normalize_timestamp(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts.astimezone(IST).isoformat()


def _local_csv_value(key: str, value: Any) -> Any:
    if value is None:
        return ""
    if key in JSON_COLUMNS:
        if isinstance(value, str):
            return value
        try:
            return jsonlib.dumps(value)
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _curr_date_iso(curr_date: Any) -> str:
    text = str(curr_date or "").strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    return _now_ist().date().isoformat()


def _timestamp_day_iso(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(IST).date().isoformat()
    except ValueError:
        pass
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _month_year_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    if text.isdigit() and len(text) == 6:
        if 1 <= int(text[:2]) <= 12:
            return text
        if text.startswith(("19", "20")) and 1 <= int(text[4:]) <= 12:
            return f"{text[4:]}{text[:4]}"

    day = _timestamp_day_iso(text)
    if day:
        try:
            return datetime.strptime(day, "%Y-%m-%d").strftime("%m%Y")
        except ValueError:
            pass

    return ""


_MODIFY_REQUEST_KEY = "POST /v1/trades/*/modify"


def _request_rate_limit_key(method: str, path: str) -> str:
    method_key = str(method or "").strip().upper()
    clean_path = "/" + str(path or "").strip().lstrip("/")
    if method_key == "POST" and clean_path.startswith("/v1/trades/") and clean_path.endswith("/modify"):
        return _MODIFY_REQUEST_KEY
    return f"{method_key} {clean_path}"


def _retry_after_seconds(response: Response) -> Optional[float]:
    value = response.headers.get("Retry-After") if response is not None else None
    if value is None or str(value).strip() == "":
        return None
    try:
        seconds = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def _is_rate_limited_response(response: Response, payload: Any) -> bool:
    if response.status_code == 429:
        return True
    if response.status_code not in {500, 502, 503, 504}:
        return False
    return _message_looks_rate_limited(_response_error_message(response, payload))


def _is_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, OrderSystemError) and exc.status_code == 429:
        return True
    return _message_looks_rate_limited(str(exc))


def _message_looks_rate_limited(message: str) -> bool:
    lower = str(message or "").lower()
    return any(token in lower for token in ("rate limit", "rate-limited", "rate limited", "too many requests", "(429)", "status 429"))


def _decode_json(response: Response) -> Any:
    if not response.content:
        return {}
    try:
        return response.json()
    except ValueError:
        return response.text


def _response_error_message(response: Response, payload: Any) -> str:
    if isinstance(payload, Mapping):
        for key in ("error", "message"):
            value = payload.get(key)
            if value:
                return str(value)
    text = response.text.strip()
    return text or response.reason or "unknown error"


def _without_none(values: Mapping[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}


def _to_float(value: Any, default: Optional[float]) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _to_bool(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _first_float(*values: Any) -> Optional[float]:
    for value in values:
        parsed = _to_float(value, None)
        if parsed is not None:
            return parsed
    return None


def _first_env(*keys: str) -> Optional[str]:
    for key in keys:
        value = os.getenv(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _first_mapping(values: Mapping[str, Any], *keys: str) -> Dict[str, Any]:
    for key in keys:
        value = values.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _start_after_points(entry_price: float, start_after: float) -> float:
    if start_after <= 0:
        return 0.0
    if start_after <= 1:
        return entry_price * start_after
    return start_after


def _positive_gap(value: float, fallback: float) -> float:
    if value > 0:
        return value
    return fallback if fallback > 0 else 1.0


def _now_ist() -> datetime:
    return datetime.now(IST)
