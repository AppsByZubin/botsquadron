#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Local-only order manager for SoloBot mock execution mode."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from common import constants
from oms.order_system_client import (
    OrderSystemClient,
    _normalize_timestamp,
    _now_ist,
    _to_float,
)


class MockOrderSystemClient(OrderSystemClient):
    """Drop-in OrderSystemClient replacement that persists trades only to files."""

    def health(self) -> Dict[str, Any]:
        return {"status": constants.SUCCESS, "mode": self.mode, "message": "mock ordersystem is local"}

    def create_account(
        self,
        bot_name: Optional[str] = None,
        curr_date: Optional[str] = None,
        month_year: Optional[str] = None,
        init_cash: Optional[float] = None,
    ) -> Dict[str, Any]:
        account = self._local_account_response(
            bot_name=bot_name,
            curr_date=curr_date,
            month_year=month_year,
            init_cash=init_cash,
        )
        self._sync_account_state(account)
        return account

    def get_account_details(
        self,
        bot_name: Optional[str] = None,
        curr_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        account = self._local_account_response(bot_name=bot_name, curr_date=curr_date)
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

        trade_id = f"mock-{uuid.uuid4().hex[:12]}"
        entry_order_id = f"mock-entry-{uuid.uuid4().hex[:10]}"
        sl_order_id = f"mock-sl-{uuid.uuid4().hex[:10]}" if payload.get("sl_trigger") else ""
        response = {
            "trade_id": trade_id,
            "account_id": self.account_id or self._local_account_id(),
            "entry_order_ids": [entry_order_id],
            "sl_order_ids": [sl_order_id] if sl_order_id else [],
            "status": constants.OPEN,
            "message": "mock trade created locally",
        }

        local_row = self._local_row_from_create(payload, response, ts=ts)
        if sl_order_id:
            local_row["sl_order_qty_map"] = {sl_order_id: int(payload.get("qty") or 0)}

        self._last_trade_id = trade_id
        self._active_trade_by_symbol[str(symbol)] = trade_id
        self._remember_trade(local_row)
        self._upsert_local_trade(local_row)
        self._log_local_event("MOCK_PLACE_ENTRY", local_row, ts=ts, extra={"order_id": entry_order_id})
        if sl_order_id:
            self._log_local_event("MOCK_PLACE_SL", local_row, ts=ts, extra={"order_id": sl_order_id})
        return response

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
        del disclosed_quantity, mode
        updates = {
            "stoploss": _to_float(stoploss, None),
            "sl_limit": _to_float(sl_limit, None),
            "_sl_limit": _to_float(sl_limit, None),
            "spot_trail_anchor": _to_float(spot_trail_anchor, None),
            "_spot_trail_anchor": _to_float(spot_trail_anchor, None),
        }
        trade = self._patch_cached_trade(trade_id, updates)
        self._patch_local_trade(trade_id, updates)
        trade = self._get_cached_trade(trade_id) or trade or {"id": trade_id}
        self._log_local_event(
            "MOCK_MODIFY_TRADE",
            trade,
            extra={
                "order_type": str(order_type or constants.SL).upper(),
                "validity": str(validity or self.validity).upper(),
            },
        )
        return {
            "trade_id": trade_id,
            "modified_order_ids": trade.get("sl_order_ids") or [],
            "message": "mock trade modified locally",
        }

    def get_trade_by_id(self, trade_id: str) -> Dict[str, Any]:
        trade = self._get_cached_trade(trade_id) or self._read_local_trade(trade_id)
        if not trade:
            raise KeyError(f"mock trade not found: {trade_id}")
        return self._remember_trade(trade)

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
        resolved_trade_id = str(
            trade_id
            or self._active_trade_by_symbol.get(str(symbol))
            or self._last_trade_id
            or ""
        ).strip()
        if not resolved_trade_id:
            return None

        trade = self._get_cached_trade(resolved_trade_id)
        if not trade:
            return None

        status = str(trade.get("status") or "").strip().upper()
        if status in self.CLOSED_STATUSES:
            self._forget_trade(trade)
            return trade
        if status and status != constants.OPEN:
            return trade

        exit_signal = self._local_exit_signal(trade, o=o, h=h, l=l, c=c, include_target=True)
        if exit_signal:
            return self.square_off_trade(
                trade_id=resolved_trade_id,
                exit_price=exit_signal.get("exit_price"),
                ts=ts,
                reason=exit_signal.get("reason"),
            )

        return super().on_tick(symbol=symbol, o=o, h=h, l=l, c=c, ts=ts, trade_id=resolved_trade_id)

    def square_off_trade(
        self,
        trade_id: str,
        exit_price: Optional[float] = None,
        ts: Optional[datetime] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        trade = self._get_cached_trade(trade_id) or self._read_local_trade(trade_id)
        if not trade:
            raise KeyError(f"mock trade not found: {trade_id}")

        status = str(trade.get("status") or "").strip().upper()
        if status in self.CLOSED_STATUSES:
            return self._remember_trade(trade)

        exit_px = _to_float(exit_price, None)
        if exit_px is None or exit_px <= 0:
            exit_px = _to_float(trade.get("stoploss"), None) or _to_float(trade.get("entry_price"), 0.0) or 0.0

        updates = {
            "status": reason or constants.MANUAL_EXIT,
            "exit_price": exit_px,
            "exit_time": _normalize_timestamp(ts) or _now_ist().isoformat(),
        }
        self._patch_cached_trade(trade_id, updates)
        self._patch_local_trade(trade_id, updates)
        closed_trade = self._read_local_trade(trade_id) or self._get_cached_trade(trade_id) or {"id": trade_id}
        closed_trade = self._remember_trade(closed_trade)
        self._log_local_event("MOCK_CLOSE_TRADE", closed_trade, ts=ts)
        return closed_trade

    def _local_account_response(
        self,
        bot_name: Optional[str] = None,
        curr_date: Optional[str] = None,
        month_year: Optional[str] = None,
        init_cash: Optional[float] = None,
    ) -> Dict[str, Any]:
        if bot_name:
            self.bot_name = str(bot_name).strip()
        if curr_date:
            self.curr_date = str(curr_date).strip()
        if month_year:
            self.month_year = str(month_year).strip()
        if init_cash is not None:
            self.initial_cash = float(init_cash)

        account_id = self.account_id or self._local_account_id()
        return {
            "account_id": account_id,
            "bot_name": self.bot_name,
            "curr_date": self.curr_date,
            "month_year": self.month_year,
            "init_cash": self.initial_cash,
            "net_profit": self._local_net_profit(),
            "trades": self._read_local_rows(),
            "message": "mock account loaded locally",
        }

    def _local_account_id(self) -> str:
        key = f"{self.bot_name}-{self.mode}-{self.curr_date}"
        normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in key).strip("-")
        return f"mock-{normalized or 'haemabot'}"

    def _local_net_profit(self) -> float:
        pnl = 0.0
        for row in self._read_local_rows():
            status = str(row.get("status") or "").strip().upper()
            if status in self.CLOSED_STATUSES:
                pnl += _to_float(row.get("pnl"), 0.0) or 0.0
        return round(float(pnl), 2)
