#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
==================================================
 File:        order_system.py  (mock / backtest OMS for garageforbots)
 Author:      (modified for prod-parity + prop-desk reporting)

 Goals:
 - Keep your existing CSV + JSON event log storage model + internal reporting.
 - Add feature parity with upstox_order_system (slice-aware IDs + stable schema coercion),
   BUT WITHOUT making ANY broker API calls.
 - Backtesting deterministic: timestamps derived from bar timestamps (ts) wherever passed.

 Notes:
 - This manager maintains ONE trade-row per position (not a full orderbook).
 - It stores slice-aware fields:
     * entry_order_ids: list[str]
     * sl_order_ids: list[str]
     * sl_order_qty_map: dict[order_id, qty]
 - For backward compatibility (and some older reporting/UX scripts),
   it also maintains:
     * entry_order_id (first entry slice)
     * sl_order_id (first SL slice)
==================================================
"""

from __future__ import annotations

import os
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd

# Keep your project imports
import common.constants as constants
from logger import create_logger

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("MockOrderManagerLogger")


# ----------------------------- Helpers -----------------------------

def _ensure_tz(ts: Optional[datetime]) -> datetime:
    if ts is None:
        ts = datetime.now(ist)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ist)
    return ts.astimezone(ist)


def _safe_float(x: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    try:
        missing = pd.isna(x)
        if isinstance(missing, bool) and missing:
            return default
    except Exception:
        pass
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        norm = x.strip().lower()
        if norm in {"1", "true", "yes", "y", "on"}:
            return True
        if norm in {"0", "false", "no", "n", "off", "", "none", "nan"}:
            return False
    return bool(x)


def _mkdirs_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _split_qty(total: int, n: int) -> List[int]:
    """Split total into n parts (nearly equal, last gets remainder)."""
    if n <= 0:
        return []
    base = total // n
    rem = total % n
    parts = [base] * n
    for i in range(rem):
        parts[i] += 1
    return parts


def _new_mock_order_id(prefix: str) -> str:
    # Upstox-like id surrogate
    return f"{prefix}{uuid.uuid4().hex[:12]}"


# ----------------------------- CSV schema + dtype coercion -----------------------------

# Keep the columns used by your internal reporting unchanged, but add slice-aware parity fields.
ORDER_COLUMNS = [
    # internal trade id
    "id",

    # identity
    "symbol",
    "instrument_token",
    "side",
    "qty",
    "product",
    "validity",

    # slice-aware ids (NEW)
    "entry_order_ids",
    "sl_order_ids",
    "sl_order_qty_map",

    # backward-compat ids (kept)
    "entry_order_id",
    "sl_order_id",
    "target_order_id",

    # price & risk
    "entry_price",
    "target",
    "stoploss",
    "_sl_limit",
    "tsl_active",
    "_trail_points",
    "start_trail_after",
    "entry_spot",
    "spot_ltp",
    "_spot_trail_anchor",

    # tracking
    "max_price",
    "min_price",
    "status",
    "timestamp",

    # exit info
    "exit_price",
    "pnl",
    "exit_time",

    # tags/metadata
    "tag_entry",
    "tag_sl",
    "description",
]

JSON_COLUMNS = ["entry_order_ids", "sl_order_ids", "sl_order_qty_map"]

NUMERIC_COLUMNS = [
    "qty",
    "entry_price",
    "target",
    "stoploss",
    "_sl_limit",
    "_trail_points",
    "start_trail_after",
    "max_price",
    "min_price",
    "exit_price",
    "pnl",
]

BOOL_COLUMNS = ["tsl_active"]


def _normalize_json_cell(v: Any) -> Any:
    """Store list/dict consistently in CSV (string)."""
    if isinstance(v, (list, dict)):
        try:
            return json.dumps(v)
        except Exception:
            return str(v)
    return v


def _coerce_orders_df(df: pd.DataFrame) -> pd.DataFrame:
    """Force stable dtypes so updates never throw incompatible dtype errors."""
    df = df.copy()

    # ensure all known columns exist
    for c in ORDER_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # json-ish columns must be object
    for c in JSON_COLUMNS:
        df[c] = df[c].astype("object")

    # booleans
    for c in BOOL_COLUMNS:
        df[c] = df[c].apply(lambda x: _safe_bool(x, False)).astype("bool")

    # numerics
    for c in NUMERIC_COLUMNS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # everything else: keep as object to safely hold strings like order IDs
    for c in df.columns:
        if c not in set(NUMERIC_COLUMNS + BOOL_COLUMNS):
            df[c] = df[c].astype("object")

    return df[ORDER_COLUMNS].copy()


def _empty_orders_df() -> pd.DataFrame:
    df = pd.DataFrame({c: pd.Series(dtype="object") for c in ORDER_COLUMNS})
    for c in NUMERIC_COLUMNS:
        df[c] = pd.Series(dtype="float64")
    for c in BOOL_COLUMNS:
        df[c] = pd.Series(dtype="bool")
    return df


# ----------------------------- Main OMS -----------------------------

class MockOrderManager:
    """
    Event-sourced-ish local OMS:
      - orders_csv: row per trade (internal trade_id)
      - events_json_path: JSON file with {"events": [ ... ]}
      - daily_csv: daily pnl + daily winrate + equity curve

    IMPORTANT:
      - NO Broker API calls here (purely local).
    """

    def __init__(
        self,
        tsl_buffer: float = 5.0,
        orders_csv: str = constants.ORDER_LOG,
        daily_csv: str = constants.DAILY_PNL,
        initial_cash: float = 100000.0,
        events_json_path: str = constants.ORDER_EVENT_LOG,
        intrabar_policy: str = "WORST_CASE",  # WORST_CASE/BEST_CASE/SL_FIRST/TP_FIRST
    ):
        self.tsl_buffer = float(tsl_buffer)
        self.orders_csv = orders_csv
        self.daily_csv = daily_csv
        self.initial_cash = float(initial_cash)
        self.cash = float(initial_cash)
        self.events_json_path = events_json_path
        self.intrabar_policy = (intrabar_policy or "WORST_CASE").upper()

        # in-memory mirrors
        self.orders: List[Dict[str, Any]] = []
        self.positions: Dict[str, int] = {}
        self.strategy_parameters: Dict[str, Any] = {}

        self._init_event_log()
        self._init_csvs()
        self._load_orders_into_memory()
        self._restore_cash_from_daily()

        logger.info(
            f"[INIT] initial_cash={self.initial_cash:.2f} cash={self.cash:.2f} "
            f"orders_csv={self.orders_csv} daily_csv={self.daily_csv} intrabar_policy={self.intrabar_policy}"
        )

    def set_strategy_params(self,strategy_parameters: dict):
        self.strategy_parameters = strategy_parameters or {}

    # ----------------------------- Init / restore -----------------------------

    def _init_event_log(self) -> None:
        _mkdirs_for_file(self.events_json_path)
        if not os.path.exists(self.events_json_path):
            with open(self.events_json_path, "w", encoding="utf-8") as f:
                json.dump({"events": []}, f, indent=2)

    def _init_csvs(self) -> None:
        _mkdirs_for_file(self.orders_csv)
        _mkdirs_for_file(self.daily_csv)

        if not os.path.exists(self.orders_csv):
            _empty_orders_df().to_csv(self.orders_csv, index=False)

        if not os.path.exists(self.daily_csv):
            pd.DataFrame(
                columns=[
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
            ).to_csv(self.daily_csv, index=False)

    def _restore_cash_from_daily(self) -> None:
        try:
            ddf = pd.read_csv(self.daily_csv)
            if not ddf.empty and "daily_pnl" in ddf.columns:
                total_realized = float(pd.to_numeric(ddf["daily_pnl"], errors="coerce").fillna(0.0).sum())
                self.cash = self.initial_cash + total_realized
        except Exception as e:
            logger.error(f"[RESTORE] Failed reading daily_csv: {e}")

    # ----------------------------- Event logging -----------------------------

    def _load_events(self) -> Dict[str, Any]:
        try:
            with open(self.events_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "events" not in data or not isinstance(data["events"], list):
                return {"events": []}
            return data
        except Exception:
            return {"events": []}

    def _save_events(self, data: Dict[str, Any]) -> None:
        with open(self.events_json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _log_event(
        self,
        event_type: str,
        trade: Dict[str, Any],
        ts: Optional[datetime] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        ts = _ensure_tz(ts)
        evt = {
            "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "trade_id": trade.get("id"),
            "symbol": trade.get("symbol"),
            "instrument_token": trade.get("instrument_token"),
            "side": trade.get("side"),
            "qty": trade.get("qty"),
            "status": trade.get("status"),

            # keep both forms for easy debugging
            "entry_order_ids": trade.get("entry_order_ids"),
            "sl_order_ids": trade.get("sl_order_ids"),
            "entry_order_id": trade.get("entry_order_id"),
            "sl_order_id": trade.get("sl_order_id"),
            "target_order_id": trade.get("target_order_id"),
        }
        if extra:
            evt.update(extra)

        try:
            data = self._load_events()
            data["events"].append(evt)
            self._save_events(data)
        except Exception as e:
            logger.error(f"[EVENT_LOG_ERROR] {e}")

    # ----------------------------- CSV helpers -----------------------------

    def _read_orders_df(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.orders_csv)
            return _coerce_orders_df(df)
        except Exception:
            return _empty_orders_df()

    def _write_orders_df(self, df: pd.DataFrame) -> None:
        df = df.copy()
        # json columns store as strings
        for col in JSON_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        df.to_csv(self.orders_csv, index=False)

    def _load_orders_into_memory(self) -> None:
        df = self._read_orders_df()
        df = _coerce_orders_df(df)

        orders: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            d = row.to_dict()

            # restore json columns
            for col in JSON_COLUMNS:
                v = d.get(col)
                if isinstance(v, str) and v.strip().startswith(("[", "{")):
                    try:
                        d[col] = json.loads(v)
                    except Exception:
                        pass

            # backward compat derivation
            if not d.get("entry_order_id") and isinstance(d.get("entry_order_ids"), list) and d["entry_order_ids"]:
                d["entry_order_id"] = str(d["entry_order_ids"][0])
            if not d.get("sl_order_id") and isinstance(d.get("sl_order_ids"), list) and d["sl_order_ids"]:
                d["sl_order_id"] = str(d["sl_order_ids"][0])

            orders.append(d)

        self.orders = orders

        # rebuild positions from OPEN trades
        self.positions = {}
        for t in self.orders:
            if str(t.get("status", "")).upper() == "OPEN":
                sym = str(t.get("symbol", ""))
                qty = _safe_int(t.get("qty"), 0)
                side = str(t.get("side", "")).upper()
                if sym:
                    self.positions[sym] = self.positions.get(sym, 0) + (qty if side == "BUY" else -qty)

    def _upsert_trade_row(self, trade: Dict[str, Any]) -> None:
        df = self._read_orders_df()
        df = _coerce_orders_df(df)

        tid = str(trade.get("id"))
        mask = df["id"].astype(str) == tid

        # ensure backward compat ids always exist
        entry_ids = trade.get("entry_order_ids") or []
        sl_ids = trade.get("sl_order_ids") or []
        if isinstance(entry_ids, str):
            try:
                entry_ids = json.loads(entry_ids)
            except Exception:
                entry_ids = [entry_ids]
        if isinstance(sl_ids, str):
            try:
                sl_ids = json.loads(sl_ids)
            except Exception:
                sl_ids = [sl_ids]

        trade["entry_order_ids"] = list(entry_ids) if isinstance(entry_ids, list) else []
        trade["sl_order_ids"] = list(sl_ids) if isinstance(sl_ids, list) else []
        trade["entry_order_id"] = str(trade["entry_order_ids"][0]) if trade["entry_order_ids"] else trade.get("entry_order_id")
        trade["sl_order_id"] = str(trade["sl_order_ids"][0]) if trade["sl_order_ids"] else trade.get("sl_order_id")

        if mask.any():
            for k, v in trade.items():
                if k not in df.columns:
                    continue

                if k in JSON_COLUMNS:
                    df[k] = df[k].astype("object")
                    df.loc[mask, k] = _normalize_json_cell(v)
                elif k in NUMERIC_COLUMNS:
                    df.loc[mask, k] = _safe_float(v, None)  # type: ignore
                elif k in BOOL_COLUMNS:
                    df.loc[mask, k] = _safe_bool(v, False)
                else:
                    df.loc[mask, k] = v
        else:
            new_row = {c: None for c in ORDER_COLUMNS}
            for c in ORDER_COLUMNS:
                if c in JSON_COLUMNS:
                    new_row[c] = _normalize_json_cell(trade.get(c))
                else:
                    new_row[c] = trade.get(c)
            df.loc[len(df)] = new_row

        df = _coerce_orders_df(df)
        self._write_orders_df(df)

    # ----------------------------- Query helpers -----------------------------

    def get_open_trade(self, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        for o in self.orders:
            if str(o.get("status", "")).upper() == "OPEN" and (symbol is None or o.get("symbol") == symbol):
                return o
        return None

    def get_trade_by_id(self, trade_id: str) -> Optional[Dict[str, Any]]:
        for o in self.orders:
            if str(o.get("id")) == str(trade_id):
                return o

        # fallback to csv (in case)
        df = self._read_orders_df()
        hit = df[df["id"].astype(str) == str(trade_id)]
        if hit.empty:
            return None
        d = hit.iloc[0].to_dict()
        for col in JSON_COLUMNS:
            v = d.get(col)
            if isinstance(v, str) and v.strip().startswith(("[", "{")):
                try:
                    d[col] = json.loads(v)
                except Exception:
                    pass
        return d

    def get_positions(self) -> Dict[str, int]:
        return dict(self.positions)

    def get_orderbook(self) -> pd.DataFrame:
        return pd.DataFrame(self.orders)

    # ----------------------------- PROD-PARITY FLOW METHODS (LOCAL ONLY) -----------------------------

    def place_market_entry(
        self,
        *,
        symbol: str,
        instrument_token: str,
        side: str,  # BUY or SELL
        qty: int,
        entry_price: float,
        product: str = "D",
        validity: str = "DAY",
        tag_entry: str = "entry",
        description: str = "",
        target: Optional[float] = None,
        stoploss_trigger: Optional[float] = None,
        stoploss_limit: Optional[float] = None,
        ts: Optional[datetime] = None,
        entry_slices: int = 1,
    ) -> str:
        """
        Local equivalent of Upstox MARKET entry.
        - Generates mock entry_order_ids (slice aware)
        - Updates local position immediately (market fill assumption)
        """
        ts = _ensure_tz(ts)
        side = (side or "").upper()
        if side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")

        # forbid multiple open positions per symbol (simple & safe)
        if self.get_open_trade(symbol=symbol) is not None:
            logger.warning(f"[SKIP] Open trade exists for {symbol}. Not opening another.")
            return str(self.get_open_trade(symbol=symbol)["id"])

        qty = int(qty)
        if qty <= 0:
            raise ValueError("qty must be > 0")

        entry_price_f = _safe_float(entry_price, None)
        if entry_price_f is None:
            raise ValueError("entry_price must be numeric")

        trade_id = str(uuid.uuid4())[:8]

        # slice-aware entry ids (local)
        entry_slices = max(int(entry_slices), 1)
        entry_ids = [_new_mock_order_id("MOCKE") for _ in range(entry_slices)]

        trade = {
            "id": trade_id,
            "symbol": symbol,
            "instrument_token": instrument_token,
            "side": side,
            "qty": qty,
            "product": product,
            "validity": validity,

            # slice aware
            "entry_order_ids": entry_ids,
            "sl_order_ids": [],
            "sl_order_qty_map": {},

            # backward compat
            "entry_order_id": str(entry_ids[0]) if entry_ids else None,
            "sl_order_id": None,
            "target_order_id": None,

            "entry_price": float(entry_price_f),
            "target": float(target) if target is not None else None,
            "stoploss": float(stoploss_trigger) if stoploss_trigger is not None else None,
            "_sl_limit": float(stoploss_limit) if stoploss_limit is not None else None,
            "tsl_active": False,
            "_trail_points": None,
            "start_trail_after": None,
            "entry_spot": float(entry_price),
            "spot_ltp": float(entry_price),
            "_spot_trail_anchor": None,
            "status": "OPEN",
            "max_price": float(entry_price_f) if side == "BUY" else None,
            "min_price": float(entry_price_f) if side == "SELL" else None,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),

            "exit_price": None,
            "pnl": None,
            "exit_time": None,

            "tag_entry": tag_entry,
            "tag_sl": "my_stop_loss",
            "description": description,
        }

        # update positions immediately
        self.positions[symbol] = self.positions.get(symbol, 0) + (qty if side == "BUY" else -qty)

        self.orders.append(trade)
        self._upsert_trade_row(trade)

        self._log_event(
            "PLACE_ENTRY",
            trade,
            ts=ts,
            extra={
                "endpoint": "LOCAL",
                "order_type": "MARKET",
                "transaction_type": side,
                "entry_order_ids": entry_ids,
                "tag": tag_entry,
                "entry_price": entry_price
            },
        )

        return trade_id

    def place_stoploss_order(
        self,
        *,
        trade_id: str,
        trigger_price: float,
        limit_price: float,
        tag_sl: str = "my_stop_loss",
        ts: Optional[datetime] = None,
        sl_slices: int = 1,
    ) -> List[str]:
        """
        Local equivalent of Upstox SL order placement (slice aware).
        - Generates mock sl_order_ids (count=sl_slices)
        - Builds sl_order_qty_map based on qty split
        """
        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t:
            raise ValueError(f"trade_id not found: {trade_id}")
        if str(t.get("status", "")).upper() != "OPEN":
            return []

        trig = _safe_float(trigger_price, None)
        lim = _safe_float(limit_price, None)
        if trig is None or lim is None:
            raise ValueError("trigger_price/limit_price must be numeric")

        sl_slices = max(int(sl_slices), 1)
        sl_ids = [_new_mock_order_id("MOCKS") for _ in range(sl_slices)]

        parts = _split_qty(_safe_int(t.get("qty"), 0), len(sl_ids))
        qty_map: Dict[str, int] = {}
        for oid, q in zip(sl_ids, parts):
            if q > 0:
                qty_map[str(oid)] = int(q)

        t["sl_order_ids"] = list(sl_ids)
        t["sl_order_qty_map"] = qty_map

        # backward compat
        t["sl_order_id"] = str(sl_ids[0]) if sl_ids else None

        t["stoploss"] = float(trig)
        t["_sl_limit"] = float(lim)
        t["tag_sl"] = tag_sl

        self._upsert_trade_row(t)

        entry_side = str(t.get("side", "")).upper()
        opp_side = "SELL" if entry_side == "BUY" else "BUY"

        self._log_event(
            "PLACE_SL",
            t,
            ts=ts,
            extra={
                "endpoint": "LOCAL",
                "order_type": "SL",
                "transaction_type": opp_side,
                "trigger_price": float(trig),
                "price": float(lim),
                "sl_order_ids": list(sl_ids),
                "sl_order_qty_map": qty_map,
                "tag": tag_sl,
            },
        )

        # keep memory object consistent
        self._replace_trade_in_memory(t)

        return list(sl_ids)

    def modify_sl_order(
        self,
        *,
        trade_id: str,
        ltp_now: Optional[float] = None,
        new_trigger: float,
        new_limit: float,
        ts: Optional[datetime] = None,
    ) -> bool:
        """
        Local equivalent of Upstox SL modify (slice aware).
        - Updates stoploss/_sl_limit and logs MODIFY_SL.
        - No broker sync (purely local).
        """
        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t or str(t.get("status", "")).upper() != "OPEN":
            return False

        trig = _safe_float(new_trigger, None)
        lim = _safe_float(new_limit, None)
        if trig is None or lim is None:
            return False

        t["stoploss"] = float(trig)
        t["_sl_limit"] = float(lim)
        self._upsert_trade_row(t)
        self._replace_trade_in_memory(t)

        self._log_event(
            "MODIFY_SL",
            t,
            ts=ts,
            extra={
                "endpoint": "LOCAL",
                "sl_order_ids": t.get("sl_order_ids") or ([] if not t.get("sl_order_id") else [t.get("sl_order_id")]),
                "trigger_price": float(trig),
                "price": float(lim),
                "ltp": float(ltp_now) if ltp_now is not None else None,
            },
        )
        return True

    def cancel_order(
        self,
        *,
        trade_id: str,
        order_kind: str = "SL",  # SL or TARGET
        ts: Optional[datetime] = None,
    ) -> bool:
        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t:
            return False

        order_kind = (order_kind or "SL").upper()
        if order_kind == "SL":
            sl_ids = t.get("sl_order_ids") or []
            if isinstance(sl_ids, str):
                try:
                    sl_ids = json.loads(sl_ids)
                except Exception:
                    sl_ids = [sl_ids]
            if not sl_ids and not t.get("sl_order_id"):
                return False

            self._log_event("CANCEL_SL", t, ts=ts, extra={"endpoint": "LOCAL", "sl_order_ids": sl_ids})
            t["sl_order_ids"] = []
            t["sl_order_qty_map"] = {}
            t["sl_order_id"] = None
            self._upsert_trade_row(t)
            self._replace_trade_in_memory(t)
            return True

        if order_kind == "TARGET":
            oid = t.get("target_order_id")
            if not oid:
                return False
            self._log_event("CANCEL_TARGET", t, ts=ts, extra={"endpoint": "LOCAL", "order_id": oid})
            t["target_order_id"] = None
            self._upsert_trade_row(t)
            self._replace_trade_in_memory(t)
            return True

        return False

    def square_off_by_modify(
        self,
        *,
        trade_id: str,
        exit_price: float,
        ts: Optional[datetime] = None,
        reason: str = "MANUAL EXIT",
    ) -> bool:
        """Local equivalent of 'modify to market' square-off."""
        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t or str(t.get("status", "")).upper() != "OPEN":
            return False

        self._log_event(
            "MODIFY_TO_MARKET",
            t,
            ts=ts,
            extra={
                "endpoint": "LOCAL",
                "order_id": t.get("entry_order_id"),
                "order_type": "MARKET",
            },
        )

        # cancel SL if any
        if (t.get("sl_order_ids") and len(t.get("sl_order_ids")) > 0) or t.get("sl_order_id"):
            self.cancel_order(trade_id=trade_id, order_kind="SL", ts=ts)

        t["status"] = reason
        self._close_trade(trade=t, exit_price=float(exit_price), ts=ts)
        return True

    # ----------------------------- Simple convenience API (engine-friendly) -----------------------------

    def buy(
        self,
        *,
        symbol: str,
        instrument_token: str,
        qty: int,
        entry_price: float,
        sl_trigger: float,
        sl_limit: float,
        target: Optional[float] = None,
        product: str = "D",
        validity: str = "DAY",
        tag_entry: str = "entry",
        tag_sl: str = "my_stop_loss",
        description: str = "",
        ts: Optional[datetime] = None,
        entry_slices: int = 1,
        sl_slices: int = 1,
    ) -> str:
        
        # Defensive casting: prevent strings like "my_stop_loss" from breaking float columns
        entry_price = _safe_float(entry_price, None)
        sl_trigger = _safe_float(sl_trigger, None)
        sl_limit = _safe_float(sl_limit, None)
        if entry_price is None or sl_trigger is None or sl_limit is None:
            raise ValueError(f"entry_price/sl_trigger/sl_limit must be numeric. got entry_price={entry_price}, sl_trigger={sl_trigger}, sl_limit={sl_limit}")

        trade_id = self.place_market_entry(
            symbol=symbol,
            instrument_token=instrument_token,
            side="BUY",
            qty=qty,
            entry_price=entry_price,
            product=product,
            validity=validity,
            tag_entry=tag_entry,
            description=description,
            target=target,
            stoploss_trigger=sl_trigger,
            stoploss_limit=sl_limit,
            ts=ts,
            entry_slices=entry_slices,
        )
        self.place_stoploss_order(
            trade_id=trade_id,
            trigger_price=sl_trigger,
            limit_price=sl_limit,
            tag_sl=tag_sl,
            ts=ts,
            sl_slices=sl_slices,
        )
        return trade_id

    def sell(
        self,
        *,
        symbol: str,
        instrument_token: str,
        qty: int,
        entry_price: float,
        sl_trigger: float,
        sl_limit: float,
        target: Optional[float] = None,
        product: str = "D",
        validity: str = "DAY",
        tag_entry: str = "entry",
        tag_sl: str = "my_stop_loss",
        description: str = "",
        ts: Optional[datetime] = None,
        entry_slices: int = 1,
        sl_slices: int = 1,
    ) -> str:
        trade_id = self.place_market_entry(
            symbol=symbol,
            instrument_token=instrument_token,
            side="SELL",
            qty=qty,
            entry_price=entry_price,
            product=product,
            validity=validity,
            tag_entry=tag_entry,
            description=description,
            target=target,
            stoploss_trigger=sl_trigger,
            stoploss_limit=sl_limit,
            ts=ts,
            entry_slices=entry_slices,
        )
        self.place_stoploss_order(
            trade_id=trade_id,
            trigger_price=sl_trigger,
            limit_price=sl_limit,
            tag_sl=tag_sl,
            ts=ts,
            sl_slices=sl_slices,
        )
        return trade_id

    def square_off_trade(
        self,
        *,
        trade_id: str,
        exit_price: float,
        ts: Optional[datetime] = None,
        reason: str = "MANUAL EXIT",
    ) -> bool:
        return self.square_off_by_modify(trade_id=trade_id, exit_price=exit_price, ts=ts, reason=reason)

    # ----------------------------- Backtest: OHLC-driven risk management -----------------------------

    def on_tick(self, symbol: str, o: float, h: float, l: float, c: float, ts: datetime) -> bool:
        """Call once per candle for this symbol and decide fixed SL/TP exits."""
        ts = _ensure_tz(ts)

        for t in list(self.orders):
            if t.get("symbol") != symbol or str(t.get("status", "")).upper() != "OPEN":
                continue

            side = str(t.get("side", "")).upper()
            entry = _safe_float(t.get("entry_price"), 0.0) or 0.0
            qty = _safe_int(t.get("qty"), 0)
            if qty <= 0:
                continue
            
            sl = t.get("stoploss")
            tgt = t.get("target")

            if side == "BUY":
                tp_hit = (tgt is not None) and (float(h) >= float(tgt))
                sl_hit = (sl is not None) and (float(l) <= float(sl))
            else:
                tp_hit = (tgt is not None) and (float(l) <= float(tgt))
                sl_hit = (sl is not None) and (float(h) >= float(sl))

            # Resolve both-hit candle ambiguity
            if tp_hit and sl_hit:
                pol = self.intrabar_policy
                if pol in ("WORST_CASE", "SL_FIRST"):
                    self._exit_by_sl(t, ts=ts)
                    continue
                else:
                    self._exit_by_target(t, ts=ts)
                    continue

            if tp_hit:
                self._exit_by_target(t, ts=ts)
                continue

            if sl_hit:
                self._exit_by_sl(t, ts=ts)
                continue
            
        return False

    def _exit_by_target(self, trade: Dict[str, Any], ts: datetime) -> None:
        trade["status"] = "TARGET HIT"
        exit_price = float(trade.get("target"))
        if trade.get("sl_order_id") or (trade.get("sl_order_ids") and len(trade.get("sl_order_ids")) > 0):
            self.cancel_order(trade_id=trade["id"], order_kind="SL", ts=ts)
        self._close_trade(trade, exit_price=exit_price, ts=ts, exit_reason="TP_HIT")

    def _exit_by_sl(self, trade: Dict[str, Any], ts: datetime) -> None:
        trade["status"] = "STOPLOSS HIT"
        exit_price = float(trade.get("stoploss"))
        self._close_trade(trade, exit_price=exit_price, ts=ts, exit_reason="SL_HIT")

    # ----------------------------- Close trade + daily accounting -----------------------------

    def _close_trade(self, trade: Dict[str, Any], exit_price: float, ts: datetime, exit_reason: str = "") -> None:
        ts = _ensure_tz(ts)

        side = str(trade.get("side", "")).upper()
        qty = _safe_int(trade.get("qty"), 0)
        entry = _safe_float(trade.get("entry_price"), 0.0) or 0.0
        exit_price = float(exit_price)

        pnl = (exit_price - entry) * qty if side == "BUY" else (entry - exit_price) * qty

        trade["exit_price"] = float(exit_price)
        trade["pnl"] = float(pnl)
        trade["exit_time"] = ts.strftime("%Y-%m-%d %H:%M:%S")

        sym = str(trade.get("symbol", ""))
        self.positions[sym] = self.positions.get(sym, 0) - (qty if side == "BUY" else -qty)

        self.cash += float(pnl)

        self._upsert_trade_row(trade)
        self._replace_trade_in_memory(trade)

        self._log_event(
            "CLOSE",
            trade,
            ts=ts,
            extra={"exit_price": float(exit_price), "pnl": float(pnl), "exit_reason": exit_reason},
        )

        self._update_daily_pnl(exit_ts=ts)

        logger.info(f"[CLOSE] {sym} {side} qty={qty} entry={entry} exit={exit_price} pnl={pnl:.2f} cash={self.cash:.2f}")

    def _update_daily_pnl(self, exit_ts: datetime) -> None:
        """
        Computes daily pnl from closed trades in orders_csv for the specific day (exit date).
        Backtest-safe: uses exit_ts, NOT datetime.now().
        """
        exit_ts = _ensure_tz(exit_ts)
        day = exit_ts.strftime("%Y-%m-%d")

        df = self._read_orders_df()
        if df.empty:
            return

        closed_statuses = ["TARGET HIT", "STOPLOSS HIT", "MANUAL EXIT", "EOD_SQUARE_OFF"]
        df_closed = df[df["status"].isin(closed_statuses)].copy()
        if df_closed.empty:
            return

        df_closed["exit_time"] = df_closed["exit_time"].astype(str)
        daily_trades = df_closed[df_closed["exit_time"].str.startswith(day)].copy()
        if daily_trades.empty:
            return

        daily_trades["pnl"] = pd.to_numeric(daily_trades["pnl"], errors="coerce").fillna(0.0)

        daily_pnl = float(daily_trades["pnl"].sum())
        num_trades = int(len(daily_trades))
        wins = int((daily_trades["pnl"] > 0).sum())
        win_rate = (wins / num_trades) * 100.0 if num_trades else 0.0

        daily_df = pd.read_csv(self.daily_csv)
        for col in ["cash", "equity", "peak_equity", "drawdown", "drawdown_pct"]:
            if col not in daily_df.columns:
                daily_df[col] = 0.0

        if day in daily_df["date"].astype(str).values:
            daily_df.loc[daily_df["date"].astype(str) == day, ["daily_pnl", "num_trades", "win_rate"]] = [
                daily_pnl, num_trades, win_rate
            ]
        else:
            daily_df = pd.concat([daily_df, pd.DataFrame([{
                "date": day,
                "daily_pnl": daily_pnl,
                "num_trades": num_trades,
                "win_rate": win_rate,
                "cash": 0.0,
                "equity": 0.0,
                "peak_equity": 0.0,
                "drawdown": 0.0,
                "drawdown_pct": 0.0,
            }])], ignore_index=True)

        daily_df["date"] = daily_df["date"].astype(str)
        daily_df = daily_df.sort_values("date").reset_index(drop=True)

        running_pnl = 0.0
        peak = self.initial_cash
        for idx, row in daily_df.iterrows():
            running_pnl += float(_safe_float(row.get("daily_pnl"), 0.0) or 0.0)
            equity = self.initial_cash + running_pnl
            peak = max(peak, equity)
            drawdown = equity - peak
            dd_pct = (drawdown / peak) if peak != 0 else 0.0

            daily_df.at[idx, "equity"] = equity
            daily_df.at[idx, "cash"] = equity
            daily_df.at[idx, "peak_equity"] = peak
            daily_df.at[idx, "drawdown"] = drawdown
            daily_df.at[idx, "drawdown_pct"] = dd_pct

        daily_df.to_csv(self.daily_csv, index=False)

        logger.info(f"[DAILY] {day} pnl={daily_pnl:.2f} trades={num_trades} win%={win_rate:.1f} equity={daily_df.iloc[-1]['equity']:.2f}")


# ----------------------------- Prop-desk cumulative reporting -----------------------------

    def _build_monthly_report(self, df_daily: pd.DataFrame) -> List[Dict[str, Any]]:
        if df_daily.empty:
            return []

        monthly_df = df_daily.copy()
        monthly_df["_date"] = pd.to_datetime(monthly_df["date"], errors="coerce")
        monthly_df = monthly_df.dropna(subset=["_date"]).sort_values("_date").reset_index(drop=True)
        if monthly_df.empty:
            return []

        for col in ["daily_pnl", "num_trades", "win_rate"]:
            if col not in monthly_df.columns:
                monthly_df[col] = 0.0
            monthly_df[col] = pd.to_numeric(monthly_df[col], errors="coerce").fillna(0.0)

        monthly_df["_month"] = monthly_df["_date"].dt.to_period("M")

        monthly_report: List[Dict[str, Any]] = []
        opening_cash = float(self.initial_cash)

        for month, month_df in monthly_df.groupby("_month", sort=True):
            monthly_pnl = float(month_df["daily_pnl"].sum())
            ending_cash = opening_cash + monthly_pnl

            total_trades = int(month_df["num_trades"].sum())
            estimated_wins = float(((month_df["win_rate"] / 100.0) * month_df["num_trades"]).sum())
            trade_win_rate = (estimated_wins / total_trades) if total_trades else 0.0

            monthly_equity = opening_cash + month_df["daily_pnl"].cumsum()
            monthly_peak = monthly_equity.cummax().clip(lower=opening_cash)
            monthly_drawdown = monthly_equity - monthly_peak
            monthly_drawdown_pct = monthly_drawdown / monthly_peak.replace(0.0, pd.NA)

            best_day = month_df.loc[month_df["daily_pnl"].idxmax()]
            worst_day = month_df.loc[month_df["daily_pnl"].idxmin()]

            monthly_report.append({
                "month": str(month),
                "month_name": month_df["_date"].iloc[0].strftime("%b %Y"),
                "start_date": month_df["_date"].iloc[0].strftime("%Y-%m-%d"),
                "end_date": month_df["_date"].iloc[-1].strftime("%Y-%m-%d"),
                "initial_cash": opening_cash,
                "ending_cash": ending_cash,
                "monthly_pnl": monthly_pnl,
                "return_pct": (monthly_pnl / opening_cash) if opening_cash else 0.0,
                "num_trading_days": int(len(month_df)),
                "green_days": int((month_df["daily_pnl"] > 0).sum()),
                "red_days": int((month_df["daily_pnl"] < 0).sum()),
                "flat_days": int((month_df["daily_pnl"] == 0).sum()),
                "total_trades": total_trades,
                "trade_win_rate": float(trade_win_rate),
                "avg_daily_pnl": float(month_df["daily_pnl"].mean()),
                "best_day": {
                    "date": best_day["_date"].strftime("%Y-%m-%d"),
                    "daily_pnl": float(best_day["daily_pnl"]),
                },
                "worst_day": {
                    "date": worst_day["_date"].strftime("%Y-%m-%d"),
                    "daily_pnl": float(worst_day["daily_pnl"]),
                },
                "max_drawdown": float(monthly_drawdown.min()) if not monthly_drawdown.empty else 0.0,
                "max_drawdown_pct": float(monthly_drawdown_pct.fillna(0.0).min()) if not monthly_drawdown_pct.empty else 0.0,
            })

            opening_cash = ending_cash

        return monthly_report

    def build_cumulative_report(self, out_json_path: str = "files/cumulative_report.json") -> Dict[str, Any]:
        """
        Prop-desk level cumulative report based on:
          - daily_csv: equity curve + dd series
          - orders_csv: trade pnl distribution
        """
        df_orders = self._read_orders_df()
        df_daily = pd.read_csv(self.daily_csv) if os.path.exists(self.daily_csv) else pd.DataFrame()

        # normalize daily
        if not df_daily.empty:
            df_daily["date"] = df_daily["date"].astype(str)
            for col in ["daily_pnl", "num_trades", "win_rate", "equity", "peak_equity", "drawdown", "drawdown_pct"]:
                if col not in df_daily.columns:
                    df_daily[col] = 0.0
                df_daily[col] = pd.to_numeric(df_daily[col], errors="coerce").fillna(0.0)
            df_daily = df_daily.sort_values("date").reset_index(drop=True)

        # trade stats
        closed_statuses = ["TARGET HIT", "STOPLOSS HIT", "MANUAL EXIT", "EOD_SQUARE_OFF"]
        if not df_orders.empty:
            df_closed = df_orders[df_orders["status"].isin(closed_statuses)].copy()
            if not df_closed.empty:
                df_closed["pnl"] = pd.to_numeric(df_closed["pnl"], errors="coerce").fillna(0.0)
            else:
                df_closed = pd.DataFrame()
        else:
            df_closed = pd.DataFrame()

        # --- portfolio summary ---
        total_net = float(df_daily["daily_pnl"].sum()) if not df_daily.empty else 0.0
        avg_daily = float(df_daily["daily_pnl"].mean()) if not df_daily.empty else 0.0
        green_days_ratio = float((df_daily["daily_pnl"] > 0).mean()) if not df_daily.empty else 0.0

        best_day = df_daily.loc[df_daily["daily_pnl"].idxmax()].to_dict() if not df_daily.empty else {}
        worst_day = df_daily.loc[df_daily["daily_pnl"].idxmin()].to_dict() if not df_daily.empty else {}

        max_dd = float(df_daily["drawdown"].min()) if not df_daily.empty else 0.0
        max_dd_pct = float(df_daily["drawdown_pct"].min()) if not df_daily.empty else 0.0
        monthly_report = self._build_monthly_report(df_daily)

        # profit factor, expectancy, avg win/loss, streak
        if not df_closed.empty:
            pnls = df_closed["pnl"].astype(float).tolist()
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            sum_win = float(sum(wins)) if wins else 0.0
            sum_loss = float(sum(losses)) if losses else 0.0  # negative or 0

            profit_factor = (sum_win / abs(sum_loss)) if sum_loss != 0 else (float("inf") if sum_win > 0 else 0.0)
            trade_win_rate = float(len(wins) / len(pnls)) if pnls else 0.0
            avg_win = float(sum_win / len(wins)) if wins else 0.0
            avg_loss = float(sum_loss / len(losses)) if losses else 0.0
            expectancy = (trade_win_rate * avg_win) + ((1 - trade_win_rate) * avg_loss)

            # max consecutive losses (trade sequence)
            max_consec_losses = 0
            cur = 0
            for p in pnls:
                if p <= 0:
                    cur += 1
                    max_consec_losses = max(max_consec_losses, cur)
                else:
                    cur = 0
        else:
            profit_factor = 0.0
            trade_win_rate = 0.0
            avg_win = 0.0
            avg_loss = 0.0
            expectancy = 0.0
            max_consec_losses = 0

        # Drawdown episode details (peak/trough/recovery)
        dd_episode = {}
        if not df_daily.empty and "drawdown" in df_daily.columns:
            trough_idx = int(df_daily["drawdown"].idxmin())
            trough_date = str(df_daily.loc[trough_idx, "date"])
            peak_equity_at_trough = float(df_daily.loc[trough_idx, "peak_equity"]) if "peak_equity" in df_daily.columns else float(df_daily["equity"].cummax().iloc[trough_idx])
            trough_equity = float(df_daily.loc[trough_idx, "equity"])

            # peak date is most recent date before trough where equity==peak_equity_at_trough
            before = df_daily.iloc[:trough_idx + 1]
            peak_candidates = before[before["equity"] == peak_equity_at_trough]
            peak_date = str(peak_candidates.iloc[-1]["date"]) if not peak_candidates.empty else str(before.iloc[0]["date"])

            # recovery date: first date after trough where equity >= peak_equity_at_trough
            after = df_daily.iloc[trough_idx + 1:]
            rec = after[after["equity"] >= peak_equity_at_trough]
            recovery_date = str(rec.iloc[0]["date"]) if not rec.empty else None

            dd_episode = {
                "peak_date": peak_date,
                "trough_date": trough_date,
                "recovery_date": recovery_date,
                "peak_equity": peak_equity_at_trough,
                "trough_equity": trough_equity,
                "max_drawdown": float(df_daily["drawdown"].min()),
                "max_drawdown_pct": float(df_daily["drawdown_pct"].min()) if "drawdown_pct" in df_daily.columns else 0.0,
            }

        report = {
            "meta": {
                "created_at": datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S"),
                "initial_cash": float(self.initial_cash),
                "current_cash": float(self.cash),
                "orders_csv": self.orders_csv,
                "daily_csv": self.daily_csv,
                "intrabar_policy": self.intrabar_policy,
                "tsl_buffer": float(self.tsl_buffer),
            },
            "summary": {
                "total_net_pnl": total_net,
                "avg_daily_pnl": avg_daily,
                "green_days_ratio": green_days_ratio,
                "best_day": best_day,
                "worst_day": worst_day,
                "max_drawdown": max_dd,
                "max_drawdown_pct": max_dd_pct,

                "total_days": int(len(df_daily)) if not df_daily.empty else 0,
                "total_trades": int(len(df_closed)) if not df_closed.empty else 0,

                "trade_win_rate": trade_win_rate,
                "profit_factor": profit_factor,
                "expectancy_per_trade": expectancy,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "max_consecutive_losses": int(max_consec_losses),
            },
            "drawdown_episode": dd_episode,
            "monthly": monthly_report,
            "daily": df_daily.to_dict(orient="records") if not df_daily.empty else [],
        }

        with open(out_json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        logger.info(f"[REPORT] Wrote cumulative report: {out_json_path}")
        return report

    # ----------------------------- Internal: memory sync helpers -----------------------------

    def _get_trade_obj_in_memory(self, trade_id: str) -> Optional[Dict[str, Any]]:
        for i, o in enumerate(self.orders):
            if o.get("id") == trade_id:
                return self.orders[i]
        return None

    def _replace_trade_in_memory(self, trade: Dict[str, Any]) -> None:
        for i, o in enumerate(self.orders):
            if o.get("id") == trade.get("id"):
                self.orders[i] = trade
                return
        # if not found, append
        self.orders.append(trade)
