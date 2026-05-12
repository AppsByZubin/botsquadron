#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File: sandbox_order_system.py

Purpose:
- A "sandbox" order-manager that still uses Upstox V3 order placement/modification (slice aware),
  but maintains a local trade ledger (CSV + JSON events) used by your strategy engine.

Key fixes vs your current file:
1) Removed accidental debugging early-return + hardcoded trade_id in place_stoploss_order.
2) Slice-aware tracking:
   - entry_order_ids: list[str]
   - sl_order_ids: list[str]
   - sl_order_qty_map: {order_id: qty_for_that_slice}
3) Broker-side monitoring before trailing/modify:
   - refresh_trade_status() checks Upstox trades for each SL order_id (get_trades_by_order)
   - if SL executed, closes locally and prevents further trailing API calls.
4) Pandas FutureWarnings removed by using a fixed schema + concat-free row appends.

NOTE:
- This module expects you to pass a valid UpstoxHelper instance as upstox_helper.
- It does NOT subscribe to websockets; it only places/modifies orders and records trades.
"""

from __future__ import annotations
import math
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
import random

import pandas as pd

import common.constants as constants
from logger import create_logger

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("UpstoxOrderManagerLogger")


# ----------------------------- utilities -----------------------------

def _ensure_tz(ts: Optional[datetime]) -> datetime:
    if ts is None:
        return datetime.now(ist)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ist)
    return ts.astimezone(ist)


def _safe_float(x: Any, default: float = 0.0) -> float:
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


def _split_qty(total: int, n: int) -> List[int]:
    """Split total into n parts (nearly equal, last gets remainder)."""
    if n <= 0:
        return []
    base = total // n
    rem = total % n
    parts = [base] * n
    for i in range(rem):
        parts[i] += 1
    # if base is 0 and total < n, some parts will be 0; filter those by caller if needed
    return parts


def _mkdirs_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _assert_persistence_path_ready(path: str, label: str) -> None:
    """Fail fast if a persistence file cannot be read or replaced atomically."""
    _mkdirs_for_file(path)
    directory = os.path.dirname(path) or "."

    try:
        fd, probe_path = tempfile.mkstemp(
            prefix=f".{Path(path).name}.permcheck.",
            dir=directory,
        )
        os.close(fd)
        os.remove(probe_path)
    except Exception as e:
        raise PermissionError(
            f"{label} parent directory is not writable: {directory} ({e})"
        ) from e

    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                f.read(1)
        except Exception as e:
            raise PermissionError(
                f"{label} is not readable: {path} ({e})"
            ) from e


def _atomic_write_dataframe(df: pd.DataFrame, path: str, *, index: bool = False) -> None:
    """Write a dataframe via a temp file so directory permissions govern replacement."""
    _mkdirs_for_file(path)
    directory = os.path.dirname(path) or "."
    tmp_path = ""
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{Path(path).name}.",
            suffix=".tmp",
            dir=directory,
        )
        os.close(fd)
        df.to_csv(tmp_path, index=index)
        os.chmod(tmp_path, 0o664)
        os.replace(tmp_path, path)
    except PermissionError as e:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise PermissionError(f"Cannot write {path}: {e}") from e
    except Exception:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise


def _atomic_write_json(path: str, payload: Any) -> None:
    """Write JSON atomically so a stale file mode does not block updates."""
    _mkdirs_for_file(path)
    directory = os.path.dirname(path) or "."
    tmp_path = ""
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{Path(path).name}.",
            suffix=".tmp",
            dir=directory,
        )
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.chmod(tmp_path, 0o664)
        os.replace(tmp_path, path)
    except PermissionError as e:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise PermissionError(f"Cannot write {path}: {e}") from e
    except Exception:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise


def _monthly_file_path(path: str, now: Optional[datetime] = None) -> str:
    """Return a path for a monthly rolling file alongside `path`.

    E.g., if path is .../order_log.csv and now is 2026-03-18, this returns
    .../order_log_2026-03.csv.
    """
    now = now or datetime.now(ist)
    p = Path(path)
    stem = p.stem
    suffix = p.suffix
    month_str = now.strftime("%Y-%m")
    new_name = f"{stem}_{month_str}{suffix}"
    return str(p.with_name(new_name))


# ----------------------------- schema -----------------------------

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

# ----------------------------- column typing helpers -----------------------------

JSON_COLUMNS = ["entry_order_ids", "sl_order_ids", "sl_order_qty_map"]

NUMERIC_COLUMNS = [
    "qty", "entry_price", "target", "stoploss", "_sl_limit", 
    "start_trail_after",
    "entry_spot",
    "spot_ltp",
    "_spot_trail_anchor",
    "_trail_points", "exit_price", "pnl",
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
    """Force stable dtypes so updates don't throw incompatible dtype warnings."""
    df = df.copy()

    # ensure all known columns exist
    for c in ORDER_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # json-ish columns must be object
    for c in JSON_COLUMNS:
        if c in df.columns:
            df[c] = df[c].astype("object")

    # booleans
    for c in BOOL_COLUMNS:
        if c in df.columns:
            # pandas may infer object/float; normalize to bool with NaN -> False
            df[c] = df[c].apply(lambda x: bool(x) if pd.notna(x) else False).astype("bool")

    # numerics
    for c in NUMERIC_COLUMNS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # everything else: keep as object to safely hold strings like order IDs
    for c in df.columns:
        if c not in set(NUMERIC_COLUMNS + BOOL_COLUMNS):
            df[c] = df[c].astype("object")

    return df[ORDER_COLUMNS].copy()


def _empty_orders_df() -> pd.DataFrame:
    df = pd.DataFrame({c: pd.Series(dtype="object") for c in ORDER_COLUMNS})
    # cast numeric columns
    for c in ["qty", "entry_price", "target", "stoploss", "_sl_limit", "start_trail_after", "entry_spot", "spot_ltp", "_spot_trail_anchor", "_trail_points", "exit_price", "pnl"]:
        df[c] = pd.Series(dtype="float64")
    for c in ["tsl_active"]:
        df[c] = pd.Series(dtype="bool")
    return df


# ----------------------------- manager -----------------------------

class UpstoxOrderManager:
    def __init__(
        self,
        upstox_helper,
        strategy_parameters: dict = None,
        tsl_buffer: float = 1.0,
        orders_csv: str = constants.ORDER_SANDBOX_LOG,
        daily_csv: str = constants.DAILY_SANDBOX_PNL,
        initial_cash: float = 200000.0,
        events_json_path: str = constants.ORDER_SANDBOX_EVENT_LOG,
        intrabar_policy: str = "WORST_CASE",
    ):
        self.upstox_helper = upstox_helper
        self.tsl_buffer = float(tsl_buffer)
        self.orders_csv = orders_csv
        self.monthly_orders_csv = _monthly_file_path(orders_csv)
        self.daily_csv = daily_csv
        self.initial_cash = float(initial_cash)
        self.cash = float(initial_cash)
        self.events_json_path = events_json_path
        self.intrabar_policy = (intrabar_policy or "WORST_CASE").upper()
        self.strategy_parameters = strategy_parameters or {}
        self.orders: List[Dict[str, Any]] = []
        self.positions: Dict[str, int] = {}

        self._init_csvs()
        self._load_orders_into_memory()
        self._load_state_from_daily_csv()

        self._order_details_cache = {}        # order_id -> (ts_epoch, response_dict)
        self._order_details_next_ok = {}      # order_id -> next_allowed_epoch
        self._order_details_backoff = {}      # order_id -> backoff_seconds

        logger.info(
            f"[INIT] initial_cash={self.initial_cash:.2f} cash={self.cash:.2f} "
            f"orders_csv={self.orders_csv} daily_csv={self.daily_csv}"
        )

    # ----------------------------- persistence -----------------------------

    def _init_csvs(self) -> None:
        _mkdirs_for_file(self.orders_csv)
        _mkdirs_for_file(self.daily_csv)
        _mkdirs_for_file(self.events_json_path)

        if not os.path.exists(self.orders_csv):
            _atomic_write_dataframe(_empty_orders_df(), self.orders_csv)

        # Maintain a monthly rolling order log alongside the main log so we can
        # generate cumulative reports across restarts/deploys (even if the
        # main file gets reset by the runtime environment).
        if not os.path.exists(self.monthly_orders_csv):
            _atomic_write_dataframe(_empty_orders_df(), self.monthly_orders_csv)

        if not os.path.exists(self.daily_csv):
            _atomic_write_dataframe(pd.DataFrame(
                [{"date": datetime.now(ist).date().isoformat(), "initial_cash": self.initial_cash, "cash": self.cash, "pnl": 0.0}]
            ), self.daily_csv)

        if not os.path.exists(self.events_json_path):
            _atomic_write_json(self.events_json_path, [])

        _assert_persistence_path_ready(self.orders_csv, "orders_csv")
        _assert_persistence_path_ready(self.monthly_orders_csv, "monthly_orders_csv")
        _assert_persistence_path_ready(self.daily_csv, "daily_csv")
        _assert_persistence_path_ready(self.events_json_path, "events_json_path")

    def _load_state_from_daily_csv(self) -> None:
        """Restore cash/initial_cash from existing daily PnL if it exists.

        This allows the bot to be restarted mid-month without resetting cash back
        to the hardcoded initial_cash.
        """
        try:
            if not os.path.exists(self.daily_csv):
                return

            df = pd.read_csv(self.daily_csv)
            if df.empty or "date" not in df.columns:
                return

            # Restore only from current month rows so a new month can start with
            # fresh capital from constructor/config defaults.
            parsed_dates = pd.to_datetime(df["date"], errors="coerce")
            if parsed_dates.dropna().empty:
                return

            today = datetime.now(ist).date()
            month_mask = (parsed_dates.dt.year == today.year) & (parsed_dates.dt.month == today.month)
            if not month_mask.any():
                return

            month_df = df.loc[month_mask]

            if "initial_cash" in month_df.columns and not month_df["initial_cash"].dropna().empty:
                self.initial_cash = float(month_df["initial_cash"].dropna().iloc[0])

            if "cash" in month_df.columns and not month_df["cash"].dropna().empty:
                self.cash = float(month_df["cash"].dropna().iloc[-1])
        except Exception:
            # Best effort restore; do not prevent bot from starting.
            pass

    def _read_orders_df(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.orders_csv)
            # ensure missing columns exist
            for c in ORDER_COLUMNS:
                if c not in df.columns:
                    df[c] = None
            return _coerce_orders_df(df)
        except Exception:
            return _empty_orders_df()

    def _write_orders_df(self, df: pd.DataFrame) -> None:
        """Persist the full orders dataframe (safely overwrite)."""
        df = df.copy()
        for col in ["entry_order_ids", "sl_order_ids", "sl_order_qty_map"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        try:
            _atomic_write_dataframe(df, self.orders_csv)
        except Exception as e:
            logger.exception(f"_write_orders_df failed: {e}")

    def _append_trade_to_monthly_log(self, trade: Dict[str, Any]) -> None:
        """Append a newly-inserted trade to the monthly rolling log."""
        try:
            try:
                monthly_df = pd.read_csv(self.monthly_orders_csv)
            except Exception:
                monthly_df = _empty_orders_df()

            monthly_df = _coerce_orders_df(monthly_df)
            row_df = _coerce_orders_df(pd.DataFrame([trade]))
            monthly_df.loc[len(monthly_df)] = row_df.iloc[0]
            _atomic_write_dataframe(monthly_df, self.monthly_orders_csv)
        except Exception:
            # Best effort; do not block normal order processing.
            pass

    def _load_orders_into_memory(self) -> None:
        df = self._read_orders_df()
        df = _coerce_orders_df(df)
        orders: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            d = row.to_dict()
            # restore json columns
            for col in ["entry_order_ids", "sl_order_ids", "sl_order_qty_map"]:
                v = d.get(col)
                if isinstance(v, str) and v.strip().startswith(("[", "{")):
                    try:
                        d[col] = json.loads(v)
                    except Exception:
                        pass
            orders.append(d)
        self.orders = orders

        # rebuild positions from OPEN trades
        self.positions = {}
        for t in self.orders:
            if str(t.get("status", "")).upper() == "OPEN":
                sym = t.get("symbol")
                qty = _safe_int(t.get("qty"), 0)
                side = str(t.get("side", "")).upper()
                if sym:
                    self.positions[sym] = self.positions.get(sym, 0) + (qty if side == "BUY" else -qty)

    def _upsert_trade_row(self, trade: Dict[str, Any]) -> None:
        df = self._read_orders_df()
        df = _coerce_orders_df(df)
        tid = trade.get("id")
        if tid is None:
            return

        mask = df["id"].astype(str) == str(tid)
        is_new_trade = False
        if mask.any():
            for k, v in trade.items():
                if k not in df.columns:
                    df[k] = None

                # keep df dtypes stable across runs
                if k in JSON_COLUMNS:
                    df[k] = df[k].astype("object")
                    df.loc[mask, k] = _normalize_json_cell(v)
                elif k in NUMERIC_COLUMNS:
                    df.loc[mask, k] = _safe_float(v, None)  # type: ignore
                elif k in BOOL_COLUMNS:
                    df.loc[mask, k] = bool(v) if v is not None else False
                else:
                    # ids / tags / status etc should remain strings/objects
                    df.loc[mask, k] = v

        else:
            # append without concat warning
            new_row = {c: (_normalize_json_cell(trade.get(c, None)) if c in JSON_COLUMNS else trade.get(c, None)) for c in ORDER_COLUMNS}
            df.loc[len(df)] = new_row
            is_new_trade = True

        df = _coerce_orders_df(df)
        self._write_orders_df(df)

        if is_new_trade:
            self._append_trade_to_monthly_log(trade)

    def _log_event(self, event: str, trade: Dict[str, Any], ts: Optional[datetime] = None, extra: Optional[Dict[str, Any]] = None) -> None:
        ts = _ensure_tz(ts)
        rec = {
            "ts": ts.isoformat(),
            "event": event,
            "trade_id": trade.get("id"),
            "symbol": trade.get("symbol"),
            "status": trade.get("status"),
            "remaining_cash": float(self.cash),
            "extra": extra or {},
        }
        try:
            with open(self.events_json_path, "r", encoding="utf-8") as f:
                arr = json.load(f)
        except Exception:
            arr = []
        arr.append(rec)
        _atomic_write_json(self.events_json_path, arr)

    # ----------------------------- lookups -----------------------------

    def get_details_by_order_id(self, order_id: str, *, min_interval_sec: float = 2.0) -> Optional[dict]:
        """
        Rate-limit safe order details fetch.
        - Returns cached result if called too soon.
        - On 429, increases backoff and returns cached (or None).
        """
        import time as _time
        now = _time.time()
        oid = str(order_id)

        # If we're still in backoff window, return cache
        next_ok = self._order_details_next_ok.get(oid, 0.0)
        if now < next_ok:
            cached = self._order_details_cache.get(oid)
            return cached[1] if cached else None

        # If called too frequently, return cache
        cached = self._order_details_cache.get(oid)
        if cached and (now - cached[0]) < min_interval_sec:
            return cached[1]

        try:
            # --- your existing call ---
            resp = self.upstox_helper.get_details_by_order_id(order_id=oid)  # adapt to your helper
            # normalize to dict if SDK model
            if hasattr(resp, "to_dict"):
                resp = resp.to_dict()
            elif not isinstance(resp, dict):
                resp = getattr(resp, "__dict__", {"raw": str(resp)})

            # success => reset backoff
            self._order_details_cache[oid] = (now, resp)
            self._order_details_backoff[oid] = 0.0
            self._order_details_next_ok[oid] = now  # ok immediately
            return resp

        except Exception as e:
            msg = str(e)

            # Detect 429 from SDK exception text
            is_429 = ("429" in msg) or ("Too Many Requests" in msg) or ("UDAPI10005" in msg)

            if is_429:
                prev = float(self._order_details_backoff.get(oid, 0.0) or 0.0)
                # exponential backoff: 2,4,8,16,... capped
                new_backoff = 2.0 if prev <= 0 else min(prev * 2.0, 60.0)
                jitter = random.uniform(0.1, 0.5)
                self._order_details_backoff[oid] = new_backoff
                self._order_details_next_ok[oid] = now + new_backoff + jitter

                logger.warning(
                    f"get_details_by_order_id rate-limited (429). order_id={oid} "
                    f"backoff={new_backoff:.1f}s"
                )
                # return last cached data if any
                return cached[1] if cached else None

            # non-429: log and return cache/None
            logger.warning(f"get_details_by_order_id failed order_id={oid}: {e}")
            return cached[1] if cached else None


    def get_trade_by_id(self, trade_id: str) -> Optional[Dict[str, Any]]:
        for t in self.orders:
            if str(t.get("id")) == str(trade_id):
                return t
        # fallback to csv (in case)
        df = self._read_orders_df()
        m = df[df["id"].astype(str) == str(trade_id)]
        if m.empty:
            return None
        d = m.iloc[0].to_dict()
        for col in ["entry_order_ids", "sl_order_ids", "sl_order_qty_map"]:
            v = d.get(col)
            if isinstance(v, str) and v.strip().startswith(("[", "{")):
                try:
                    d[col] = json.loads(v)
                except Exception:
                    pass
        return d

    def get_open_trade(self, symbol: str) -> Optional[Dict[str, Any]]:
        for t in self.orders:
            if t.get("symbol") == symbol and str(t.get("status", "")).upper() == "OPEN":
                return t
        return None

    # ----------------------------- order placement -----------------------------

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
        trail_points: Optional[float] = None,
        start_trail_after: Optional[float] = None,
        product: str = "D",
        validity: str = "DAY",
        tag_entry: str = "entry",
        tag_sl: str = "stoploss",
        description: str = "",
        ts: Optional[datetime] = None,
    ) -> str:
        # Defensive casting: prevent strings like "my_stop_loss" from breaking float columns
        entry_price = _safe_float(entry_price, None)
        sl_trigger = _safe_float(sl_trigger, None)
        sl_limit = _safe_float(sl_limit, None)
        if entry_price is None or sl_trigger is None or sl_limit is None:
            raise ValueError(f"entry_price/sl_trigger/sl_limit must be numeric. got entry_price={entry_price}, sl_trigger={sl_trigger}, sl_limit={sl_limit}")

        existing_trade = self.get_open_trade(symbol=symbol)
        if existing_trade is not None:
            logger.warning(f"[SKIP] Open trade exists for {symbol}. Not opening another.")
            return str(existing_trade["id"])

        if trail_points is not None:
            trail_points = _safe_float(trail_points, None)
        if start_trail_after is not None:
            start_trail_after = _safe_float(start_trail_after, None)
        trailing_active = trail_points is not None and trail_points > 0 and start_trail_after is not None

        trade_id,tag_entry = self.place_market_entry(
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
                                    tsl_active=trailing_active,
                                    trail_points=trail_points,
                                    start_trail_after=start_trail_after,
                                    ts=ts,
                                )

        self.place_stoploss_order(
            trade_id=trade_id,
            instrument_token=instrument_token,
            qty=qty,
            trigger_price=sl_trigger,
            limit_price=sl_limit,
            tag_sl=tag_entry,
            ts=ts,
        )
        return trade_id
    

    def place_market_entry(
        self,
        *,
        symbol: str,
        instrument_token: str,
        side: str,
        qty: int,
        entry_price: float,
        product: str = "D",
        validity: str = "DAY",
        tag_entry: str = "entry",
        description: str = "",
        target: Optional[float] = None,
        stoploss_trigger: Optional[float] = None,
        stoploss_limit: Optional[float] = None,
        tsl_active: bool = False,
        trail_points: Optional[float] = None,
        start_trail_after: Optional[float] = None,
        ts: Optional[datetime] = None,
    ) -> str:
        ts = _ensure_tz(ts)
        side = (side or "").upper()
        if side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")

        if self.get_open_trade(symbol=symbol) is not None:
            logger.warning(f"[SKIP] Open trade exists for {symbol}. Not opening another.")
            return str(self.get_open_trade(symbol=symbol)["id"])

        trade_id = str(uuid.uuid4())[:8]
        tag_entry = f"{ts.strftime('%d%m%Y%H%M')}_{trade_id}"

        result = self.upstox_helper.asset_place_order(
            instrument_token=instrument_token,
            quantity=int(qty),
            tag=tag_entry,
            transaction_type=side,
            order_type=constants.MARKET,
            trigger_price=0,
        )
        response = result.to_dict() if hasattr(result, "to_dict") else result
        if not response or response.get("status") != constants.SUCCESS:
            raise Exception(f"Failed to place entry order: {response}")

        order_ids = ((response.get("data") or {}).get("order_ids")) or []
        if not order_ids:
            raise Exception(f"Entry order_ids missing: {response}")
        trade = {
            "id": trade_id,
            "symbol": symbol,
            "instrument_token": instrument_token,
            "side": side,
            "qty": int(qty),
            "product": product,
            "validity": validity,
            "entry_order_ids": list(order_ids),
            "sl_order_ids": [],
            "target_order_id": None,
            "entry_price": float(entry_price),
            "target": float(target) if target is not None else None,
            "stoploss": float(stoploss_trigger) if stoploss_trigger is not None else None,
            "_sl_limit": float(stoploss_limit) if stoploss_limit is not None else None,
            "tsl_active": bool(tsl_active),
            "start_trail_after": float(start_trail_after) if start_trail_after is not None else None,
            "entry_spot": float(entry_price),
            "spot_ltp": float(entry_price),
            "_spot_trail_anchor": None,
            "_trail_points": float(trail_points) if trail_points is not None else None,
            "status": "OPEN",
            "timestamp": ts.isoformat(),
            "exit_price": None,
            "pnl": None,
            "exit_time": None,
            "tag_entry": tag_entry,
            "tag_sl": tag_entry,
            "description": description,
            "sl_order_qty_map": {},
        }

        self.orders.append(trade)
        self.positions[symbol] = self.positions.get(symbol, 0) + (int(qty) if side == "BUY" else -int(qty))
        self._upsert_trade_row(trade)

        self._log_event(
            "PLACE_ENTRY",
            trade,
            ts=ts,
            extra={
                "entry_order_ids": list(order_ids),
                "endpoint": "/v3/order/place",
                "order_type": "MARKET",
                "transaction_type": side,
                "price": float(entry_price),
                "trigger_price": float(entry_price),
                "trail_points": float(trail_points) if trail_points is not None else None,
                "tsl_active": bool(tsl_active),
                "tag": tag_entry,
            },
        )
        return trade_id,tag_entry

    def place_stoploss_order(
        self,
        *,
        instrument_token: str,
        trade_id: str,
        qty: int,
        trigger_price: float,
        limit_price: float,
        tag_sl: str = "stoploss",
        ts: Optional[datetime] = None,
    ) -> List[str]:
        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t:
            raise ValueError(f"trade_id not found: {trade_id}")
        if str(t.get("status", "")).upper() != "OPEN":
            return []

        entry_side = str(t.get("side", "")).upper()
        opp_side = "SELL" if entry_side == "BUY" else "BUY"

        result = self.upstox_helper.asset_place_order(
            instrument_token=instrument_token,
            quantity=int(qty),
            tag=tag_sl,
            transaction_type=opp_side,
            order_type=constants.SL,
            price=float(limit_price),
            trigger_price=float(trigger_price),
            is_slice=True,
        )
        response = result.to_dict() if hasattr(result, "to_dict") else result
        if not response or response.get("status") != constants.SUCCESS:
            raise Exception(f"Failed to place SL order: {response}")

        order_ids = ((response.get("data") or {}).get("order_ids")) or []
        if not order_ids:
            raise Exception(f"SL order_ids missing: {response}")

        # attach to trade (slice aware)
        t["sl_order_ids"] = list(order_ids)
        t["stoploss"] = float(trigger_price)
        t["_sl_limit"] = float(limit_price)
        t["tag_sl"] = tag_sl

        # best-effort qty map (Upstox doesn't return slice qty here)
        parts = _split_qty(int(qty), len(order_ids))
        qty_map = {}
        for oid, q in zip(order_ids, parts):
            if q > 0:
                qty_map[str(oid)] = int(q)
        t["sl_order_qty_map"] = qty_map

        self._upsert_trade_row(t)
        self._log_event(
            "PLACE_SL",
            t,
            ts=ts,
            extra={
                "sl_order_ids": list(order_ids),
                "endpoint": "/v3/order/place",
                "order_type": "SL",
                "transaction_type": opp_side,
                "entry_price": _safe_float(t.get("entry_price"), None),
                "trigger_price": float(trigger_price),
                "limit_price": float(limit_price),
                "tag": tag_sl,
            },
        )
        return list(order_ids)

    # ----------------------------- broker sync -----------------------------

    def _trades_by_order_avg_price(self, order_id: str) -> Tuple[bool, Optional[float]]:
        """
        Returns: (filled, avg_price)
        We consider 'filled' True if get_trades_by_order returns at least one trade row.
        """
        try:
            res = self.get_details_by_order_id(order_id)
            d = res.to_dict() if hasattr(res, "to_dict") else res
        except Exception as e:
            logger.warning(f"get_details_by_order_id failed order_id={order_id}: {e}")
            return False, None

        if not d or d.get("status") != constants.SUCCESS:
            return False, None

        data = d.get("data")
        if not data:
            return False, None

        # Sometimes data is a dict, sometimes a list; normalize to list of trades.
        trades = data if isinstance(data, list) else (data.get("trades") if isinstance(data, dict) else None)
        if not trades or not isinstance(trades, list):
            # If API returns a dict with status COMPLETE, treat as filled
            status = str(data.get("status", "")).upper() if isinstance(data, dict) else ""
            if status in ("COMPLETE", "FILLED", "EXECUTED"):
                px = _safe_float(data.get("average_price", data.get("price", None)), None)
                return True, px
            return False, None

        tot_qty = 0
        tot_val = 0.0
        for tr in trades:
            if not isinstance(tr, dict):
                continue
            q = _safe_int(tr.get("quantity", tr.get("qty", 0)), 0)
            px = _safe_float(tr.get("traded_price", tr.get("trade_price", tr.get("price", tr.get("average_price", 0)))), 0.0)
            if q <= 0:
                continue
            tot_qty += q
            tot_val += px * q
        if tot_qty <= 0:
            return True, None  # trade rows exist but qty unknown
        return True, (tot_val / tot_qty)

    def refresh_trade_status(
        self,
        trade_id: str,
        ts: Optional[datetime] = None,
        *,
        min_interval_sec: float = 3.0,
    ) -> Optional[str]:
        """
        Checks Upstox for SL execution (slice-aware) with throttling to avoid 429.

        Returns:
        - constants.STOPLOSS_HIT if SL is executed and trade is closed locally
        - "SL_CANCELLED_OR_REJECTED" if SL is terminal but not filled (optional handling)
        - None otherwise
        """
        import time as _time

        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t or str(t.get("status", "")).upper() != "OPEN":
            return None

        # ---- throttle per trade (prevents Too Many Requests) ----
        if not hasattr(self, "_trade_refresh_last_ts"):
            self._trade_refresh_last_ts = {}  # trade_id -> epoch seconds

        now = _time.time()
        last = float(self._trade_refresh_last_ts.get(trade_id, 0.0) or 0.0)
        if (now - last) < float(min_interval_sec):
            return None
        self._trade_refresh_last_ts[trade_id] = now

        # ---- normalize sl_order_ids ----
        sl_ids = t.get("sl_order_ids") or []
        if isinstance(sl_ids, str):
            try:
                sl_ids = json.loads(sl_ids)
            except Exception:
                sl_ids = [sl_ids]

        if not sl_ids:
            return None

        # ---- Helper: fetch broker status lightly (uses your get_details_by_order_id wrapper if present) ----
        def _extract_status(details: dict) -> str:
            if not isinstance(details, dict):
                return ""
            data = details.get("data", details)
            if isinstance(data, dict):
                return str(data.get("status") or data.get("order_status") or data.get("state") or "").upper().strip()
            if isinstance(data, list) and data:
                d0 = data[0] if isinstance(data[0], dict) else {}
                return str(d0.get("status") or d0.get("order_status") or d0.get("state") or "").upper().strip()
            return str(details.get("status") or "").upper().strip()

        terminal_states = {"COMPLETE", "COMPLETED", "CANCELLED", "CANCELED", "REJECTED"}

        for oid in sl_ids:
            oid = str(oid)

            # 1) Prefer your avg-price based fill detection (fast path)
            try:
                filled, avg_px = self._trades_by_order_avg_price(oid)
                if filled:
                    exit_px = float(avg_px) if avg_px is not None else float(t.get("stoploss") or 0.0)
                    self._close_trade(trade_id=trade_id, exit_price=exit_px, ts=ts, reason=constants.STOPLOSS_HIT)
                    return constants.STOPLOSS_HIT
            except Exception as e:
                logger.warning(f"refresh_trade_status: _trades_by_order_avg_price failed oid={oid}: {e}")

            # 2) Light broker status check to stop trailing when SL is rejected/cancelled/completed
            details = None
            try:
                if hasattr(self, "get_details_by_order_id"):
                    details = self.get_details_by_order_id(oid)  # should be cached/throttled
            except Exception as e:
                logger.warning(f"refresh_trade_status: get_details_by_order_id failed oid={oid}: {e}")

            if details is None:
                # likely 429/backoff; don't treat as failure
                continue

            st = _extract_status(details)
            if st in terminal_states:
                # If COMPLETE but we didn't detect fill above, treat as SL hit with fallback price
                if st in {"COMPLETE", "COMPLETED"}:
                    exit_px = float(t.get("stoploss") or 0.0)
                    self._close_trade(trade_id=trade_id, exit_price=exit_px, ts=ts, reason=constants.STOPLOSS_HIT)
                    return constants.STOPLOSS_HIT

                # CANCELLED/REJECTED: stop trailing to avoid UDAPI100041 spam.
                # Keep trade OPEN by default (safer); you can choose to close if you prefer.
                t["tsl_active"] = False
                t["sl_state"] = st
                self._upsert_trade_row(t)
                self._log_event(
                    "SL_TERMINAL_NOT_FILLED",
                    t,
                    ts=ts,
                    extra={"sl_order_id": oid, "sl_state": st},
                )
                return "SL_CANCELLED_OR_REJECTED"

        return None


    # ----------------------------- trailing / modify -----------------------------

    def modify_sl_order(
        self,
        *,
        trade_id: str,
        ltp_now: float,
        new_trigger: float,
        new_limit: float,
        ts: Optional[datetime] = None,
    ) -> bool:
        """
        Modify SL orders (slice aware).
        - Broker-sync is done to prevent repeated failures after SL is hit.
        - Tick-align trigger/limit to avoid exchange rejection.
        - Prevent "instant trigger" by ensuring trigger stays on correct side of LTP.
        - If broker says UDAPI100041 (order already cancelled/rejected/completed), disable trailing.
        """
        # ---------- broker sync ----------
        logger.info("initiate modify_sl_order")
        closed = self.refresh_trade_status(trade_id, ts=ts)
        if closed:
            return False
        
        t = self.get_trade_by_id(trade_id)
        if not t or str(t.get("status", "")).upper() != "OPEN":
            return False

        # Load slice ids + qty map
        sl_ids = t.get("sl_order_ids") or []
        if isinstance(sl_ids, str):
            try:
                sl_ids = json.loads(sl_ids)
            except Exception:
                sl_ids = [sl_ids]

        qty_map = t.get("sl_order_qty_map") or {}
        if isinstance(qty_map, str):
            try:
                qty_map = json.loads(qty_map)
            except Exception:
                qty_map = {}

        logger.info(f"Quantity Map : {qty_map}")
        ok_any = False
        udapi_not_modifiable = False

        for oid in sl_ids:
            logger.info(f"Modify for SL OrderID: {oid}")
            q = _safe_int(qty_map.get(str(oid), t.get("qty", 0)), 0)
            if q <= 0:
                continue
            
            logger.info(f"Initiate Broker SL modify call OrderID: {oid}")
            try:
                modify_response = self.upstox_helper.asset_modify_order(
                    sl_order_id=str(oid),
                    quantity=int(q),
                    validity="DAY",
                    order_type=constants.SL,
                    disclosed_quantity=0,
                    trigger_price=float(new_trigger),
                    price=float(new_limit),
                    is_amo=False,
                    slice=True,
                )

                response = modify_response.to_dict() if hasattr(modify_response, "to_dict") else modify_response
                logger.info(f"SL modify response: {response}")
                if not response or response.get("status") != constants.SUCCESS:
                    t = self.get_trade_by_id(trade_id) or t
                    t["tsl_active"] = True
                    self._upsert_trade_row(t)
                    self._log_event(
                        "TSL_UPDATE_FAILED",
                        t,
                        ts=ts,
                        response=response,
                    )
                    return False

                ok_any = True

            except Exception as e:
                msg = str(e)
                logger.warning(f"modify_sl failed order_id={oid}: {e}")

                # Order already cancelled/rejected/completed => stop trailing to avoid spam
                if ("UDAPI100041" in msg) or ("cancelled/rejected/completed" in msg.lower()):
                    udapi_not_modifiable = True
                    self.refresh_trade_status(trade_id, ts=ts)
                    continue

                # Other errors: refresh once (keeps state sane)
                self.refresh_trade_status(trade_id, ts=ts)

        if not ok_any and udapi_not_modifiable:
            # Disable trailing so bot stops calling modify repeatedly
            t = self.get_trade_by_id(trade_id) or t
            t["tsl_active"] = False
            self._upsert_trade_row(t)
            self._log_event(
                "TSL_DISABLED",
                t,
                ts=ts,
                extra={"reason": "SL not modifiable (UDAPI100041)", "sl_order_ids": sl_ids},
            )
            return False

        if ok_any:
            logger.info("SL modified updating event log")
            t["stoploss"] = float(new_trigger)
            t["_sl_limit"] = float(new_limit)
            self._upsert_trade_row(t)
            self._log_event(
                "MODIFY_SL",
                t,
                ts=ts,
                extra={
                    "endpoint": "/v3/order/modify",
                    "sl_order_ids": sl_ids,
                    "trigger_price": float(new_trigger),
                    "price": float(new_limit),
                    "ltp": ltp_now,
                },
            )

        return ok_any

    def exit_position(
        self,
        trade=None,
        close_sl: bool = True
    ):
        
        if trade is None:
            raise Exception(f"Trade details not found")
        
        tag = trade.get("tag_entry", "entry")
        try:
            result = self.upstox_helper.exit_all_positions(tag=tag)
            response = result.to_dict() if hasattr(result, "to_dict") else result
            if not response or response.get("status") != constants.SUCCESS:
                logger.warning(f"Failed to exit position for tag={tag}: {response}")
                return constants.FAIL
        except Exception as e:
            logger.warning(f"Failed to exit position for tag={tag}: {e}")
        
        logger.info(f"Exit position placed for tag={tag}")

        if close_sl == True:
            sl_ids = trade.get("sl_order_ids") or []
            if isinstance(sl_ids, str):
                try:
                    sl_ids = json.loads(sl_ids)
                except Exception:
                    sl_ids = []
            for oid in sl_ids:
                try:
                    response=self.upstox_helper.cancel_order(str(oid))
                    if not response or response.get("status") != constants.SUCCESS:
                        logger.warning(f"Failed to cancel SL order_id={oid}: {response}")
                except Exception as e:
                    logger.warning(f"Failed to cancel SL order_id={oid} during exit: {e}")

        return constants.SUCCESS

    # ----------------------------- on_tick management -----------------------------

    def on_tick(self, *, symbol: str,  o: float, h: float, l: float, c: float, ts: Optional[datetime] = None) -> None:

        ts = _ensure_tz(ts)
        t = self.get_open_trade(symbol=symbol)
        if not t:
            return

        # broker sync first (prevents trailing spam after SL hit)
        self.refresh_trade_status(t["id"], ts=ts)
        if str(t.get("status", "")).upper() != "OPEN":
            return

        side = str(t.get("side", "")).upper()

        # SL/TP checks (local simulation based on bar)
        target = _safe_float(t.get("target"), None)

        if side == "BUY":
            if target is not None and float(h) >= float(target):
                resp = self.exit_position(trade=t)
                if resp == constants.SUCCESS:
                    logger.info(f"Exited position for trade_id={t['id']} due to target hit")
                    self._close_trade(trade_id=t["id"], exit_price=float(target), ts=ts, reason=constants.TARGET_HIT)
                    return
                else:
                    logger.warning(f"Failed to exit position for trade_id={t['id']} on target hit: {resp}")
        else:
            if target is not None and float(l) <= float(target):
                resp = self.exit_position(trade=t)
                if resp == constants.SUCCESS:
                    logger.info(f"Exited position for trade_id={t['id']} due to target hit")
                    self._close_trade(trade_id=t["id"], exit_price=float(target), ts=ts, reason=constants.TARGET_HIT)
                    return
                else:
                    logger.warning(f"Failed to exit position for trade_id={t['id']} on target hit: {resp}")

        self._upsert_trade_row(t)

    # ----------------------------- exits -----------------------------

    def square_off_trade(self, *, trade_id: str, exit_price: float, ts: Optional[datetime] = None, reason: str = constants.MANUAL_EXIT) -> None:
        ts = _ensure_tz(ts)
        t = self.get_trade_by_id(trade_id)
        if not t or str(t.get("status", "")).upper() != "OPEN":
            return
        self._close_trade(trade_id=trade_id, exit_price=float(exit_price), ts=ts, reason=reason)

    def _close_trade(self, *, trade_id: str, exit_price: float, ts: datetime, reason: str) -> None:
        t = self.get_trade_by_id(trade_id)
        if not t or str(t.get("status", "")).upper() != "OPEN":
            return

        side = str(t.get("side", "")).upper()
        entry = _safe_float(t.get("entry_price"), 0.0)
        qty = _safe_int(t.get("qty"), 0)

        pnl = (float(exit_price) - entry) * qty if side == "BUY" else (entry - float(exit_price)) * qty
        t["exit_price"] = float(exit_price)
        t["pnl"] = float(pnl)
        t["exit_time"] = ts.isoformat()
        t["status"] = reason

        sym = t.get("symbol")
        if sym:
            self.positions[sym] = self.positions.get(sym, 0) - (qty if side == "BUY" else -qty)

        self.cash += pnl

        self._upsert_trade_row(t)
        self._log_event("CLOSE", t, ts=ts, extra={"reason": reason, "exit_price": float(exit_price), "pnl": float(pnl)})

        # update daily pnl file (simple overwrite last row for today)
        try:
            df = pd.read_csv(self.daily_csv)
        except Exception:
            df = pd.DataFrame()

        today = ts.date().isoformat()
        if df.empty or "date" not in df.columns:
            df = pd.DataFrame([{"date": today, "initial_cash": self.initial_cash, "cash": self.cash, "pnl": self.cash - self.initial_cash}])
        else:
            mask = df["date"].astype(str) == str(today)
            if mask.any():
                df.loc[mask, "cash"] = float(self.cash)
                df.loc[mask, "pnl"] = float(self.cash - self.initial_cash)
            else:
                df.loc[len(df)] = {"date": today, "initial_cash": self.initial_cash, "cash": self.cash, "pnl": self.cash - self.initial_cash}

        _atomic_write_dataframe(df, self.daily_csv)

    def _round_to_tick(self, x: float, tick: float, mode: str) -> float:
        x = float(x); tick = float(tick)
        if tick <= 0:
            return x
        n = x / tick
        if mode == "FLOOR":
            return math.floor(n) * tick
        if mode == "CEIL":
            return math.ceil(n) * tick
        return round(n) * tick
