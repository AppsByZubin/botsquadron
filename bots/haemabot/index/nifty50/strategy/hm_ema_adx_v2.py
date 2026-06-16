import json
import sys
import os
import yaml

os.environ.setdefault("PANDAS_USE_NUMEXPR", "0")
os.environ.setdefault("PANDAS_USE_BOTTLENECK", "0")

import pandas as pd
import math
from threading import RLock
from zoneinfo import ZoneInfo
import common.constants as constants
import logger
from typing import Any, Dict, List, Optional
from datetime import datetime, time, timedelta
import numpy as np
import talib
from technicals.atr.atr_for_ticks import AtrEngine

from utils.generic_utils import (
    calculate_gap_percent,
    classify_gap_direction,
    get_previous_close_for_gap,
    safe_float,
)

ist = ZoneInfo("Asia/Kolkata")

logger = logger.create_logger("HmEmaAdxV2StrategyLogger")


class HmEmaAdxV2Strategy:
    """
    Spot-only EMA + ADX intraday strategy using 5-minute index candles.

    Flow:
    1) Build 5-minute index candles from ticks.
    2) Compute EMA, RSI-MA, ATR, ADX and price-action context.
    3) Delegate live trade lifecycle to order manager.
    """
    INDEX_CANDLE_INTERVAL_MINUTES = 5

    def __init__(
        self,
        current_date=None,
        expiry_date=None,
        order_manager=None,
        params=None,
        uptox_client=None,
        previous_day_trend: Optional[str] = None,
        selected_contracts: Optional[Dict[str, Any]] = None,
        index_minutes_processed: Optional[Dict[str, bool]] = None,
        future_minutes_processed: Optional[Dict[str, bool]] = None,
        intraday_index_candles=None,
        intraday_future_candles=None,
        option_exipry_date: Optional[str] = None,
    ):
        self.current_date = current_date or datetime.now(ist).strftime("%Y%m%d")
        self.expiry_date = expiry_date or option_exipry_date
        self.uptox_client = uptox_client
        self.previous_day_trend = previous_day_trend
        self.selected_contracts = selected_contracts or {}
        self.index_minutes_processed = index_minutes_processed or {}
        self.future_minutes_processed = future_minutes_processed or {}
        self.curr_index_candle = None
        self.curr_index_minute = None
        self.curr_index_bucket_minute = None
        self._index_candle_finalized_since_engine_check = False
        self.curr_fut_candle = None
        self.curr_fut_minute = None
        self.last_fut_bar: Optional[Dict] = None
        self.future_data_from_parquet = False
        self.last_index_bar: Optional[Dict] = None
        self._fut_vol_minute = None
        self._fut_vol_start_vtt = None
        self._fut_vol_last_vtt = None
        self._fut_vol_by_minute: Dict[str, float] = {}
        self._indicator_revision = 0
        self._last_engine_revision = -1
        self._last_indicator_row = -1
        self._fast_vwap_day: Optional[str] = None
        self._fast_cum_pv = 0.0
        self._fast_cum_vol = 0.0
        self._candle_lock = RLock()

        # DataFrames (initialized with fixed dtypes to avoid warnings)
        self.df_index_future = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "oi": pd.Series(dtype="float64")
        })

        self.df_merged = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "close": pd.Series(dtype="float64"),
            "close_fut": pd.Series(dtype="float64"),
            "volume_fut": pd.Series(dtype="float64"),
        })

        self.params = params if isinstance(params, dict) else self._get_params_from_yaml()
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        if not self.expiry_date:
            self.expiry_date = sp.get("trade_expiry")
        ht: Dict[str, Any] = {}
        if isinstance(self.params, dict):
            ht_legacy = self.params.get("historical-trend")
            ht_new = self.params.get("historical-trends")
            if isinstance(ht_legacy, dict):
                ht.update(ht_legacy)
            if isinstance(ht_new, dict):
                ht.update(ht_new)
        self.index_fut_path = self._get_index_fut_path()
        configured_index_fut_key = self._get_index_fut_key()
        nifty_fut = self.selected_contracts.get("Nifty_Future") if isinstance(self.selected_contracts, dict) else None
        selected_index_fut_key = nifty_fut.get("instrument_key") if isinstance(nifty_fut, dict) else None
        if configured_index_fut_key and selected_index_fut_key and configured_index_fut_key != selected_index_fut_key:
            logger.info(
                "Using selected Nifty future instrument_key=%s instead of configured instrument_key=%s",
                selected_index_fut_key,
                configured_index_fut_key,
            )
        self.index_fur_key = selected_index_fut_key or configured_index_fut_key
        self.df_index_future = self._populate_index_future_data()

        self._oi_previous_snapshot= {}
        self._sum_oi_changes= {}
        self._slope_window = int(sp.get("slope_window", self.params.get("slope_window", 3) or 3))
        self.enable_trading_engine = self._coerce_bool(
            sp.get("enable_trading_engine", self.params.get("enable_trading_engine", True)),
            True,
        )

        self._trader_sentiment = ht.get("trader-sentiment", constants.SIDEWAYS)
        self._daily_sentiment = ht.get("daily", ht.get("trader-sentiment", constants.SIDEWAYS))

        self.atr5_engine = AtrEngine(atr_period=int(sp.get("option_atr_period", 5) or 5))

        # DataFrames (initialized with fixed dtypes to avoid warnings)
        self.df_index = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "fut_volume": pd.Series(dtype="float64"),
            "ema_9": pd.Series(dtype="float64"),
            "atr_14": pd.Series(dtype="float64"),
            "adx_14": pd.Series(dtype="float64"),
            "rsi_7": pd.Series(dtype="float64"),
            "rsi_ma_14": pd.Series(dtype="float64"),
            "hm_rsi_9": pd.Series(dtype="float64"),
            "hm_wma_21": pd.Series(dtype="float64"),
            "hm_ema_3": pd.Series(dtype="float64"),
            "hm_above_50": pd.Series(dtype="bool"),
            "hm_signal": pd.Series(dtype="object"),
            "angle_ema_9": pd.Series(dtype="float64"),
            "angle_rsi_ma_14": pd.Series(dtype="float64"),

            "candle_range": pd.Series(dtype="float64"),
            "volatile_count":pd.Series(dtype="float64"),
            "is_volatile":pd.Series(dtype="bool"),
            "recent_high_max":pd.Series(dtype="float64"),
            "recent_low_min":pd.Series(dtype="float64"),
            "is_hh": pd.Series(dtype="bool"),
            "is_ll": pd.Series(dtype="bool"),

            "is_bearish_thrust": pd.Series(dtype="bool"),
            "is_bullish_thrust":pd.Series(dtype="bool")
        })

        self._max_order_counter = int(sp.get("trade-per-day", sp.get("trade_per_day", 2)) or 2)
        self._order_counter = 0
        self._max_daily_loss_pct_of_initial_cash = float(sp.get("max_daily_loss_pct_of_initial_cash", 0.03) or 0.03)
        self._daily_loss_blocked_day: Optional[str] = None
        self._today_realized_pnl_day: Optional[str] = None
        self._today_realized_pnl: float = 0.0
        self._today_realized_pnl_trade_ids = set()
        self.order_maneger = order_manager
        self.index_atr: Optional[float] = None
        self.index_adx: Optional[float] = None
        self.index_prev_adx: Optional[float] = None

        # In-memory trade state machine used by _trade_processing():
        # None -> WAITING -> OPEN -> cleared.
        self._order_container = {
            "trade_id": None,
            "side": None,
            "instrument_key":None,
            "instrument_symbol":None,
            "status": None,
            "ltp": None,
            "entry_price": None,
            "lot": None,
            "max_gamma": None,
            "start_trail_after": None,
            "trade_create_time": None,
            "force_trail_lock": False
        }
        self._trade_end_time=None
        self._init_trade_window_times()
        self._setup_gap_state()
        if (
            intraday_index_candles is not None
            or intraday_future_candles is not None
        ):
            self._initialize_from_intraday_candles(
                intraday_index_candles,
                intraday_future_candles,
            )
        self._restore_open_order_container_from_ordersystem()

    # ------------------------------------------------------------------
    # Gap helpers
    # ------------------------------------------------------------------
    def _setup_gap_state(self) -> None:
        prev_close = get_previous_close_for_gap(self.params)
        if prev_close is None and isinstance(self.params, dict):
            input_data = self.params.get("input-data") or self.params.get("input_data") or {}
            if isinstance(input_data, dict):
                prev_close = safe_float(
                    input_data.get("previous-day-close")
                    or input_data.get("previous_day_close")
                    or input_data.get("day-close")
                    or input_data.get("day_close")
                )

        self._previous_day_close: Optional[float] = prev_close
        self._gap_day: Optional[str] = None
        self._gap_open: Optional[float] = None
        self._gap_pct: Optional[float] = None
        self._gap_direction: Optional[str] = None

    def _extract_day_key(self, minute_key: str) -> Optional[str]:
        try:
            return datetime.strptime(str(minute_key), "%Y-%m-%d %H:%M").strftime("%Y-%m-%d")
        except Exception:
            return None

    @staticmethod
    def _coerce_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        try:
            missing = pd.isna(value)
            if isinstance(missing, bool) and missing:
                return default
        except Exception:
            pass
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            norm = value.strip().lower()
            if norm in {"1", "true", "yes", "y", "on"}:
                return True
            if norm in {"0", "false", "no", "n", "off"}:
                return False
        return default

    def _restore_open_order_container_from_ordersystem(self) -> None:
        if self.order_maneger is None or not hasattr(self.order_maneger, "get_account_details"):
            return
        if self._order_container.get("status") == constants.OPEN:
            return

        try:
            account = self.order_maneger.get_account_details()
        except Exception as exc:
            logger.warning(f"Open trade restore skipped; ordersystem account lookup failed: {exc}")
            return

        trades = account.get("trades") if isinstance(account, dict) else None
        if not isinstance(trades, list):
            return

        open_trades = [
            trade
            for trade in trades
            if isinstance(trade, dict)
            and str(trade.get("status") or "").strip().upper() == constants.OPEN
        ]
        if not open_trades:
            return
        if len(open_trades) > 1:
            logger.warning("Multiple OPEN trades found during startup restore; using latest trade from ordersystem response.")

        if self._restore_order_container_from_trade(open_trades[-1]):
            self._order_counter = max(self._order_counter, 1)

    def _restore_order_container_from_trade(self, trade: Dict[str, Any]) -> bool:
        trade_id = str(trade.get("id") or trade.get("trade_id") or "").strip()
        instrument_key = str(trade.get("instrument_token") or "").strip()
        if not trade_id or not instrument_key:
            logger.warning(f"Cannot restore OPEN trade with missing id/instrument_token: {trade}")
            return False

        contract = self._find_selected_contract(instrument_key, str(trade.get("symbol") or ""))
        instrument_symbol = str(
            trade.get("symbol")
            or (contract or {}).get("trading_symbol")
            or (contract or {}).get("symbol")
            or ""
        ).strip()
        qty = safe_float(trade.get("qty"))
        ltp = (
            safe_float(trade.get("entry_price"))
            or safe_float((contract or {}).get("ltp"))
        )

        self._order_container.update({
            "trade_id": trade_id,
            "side": self._strategy_side_from_trade(trade, contract),
            "instrument_key": instrument_key,
            "instrument_symbol": instrument_symbol,
            "status": constants.OPEN,
            "ltp": ltp,
            "entry_price": safe_float(trade.get("entry_price")),
            "lot": int(qty) if qty is not None and qty > 0 else None,
            "max_gamma": None,
            "start_trail_after": safe_float(trade.get("start_trail_after")),
            "trade_create_time": trade.get("timestamp") or trade.get("entry_time") or trade.get("created_at"),
            "force_trail_lock": False,
        })
        logger.info(f"Restored OPEN trade from ordersystem into _order_container: {self._order_container}")
        return True

    def _find_selected_contract(self, instrument_key: str, symbol: str = "") -> Optional[Dict[str, Any]]:
        target_key = str(instrument_key or "").strip()
        target_symbol = str(symbol or "").strip().upper()

        def visit(value: Any) -> Optional[Dict[str, Any]]:
            if isinstance(value, dict):
                value_key = str(value.get("instrument_key") or "").strip()
                value_symbol = str(value.get("trading_symbol") or value.get("symbol") or "").strip().upper()
                if (target_key and value_key == target_key) or (target_symbol and value_symbol == target_symbol):
                    return value
                for nested in value.values():
                    if isinstance(nested, (dict, list, tuple)):
                        found = visit(nested)
                        if found is not None:
                            return found
            elif isinstance(value, (list, tuple)):
                for item in value:
                    found = visit(item)
                    if found is not None:
                        return found
            return None

        return visit(self.selected_contracts)

    def _strategy_side_from_trade(self, trade: Dict[str, Any], contract: Optional[Dict[str, Any]]) -> Optional[str]:
        values = [trade.get("symbol"), trade.get("description")]
        if isinstance(contract, dict):
            values.extend([
                contract.get("trading_symbol"),
                contract.get("symbol"),
                contract.get("instrument_type"),
                contract.get("option_type"),
                contract.get("type"),
            ])

        for value in values:
            text = str(value or "").strip().upper()
            if not text:
                continue
            tokens = text.replace("_", " ").replace("-", " ").replace("|", " ").split()
            if "CALL" in tokens or "CE" in tokens or text.endswith("CE"):
                return constants.CALL
            if "PUT" in tokens or "PE" in tokens or text.endswith("PE"):
                return constants.PUT
        return None

    def _update_gap_stats(self, candle: Dict[str, Any]) -> None:
        minute_key = str(candle.get("time") or "")
        try:
            dt_obj = datetime.strptime(minute_key, "%Y-%m-%d %H:%M")
        except Exception:
            return
        day_key = dt_obj.strftime("%Y-%m-%d")

        today_open = safe_float(candle.get("open"))
        if today_open is None or today_open <= 0:
            return

        # Keep day/open even when previous close is unavailable, so other rules
        # (like LTP-vs-open gate) can still work.
        if self._gap_day != day_key:
            self._gap_day = day_key
            self._gap_open = today_open
            self._gap_pct = None
            self._gap_direction = None
        elif self._gap_open is None:
            self._gap_open = today_open

        if self._previous_day_close is None or self._previous_day_close <= 0:
            return
        if self._gap_day == day_key and self._gap_pct is not None:
            return

        gap_pct = calculate_gap_percent(self._previous_day_close, today_open, precision=4)
        if gap_pct is None:
            return
        self._gap_pct = gap_pct
        self._gap_direction = classify_gap_direction(
            self._gap_pct,
            previous_close=self._previous_day_close,
            today_open=today_open,
        )
        logger.info(
            f"Gap {self._gap_direction}: {self._gap_pct:.4f}% "
            f"(open={today_open:.2f}, prev_close={self._previous_day_close:.2f}, day={self._gap_day})"
        )

    def get_gap_info(self) -> Dict[str, Any]:
        return {
            "day": self._gap_day,
            "previous_close": self._previous_day_close,
            "today_open": self._gap_open,
            "gap_pct": self._gap_pct,
            "direction": self._gap_direction,
        }

    def _get_day_open_price(self) -> Optional[float]:
        open_price = safe_float(self._gap_open)
        if open_price is None or open_price <= 0:
            return None
        return open_price

    def _is_ltp_within_open_distance(self, ltp: float, max_points: Optional[float] = None) -> bool:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        threshold = safe_float(max_points)
        if threshold is None:
            threshold = safe_float(
                sp.get(
                    "ltp_open_max_distance_points",
                    sp.get("ltp_open_distance_points", 210),
                )
            )
        if threshold is None or threshold <= 0:
            threshold = 210.0

        ltp_f = safe_float(ltp)
        open_price = self._get_day_open_price()
        if ltp_f is None or ltp_f <= 0 or open_price is None or open_price <= 0:
            return False

        diff_points = abs(ltp_f - open_price)
        allowed = diff_points <= threshold
        if not allowed:
            logger.debug(
                f"LTP-open gate blocked: ltp={ltp_f:.2f}, open={open_price:.2f}, "
                f"distance={diff_points:.2f}, threshold={threshold:.2f}"
            )
        return allowed

    # ------------------------------------------------------------------
    # Bootstrap helpers
    # ------------------------------------------------------------------
    def _initialize_from_intraday_candles(self, index_candles, fut_candles) -> None:
        def build_df(candles, include_volume: bool) -> pd.DataFrame:
            if not candles:
                return pd.DataFrame()

            df = pd.DataFrame(candles, columns=["time", "open", "high", "low", "close", "volume", "oi"])
            df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
            df = df.dropna(subset=["time"])
            numeric_cols = ["open", "high", "low", "close"]
            if include_volume:
                numeric_cols.extend(["volume", "oi"])
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["open", "high", "low", "close"])
            if include_volume:
                return df[["time", "open", "high", "low", "close", "volume", "oi"]]
            return df[["time", "open", "high", "low", "close"]]

        df_i_raw = build_df(index_candles, include_volume=False)
        df_f_raw = build_df(fut_candles, include_volume=True)
        df_i, partial_i = self._bucketize_intraday_frame(df_i_raw, include_volume=False)
        df_f, partial_f = self._bucketize_intraday_frame(df_f_raw, include_volume=True)

        if not df_i.empty:
            first_row = df_i.iloc[0]
            self._update_gap_stats({
                "time": first_row["time"],
                "open": first_row["open"],
            })
            self.df_index = pd.concat([self.df_index, df_i], ignore_index=True)
            self.last_index_bar = df_i.iloc[-1].to_dict()
            for minute_key in df_i["time"].astype(str):
                self.index_minutes_processed[minute_key] = True

        if not partial_i.empty:
            row = partial_i.iloc[-1].to_dict()
            self.curr_index_bucket_minute = str(row.get("time") or "")
            self.curr_index_candle = {
                "time": self.curr_index_bucket_minute,
                "open": safe_float(row.get("open")),
                "high": safe_float(row.get("high")),
                "low": safe_float(row.get("low")),
                "close": safe_float(row.get("close")),
            }

        if not df_f.empty:
            self.df_index_future = pd.concat([self.df_index_future, df_f], ignore_index=True)
            self.last_fut_bar = df_f.iloc[-1].to_dict()
            for minute_key in df_f["time"].astype(str):
                self.future_minutes_processed[minute_key] = True

        if not partial_f.empty:
            row = partial_f.iloc[-1].to_dict()
            self.curr_fut_minute = str(row.get("time") or "")
            self.curr_fut_candle = row

        self._refresh_merged_dataframe()
        if not df_i.empty:
            self._apply_indicators_and_engine()

    def _index_bucket_key(self, minute_key: str) -> str:
        dt_obj = datetime.strptime(str(minute_key), "%Y-%m-%d %H:%M")
        bucket_minute = (
            dt_obj.minute // self.INDEX_CANDLE_INTERVAL_MINUTES
        ) * self.INDEX_CANDLE_INTERVAL_MINUTES
        return dt_obj.replace(minute=bucket_minute).strftime("%Y-%m-%d %H:%M")

    def _bucketize_intraday_frame(self, df: pd.DataFrame, include_volume: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame()

        work = df.copy()
        work["bucket"] = work["time"].apply(self._index_bucket_key)
        agg = {
            "time": ("bucket", "first"),
            "open": ("open", "first"),
            "high": ("high", "max"),
            "low": ("low", "min"),
            "close": ("close", "last"),
            "minute_count": ("time", "nunique"),
        }
        if include_volume:
            agg["volume"] = ("volume", "sum")
            agg["oi"] = ("oi", "last")

        grouped = work.groupby("bucket", sort=True).agg(**agg).reset_index(drop=True)
        complete_mask = pd.to_numeric(grouped["minute_count"], errors="coerce").fillna(0) >= self.INDEX_CANDLE_INTERVAL_MINUTES
        complete = grouped.loc[complete_mask].drop(columns=["minute_count"], errors="ignore").reset_index(drop=True)
        partial = grouped.loc[~complete_mask].drop(columns=["minute_count"], errors="ignore").reset_index(drop=True)
        return complete, partial

    def _populate_index_future_data(self):
        if self.index_fur_key is not None:
            self.future_data_from_parquet = True
            logger.info(f"Using parquet feed for Nifty future candles. instrument_key={self.index_fur_key}")
            return pd.DataFrame({
                "time": pd.Series(dtype="object"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "oi": pd.Series(dtype="float64"),
            })

        self.future_data_from_parquet = False
        if not self.index_fut_path or not os.path.exists(self.index_fut_path):
            logger.debug(f"Index future data file not found at {self.index_fut_path}")
            self.future_data_from_parquet = True
            # Return an empty DataFrame so callers can still operate safely.
            return pd.DataFrame({
                "time": pd.Series(dtype="object"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "oi": pd.Series(dtype="float64"),
            })

        with open(self.index_fut_path, "r") as f:
            data = json.load(f) or {}

        # Support both legacy and newer JSON schema
        candles = None
        if isinstance(data, dict):
            if isinstance(data.get("data"), dict):
                candles = data["data"].get("candles")
            elif "candles" in data:
                candles = data.get("candles")

        if not isinstance(candles, list) or len(candles) == 0:
            logger.warning(f"Unexpected future data format, unable to parse candles from {self.index_fut_path}")
            return pd.DataFrame({
                "time": pd.Series(dtype="object"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "oi": pd.Series(dtype="float64"),
            })

        rows = []
        for candle in candles:
            if isinstance(candle, (list, tuple)) and len(candle) >= 7:
                candle_time = pd.to_datetime(candle[0]).strftime("%Y-%m-%d %H:%M")
                rows.append({
                    "time": candle_time,
                    "open": candle[1],
                    "high": candle[2],
                    "low": candle[3],
                    "close": candle[4],
                    "volume": candle[5],
                    "oi": candle[6],
                })
            elif isinstance(candle, dict):
                candle_time = pd.to_datetime(candle.get("datetime") or candle.get("time") or candle.get("date")).strftime("%Y-%m-%d %H:%M")
                rows.append({
                    "time": candle_time,
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume"),
                    "oi": candle.get("open_interest") or candle.get("oi"),
                })
            else:
                continue

        df = pd.DataFrame(rows)
        if not df.empty:
            # Ensure chronological ordering (oldest first)
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df = df.sort_values("time").reset_index(drop=True)
            df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M")
        return df

    def _get_params_from_yaml(self):
        candidate_paths = []
        if self.current_date:
            candidate_paths.append(f"data/{self.current_date}/param.yaml")
        candidate_paths.append(constants.PARAM_PATH)

        for path in candidate_paths:
            if not path or not os.path.exists(path):
                continue
            with open(path, 'r') as file:
                params = yaml.safe_load(file) or {}
                logger.info(f"Loaded parameters from {path}")
                return params

        logger.error(f"Parameter file not found. Checked paths: {candidate_paths}")
        sys.exit(constants.FAIL_CODE)

    def _get_index_fut_path(self):
        # Check if 'data-sources' exists in parameters
        if self.params and 'data-sources' in self.params:
            sources_dict = self.params['data-sources']

            # Check directly if 'nifty-volume' is a key in the sources dictionary
            if 'nifty-volume' in sources_dict:
                return sources_dict['nifty-volume'] # Access the value by its key
        return None

    def _get_index_fut_key(self):
        # Check if 'data-sources' exists in parameters
        if self.params and 'data-sources' in self.params:
            sources_dict = self.params['data-sources']

            # Check directly if 'nifty-future' is a key in the sources dictionary
            if 'nifty-future' in sources_dict:
                return sources_dict['nifty-future'] # Access the value by its key
        return None

    def _prepare_merge_frame(self, df: pd.DataFrame, column_map: Dict[str, str]) -> pd.DataFrame:
        output_cols = ["time"] + list(column_map.values())
        if df is None or df.empty or "time" not in df.columns:
            return pd.DataFrame(columns=output_cols)

        cols = ["time"] + [src for src in column_map.keys() if src in df.columns]
        out = df[cols].copy()
        out["time"] = pd.to_datetime(out["time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        out = out.dropna(subset=["time"])

        for src in column_map.keys():
            if src in out.columns:
                out[src] = pd.to_numeric(out[src], errors="coerce")

        out = out.rename(columns=column_map)
        for col in output_cols:
            if col not in out.columns:
                out[col] = np.nan

        return out[output_cols].drop_duplicates(subset=["time"], keep="last")

    def _refresh_merged_dataframe(self) -> None:
        base = self._prepare_merge_frame(self.df_index, {"close": "close"})
        if base.empty:
            return

        merged = base

        fut = self._prepare_merge_frame(
            self.df_index_future,
            {
                "close": "close_fut",
                "volume": "volume_fut",
            },
        )
        if not fut.empty:
            merged = merged.merge(fut, on="time", how="left")
        else:
            merged["close_fut"] = np.nan
            merged["volume_fut"] = np.nan

        sort_key = pd.to_datetime(merged["time"], errors="coerce")
        merged = merged.assign(_sort_time=sort_key).sort_values("_sort_time").drop(columns=["_sort_time"])
        self.df_merged = merged.reset_index(drop=True)

    def _sync_merged_features_to_index(self) -> None:
        if self.df_index is None or self.df_index.empty or self.df_merged is None or self.df_merged.empty:
            return

        feature_cols = ["close_fut", "volume_fut"]
        available_features = [col for col in feature_cols if col in self.df_merged.columns]
        if not available_features:
            return

        tmp_col = "__merge_minute"
        base = self.df_index.drop(columns=feature_cols, errors="ignore").copy()
        base[tmp_col] = pd.to_datetime(base["time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

        features = self.df_merged[["time"] + available_features].copy()
        features[tmp_col] = pd.to_datetime(features["time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        features = features.dropna(subset=[tmp_col]).drop_duplicates(subset=[tmp_col], keep="last")
        features = features.drop(columns=["time"])

        merged = base.merge(features, on=tmp_col, how="left")
        self.df_index = merged.drop(columns=[tmp_col])

    # ------------------------------------------------------------------
    # WS lifecycle
    # ------------------------------------------------------------------
    def start(self):
        return None

    def stop(self):
        return None

    def on_ws_reconnected(self):
        logger.info("WebSocket reconnected; hm_ema_adx_v2 strategy state preserved.")

    def get_subscription_instruments(self) -> List[str]:
        instruments: List[str] = []

        def add_instrument(instrument_key: Any) -> None:
            if instrument_key and instrument_key not in instruments:
                instruments.append(instrument_key)

        add_instrument(constants.NIFTY50_SYMBOL)

        if isinstance(self.selected_contracts, dict):
            for value in self.selected_contracts.values():
                if isinstance(value, dict):
                    add_instrument(value.get("instrument_key"))
                    continue
                if isinstance(value, list):
                    for contract in value:
                        if isinstance(contract, dict):
                            add_instrument(contract.get("instrument_key"))

        return instruments

    # ------------------------------------------------------------------
    # WS message handler (called by engine)
    # ------------------------------------------------------------------
    def _normalize_feed_item(self, instrument_key: str, feed: Dict[str, Any], current_ts: Optional[float]) -> Optional[Dict[str, Any]]:
        if not isinstance(feed, dict):
            return None

        ltpc = feed.get("ltpc") or {}
        full_feed = feed.get("fullFeed") or {}
        market_ff = full_feed.get("marketFF") or {}
        index_ff = full_feed.get("indexFF") or {}
        first_level = feed.get("firstLevelWithGreeks") or {}

        if not ltpc:
            ltpc = market_ff.get("ltpc") or index_ff.get("ltpc") or first_level.get("ltpc") or {}

        ltp = safe_float(ltpc.get("ltp"))
        ltt = safe_float(ltpc.get("ltt")) or current_ts
        if ltp is None or ltt is None:
            return None

        option_greeks = market_ff.get("optionGreeks") or market_ff.get("greeks") or first_level.get("optionGreeks") or {}
        return {
            "instrument_key": instrument_key,
            "ltp": ltp,
            "ltt": int(ltt),
            "ts_epoch_ms": int(ltt),
            "oi": safe_float(market_ff.get("oi") or first_level.get("oi")),
            "vtt": safe_float(market_ff.get("vtt")),
            "gamma": safe_float(option_greeks.get("gamma")),
        }

    def _normalize_feed_response(self, feed_response: Any) -> List[Dict[str, Any]]:
        if isinstance(feed_response, list):
            return feed_response
        if not isinstance(feed_response, dict):
            return []

        feeds = feed_response.get("feeds")
        if not isinstance(feeds, dict):
            return []

        current_ts = safe_float(feed_response.get("currentTs"))
        normalized: List[Dict[str, Any]] = []
        for instrument_key, feed in feeds.items():
            item = self._normalize_feed_item(instrument_key, feed, current_ts)
            if item is not None:
                normalized.append(item)
        return normalized

    def _handle_normalized_feed_item(self, item: Dict[str, Any]) -> None:
        ltt_f = safe_float(item.get("ts_epoch_ms"))
        if ltt_f is None:
            ltt_f = safe_float(item.get("ltt"))
        if ltt_f is None:
            return

        ts_ms = int(ltt_f)
        dt_object = datetime.fromtimestamp(ts_ms / 1000, ist)
        minute_key = dt_object.strftime("%Y-%m-%d %H:%M")
        instrument_key = item.get("instrument_key")

        if instrument_key == constants.NIFTY50_SYMBOL:
            ltp = safe_float(item.get("ltp"))
            if ltp is None:
                logger.warning(f"Skipping index tick with invalid ltp: {item}")
                return

            self._handle_index_tick(minute_key, float(ltp))
            return

        if self.index_fur_key is not None and instrument_key == self.index_fur_key:
            ltp = safe_float(item.get("ltp"))
            if ltp is None:
                return
            with self._candle_lock:
                vtt = safe_float(item.get("vtt"))
                if vtt is not None:
                    finished_minute, finished_vol = self._update_1m_volume_from_vtt(minute_key, vtt)
                    if finished_minute is not None:
                        self._fut_vol_by_minute[finished_minute] = float(finished_vol)
                self._handle_fut_tick(minute_key, float(ltp))
            return

        ltp = safe_float(item.get("ltp"))
        if ltp is None:
            return
        self.atr5_engine.on_tick(str(instrument_key), float(ltp), dt_object)

    def on_ws_message(self, message: Dict[str, Any]):
        feed_response = self._normalize_feed_response(message)
        if not feed_response:
            return

        # Order lifecycle gets a chance on every WS message
        try:
            self._trade_processing_from_ws(feed_response)
        except Exception as e:
            logger.warning(f"_trade_processing_from_ws error: {e}")

        for item in feed_response:
            try:
                self._handle_normalized_feed_item(item)
            except Exception as e:
                logger.warning(f"Skipping malformed feed for {item.get('instrument_key')}: {e}")
                continue

    # ------------------------------------------------------------------
    # Candle building
    # ------------------------------------------------------------------
    def _update_1m_volume_from_vtt(self, minute_key: str, vtt_now: float):
        """
        Compute one-minute traded volume from cumulative future VTT.
        Returns the completed minute and its volume when the tick rolls forward.
        """
        try:
            if self._fut_vol_minute is None:
                self._fut_vol_minute = minute_key
                self._fut_vol_start_vtt = vtt_now
                self._fut_vol_last_vtt = vtt_now
                return None, None

            if self._fut_vol_last_vtt is not None and vtt_now < float(self._fut_vol_last_vtt):
                self._fut_vol_minute = minute_key
                self._fut_vol_start_vtt = vtt_now
                self._fut_vol_last_vtt = vtt_now
                return None, None

            if minute_key == self._fut_vol_minute:
                self._fut_vol_last_vtt = vtt_now
                return None, None

            finished_minute = self._fut_vol_minute
            finished_volume = max(float(self._fut_vol_last_vtt) - float(self._fut_vol_start_vtt), 0.0)

            self._fut_vol_minute = minute_key
            self._fut_vol_start_vtt = vtt_now
            self._fut_vol_last_vtt = vtt_now

            return finished_minute, finished_volume
        except Exception as e:
            logger.error(f"Error in _update_1m_volume_from_vtt: {e}")
            return None, None

    def _upsert_future_candle(self, candle: Dict[str, Any]) -> None:
        minute_key = str(candle.get("time") or "")
        if not minute_key:
            return

        row = {
            "time": minute_key,
            "open": safe_float(candle.get("open")),
            "high": safe_float(candle.get("high")),
            "low": safe_float(candle.get("low")),
            "close": safe_float(candle.get("close")),
            "volume": safe_float(candle.get("volume")),
            "oi": safe_float(candle.get("oi")),
        }

        if self.df_index_future is None or self.df_index_future.empty:
            self.df_index_future = pd.DataFrame([row])
            return

        time_col = self.df_index_future["time"]
        if pd.api.types.is_datetime64_any_dtype(time_col):
            minute_dt = pd.to_datetime(minute_key, errors="coerce")
            if pd.isna(minute_dt):
                return
            row["time"] = minute_dt
            matched = self.df_index_future.index[time_col == minute_dt]
        else:
            matched = self.df_index_future.index[time_col.astype(str) == minute_key]

        if len(matched) > 0:
            idx = matched[-1]
            for col, value in row.items():
                self.df_index_future.at[idx, col] = value
        else:
            self.df_index_future = pd.concat([self.df_index_future, pd.DataFrame([row])], ignore_index=True)

    def _finalize_fut_candle(self) -> None:
        with self._candle_lock:
            candle = self.curr_fut_candle
            if candle is None:
                return

            candle = dict(candle)
            self.curr_fut_candle = None
            minute = str(candle.get("time") or "")
            if minute in self._fut_vol_by_minute:
                candle["volume"] = float(self._fut_vol_by_minute.pop(minute, 0.0))
            logger.info(f"Finalizing future candle: {candle}")
            self._upsert_future_candle(candle)
            self.last_fut_bar = candle
            self._try_make_merged_bar()

    def _handle_fut_tick(self, minute_key: str, ltp: float) -> None:
        """Build 5-minute OHLC for FUT using ltp."""
        try:
            with self._candle_lock:
                if minute_key is None:
                    return
                minute_key = str(minute_key)
                try:
                    bucket_key = self._index_bucket_key(minute_key)
                except Exception:
                    bucket_key = minute_key

                ltp_f = safe_float(ltp)
                if ltp_f is None or ltp_f <= 0:
                    return

                if self.curr_fut_minute != bucket_key:
                    if self.curr_fut_candle is not None:
                        try:
                            self._finalize_fut_candle()
                        except Exception as e:
                            logger.error(f"Error in _finalize_fut_candle: {e}")

                    self.curr_fut_minute = bucket_key
                    self.curr_fut_candle = {
                        "time": bucket_key,
                        "open": ltp_f,
                        "high": ltp_f,
                        "low": ltp_f,
                        "close": ltp_f,
                        "volume": 0.0,
                        "oi": float("nan"),
                    }
                    return

                c = self.curr_fut_candle
                if c is None:
                    return

                c["high"] = max(float(c.get("high", ltp_f)), ltp_f)
                c["low"] = min(float(c.get("low", ltp_f)), ltp_f)
                c["close"] = ltp_f
        except Exception as e:
            logger.error(f"Error in _handle_fut_tick: {e}")

    def _handle_index_tick(self, minute_key: str, ltp: float):
        """Aggregate spot ticks into 5-minute OHLC candles."""
        with self._candle_lock:
            try:
                bucket_key = self._index_bucket_key(minute_key)
            except Exception:
                bucket_key = minute_key

            self.curr_index_minute = minute_key

            if self.curr_index_bucket_minute is None or bucket_key != self.curr_index_bucket_minute:
                if self.curr_index_candle is not None:
                    self._finalize_index_candle()

                self.curr_index_bucket_minute = bucket_key
                self.curr_index_candle = {
                    "time": bucket_key,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                }
                # Compute day gap from first observed tick/candle open for the day.
                day_key = self._extract_day_key(bucket_key)
                if day_key and self._gap_day != day_key:
                    self._update_gap_stats(self.curr_index_candle)
            else:
                c = self.curr_index_candle
                if c is None:
                    return
                c["high"] = max(c["high"], ltp)
                c["low"] = min(c["low"], ltp)
                c["close"] = ltp


    def _finalize_index_candle(self):
        """Persist completed candle and run dependent analytics."""
        with self._candle_lock:
            c = self.curr_index_candle
            if c is None:
                return

            c = dict(c)
            self.curr_index_candle = None
            logger.info(f"Current minute:{self.curr_index_minute}, Finalizing index candle: {c}")
            self.df_index = pd.concat([self.df_index, pd.DataFrame([c])], ignore_index=True)
            self.last_index_bar = c
            self._index_candle_finalized_since_engine_check = True
            self._try_make_merged_bar()

            if self.index_fur_key is None:
                self._apply_indicators_and_engine()

    def _try_make_merged_bar(self) -> None:
        if self.last_index_bar is None:
            return
        if self.index_fur_key is not None and self.last_fut_bar is None:
            return
        if (
            self.index_fur_key is not None
            and self.last_fut_bar is not None
            and self.last_index_bar.get("time") != self.last_fut_bar.get("time")
        ):
            return

        self._refresh_merged_dataframe()
        if self.df_merged.empty:
            return
        self._apply_indicators_and_engine()

    # ------------------------------------------------------------------
    # Indicators + price action + trading engine
    # ------------------------------------------------------------------
    def _apply_indicators_and_engine(self) -> None:
        self._apply_indicators()
        if not self.curr_index_minute:
            return

        if self._is_trading_window(self.curr_index_minute):
            self._trading_engine_active()
        else:
            logger.info(f"Outside Trading Window at {self.curr_index_minute}")

    @staticmethod
    def _wilder_rma(series: pd.Series, length: int) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce").astype(float)
        return numeric.ewm(alpha=1 / float(length), adjust=False, min_periods=length).mean()

    @staticmethod
    def _calculate_ema(series: pd.Series, length: int) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce").astype(float)
        return numeric.ewm(span=int(length), adjust=False, min_periods=int(length)).mean()

    @staticmethod
    def _calculate_wma(series: pd.Series, length: int) -> pd.Series:
        length = int(length)
        numeric = pd.to_numeric(series, errors="coerce").astype(float)
        weights = np.arange(1, length + 1, dtype="float64")
        weight_sum = float(weights.sum())
        return numeric.rolling(window=length, min_periods=length).apply(
            lambda values: float(np.dot(values, weights) / weight_sum),
            raw=True,
        )

    def _calculate_rsi(self, length: int = 7) -> pd.Series:
        close = pd.to_numeric(self.df_index["close"], errors="coerce").astype(float)
        delta = close.diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        avg_gain = self._wilder_rma(gain, length)
        avg_loss = self._wilder_rma(loss, length)
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
        rsi = rsi.mask((avg_gain == 0) & (avg_loss > 0), 0.0)
        rsi = rsi.mask((avg_gain == 0) & (avg_loss == 0), 50.0)
        return rsi.replace([np.inf, -np.inf], np.nan)

    def _calculate_atr(self, length: int = 14) -> pd.Series:
        high = pd.to_numeric(self.df_index["high"], errors="coerce").astype(float)
        low = pd.to_numeric(self.df_index["low"], errors="coerce").astype(float)
        close = pd.to_numeric(self.df_index["close"], errors="coerce").astype(float)
        prev_close = close.shift(1)
        true_range = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return self._wilder_rma(true_range, length)

    def _reset_fast_indicator_state_from_frame(self) -> None:
        if self.df_index is None or self.df_index.empty:
            self._last_indicator_row = -1
            self._fast_vwap_day = None
            self._fast_cum_pv = 0.0
            self._fast_cum_vol = 0.0
            return

        last_idx = len(self.df_index) - 1
        minute_key = self.df_index["time"].astype(str).str.slice(0, 16)
        day_key = str(minute_key.iloc[-1])[:10]
        same_day = minute_key.str.slice(0, 10) == day_key
        close = pd.to_numeric(self.df_index["close"], errors="coerce")
        if "volume_fut" in self.df_index.columns:
            volume = pd.to_numeric(self.df_index["volume_fut"], errors="coerce")
        else:
            fallback = pd.Series(np.nan, index=self.df_index.index)
            volume = pd.to_numeric(self.df_index.get("fut_volume", fallback), errors="coerce")
        pv = (close * volume).where(same_day)
        self._fast_vwap_day = day_key
        self._fast_cum_pv = float(pv.sum(skipna=True))
        self._fast_cum_vol = float(volume.where(same_day).sum(skipna=True))
        self._last_indicator_row = last_idx

    def _apply_indicators(self):
        """
        Applies spot-only indicators:
        - EMA 9
        - RSI 7 + RSI MA 14
        - Hilega-Milega RSI 9, WMA 21 and EMA 3
        - ATR 14 + ADX 14
        - price-action volatility/thrust context
        """
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        hm_rsi_len = max(1, int(sp.get("hm_rsi_length", sp.get("hilega_rsi_length", 9)) or 9))
        hm_wma_len = max(1, int(sp.get("hm_wma_length", sp.get("hilega_wma_length", 21)) or 21))
        hm_ema_len = max(1, int(sp.get("hm_ema_length", sp.get("hilega_ema_length", 3)) or 3))
        hm_midline = float(sp.get("hm_midline", sp.get("hilega_midline", 50)) or 50)

        self._refresh_merged_dataframe()
        self._sync_merged_features_to_index()

        self.df_index['time'] = pd.to_datetime(self.df_index['time'])

        # -------------------------------------------------------
        # 1. Breakout context from recent structure + volatility
        # -------------------------------------------------------
        # Find the Highest High and Lowest Low of the PREVIOUS 'window' candles
        # We use .shift(1) because we want to compare the CURRENT candle against the PAST,
        # not include the current candle in the calculation.

        # 1. Calculate Candle Range (High - Low)
        self.df_index['candle_range'] = self.df_index['high'] - self.df_index['low']

        # 2. Volatility Filter: Count candles with Range > 7 in the rolling window
        # We use .shift(1) to check the 'setup' candles before the current one.
        # (range > 7) gives True/False (1/0). Rolling sum counts them.
        self.df_index['volatile_count'] = (self.df_index['candle_range'] > 8).astype(int).shift(1).rolling(window=4).sum()

        # The condition: Count must be >= 2
        self.df_index['is_volatile'] = self.df_index['volatile_count'] >= 2

        # 3. Find Resistance (Max High) and Support (Min Low) of previous 'window' candles
        self.df_index['recent_high_max'] = self.df_index['high'].shift(1).rolling(window=4).max()
        self.df_index['recent_low_min'] = self.df_index['low'].shift(1).rolling(window=4).min()

        # 4. Generate Signals (Breakout + Volatility Confirmation)
        # Higher High: Breakout AND Volatile Context
        self.df_index['is_hh'] = (self.df_index['high'] > self.df_index['recent_high_max']) & self.df_index['is_volatile']

        # Lower Low: Breakdown AND Volatile Context
        self.df_index['is_ll'] = (self.df_index['low'] < self.df_index['recent_low_min']) & self.df_index['is_volatile']

        # -------------------------------------------------------
        # 2. Standard Indicators (EMA, RSI, ATR, ADX)
        # -------------------------------------------------------
        if len(self.df_index) >= 14:
            self.df_index["ema_9"] = self._calculate_ema(self.df_index['close'], length=9)
            self.df_index['rsi_7'] = self._calculate_rsi(length=7)
            self.df_index['rsi_ma_14'] = self.df_index['rsi_7'].rolling(window=14, min_periods=14).mean()
            self.df_index['hm_rsi_9'] = self._calculate_rsi(length=hm_rsi_len)
            self.df_index['hm_wma_21'] = self._calculate_wma(self.df_index['hm_rsi_9'], length=hm_wma_len)
            self.df_index['hm_ema_3'] = self._calculate_ema(self.df_index['hm_rsi_9'], length=hm_ema_len)
            self.df_index['hm_above_50'] = self.df_index['hm_rsi_9'] > hm_midline
            self.calculate_hm_signals(hm_midline=hm_midline)

            slope_ema = (self.df_index["ema_9"].astype(float) - self.df_index["ema_9"].shift(self._slope_window).astype(float)) / self._slope_window
            slope_rsi_ma = (self.df_index["rsi_ma_14"].astype(float) - self.df_index["rsi_ma_14"].shift(self._slope_window).astype(float)) / self._slope_window

            self.df_index["angle_ema_9"] = np.degrees(np.arctan(np.clip(slope_ema, -10, 10)))
            self.df_index["angle_rsi_ma_14"] = np.degrees(np.arctan(np.clip(slope_rsi_ma, -10, 10)))

            atr = self._calculate_atr(length=14)
            self.df_index['atr_14'] = atr
            high = pd.to_numeric(self.df_index["high"], errors="coerce").to_numpy(dtype="float64")
            low = pd.to_numeric(self.df_index["low"], errors="coerce").to_numpy(dtype="float64")
            close = pd.to_numeric(self.df_index["close"], errors="coerce").to_numpy(dtype="float64")
            adx_14 = talib.ADX(high, low, close, timeperiod=14)
            self.df_index["adx_14"] = pd.Series(np.asarray(adx_14, dtype="float64"), index=self.df_index.index)
            self._refresh_index_trail_state()
            self.check_price_action(safe_float(self.df_index['atr_14'].iloc[-1]))

        self._indicator_revision += 1
        self._reset_fast_indicator_state_from_frame()

    def calculate_hm_signals(self, hm_midline: Optional[float] = None) -> None:
        """
        Calculates Hilega-Milega RSI-WMA-EMA combo signals.
        - bullish: RSI above midline + EMA(3) > WMA(21)
        - bearish: RSI below midline + EMA(3) < WMA(21)
        - neutral: no directional confirmation
        """
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        if hm_midline is None:
            hm_midline = float(sp.get("hm_midline", sp.get("hilega_midline", 50)) or 50)

        hm_upper = safe_float(sp.get("hm_upper", sp.get("hilega_upper", 65)))
        hm_lower = safe_float(sp.get("hm_lower", sp.get("hilega_lower", 35)))
        if hm_upper is None:
            hm_upper = 65.0
        if hm_lower is None:
            hm_lower = 35.0

        hm_wma_21 = pd.to_numeric(self.df_index['hm_wma_21'], errors="coerce")
        hm_ema_3 = pd.to_numeric(self.df_index['hm_ema_3'], errors="coerce")
        hm_rsi_9 = pd.to_numeric(self.df_index['hm_rsi_9'], errors="coerce")

        bullish = (hm_rsi_9 > hm_upper) & (hm_rsi_9 > hm_midline) & (hm_ema_3 > hm_wma_21)
        bearish = (hm_rsi_9 < hm_lower) & (hm_rsi_9 < hm_midline) & (hm_ema_3 < hm_wma_21)
        self.df_index['hm_signal'] = np.select(
            [bullish, bearish],
            ["bullish", "bearish"],
            default="neutral",
        )


    def check_price_action(self,atr):
        """
        Checks for Momentum Thrusts.
        Condition: Consecutive candles + Trend + ONE candle > 10 pts.
        """
        # 1. Basic Candle Properties
        is_red = self.df_index['close'] < self.df_index['open']
        is_green = self.df_index['close'] > self.df_index['open']

        # Stepping Logic
        prev_low = self.df_index['low'].shift(1)
        prev_high = self.df_index['high'].shift(1)

        making_lower_low = self.df_index['low'] < prev_low
        making_higher_high = self.df_index['high'] > prev_high

        # 2. Calculate Ranges
        curr_range = self.df_index['high'] - self.df_index['low']
        prev_range = curr_range.shift(1)

        # 3. Strength Filters
        # A. Minimum 'Pulse' Check: Both candles should be > 3 (Optional, keeps quality high)
        is_alive = (curr_range > 3) & (prev_range > 3)

        # B. THE "BIG BOSS" CANDLE: One of them MUST be > 10 points
        atr_threshold = safe_float(atr)
        if atr_threshold is None or np.isnan(atr_threshold):
            atr_threshold = 10.0
        has_major_move = (curr_range > atr_threshold) | (prev_range > atr_threshold)

        # Final Strength Condition
        is_valid_setup = is_alive & has_major_move

        # ----------------------------------------------------------------
        # 4. PATTERN RECOGNITION
        # ----------------------------------------------------------------

        # BEARISH THRUST (Red + Red + Stepping Down + Big Candle in mix)
        self.df_index['is_bearish_thrust'] = (
            is_red &
            is_red.shift(1) &
            making_lower_low &
            is_valid_setup
        )

        # BULLISH THRUST (Green + Green + Stepping Up + Big Candle in mix)
        self.df_index['is_bullish_thrust'] = (
            is_green &
            is_green.shift(1) &
            making_higher_high &
            is_valid_setup
        )


    def _trading_engine_active(self):
        """
        Entry engine for new positions.
        Applies warm-up, volatility, EMA/RSI-MA and ADX filters
        before switching order state to WAITING.
        """
        try:
            if not self._index_candle_finalized_since_engine_check:
                return
            self._index_candle_finalized_since_engine_check = False

            if not self.enable_trading_engine:
                return

            current_revision = self._indicator_revision
            if self._last_engine_revision == current_revision:
                return
            self._last_engine_revision = current_revision

            if len(self.df_index) < 30:
                return

            if len(self.df_index) <= 27 and self.current_date in ["20260120"]:
                return
            if len(self.df_index) <= 28 and self.current_date in ["20260121"]:
                return
            if len(self.df_index) <= 90 and self.current_date in ["20260216"]:
                return

            sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}

            atr_14 = safe_float(self.df_index.iloc[-1].get('atr_14'))
            if atr_14 is None:
                return
            if atr_14 < float(sp.get("min_atr_14", 9.0)):
                logger.debug(f"ATR range is low {atr_14}")
                return

            ref_ts = self._resolve_reference_ts()
            if self._is_daily_loss_limit_active(ref_ts):
                return

            latest = self.df_index.iloc[-1]
            previous = self.df_index.iloc[-2]

            rsi_ma_14_val = safe_float(latest.get('rsi_ma_14'))
            previous_rsi_ma_14_val = safe_float(previous.get('rsi_ma_14'))
            if rsi_ma_14_val is None or previous_rsi_ma_14_val is None:
                return
            rsi_ma_14 = math.ceil(rsi_ma_14_val)
            previous_rsi_ma_14 = math.ceil(previous_rsi_ma_14_val)

            close_price = safe_float(latest.get('close'))
            ema_9 = safe_float(latest.get('ema_9'))
            adx_14 = safe_float(latest.get('adx_14'))
            previous_adx_14 = safe_float(previous.get('adx_14'))
            angle_ema_9 = safe_float(latest.get('angle_ema_9'))
            angle_rsi_ma_14 = safe_float(latest.get('angle_rsi_ma_14'))
            hm_rsi_9 = safe_float(latest.get('hm_rsi_9'))
            hm_wma_21 = safe_float(latest.get('hm_wma_21'))
            hm_ema_3 = safe_float(latest.get('hm_ema_3'))
            enable_hm_filter = self._coerce_bool(
                sp.get("enable_hilega_milega_filter", sp.get("enable_hm_filter")),
                True,
            )
            if (
                close_price is None
                or ema_9 is None
                or adx_14 is None
                or previous_adx_14 is None
                or angle_ema_9 is None
                or angle_rsi_ma_14 is None
                or (enable_hm_filter and (hm_rsi_9 is None or hm_wma_21 is None or hm_ema_3 is None))
            ):
                return

            is_bearish_thrust = bool(latest.get('is_bearish_thrust', False))
            is_bullish_thrust = bool(latest.get('is_bullish_thrust', False))
            hm_signal = str(latest.get('hm_signal') or "neutral").strip().lower()

            adx_threshold = float(sp.get("adx_threshold", 25))
            up_rsi_low = int(sp.get("up_rsi_low", self.params.get("up_rsi_low", 52)))
            up_rsi_high = int(sp.get("up_rsi_high", self.params.get("up_rsi_high", 73)))
            up_angle_ema = float(sp.get("up_angle_ema", self.params.get("up_angle_ema", 30)))
            up_angle_rsi_ma = float(sp.get("up_angle_rsi_ma", self.params.get("up_angle_rsi_ma", 20)))

            dn_rsi_low = int(sp.get("dn_rsi_low", self.params.get("dn_rsi_low", 27)))
            dn_rsi_high = int(sp.get("dn_rsi_high", self.params.get("dn_rsi_high", 48)))
            dn_angle_ema = float(sp.get("dn_angle_ema", self.params.get("dn_angle_ema", -30)))
            dn_angle_rsi_ma = float(sp.get("dn_angle_rsi_ma", self.params.get("dn_angle_rsi_ma", -20)))

            if self._coerce_bool(sp.get("trade_within_day_open_limits"), False):
                ltp = close_price
                ltp_open_max_distance_points = float(sp.get("ltp_open_max_distance_points", 210))
                if ltp is None or ltp <= 0:
                    return
                if not self._is_ltp_within_open_distance(ltp, ltp_open_max_distance_points):
                    return

            require_price_ema_alignment = self._coerce_bool(sp.get("require_price_ema_alignment"), True)

            logger.debug(
                f"Engine check rsi_ma={rsi_ma_14}/{previous_rsi_ma_14}, close={close_price}, "
                f"ema={ema_9}, adx={adx_14}/{previous_adx_14}, angle_ema={angle_ema_9}, "
                f"angle_rsi_ma={angle_rsi_ma_14}, bullish_thrust={is_bullish_thrust}, "
                f"bearish_thrust={is_bearish_thrust}, hm_rsi={hm_rsi_9}, "
                f"hm_ema={hm_ema_3}, hm_wma={hm_wma_21}, hm_signal={hm_signal}, "
                f"current_candle_range={safe_float(latest.get('candle_range', np.nan))}"
            )

            call_setup = (
                (not enable_hm_filter) or (hm_signal == "bullish")
            )

            put_setup = (
                (not enable_hm_filter) or (hm_signal == "bearish")
            )

            logger.debug(f"condition check call_setup:{call_setup}, put_setup:{put_setup}")

            if call_setup and self._order_container["status"] is None and (self._order_counter < self._max_order_counter):
                lot = self._calculate_lot_size(constants.CALL, is_bullish_thrust, is_bearish_thrust)
                if lot <= 0:
                    return

                self._order_container["side"] = constants.CALL
                self._order_container["status"] = constants.WAITING
                self._order_container["lot"] = int(lot)
                self._order_container["force_trail_lock"] = False
                logger.info(f"Order intent set side={constants.CALL}, lot={lot}, status={constants.WAITING}")
                return

            if put_setup and self._order_container["status"] is None and (self._order_counter < self._max_order_counter):
                lot = self._calculate_lot_size(constants.PUT, is_bullish_thrust, is_bearish_thrust)
                if lot <= 0:
                    return

                self._order_container["side"] = constants.PUT
                self._order_container["status"] = constants.WAITING
                self._order_container["lot"] = int(lot)
                self._order_container["force_trail_lock"] = False
                logger.info(f"Order intent set side={constants.PUT}, lot={lot}, status={constants.WAITING}")
                return

        except Exception as e:
            logger.error(f"An error occurred in _trading_engine_active: {e}", exc_info=True)
            return

    # ------------------------------------------------------------------
    # Trading window + daily guards
    # ------------------------------------------------------------------
    def _init_trade_window_times(self):
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        trade_window = sp.get("trade-window") or sp.get("trade_window") or self.params.get("trade-window") or self.params.get("trade_window") or {}
        if not isinstance(trade_window, dict):
            trade_window = {}
        end_str = str(trade_window.get("end") or "15:10").strip()
        try:
            hh, mm = map(int, end_str.split(":"))
            self._trade_end_time = time(hh, mm)
        except Exception:
            self._trade_end_time = time(15, 10)

    def _is_trading_window(self, time_str: str) -> bool:
        try:
            sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
            trade_window = sp.get("trade-window") or sp.get("trade_window") or self.params.get("trade-window") or self.params.get("trade_window") or {}
            if not isinstance(trade_window, dict):
                trade_window = {}
            market_hours = self.params.get("market-hours", {}) if isinstance(self.params, dict) else {}
            start_time = trade_window.get("start", market_hours.get("start", "09:45"))
            end_time = trade_window.get("end", market_hours.get("end", "14:45"))

            current_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M").time()
            start_time_obj = datetime.strptime(start_time, "%H:%M").time()
            end_time_obj = datetime.strptime(end_time, "%H:%M").time()

            return start_time_obj <= current_time <= end_time_obj
        except Exception as e:
            logger.warning(f"An error occurred in _is_trading_window: {e}")
            return True

    def _resolve_reference_ts(self) -> datetime:
        if self.curr_index_minute:
            try:
                return datetime.strptime(self.curr_index_minute, "%Y-%m-%d %H:%M").replace(tzinfo=ist)
            except Exception:
                pass
        return datetime.now(ist)

    def _get_today_realized_snapshot(self, day_key: str) -> Optional[Dict[str, Any]]:
        orders_csv = getattr(self.order_maneger, "orders_csv", None)
        if not isinstance(orders_csv, str) or not orders_csv:
            return None

        try:
            df = pd.read_csv(orders_csv)
        except Exception:
            return None

        required_cols = {"status", "exit_time", "pnl"}
        if df.empty or not required_cols.issubset(set(df.columns)):
            return {"pnl": 0.0, "trade_ids": set()}

        closed_statuses = {
            constants.TARGET_HIT.upper(),
            constants.STOPLOSS_HIT.upper(),
            constants.MANUAL_EXIT.upper(),
            constants.EOD_SQUARE_OFF.upper(),
        }

        status_s = df["status"].astype(str).str.upper().str.strip()
        exit_s = df["exit_time"].astype(str)
        pnl_s = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)

        mask = status_s.isin(closed_statuses) & exit_s.str.startswith(str(day_key))
        trade_ids = set()
        if "id" in df.columns:
            trade_ids = set(df.loc[mask, "id"].dropna().astype(str).str.strip().tolist())
        return {
            "pnl": float(pnl_s[mask].sum()),
            "trade_ids": trade_ids,
        }

    def _refresh_today_realized_pnl_cache(self, now_ts: Optional[datetime] = None) -> str:
        ref_ts = now_ts or self._resolve_reference_ts()
        if ref_ts.tzinfo is None:
            ref_ts = ref_ts.replace(tzinfo=ist)
        else:
            ref_ts = ref_ts.astimezone(ist)

        day_key = ref_ts.strftime("%Y-%m-%d")

        if self._today_realized_pnl_day != day_key:
            self._today_realized_pnl_day = day_key
            self._today_realized_pnl = 0.0
            self._today_realized_pnl_trade_ids = set()

            snapshot = self._get_today_realized_snapshot(day_key)
            if snapshot is not None:
                self._today_realized_pnl = float(snapshot.get("pnl", 0.0) or 0.0)
                trade_ids = snapshot.get("trade_ids") or set()
                self._today_realized_pnl_trade_ids = set(
                    tid for tid in (str(t).strip() for t in trade_ids) if tid
                )

            if self._daily_loss_blocked_day and self._daily_loss_blocked_day != day_key:
                self._daily_loss_blocked_day = None

        return day_key

    def _update_today_realized_pnl_on_trade_close(self, trade_info: Optional[Dict[str, Any]], ts: Optional[datetime] = None) -> None:
        if not isinstance(trade_info, dict):
            self._today_realized_pnl_day = None
            return

        status = str(trade_info.get("status") or "").strip().upper()
        closed_statuses = {
            constants.TARGET_HIT.upper(),
            constants.STOPLOSS_HIT.upper(),
            constants.MANUAL_EXIT.upper(),
            constants.EOD_SQUARE_OFF.upper(),
        }
        if status not in closed_statuses:
            return

        day_key = self._refresh_today_realized_pnl_cache(ts)

        exit_time = str(trade_info.get("exit_time") or "").strip()
        if exit_time and not exit_time.startswith(day_key):
            return

        trade_id = str(trade_info.get("id") or trade_info.get("trade_id") or "").strip()
        if trade_id and trade_id in self._today_realized_pnl_trade_ids:
            return

        pnl = safe_float(trade_info.get("pnl"))
        if pnl is None:
            self._today_realized_pnl_day = None
            return

        self._today_realized_pnl += float(pnl)
        if trade_id:
            self._today_realized_pnl_trade_ids.add(trade_id)

    def _is_daily_loss_limit_active(self, now_ts: Optional[datetime] = None) -> bool:
        if self._max_daily_loss_pct_of_initial_cash <= 0:
            return False

        day_key = self._refresh_today_realized_pnl_cache(now_ts)

        initial_cash = safe_float(getattr(self.order_maneger, "initial_cash", None))
        if initial_cash is None or initial_cash <= 0:
            return self._daily_loss_blocked_day == day_key

        max_loss_amount = float(initial_cash) * float(self._max_daily_loss_pct_of_initial_cash)
        today_loss_amount = max(-float(self._today_realized_pnl), 0.0)

        if today_loss_amount >= max_loss_amount:
            if self._daily_loss_blocked_day != day_key:
                self._daily_loss_blocked_day = day_key
                logger.warning(
                    f"Daily loss guard activated for {day_key}. "
                    f"Loss={today_loss_amount:.2f} >= Limit={max_loss_amount:.2f} "
                    f"({self._max_daily_loss_pct_of_initial_cash * 100:.2f}% of initial cash), "
                    f"TodayRealizedPnL={self._today_realized_pnl:.2f}"
                )
            return True

        return self._daily_loss_blocked_day == day_key

    def _calculate_lot_size(self,side,is_bullish_thrust,is_bearish_thrust)->int:
        # Position sizing scales with daily bias and current thrust confirmation.
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        lot_cfg = sp.get("lot-size") or sp.get("lot_size") or {}
        small = int(lot_cfg.get("small", 2) or 2)
        medium = int(lot_cfg.get("medium", 2) or 2)
        large = int(lot_cfg.get("large", 2) or 2)

        if side == constants.CALL:
            if self._daily_sentiment == constants.BULLISH and is_bullish_thrust == True:
                return large
            elif  self._daily_sentiment == constants.BULLISH:
                return large
            elif self._daily_sentiment == constants.SIDEWAYS:
                return medium
            elif self._daily_sentiment == constants.BEARISH:
                return small
        elif side == constants.PUT:
            if self._daily_sentiment == constants.BEARISH and is_bearish_thrust == True:
                return large
            elif  self._daily_sentiment == constants.BEARISH:
                return large
            elif self._daily_sentiment == constants.SIDEWAYS:
                return medium
            elif self._daily_sentiment == constants.BULLISH:
                return small

        return small

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

    # ------------------------------------------------------------------
    # Order processing (WAITING -> OPEN -> EOD)
    # ------------------------------------------------------------------
    def _reset_order_container(self) -> None:
        self._order_container = {k: None for k in self._order_container}
        self._order_container["force_trail_lock"] = False

    def _refresh_index_trail_state(self) -> None:
        if self.df_index is None or len(self.df_index) < 2:
            self.index_atr = None
            self.index_adx = None
            self.index_prev_adx = None
            return

        latest = self.df_index.iloc[-1]
        previous = self.df_index.iloc[-2]
        self.index_atr = safe_float(latest.get("atr_14"))
        self.index_adx = safe_float(latest.get("adx_14"))
        self.index_prev_adx = safe_float(previous.get("adx_14"))

    def _should_force_trail_open_order(
        self,
        latest_ltp: Optional[float] = None,
        ts: Optional[datetime] = None,
    ) -> bool:
        if self._order_container.get("status") != constants.OPEN:
            return False
        if self._coerce_bool(self._order_container.get("force_trail_lock"), False):
            return False

        return self._is_stale_losing_open_order(latest_ltp, ts)

    def _is_stale_losing_open_order(
        self,
        latest_ltp: Optional[float],
        ts: Optional[datetime],
    ) -> bool:
        entry_price = safe_float(self._order_container.get("entry_price"))
        current_ltp = safe_float(latest_ltp)
        if entry_price is None or current_ltp is None or current_ltp >= entry_price:
            return False

        trade_create_time = self._normalize_trade_time(self._order_container.get("trade_create_time"))
        current_time = self._normalize_trade_time(ts)
        if trade_create_time is None or current_time is None:
            return False

        return current_time >= trade_create_time + timedelta(minutes=30)

    def _normalize_trade_time(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt_obj = value
        else:
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.isna(parsed):
                return None
            dt_obj = parsed.to_pydatetime()

        if dt_obj.tzinfo is None:
            return dt_obj.replace(tzinfo=ist)
        return dt_obj.astimezone(ist)

    def _get_itm_contracts(self, side: str, index_price: float, itm_range: float) -> Dict[str, Dict[str, Any]]:
        output: Dict[str, Dict[str, Any]] = {}
        spot_price = safe_float(index_price)
        if spot_price is None or spot_price <= 0:
            return output
        if not isinstance(self.selected_contracts, dict):
            return output

        side_key = str(side or "").strip().upper()
        low = spot_price - float(itm_range)
        high = spot_price + float(itm_range)

        call_tokens = {
            str(constants.CALL).upper(),
            str(getattr(constants, "CE", "CE")).upper(),
            "CALL",
            "CE",
        }
        put_tokens = {
            str(constants.PUT).upper(),
            str(getattr(constants, "PE", "PE")).upper(),
            "PUT",
            "PE",
        }

        for strike_price, contracts in self.selected_contracts.items():
            if strike_price == "Nifty_Future":
                continue
            if not isinstance(contracts, list) or not contracts:
                continue

            first_contract = contracts[0] if isinstance(contracts[0], dict) else {}
            strike = safe_float(first_contract.get("strike_price"))
            if strike is None:
                strike = safe_float(strike_price)
            if strike is None:
                continue

            if side_key in call_tokens:
                if not (low <= strike <= spot_price):
                    continue
                allowed_types = call_tokens
            elif side_key in put_tokens:
                if not (spot_price <= strike <= high):
                    continue
                allowed_types = put_tokens
            else:
                continue

            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                instrument_type = str(contract.get("instrument_type") or "").strip().upper()
                if instrument_type not in allowed_types:
                    continue
                instrument_key = contract.get("instrument_key")
                if instrument_key:
                    output[instrument_key] = contract

        return output

    def _trade_processing_from_ws(self, feed_response: List[Dict[str, Any]]) -> None:
        st = self._order_container.get("status")
        needs_wait_pick = (
            self._order_container.get("side") is not None
            and st == constants.WAITING
            and self._order_container.get("instrument_key") is None
        )
        needs_open_manage = (st == constants.OPEN)
        if not (needs_wait_pick or needs_open_manage):
            return

        if not feed_response:
            return

        self._trade_processing(feed_response)

    def _trade_processing(self, feed_response):
        """
        Trade lifecycle processor.
        WAITING: pick best contract and place order.
        OPEN: forward latest tick to OMS and sync local state after exits.
        """
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        dict_itm = {}
        ts = None
        if not feed_response:
            return

        # -------------------------
        # 1) WAITING -> pick contract + place order
        # -------------------------
        if (
            self._order_container.get("side") is not None
            and self._order_container.get("status") == constants.WAITING
            and self._order_container.get("instrument_key") is None
        ):
            logger.info(f"Need to find {self._order_container['side']} side contracts")
            if self._is_daily_loss_limit_active():
                logger.info("Skipping order placement: daily loss guard active. No more new trades for today.")
                self._reset_order_container()
                return

            if not self.last_index_bar:
                return

            index_close = safe_float(self.last_index_bar.get("close"))
            if index_close is None or index_close <= 0:
                return

            itm_range = safe_float(sp.get("itm_strike_range", self.params.get("itm_strike_range", 200)))
            if itm_range is None or itm_range <= 0:
                itm_range = 200.0

            dict_itm = self._get_itm_contracts(
                self._order_container["side"],
                index_close,
                itm_range,
            )

            if not dict_itm:
                return

            max_gamma = -1e18
            chosen = None
            for item in feed_response:
                ik = item.get("instrument_key")
                g = item.get("gamma")
                if ik in dict_itm and g is not None and float(g) > float(max_gamma):
                    chosen = item
                    max_gamma = float(g)

            if not chosen:
                best_ltp = -1.0
                for item in feed_response:
                    ik = item.get("instrument_key")
                    ltp = safe_float(item.get("ltp"))
                    if ik in dict_itm and ltp is not None and ltp > best_ltp:
                        chosen = item
                        best_ltp = ltp
                max_gamma = None

            if not chosen:
                return

            self._order_container["instrument_key"] = chosen["instrument_key"]
            self._order_container["ltp"] = float(chosen["ltp"])
            self._order_container["max_gamma"] = max_gamma

            itm = dict_itm[chosen["instrument_key"]]
            self._order_container["instrument_symbol"] = itm.get("trading_symbol")

            ts = datetime.fromtimestamp(chosen["ts_epoch_ms"] / 1000, tz=ist)

            contract = dict_itm.get(self._order_container["instrument_key"])
            if not contract:
                return

            lot = self._order_container.get("lot")
            lot_size = contract.get("lot_size")
            try:
                lot = int(float(lot))
                lot_size = int(float(lot_size))
            except (TypeError, ValueError):
                logger.error(f"Invalid lot/lot_size. lot={lot} lot_size={lot_size}")
                return
            qty = lot * lot_size

            TICK = float(
                sp.get(
                    "tick-size",
                    sp.get("tick_size", self.params.get("tick-size", self.params.get("tick_size", 0.05))),
                )
            )
            entry_price = float(self._order_container["ltp"])

            use_option_atr_risk = self._coerce_bool(
                sp.get("use_option_atr_risk", self.params.get("use_option_atr_risk", True)),
                True,
            )
            require_option_atr = self._coerce_bool(
                sp.get("require_option_atr", self.params.get("require_option_atr", True)),
                True,
            )
            atr_target_mult = float(
                sp.get("atr_target_mult", self.params.get("atr_target_mult", 5.0))
            )
            atr_sl_mult = float(
                sp.get("atr_sl_mult", self.params.get("atr_sl_mult", 1.1))
            )
            max_atr_for_contract = float(sp.get("max_atr_for_contract", self.params.get("max_atr_for_contract", 20)))
            min_atr_for_contract = float(sp.get("min_atr_for_contract", self.params.get("min_atr_for_contract", 10)))
            trailing_factor = float(
                sp.get(
                    "trailing-factor",
                    sp.get(
                        "trailing_factor",
                        self.params.get("trailing-factor", self.params.get("trailing_factor", 2.0)),
                    ),
                )
            )

            option_atr = self.atr5_engine.get_atr(chosen["instrument_key"])
            target = None
            sl_trigger = None
            start_trail_after = None
            risk_mode = "pct"

            if use_option_atr_risk and option_atr is not None and option_atr > 0:
                atr_to_use = option_atr
                target = entry_price + (atr_target_mult * option_atr)
                sl_trigger = entry_price - (atr_sl_mult * option_atr)

                if option_atr > max_atr_for_contract:
                    atr_to_use = max_atr_for_contract

                start_trail_after = float((atr_to_use * trailing_factor) / entry_price)

                if option_atr < min_atr_for_contract:
                    sl_trigger = entry_price - (atr_sl_mult * min_atr_for_contract)

                risk_mode = "atr"
            else:
                if use_option_atr_risk and require_option_atr:
                    logger.warning(f"Skipping order; option ATR unavailable for {chosen['instrument_key']}")
                    self._order_container["instrument_key"] = None
                    self._order_container["ltp"] = None
                    self._order_container["max_gamma"] = None
                    self._order_container["instrument_symbol"] = None
                    return

                tp_pct = float(
                    sp.get(
                        "take-profit",
                        sp.get("take_profit", self.params.get("take-profit", self.params.get("take_profit", 0.30))),
                    )
                )
                sl_pct = float(
                    sp.get(
                        "stop-loss",
                        sp.get("stop_loss", self.params.get("stop-loss", self.params.get("stop_loss", 0.20))),
                    )
                )
                target = entry_price * (1.0 + tp_pct)
                sl_trigger = entry_price * (1.0 - sl_pct)
                start_trail_after = float(
                    sp.get(
                        "trail-start-after-points",
                        sp.get(
                            "trail_start_after_points",
                            self.params.get("trail-start-after-points", self.params.get("trail_start_after_points", 0.1)),
                        ),
                    )
                )
                start_trail_after = max(start_trail_after, 0.0)
                atr_to_use = safe_float(
                    sp.get("trailing-stop-distance", sp.get("trailing_stop_distance", 10))
                ) or 10.0

            gap = float(sp.get("sl-limit-gap", sp.get("sl_limit_gap", 0.5)))
            sl_limit = float(sl_trigger) - gap

            sl_trigger = self._round_to_tick(float(sl_trigger), TICK, "CEIL")
            sl_limit = self._round_to_tick(float(sl_limit), TICK, "FLOOR")
            if sl_limit >= sl_trigger:
                sl_limit = self._round_to_tick(sl_trigger - TICK, TICK, "FLOOR")
            target = self._round_to_tick(float(target), TICK, "CEIL")

            trailing_enabled = self._coerce_bool(sp.get("trailing-stop", sp.get("trailing_stop", True)), True)
            trail_points = atr_to_use
            self._order_container["start_trail_after"] = start_trail_after

            description = (
                f"{self._order_container['side']} {self._order_container['instrument_symbol']} "
                f"entry={entry_price:.2f}"
            )

            trade_id = self.order_maneger.buy(
                symbol=self._order_container["instrument_symbol"],
                instrument_token=self._order_container["instrument_key"],
                qty=qty,
                entry_price=entry_price,
                sl_trigger=sl_trigger,
                sl_limit=sl_limit,
                target=target,
                trail_points=(trail_points if trailing_enabled else None),
                start_trail_after=start_trail_after,
                description=description,
                ts=ts,
            )

            logger.info(
                f"OrderInfo TradeID: {trade_id}, Entry(PU): {entry_price:.2f}, Qty: {qty}, "
                f"Target(PU): {target:.2f}, SL_trig(PU): {sl_trigger:.2f}, "
                f"SL_lim(PU): {sl_limit:.2f}, TrailOn: {trailing_enabled}, TrailDist: {trail_points:.2f}, "
                f"TrailStartAfterPts: {(entry_price + (entry_price * start_trail_after)):.2f} "
                f"start_trail_after: {start_trail_after}, RiskMode: {risk_mode}, "
                f"OptionATR: {option_atr}, TrailingFactor: {trailing_factor}"
            )

            if trade_id:
                self._order_container["trade_id"] = trade_id
                self._order_container["status"] = constants.OPEN
                self._order_container["entry_price"] = entry_price
                self._order_container["trade_create_time"] = ts
                self._order_container["force_trail_lock"] = False
                self._order_counter += 1
                logger.info(f"{self._order_container}")

            return

        # -------------------------
        # 2) OPEN -> feed bars to OMS
        # -------------------------
        if self._order_container.get("status") == constants.OPEN:
            latest_ltp = None
            ts = None

            for item in feed_response:
                if item.get("instrument_key") == self._order_container.get("instrument_key"):
                    latest_ltp = float(item["ltp"])
                    ts = datetime.fromtimestamp(item["ts_epoch_ms"] / 1000, tz=ist)
                    break

            if latest_ltp is not None and ts is not None:
                force_trail = self._should_force_trail_open_order(latest_ltp, ts)
                tick_result = self.order_maneger.on_tick(
                    symbol=self._order_container["instrument_symbol"],
                    o=latest_ltp, h=latest_ltp, l=latest_ltp, c=latest_ltp,
                    ts=ts,
                    force_trail=force_trail,
                )
                force_trail_applied = (
                    isinstance(tick_result, dict)
                    and self._coerce_bool(tick_result.get("force_trail_applied"), False)
                )
                if force_trail_applied:
                    self._order_container["force_trail_lock"] = True

                trade_id = self._order_container.get("trade_id")
                trade_info = tick_result if isinstance(tick_result, dict) else None
                if trade_info is None and hasattr(self.order_maneger, "maybe_refresh_trade"):
                    trade_info = self.order_maneger.maybe_refresh_trade(trade_id, ts=ts)
                if trade_info is None:
                    trade_info = self.order_maneger.get_trade_by_id(trade_id)
                if trade_info and trade_info.get("status") in [
                    constants.TARGET_HIT,
                    constants.STOPLOSS_HIT,
                    constants.MANUAL_EXIT,
                    constants.EOD_SQUARE_OFF,
                ]:
                    logger.debug(f"Trade closed Info: {trade_info}")
                    self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                    self._reset_order_container()

        if self.curr_index_minute:
            current_time = datetime.strptime(self.curr_index_minute, "%Y-%m-%d %H:%M").time()

            # Hard EOD cleanup to avoid overnight carry.
            if current_time >= self._trade_end_time:
                trade_id = self._order_container.get("trade_id")
                if trade_id:
                    latest_ltp = float(self._order_container.get("ltp") or 0.0)
                    for item in feed_response:
                        if item.get("instrument_key") == self._order_container.get("instrument_key"):
                            latest_ltp = float(item["ltp"])
                            break

                    square_ts = self._resolve_reference_ts()
                    self.order_maneger.square_off_trade(
                        trade_id=trade_id,
                        exit_price=float(latest_ltp),
                        ts=square_ts,
                        reason=constants.EOD_SQUARE_OFF,
                    )
                    trade_info = self.order_maneger.get_trade_by_id(trade_id)
                    self._update_today_realized_pnl_on_trade_close(trade_info, ts=square_ts)
                    self._reset_order_container()
