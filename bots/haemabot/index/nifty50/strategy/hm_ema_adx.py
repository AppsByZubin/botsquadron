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
from datetime import datetime,time,timedelta
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

logger = logger.create_logger("HmEmaAdxStrategyLogger")


class HmEmaAdxStrategy:
    """
    Spot-only EMA + ADX intraday strategy.

    Flow:
    1) Build index candles from ticks.
    2) Compute EMA, RSI-MA, ATR, ADX and price-action context.
    3) Delegate live trade lifecycle to order manager.
    """
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
        oil_minutes_processed: Optional[Dict[str, bool]] = None,
        intraday_index_candles=None,
        intraday_future_candles=None,
        intraday_oil_candles=None,
        option_exipry_date: Optional[str] = None,
    ):
        self.current_date = current_date or datetime.now(ist).strftime("%Y%m%d")
        self.expiry_date = expiry_date or option_exipry_date
        self.uptox_client = uptox_client
        self.previous_day_trend = previous_day_trend
        self.selected_contracts = selected_contracts or {}
        self.index_minutes_processed = index_minutes_processed or {}
        self.future_minutes_processed = future_minutes_processed or {}
        self.oil_minutes_processed = oil_minutes_processed or {}
        self.curr_index_candle = None
        self.curr_index_minute = None
        self.curr_fut_candle = None
        self.curr_fut_minute = None
        self.last_fut_bar: Optional[Dict] = None
        self.curr_oil_candle = None
        self.curr_oil_minute = None
        self.last_oil_bar: Optional[Dict] = None
        self.future_data_from_parquet = False
        self.last_index_bar: Optional[Dict] = None
        self._fut_vol_minute = None
        self._fut_vol_start_vtt = None
        self._fut_vol_last_vtt = None
        self._fut_vol_by_minute: Dict[str, float] = {}
        self._oil_vol_minute = None
        self._oil_vol_start_vtt = None
        self._oil_vol_last_vtt = None
        self._oil_vol_by_minute: Dict[str, float] = {}
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
            "close_oil": pd.Series(dtype="float64"),
            "volume_oil": pd.Series(dtype="float64"),
            "oi_oil": pd.Series(dtype="float64"),
            "oil_last5_max_candle_points": pd.Series(dtype="float64"),
            "oil_large_candle_side": pd.Series(dtype="object"),
            "oil_large_candle_time": pd.Series(dtype="object"),
            "oil_has_large_up_candle": pd.Series(dtype="bool"),
            "oil_has_large_down_candle": pd.Series(dtype="bool"),
            "oil_last5_max_volume": pd.Series(dtype="float64"),
            "oil_has_large_volume": pd.Series(dtype="bool"),
            "oil_large_volume_time": pd.Series(dtype="object"),
        })

        self.df_oil = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "oi": pd.Series(dtype="float64")
        })

        self.params = params if isinstance(params, dict) else self._get_params_from_yaml()
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        self.oil_track_enabled = self._coerce_bool(
            sp.get("OIL_TRACK", self.params.get("OIL_TRACK", False)),
            False,
        )
        self._oil_large_candle_lookback = max(
            1,
            int(sp.get("oil_large_candle_lookback", self.params.get("oil_large_candle_lookback", 5)) or 5),
        )
        self._oil_large_candle_threshold = float(
            sp.get("oil_large_candle_threshold", self.params.get("oil_large_candle_threshold", 150)) or 150
        )
        self._oil_large_volume_threshold = float(
            sp.get("oil_large_volume_threshold", self.params.get("oil_large_volume_threshold", 1000)) or 1000
        )
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
        self.index_fur_key = self._get_index_fut_key()
        nifty_fut = self.selected_contracts.get("Nifty_Future") if isinstance(self.selected_contracts, dict) else None
        if self.index_fur_key is None and isinstance(nifty_fut, dict):
            self.index_fur_key = nifty_fut.get("instrument_key")
        self.oil_key = self._get_oil_key()
        crude_oil_fut = self.selected_contracts.get("CrudeOil_Future") if isinstance(self.selected_contracts, dict) else None
        if self.oil_track_enabled and self.oil_key is None and isinstance(crude_oil_fut, dict):
            self.oil_key = crude_oil_fut.get("instrument_key")
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
            "close_oil": pd.Series(dtype="float64"),
            "volume_oil": pd.Series(dtype="float64"),
            "oi_oil": pd.Series(dtype="float64"),
            "oil_last5_max_candle_points": pd.Series(dtype="float64"),
            "oil_large_candle_side": pd.Series(dtype="object"),
            "oil_large_candle_time": pd.Series(dtype="object"),
            "oil_has_large_up_candle": pd.Series(dtype="bool"),
            "oil_has_large_down_candle": pd.Series(dtype="bool"),
            "oil_last5_max_volume": pd.Series(dtype="float64"),
            "oil_has_large_volume": pd.Series(dtype="bool"),
            "oil_large_volume_time": pd.Series(dtype="object"),
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
        self._post_exit_cooldown_minutes = int(sp.get("post_exit_cooldown_minutes", 5) or 5)
        self._post_exit_cooldown_until: Optional[datetime] = None
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
            "lot": None,
            "max_gamma": None,
            "start_trail_after": None,
            "oil_context": None,
            "force_trail_lock": False
        }
        self._trade_end_time=None
        self._init_trade_window_times()
        self._setup_gap_state()
        if (
            intraday_index_candles is not None
            or intraday_future_candles is not None
            or (self.oil_track_enabled and intraday_oil_candles is not None)
        ):
            self._initialize_from_intraday_candles(
                intraday_index_candles,
                intraday_future_candles,
                intraday_oil_candles,
            )

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
    def _initialize_from_intraday_candles(self, index_candles, fut_candles, oil_candles=None) -> None:
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

        df_i = build_df(index_candles, include_volume=False)
        df_f = build_df(fut_candles, include_volume=True)
        df_o = build_df(oil_candles, include_volume=True) if self.oil_track_enabled else pd.DataFrame()

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

        if not df_f.empty:
            self.df_index_future = pd.concat([self.df_index_future, df_f], ignore_index=True)
            self.last_fut_bar = df_f.iloc[-1].to_dict()
            for minute_key in df_f["time"].astype(str):
                self.future_minutes_processed[minute_key] = True

        if not df_o.empty:
            self.df_oil = pd.concat([self.df_oil, df_o], ignore_index=True)
            self.last_oil_bar = df_o.iloc[-1].to_dict()
            for minute_key in df_o["time"].astype(str):
                self.oil_minutes_processed[minute_key] = True

        self._refresh_merged_dataframe()
        if not df_i.empty:
            self._apply_indicators_and_engine()

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

    def _get_oil_key(self):
        if self.params and 'data-sources' in self.params:
            sources_dict = self.params['data-sources']
            if 'crudeoil-future' in sources_dict:
                return sources_dict['crudeoil-future']
            if 'oil-future' in sources_dict:
                return sources_dict['oil-future']
        return None

    @staticmethod
    def _oil_context_columns() -> List[str]:
        return [
            "oil_last5_max_candle_points",
            "oil_large_candle_side",
            "oil_large_candle_time",
            "oil_has_large_up_candle",
            "oil_has_large_down_candle",
            "oil_last5_max_volume",
            "oil_has_large_volume",
            "oil_large_volume_time",
        ]

    @staticmethod
    def _oil_bool_context_columns() -> List[str]:
        return [
            "oil_has_large_up_candle",
            "oil_has_large_down_candle",
            "oil_has_large_volume",
        ]

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

    def _prepare_oil_feature_frame(self) -> pd.DataFrame:
        output_cols = ["time", "close_oil", "volume_oil", "oi_oil"] + self._oil_context_columns()
        if self.df_oil is None or self.df_oil.empty or "time" not in self.df_oil.columns:
            return pd.DataFrame(columns=output_cols)

        source_cols = ["time", "open", "close", "volume", "oi"]
        available_cols = [col for col in source_cols if col in self.df_oil.columns]
        oil = self.df_oil[available_cols].copy()
        oil["time"] = pd.to_datetime(oil["time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        oil = oil.dropna(subset=["time"])
        if oil.empty:
            return pd.DataFrame(columns=output_cols)

        for col in ["open", "close", "volume", "oi"]:
            if col not in oil.columns:
                oil[col] = np.nan
            oil[col] = pd.to_numeric(oil[col], errors="coerce")

        sort_key = pd.to_datetime(oil["time"], errors="coerce")
        oil = (
            oil.assign(_sort_time=sort_key)
            .sort_values("_sort_time")
            .drop(columns=["_sort_time"])
            .drop_duplicates(subset=["time"], keep="last")
            .reset_index(drop=True)
        )

        signed_body = oil["close"] - oil["open"]
        candle_points = signed_body.abs()
        volume = oil["volume"]
        lookback = max(1, int(self._oil_large_candle_lookback))
        candle_threshold = float(self._oil_large_candle_threshold)
        volume_threshold = float(self._oil_large_volume_threshold)

        context_rows = []
        for pos in range(len(oil)):
            start = max(0, pos - lookback + 1)
            signed_window = signed_body.iloc[start:pos + 1]
            candle_window = candle_points.iloc[start:pos + 1]
            volume_window = volume.iloc[start:pos + 1]

            valid_candles = candle_window.dropna()
            if valid_candles.empty:
                max_candle_points = np.nan
                large_candle_side = "none"
                large_candle_time = None
            else:
                max_candle_idx = valid_candles.idxmax()
                max_candle_points = float(valid_candles.loc[max_candle_idx])
                if max_candle_points > candle_threshold:
                    signed_value = safe_float(signed_body.loc[max_candle_idx])
                    large_candle_side = "up" if signed_value is not None and signed_value > 0 else "down"
                    large_candle_time = oil.at[max_candle_idx, "time"]
                else:
                    large_candle_side = "none"
                    large_candle_time = None

            valid_volume = volume_window.dropna()
            if valid_volume.empty:
                max_volume = np.nan
                large_volume_time = None
                has_large_volume = False
            else:
                max_volume_idx = valid_volume.idxmax()
                max_volume = float(valid_volume.loc[max_volume_idx])
                large_volume_time = oil.at[max_volume_idx, "time"]
                has_large_volume = bool(max_volume > volume_threshold)

            context_rows.append({
                "oil_last5_max_candle_points": max_candle_points,
                "oil_large_candle_side": large_candle_side,
                "oil_large_candle_time": large_candle_time,
                "oil_has_large_up_candle": bool((signed_window > candle_threshold).any()),
                "oil_has_large_down_candle": bool((signed_window < -candle_threshold).any()),
                "oil_last5_max_volume": max_volume,
                "oil_has_large_volume": has_large_volume,
                "oil_large_volume_time": large_volume_time,
            })

        context = pd.DataFrame(context_rows)
        out = oil[["time"]].copy()
        out["close_oil"] = oil["close"]
        out["volume_oil"] = oil["volume"]
        out["oi_oil"] = oil["oi"]
        out = pd.concat([out, context], axis=1)

        return out[output_cols]

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

        if self.oil_track_enabled:
            oil_cols = ["close_oil", "volume_oil", "oi_oil"] + self._oil_context_columns()
            oil = self._prepare_oil_feature_frame()
            if not oil.empty:
                merged = merged.merge(oil, on="time", how="left")
            else:
                for col in oil_cols:
                    merged[col] = np.nan

            for col in self._oil_bool_context_columns():
                if col in merged.columns:
                    merged[col] = merged[col].fillna(False).astype(bool)
            if "oil_large_candle_side" in merged.columns:
                merged["oil_large_candle_side"] = merged["oil_large_candle_side"].fillna("none")

        sort_key = pd.to_datetime(merged["time"], errors="coerce")
        merged = merged.assign(_sort_time=sort_key).sort_values("_sort_time").drop(columns=["_sort_time"])
        self.df_merged = merged.reset_index(drop=True)

    def _sync_merged_features_to_index(self) -> None:
        if self.df_index is None or self.df_index.empty or self.df_merged is None or self.df_merged.empty:
            return

        feature_cols = ["close_fut", "volume_fut"]
        if self.oil_track_enabled:
            feature_cols.extend(["close_oil", "volume_oil", "oi_oil"] + self._oil_context_columns())
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
        logger.info("WebSocket reconnected; hm_ema_adx strategy state preserved.")

    def get_subscription_instruments(self) -> List[str]:
        instruments: List[str] = []

        def add_instrument(instrument_key: Any) -> None:
            if instrument_key and instrument_key not in instruments:
                instruments.append(instrument_key)

        add_instrument(constants.NIFTY50_SYMBOL)

        if isinstance(self.selected_contracts, dict):
            for key, value in self.selected_contracts.items():
                if key == "CrudeOil_Future" and not self.oil_track_enabled:
                    continue
                if isinstance(value, dict):
                    add_instrument(value.get("instrument_key"))
                    continue
                if isinstance(value, list):
                    for contract in value:
                        if isinstance(contract, dict):
                            add_instrument(contract.get("instrument_key"))

        if self.oil_track_enabled:
            add_instrument(self.oil_key)

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

        if self.oil_track_enabled and self.oil_key is not None and instrument_key == self.oil_key:
            ltp = safe_float(item.get("ltp"))
            if ltp is None:
                return
            with self._candle_lock:
                vtt = safe_float(item.get("vtt"))
                if vtt is not None:
                    finished_minute, finished_vol = self._update_oil_1m_volume_from_vtt(minute_key, vtt)
                    if finished_minute is not None:
                        self._oil_vol_by_minute[finished_minute] = float(finished_vol)
                self._handle_oil_tick(minute_key, float(ltp), safe_float(item.get("oi")))
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

    def _update_oil_1m_volume_from_vtt(self, minute_key: str, vtt_now: float):
        """
        Compute one-minute traded volume from cumulative crude oil VTT.
        """
        try:
            if self._oil_vol_minute is None:
                self._oil_vol_minute = minute_key
                self._oil_vol_start_vtt = vtt_now
                self._oil_vol_last_vtt = vtt_now
                return None, None

            if self._oil_vol_last_vtt is not None and vtt_now < float(self._oil_vol_last_vtt):
                self._oil_vol_minute = minute_key
                self._oil_vol_start_vtt = vtt_now
                self._oil_vol_last_vtt = vtt_now
                return None, None

            if minute_key == self._oil_vol_minute:
                self._oil_vol_last_vtt = vtt_now
                return None, None

            finished_minute = self._oil_vol_minute
            finished_volume = max(float(self._oil_vol_last_vtt) - float(self._oil_vol_start_vtt), 0.0)

            self._oil_vol_minute = minute_key
            self._oil_vol_start_vtt = vtt_now
            self._oil_vol_last_vtt = vtt_now

            return finished_minute, finished_volume
        except Exception as e:
            logger.error(f"Error in _update_oil_1m_volume_from_vtt: {e}")
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
        """Build 1-minute OHLC for FUT using ltp."""
        try:
            with self._candle_lock:
                if minute_key is None:
                    return
                minute_key = str(minute_key)

                ltp_f = safe_float(ltp)
                if ltp_f is None or ltp_f <= 0:
                    return

                if self.curr_fut_minute != minute_key:
                    if self.curr_fut_candle is not None:
                        try:
                            self._finalize_fut_candle()
                        except Exception as e:
                            logger.error(f"Error in _finalize_fut_candle: {e}")

                    self.curr_fut_minute = minute_key
                    self.curr_fut_candle = {
                        "time": minute_key,
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

    def _upsert_oil_candle(self, candle: Dict[str, Any]) -> None:
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

        if self.df_oil is None or self.df_oil.empty:
            self.df_oil = pd.DataFrame([row])
            return

        time_col = self.df_oil["time"]
        if pd.api.types.is_datetime64_any_dtype(time_col):
            minute_dt = pd.to_datetime(minute_key, errors="coerce")
            if pd.isna(minute_dt):
                return
            row["time"] = minute_dt
            matched = self.df_oil.index[time_col == minute_dt]
        else:
            matched = self.df_oil.index[time_col.astype(str) == minute_key]

        if len(matched) > 0:
            idx = matched[-1]
            for col, value in row.items():
                self.df_oil.at[idx, col] = value
        else:
            self.df_oil = pd.concat([self.df_oil, pd.DataFrame([row])], ignore_index=True)

    def _finalize_oil_candle(self) -> None:
        with self._candle_lock:
            candle = self.curr_oil_candle
            if candle is None:
                return

            candle = dict(candle)
            self.curr_oil_candle = None
            minute = str(candle.get("time") or "")
            if minute in self._oil_vol_by_minute:
                candle["volume"] = float(self._oil_vol_by_minute.pop(minute, 0.0))
            logger.info(f"Finalizing crude oil candle: {candle}")
            self._upsert_oil_candle(candle)
            self.last_oil_bar = candle
            self._try_make_merged_bar()

    def _handle_oil_tick(self, minute_key: str, ltp: float, oi: Optional[float] = None) -> None:
        """Build 1-minute OHLCV for crude oil using ltp and cumulative VTT."""
        if not self.oil_track_enabled:
            return

        try:
            with self._candle_lock:
                if minute_key is None:
                    return
                minute_key = str(minute_key)

                ltp_f = safe_float(ltp)
                if ltp_f is None or ltp_f <= 0:
                    return

                oi_f = safe_float(oi)
                if self.curr_oil_minute != minute_key:
                    if self.curr_oil_candle is not None:
                        try:
                            self._finalize_oil_candle()
                        except Exception as e:
                            logger.error(f"Error in _finalize_oil_candle: {e}")

                    self.curr_oil_minute = minute_key
                    self.curr_oil_candle = {
                        "time": minute_key,
                        "open": ltp_f,
                        "high": ltp_f,
                        "low": ltp_f,
                        "close": ltp_f,
                        "volume": 0.0,
                        "oi": oi_f if oi_f is not None else float("nan"),
                    }
                    return

                c = self.curr_oil_candle
                if c is None:
                    return

                c["high"] = max(float(c.get("high", ltp_f)), ltp_f)
                c["low"] = min(float(c.get("low", ltp_f)), ltp_f)
                c["close"] = ltp_f
                if oi_f is not None:
                    c["oi"] = oi_f
        except Exception as e:
            logger.error(f"Error in _handle_oil_tick: {e}")

    def _handle_index_tick(self, minute_key: str, ltp: float):
        """Aggregate spot ticks into 1-minute OHLC candles."""
        with self._candle_lock:
            # New minute?
            if self.curr_index_minute is None or minute_key != self.curr_index_minute:
                # finalize previous candle if exists
                if self.curr_index_candle is not None:
                    self._finalize_index_candle()

                # start new candle
                self.curr_index_minute = minute_key
                self.curr_index_candle = {
                    "time": minute_key,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                }
                # Compute day gap from first observed tick/candle open for the day.
                day_key = self._extract_day_key(minute_key)
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
            if not self.enable_trading_engine:
                return

            if len(self.df_index) < 30:
                return

            sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}

            atr_14 = safe_float(self.df_index.iloc[-1].get('atr_14'))
            if atr_14 is None:
                return
            if atr_14 < float(sp.get("min_atr_14", 9.0)):
                logger.debug(f"ATR range is low {atr_14}")
                return

            ref_ts = self._resolve_reference_ts()
            if self._is_post_exit_cooldown_active(ref_ts):
                cooldown_left_sec = int(max((self._post_exit_cooldown_until - ref_ts).total_seconds(), 0))
                logger.debug(
                    f"Entry blocked by post-exit cooldown for {cooldown_left_sec}s "
                    f"(until {self._post_exit_cooldown_until.strftime('%H:%M:%S')})"
                )
                return

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
            oil_has_large_up_candle = self._coerce_bool(latest.get("oil_has_large_up_candle"), False)
            oil_has_large_down_candle = self._coerce_bool(latest.get("oil_has_large_down_candle"), False)
            oil_has_large_volume = self._coerce_bool(latest.get("oil_has_large_volume"), False)
            oil_blocks_call = oil_has_large_up_candle and oil_has_large_volume
            oil_blocks_put = oil_has_large_down_candle and oil_has_large_volume

            adx_threshold = float(sp.get("adx_threshold", 25))
            up_rsi_low = int(sp.get("up_rsi_low", self.params.get("up_rsi_low", 52)))
            up_rsi_high = int(sp.get("up_rsi_high", self.params.get("up_rsi_high", 73)))
            up_angle_ema = float(sp.get("up_angle_ema", self.params.get("up_angle_ema", 50)))
            up_angle_rsi_ma = float(sp.get("up_angle_rsi_ma", self.params.get("up_angle_rsi_ma", 20)))

            dn_rsi_low = int(sp.get("dn_rsi_low", self.params.get("dn_rsi_low", 27)))
            dn_rsi_high = int(sp.get("dn_rsi_high", self.params.get("dn_rsi_high", 48)))
            dn_angle_ema = float(sp.get("dn_angle_ema", self.params.get("dn_angle_ema", -50)))
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
                f"oil_up_large={oil_has_large_up_candle}, oil_down_large={oil_has_large_down_candle}, "
                f"oil_large_volume={oil_has_large_volume}, oil_blocks_call={oil_blocks_call}, "
                f"oil_blocks_put={oil_blocks_put}, "
                f"current_candle_range={safe_float(latest.get('candle_range', np.nan))}"
            )
            
            call_setup = (
                (angle_rsi_ma_14 > up_angle_rsi_ma)
                and (up_rsi_low < previous_rsi_ma_14 < rsi_ma_14 < up_rsi_high)
                and ((not require_price_ema_alignment) or (close_price > ema_9))
                and (angle_ema_9 > up_angle_ema)
                and (adx_14 > adx_threshold and adx_14 > previous_adx_14)
                and ((not enable_hm_filter) or (hm_signal == "bullish"))
                and not oil_blocks_call
            )

            put_setup = (
                (angle_rsi_ma_14 < dn_angle_rsi_ma)
                and (dn_rsi_low < rsi_ma_14 < previous_rsi_ma_14 < dn_rsi_high)
                and ((not require_price_ema_alignment) or (close_price < ema_9))
                and (angle_ema_9 < dn_angle_ema)
                and (adx_14 > adx_threshold and adx_14 > previous_adx_14)
                and ((not enable_hm_filter) or (hm_signal == "bearish"))
                and not oil_blocks_put
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

    def _set_post_exit_cooldown(self, exit_status: Optional[str], ts: Optional[datetime] = None) -> None:
        status = str(exit_status or "").strip().upper()
        if status not in {constants.STOPLOSS_HIT.upper(), constants.TARGET_HIT.upper()}:
            return
        if self._post_exit_cooldown_minutes <= 0:
            return

        ref_ts = ts or self._resolve_reference_ts()
        if ref_ts.tzinfo is None:
            ref_ts = ref_ts.replace(tzinfo=ist)

        cooldown_until = ref_ts + timedelta(minutes=self._post_exit_cooldown_minutes)
        if self._post_exit_cooldown_until is None or cooldown_until > self._post_exit_cooldown_until:
            self._post_exit_cooldown_until = cooldown_until

        logger.info(
            f"Entry cooldown started due to '{exit_status}' until "
            f"{self._post_exit_cooldown_until.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )

    def _is_post_exit_cooldown_active(self, now_ts: Optional[datetime] = None) -> bool:
        if self._post_exit_cooldown_until is None:
            return False

        ref_ts = now_ts or self._resolve_reference_ts()
        if ref_ts.tzinfo is None:
            ref_ts = ref_ts.replace(tzinfo=ist)

        if ref_ts >= self._post_exit_cooldown_until:
            self._post_exit_cooldown_until = None
            return False

        return True

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

    def _should_force_trail_open_order(self) -> bool:
        if self._order_container.get("status") != constants.OPEN:
            return False
        if self._coerce_bool(self._order_container.get("force_trail_lock"), False):
            return False

        if self.df_index is not None and not self.df_index.empty:
            latest = self.df_index.iloc[-1]
            side = str(self._order_container.get("side") or "").strip().upper()
            oil_has_large_volume = self._coerce_bool(latest.get("oil_has_large_volume"), False)
            oil_has_large_up_candle = self._coerce_bool(latest.get("oil_has_large_up_candle"), False)
            oil_has_large_down_candle = self._coerce_bool(latest.get("oil_has_large_down_candle"), False)

            if oil_has_large_volume:
                if side in {constants.CALL, constants.CE} and oil_has_large_up_candle:
                    logger.info(
                        "Force trailing CALL: crude oil has large up candle with large volume."
                    )
                    return True
                if side in {constants.PUT, constants.PE} and oil_has_large_down_candle:
                    logger.info(
                        "Force trailing PUT: crude oil has large down candle with large volume."
                    )
                    return True

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        min_atr_14 = safe_float(sp.get("min_atr_14", 9.0))
        adx_threshold = safe_float(sp.get("adx_threshold", 25))

        if self.index_atr is not None and min_atr_14 is not None and self.index_atr < min_atr_14:
            return True
        if self.index_adx is not None and adx_threshold is not None and self.index_adx < adx_threshold:
            return True
        if (
            self.index_adx is not None
            and self.index_prev_adx is not None
            and (self.index_adx + 0.5) < self.index_prev_adx
        ):
            return True

        return False

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

    def _get_latest_oil_trade_context(self) -> Dict[str, Any]:
        if not self.oil_track_enabled or self.df_index is None or self.df_index.empty:
            return {}

        latest = self.df_index.iloc[-1]
        keys = ["time", "close_oil", "volume_oil", "oi_oil"] + self._oil_context_columns()
        context: Dict[str, Any] = {}
        for key in keys:
            if key not in latest.index:
                continue
            value = latest.get(key)
            try:
                if pd.isna(value):
                    value = None
            except Exception:
                pass
            if isinstance(value, np.generic):
                value = value.item()
            context[key] = value

        return context

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

        oil_context = self._get_latest_oil_trade_context()
        oil_large_candle_side = str(oil_context.get("oil_large_candle_side") or "none")
        oil_last5_max_candle_points = safe_float(oil_context.get("oil_last5_max_candle_points"))
        oil_has_large_up_candle = self._coerce_bool(oil_context.get("oil_has_large_up_candle"), False)
        oil_has_large_down_candle = self._coerce_bool(oil_context.get("oil_has_large_down_candle"), False)
        oil_last5_max_volume = safe_float(oil_context.get("oil_last5_max_volume"))
        oil_has_large_volume = self._coerce_bool(oil_context.get("oil_has_large_volume"), False)
        oil_candle_points_txt = (
            "NA" if oil_last5_max_candle_points is None else f"{oil_last5_max_candle_points:.2f}"
        )
        oil_volume_txt = "NA" if oil_last5_max_volume is None else f"{oil_last5_max_volume:.0f}"

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

            option_atr = self.atr5_engine.get_atr(chosen["instrument_key"])
            target = None
            sl_trigger = None
            start_trail_after = None
            risk_mode = "pct"

            if use_option_atr_risk and option_atr is not None and option_atr > 0:
                atr_to_use = option_atr
                target = entry_price + (atr_target_mult * option_atr)
                sl_trigger = entry_price - (atr_sl_mult * option_atr)
                start_trail_after = float(option_atr / entry_price)

                if option_atr > max_atr_for_contract:
                    start_trail_after = float(max_atr_for_contract / entry_price)
                    atr_to_use = max_atr_for_contract

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
            self._order_container["oil_context"] = dict(oil_context) if oil_context else None

            description = (
                f"{self._order_container['side']} {self._order_container['instrument_symbol']} "
                f"entry={entry_price:.2f} | oil5_side={oil_large_candle_side} "
                f"oil5_candle={oil_candle_points_txt} oil_up={oil_has_large_up_candle} "
                f"oil_down={oil_has_large_down_candle} oil5_vol={oil_volume_txt} "
                f"oil_large_vol={oil_has_large_volume}"
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
                f"start_trail_after: {start_trail_after}, RiskMode: {risk_mode}, OptionATR: {option_atr}, "
                f"OilContext: {oil_context}"
            )

            if trade_id:
                self._order_container["trade_id"] = trade_id
                self._order_container["status"] = constants.OPEN
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
                force_trail = self._should_force_trail_open_order()
                force_trail_applied = self.order_maneger.on_tick(
                    symbol=self._order_container["instrument_symbol"],
                    o=latest_ltp, h=latest_ltp, l=latest_ltp, c=latest_ltp,
                    ts=ts,
                    force_trail=force_trail,
                )
                if force_trail_applied:
                    self._order_container["force_trail_lock"] = True

                trade_info = self.order_maneger.get_trade_by_id(self._order_container.get("trade_id"))
                if trade_info and trade_info["status"] in [
                    constants.TARGET_HIT,
                    constants.STOPLOSS_HIT,
                    constants.MANUAL_EXIT,
                    constants.EOD_SQUARE_OFF,
                ]:
                    logger.debug(f"Trade closed Info: {trade_info}")
                    self._set_post_exit_cooldown(trade_info.get("status"), ts=ts)
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
