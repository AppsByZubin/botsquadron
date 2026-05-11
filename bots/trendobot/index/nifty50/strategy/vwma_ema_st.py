import json
import sys
import os
import yaml
import pandas as pd
import math
from threading import RLock
from zoneinfo import ZoneInfo
import common.constants as constants
import logger
from typing import Any, Dict, List, Optional, Tuple
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

logger = logger.create_logger("VwmaEmaStStrategyLogger")


class VwmaEmaStStrategy:
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
        self.curr_fut_candle = None
        self.curr_fut_minute = None
        self.last_fut_bar: Optional[Dict] = None
        self.future_data_from_parquet = False
        self.last_index_bar: Optional[Dict] = None
        self._fut_vol_minute = None
        self._fut_vol_start_vtt = None
        self._fut_vol_last_vtt = None
        self._fut_vol_by_minute: Dict[str, float] = {}
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
        self.index_fur_key = self._get_index_fut_key()
        nifty_fut = self.selected_contracts.get("Nifty_Future") if isinstance(self.selected_contracts, dict) else None
        if self.index_fur_key is None and isinstance(nifty_fut, dict):
            self.index_fur_key = nifty_fut.get("instrument_key")
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
            "vwma_25": pd.Series(dtype="float64"),
            "vwap": pd.Series(dtype="float64"),
            "ema_9": pd.Series(dtype="float64"),
            "atr_14": pd.Series(dtype="float64"),
            "adx_14": pd.Series(dtype="float64"),
            "rsi_7": pd.Series(dtype="float64"),
            "rsi_ma_14": pd.Series(dtype="float64"),
            "angle_ema_9": pd.Series(dtype="float64"),
            "angle_vwma_25": pd.Series(dtype="float64"),
            "angle_rsi_ma_14": pd.Series(dtype="float64"),
            "st_atr_10": pd.Series(dtype="float64"),
            "st_upperbound": pd.Series(dtype="float64"),
            "st_lowerbound": pd.Series(dtype="float64"),
            "supertrend": pd.Series(dtype="float64"),
            "st_direction": pd.Series(dtype="float64"),
            "st_phase": pd.Series(dtype="object"),
            "st_turn_green": pd.Series(dtype="bool"),
            "st_turn_red": pd.Series(dtype="bool"),

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
        self._st_phase_skip_day: Optional[str] = None
        self._st_phase_at_trade_start: Optional[str] = None
        self._st_phase_waiting_for_next: bool = True
        self.order_maneger = order_manager

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
            "supertrend_flip_sl_modified_key": None,
        }
        self._trade_end_time=None
        self._init_trade_window_times()
        self._setup_gap_state()
        if intraday_index_candles is not None or intraday_future_candles is not None:
            self._initialize_from_intraday_candles(intraday_index_candles, intraday_future_candles)

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
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            norm = value.strip().lower()
            if norm in {"1", "true", "yes", "y", "on"}:
                return True
            if norm in {"0", "false", "no", "n", "off"}:
                return False
        return default

    @staticmethod
    def _indicator_flag_is_true(value: Any) -> bool:
        if value is None:
            return False

        try:
            missing = pd.isna(value)
            if isinstance(missing, (bool, np.bool_)) and missing:
                return False
        except (TypeError, ValueError):
            pass

        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return bool(value)

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

        df_i = build_df(index_candles, include_volume=False)
        df_f = build_df(fut_candles, include_volume=True)

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

    # ------------------------------------------------------------------
    # WS lifecycle
    # ------------------------------------------------------------------
    def start(self):
        return None

    def stop(self):
        return None

    def on_ws_reconnected(self):
        logger.info("WebSocket reconnected; vwma_ema_st strategy state preserved.")

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

        try:
            self._modify_sl_open_trade_on_supertrend_flip(feed_response)
        except Exception as e:
            logger.warning(f"_modify_sl_open_trade_on_supertrend_flip error: {e}")

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
        if self.last_index_bar is None or self.last_fut_bar is None:
            return
        if self.last_index_bar.get("time") != self.last_fut_bar.get("time"):
            return

        row = {
            "time": self.last_index_bar["time"],
            "close": float(self.last_index_bar.get("close", np.nan)),
            "close_fut": float(self.last_fut_bar.get("close", np.nan)),
            "volume_fut": float(self.last_fut_bar.get("volume", np.nan)),
        }

        if self.df_merged.empty:
            self.df_merged = pd.DataFrame([row])
        else:
            matched = self.df_merged.index[self.df_merged["time"].astype(str) == str(row["time"])]
            if len(matched) > 0:
                idx = matched[-1]
                for col, value in row.items():
                    self.df_merged.at[idx, col] = value
            else:
                self.df_merged = pd.concat([self.df_merged, pd.DataFrame([row])], ignore_index=True)

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

    @staticmethod
    def _calculate_supertrend(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        atr_period: int = 10,
        factor: float = 3.0,
    ) -> pd.DataFrame:
        high_s = pd.to_numeric(high, errors="coerce").astype("float64").reset_index(drop=True)
        low_s = pd.to_numeric(low, errors="coerce").astype("float64").reset_index(drop=True)
        close_s = pd.to_numeric(close, errors="coerce").astype("float64").reset_index(drop=True)
        atr = pd.Series(
            talib.ATR(
                high_s.to_numpy(dtype="float64"),
                low_s.to_numpy(dtype="float64"),
                close_s.to_numpy(dtype="float64"),
                timeperiod=int(atr_period),
            ),
            index=high_s.index,
            dtype="float64",
        )

        hl2 = (high_s + low_s) / 2.0
        basic_upper = hl2 + (float(factor) * atr)
        basic_lower = hl2 - (float(factor) * atr)
        final_upper = pd.Series(np.nan, index=high_s.index, dtype="float64")
        final_lower = pd.Series(np.nan, index=high_s.index, dtype="float64")
        direction = pd.Series(np.nan, index=high_s.index, dtype="float64")
        supertrend = pd.Series(np.nan, index=high_s.index, dtype="float64")

        first_valid = atr.first_valid_index()
        if first_valid is None:
            return pd.DataFrame(
                {
                    "st_atr_10": atr,
                    "st_upperbound": final_upper,
                    "st_lowerbound": final_lower,
                    "supertrend": supertrend,
                    "st_direction": direction,
                }
            )

        for i in range(int(first_valid), len(high_s)):
            if pd.isna(basic_upper.iloc[i]) or pd.isna(basic_lower.iloc[i]) or pd.isna(close_s.iloc[i]):
                continue

            if i == int(first_valid):
                final_upper.iloc[i] = basic_upper.iloc[i]
                final_lower.iloc[i] = basic_lower.iloc[i]
                direction.iloc[i] = 1.0
            else:
                prev_upper = final_upper.iloc[i - 1]
                prev_lower = final_lower.iloc[i - 1]
                prev_close = close_s.iloc[i - 1]
                prev_supertrend = supertrend.iloc[i - 1]

                if pd.isna(prev_upper):
                    final_upper.iloc[i] = basic_upper.iloc[i]
                elif basic_upper.iloc[i] < prev_upper or prev_close > prev_upper:
                    final_upper.iloc[i] = basic_upper.iloc[i]
                else:
                    final_upper.iloc[i] = prev_upper

                if pd.isna(prev_lower):
                    final_lower.iloc[i] = basic_lower.iloc[i]
                elif basic_lower.iloc[i] > prev_lower or prev_close < prev_lower:
                    final_lower.iloc[i] = basic_lower.iloc[i]
                else:
                    final_lower.iloc[i] = prev_lower

                if pd.isna(atr.iloc[i - 1]):
                    direction.iloc[i] = 1.0
                elif prev_supertrend == prev_upper:
                    direction.iloc[i] = -1.0 if close_s.iloc[i] > final_upper.iloc[i] else 1.0
                else:
                    direction.iloc[i] = 1.0 if close_s.iloc[i] < final_lower.iloc[i] else -1.0

            supertrend.iloc[i] = final_lower.iloc[i] if direction.iloc[i] < 0 else final_upper.iloc[i]

        if len(supertrend) > 0:
            supertrend.iloc[0] = np.nan

        return pd.DataFrame(
            {
                "st_atr_10": atr,
                "st_upperbound": final_upper,
                "st_lowerbound": final_lower,
                "supertrend": supertrend,
                "st_direction": direction,
            }
        )

    def _apply_indicators(self):
        """
        Applies hybrid VWMA/EMA/Supertrend indicators using spot price and
        Nifty future volume.
        """
        if self.df_index.empty:
            return
        if self.df_index_future is None or self.df_index_future.empty:
            logger.warning("Future data unavailable, skipping indicator calculations.")
            return

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}

        idx = self.df_index.copy()
        idx["_row_index"] = idx.index
        idx["time"] = pd.to_datetime(idx["time"], errors="coerce")
        idx = idx.dropna(subset=["time"]).sort_values(["time", "_row_index"])
        if idx.empty:
            return

        fut = self.df_index_future[["time", "volume"]].copy()
        fut["time"] = pd.to_datetime(fut["time"], errors="coerce")
        fut["volume"] = pd.to_numeric(fut["volume"], errors="coerce")
        fut = (
            fut.dropna(subset=["time"])
            .sort_values("time")
            .drop_duplicates(subset=["time"], keep="last")
        )

        df_merged = pd.merge(
            idx[["_row_index", "time", "open", "high", "low", "close"]],
            fut[["time", "volume"]],
            on="time",
            how="left",
        ).sort_values(["time", "_row_index"])

        for col in ["open", "high", "low", "close", "volume"]:
            df_merged[col] = pd.to_numeric(df_merged[col], errors="coerce")
        df_merged["volume"] = df_merged["volume"].fillna(0.0)
        df_merged["date"] = df_merged["time"].dt.date
        df_merged["pv"] = df_merged["close"] * df_merged["volume"]
        df_merged["cum_pv"] = df_merged.groupby("date")["pv"].cumsum()
        df_merged["cum_vol"] = df_merged.groupby("date")["volume"].cumsum()

        row_index = df_merged["_row_index"].to_numpy()
        self.df_index.loc[row_index, "vwap"] = (
            df_merged["cum_pv"] / df_merged["cum_vol"].replace(0, np.nan)
        ).to_numpy(dtype="float64")
        self.df_index.loc[row_index, "fut_volume"] = df_merged["volume"].to_numpy(dtype="float64")

        latest_time = self.df_index["time"].iloc[-1] if not self.df_index.empty else None
        latest_volume = df_merged["volume"].iloc[-1] if not df_merged.empty else None
        logger.info(f"Current Future index minute:{latest_time} , Finalizing index volume: {latest_volume}")

        vwma_len = max(1, int(sp.get("vwma_len", 25) or 25))
        roll_pv = df_merged["pv"].rolling(window=vwma_len, min_periods=1).sum()
        roll_vol = df_merged["volume"].rolling(window=vwma_len, min_periods=1).sum()
        self.df_index.loc[row_index, "vwma_25"] = (
            roll_pv / roll_vol.replace(0, np.nan)
        ).to_numpy(dtype="float64")

        high = pd.to_numeric(self.df_index["high"], errors="coerce")
        low = pd.to_numeric(self.df_index["low"], errors="coerce")
        close = pd.to_numeric(self.df_index["close"], errors="coerce")
        self.df_index["candle_range"] = high - low
        self.df_index["volatile_count"] = (
            (self.df_index["candle_range"] > 8).astype(int).shift(1).rolling(window=4).sum()
        )
        self.df_index["is_volatile"] = self.df_index["volatile_count"] >= 2
        self.df_index["recent_high_max"] = high.shift(1).rolling(window=4).max()
        self.df_index["recent_low_min"] = low.shift(1).rolling(window=4).min()
        self.df_index["is_hh"] = (high > self.df_index["recent_high_max"]) & self.df_index["is_volatile"]
        self.df_index["is_ll"] = (low < self.df_index["recent_low_min"]) & self.df_index["is_volatile"]

        if len(self.df_index) >= 14:
            st_atr_period = int(sp.get("supertrend_atr_period", 10) or 10)
            st_factor = float(sp.get("supertrend_factor", 3) or 3)

            self.df_index["ema_9"] = self._calculate_ema(close, length=9)
            self.df_index["rsi_7"] = self._calculate_rsi(length=7)
            self.df_index["rsi_ma_14"] = self.df_index["rsi_7"].rolling(window=14, min_periods=14).mean()

            slope_ema = (self.df_index["ema_9"].astype(float) - self.df_index["ema_9"].shift(self._slope_window).astype(float)) / self._slope_window
            slope_vwma = (self.df_index["vwma_25"].astype(float) - self.df_index["vwma_25"].shift(self._slope_window).astype(float)) / self._slope_window
            slope_rsi_ma = (self.df_index["rsi_ma_14"].astype(float) - self.df_index["rsi_ma_14"].shift(self._slope_window).astype(float)) / self._slope_window

            self.df_index["angle_ema_9"] = np.degrees(np.arctan(np.clip(slope_ema, -10, 10)))
            self.df_index["angle_vwma_25"] = np.degrees(np.arctan(np.clip(slope_vwma, -10, 10)))
            self.df_index["angle_rsi_ma_14"] = np.degrees(np.arctan(np.clip(slope_rsi_ma, -10, 10)))

            self.df_index["atr_14"] = self._calculate_atr(length=14)
            adx_14 = talib.ADX(
                high.to_numpy(dtype="float64"),
                low.to_numpy(dtype="float64"),
                close.to_numpy(dtype="float64"),
                timeperiod=14,
            )
            self.df_index["adx_14"] = pd.Series(np.asarray(adx_14, dtype="float64"), index=self.df_index.index)

            st = self._calculate_supertrend(
                high=high,
                low=low,
                close=close,
                atr_period=st_atr_period,
                factor=st_factor,
            )
            for col in ["st_atr_10", "st_upperbound", "st_lowerbound", "supertrend", "st_direction"]:
                self.df_index[col] = pd.Series(st[col].to_numpy(), index=self.df_index.index, dtype="float64")

            st_direction = self.df_index["st_direction"]
            prev_st_direction = st_direction.shift(1)
            self.df_index["st_phase"] = np.select(
                [st_direction < 0, st_direction > 0],
                ["green", "red"],
                default=None,
            )
            self.df_index["st_turn_green"] = (st_direction < 0) & (prev_st_direction > 0)
            self.df_index["st_turn_red"] = (st_direction > 0) & (prev_st_direction < 0)

            self.check_price_action(safe_float(self.df_index["atr_14"].iloc[-1]))

    def check_price_action(self,atr):
        """
        Checks for momentum thrusts using consecutive candles plus ATR-sized range.
        """
        is_red = self.df_index['close'] < self.df_index['open']
        is_green = self.df_index['close'] > self.df_index['open']

        prev_low = self.df_index['low'].shift(1)
        prev_high = self.df_index['high'].shift(1)

        making_lower_low = self.df_index['low'] < prev_low
        making_higher_high = self.df_index['high'] > prev_high

        curr_range = self.df_index['high'] - self.df_index['low']
        prev_range = curr_range.shift(1)

        is_alive = (curr_range > 3) & (prev_range > 3)

        atr_threshold = safe_float(atr)
        if atr_threshold is None or np.isnan(atr_threshold):
            atr_threshold = 10.0
        has_major_move = (curr_range > atr_threshold) | (prev_range > atr_threshold)

        is_valid_setup = is_alive & has_major_move

        self.df_index['is_bearish_thrust'] = (
            is_red &
            is_red.shift(1) &
            making_lower_low &
            is_valid_setup
        )

        self.df_index['is_bullish_thrust'] = (
            is_green &
            is_green.shift(1) &
            making_higher_high &
            is_valid_setup
        )

    def _should_modify_sl_on_supertrend_flip(self, latest: Optional[pd.Series]) -> Tuple[bool, str]:
        if self._order_container.get("status") != constants.OPEN:
            return False, ""
        if latest is None:
            return False, ""

        side = self._order_container.get("side")
        st_turn_red = self._indicator_flag_is_true(latest.get("st_turn_red", False))
        st_turn_green = self._indicator_flag_is_true(latest.get("st_turn_green", False))
        if side == constants.CALL and st_turn_red:
            return True, "Supertrend turned red"
        if side == constants.PUT and st_turn_green:
            return True, "Supertrend turned green"
        return False, ""


    def _trading_engine_active(self):
        """
        Entry engine for new positions.
        Applies warm-up, volatility, VWMA/EMA, RSI-MA and Supertrend filters
        before switching order state to WAITING.
        """
        try:
            if not self.enable_trading_engine:
                return

            if len(self.df_index) < 30:
                return

            sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
            if self._order_container["status"] is not None:
                return

            ref_ts = self._resolve_reference_ts()
            latest = self.df_index.iloc[-1]
            if self._should_skip_st_phase_passing_trade_start(latest, ref_ts):
                return

            atr_14 = safe_float(latest.get('atr_14'))
            if atr_14 is None:
                return
            if atr_14 < 7:
                logger.debug(f"ATR range is low {atr_14}")
                return

            if self._is_post_exit_cooldown_active(ref_ts):
                cooldown_left_sec = int(max((self._post_exit_cooldown_until - ref_ts).total_seconds(), 0))
                logger.debug(
                    f"Entry blocked by post-exit cooldown for {cooldown_left_sec}s "
                    f"(until {self._post_exit_cooldown_until.strftime('%H:%M:%S')})"
                )
                return

            if self._is_daily_loss_limit_active(ref_ts):
                return

            rsi_ma_14_val = latest.get('rsi_ma_14', np.nan)
            previous_rsi_ma_14_val = self.df_index.iloc[-2].get('rsi_ma_14', np.nan)
            if pd.isna(rsi_ma_14_val) or pd.isna(previous_rsi_ma_14_val):
                return
            rsi_ma_14 = math.ceil(float(rsi_ma_14_val))
            previous_rsi_ma_14 = math.ceil(float(previous_rsi_ma_14_val))
            ema_9 = float(latest.get('ema_9', np.nan))
            vma_25 = float(latest.get('vwma_25', np.nan))
            angle_ema_9 = float(latest.get('angle_ema_9', np.nan))
            angle_vwma_25 = float(latest.get('angle_vwma_25', np.nan))
            angle_rsi_ma_14 = float(latest.get('angle_rsi_ma_14', np.nan))
            is_bearish_thrust = self._indicator_flag_is_true(latest.get('is_bearish_thrust', False))
            is_bullish_thrust = self._indicator_flag_is_true(latest.get('is_bullish_thrust', False))
            future_volume = safe_float(latest.get('fut_volume', np.nan))
            supertrend = safe_float(latest.get('supertrend', np.nan))
            st_direction = safe_float(latest.get('st_direction', np.nan))
            if supertrend is None or st_direction is None:
                return
            st_phase = self._resolve_st_phase(latest)
            st_turn_green = self._indicator_flag_is_true(latest.get('st_turn_green', False))
            st_turn_red = self._indicator_flag_is_true(latest.get('st_turn_red', False))

            up_angle_ema = float(sp.get("up_angle_ema", self.params.get("up_angle_ema", 50)))
            up_angle_vwma = float(sp.get("up_angle_vwma", self.params.get("up_angle_vwma", 20)))
            up_angle_rsi_ma = float(sp.get("up_angle_rsi_ma", self.params.get("up_angle_rsi_ma", 20)))

            dn_angle_ema = float(sp.get("dn_angle_ema", self.params.get("dn_angle_ema", -50)))
            dn_angle_vwma = float(sp.get("dn_angle_vwma", self.params.get("dn_angle_vwma", -20)))
            dn_angle_rsi_ma = float(sp.get("dn_angle_rsi_ma", self.params.get("dn_angle_rsi_ma", -20)))
            orb_enabled = False
            orb_side = None

            logger.debug(
                f"Engine check rsi_ma={rsi_ma_14}/{previous_rsi_ma_14}, ema={ema_9}, vwma={vma_25}, "
                f"angle_ema={angle_ema_9}, angle_vwma={angle_vwma_25}, angle_rsi_ma={angle_rsi_ma_14}, "
                f"future_volume={future_volume}, "
                f"supertrend={supertrend}, st_direction={st_direction}, "
                f"st_phase={st_phase}, "
                f"st_turn_green={st_turn_green}, st_turn_red={st_turn_red}, "
                f"orb_side={orb_side}, bullish_thrust={is_bullish_thrust}, bearish_thrust={is_bearish_thrust}, "
                f"current_candle_range={safe_float(self.df_index.iloc[-1].get('candle_range', np.nan))}"
            )

            call_setup = (
                (angle_rsi_ma_14 > up_angle_rsi_ma)
                and (ema_9 > vma_25)
                and (angle_ema_9 > up_angle_ema)
                and (angle_vwma_25 > up_angle_vwma)
                and (st_phase == "green")
                and (st_direction < 0)
                and ((not orb_enabled) or (orb_side == constants.CALL))
            )

            put_setup = (
                (angle_rsi_ma_14 < dn_angle_rsi_ma)
                and (ema_9 < vma_25)
                and (angle_ema_9 < dn_angle_ema)
                and (angle_vwma_25 < dn_angle_vwma)
                and (st_phase == "red")
                and (st_direction > 0)
                and ((not orb_enabled) or (orb_side == constants.PUT))
            )

            logger.debug(f"condition check call_setup:{call_setup}, put_setup:{put_setup}")

            if call_setup and self._order_container["status"] is None and (self._order_counter < self._max_order_counter):
                lot = self._calculate_lot_size(constants.CALL, is_bullish_thrust, is_bearish_thrust)
                if lot <= 0:
                    return

                self._order_container["side"] = constants.CALL
                self._order_container["status"] = constants.WAITING
                self._order_container["lot"] = int(lot)
                logger.info(f"Order intent set side={constants.CALL}, lot={lot}, status={constants.WAITING}")
                return

            if put_setup and self._order_container["status"] is None and (self._order_counter < self._max_order_counter):
                lot = self._calculate_lot_size(constants.PUT, is_bullish_thrust, is_bearish_thrust)
                if lot <= 0:
                    return

                self._order_container["side"] = constants.PUT
                self._order_container["status"] = constants.WAITING
                self._order_container["lot"] = int(lot)
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
        market_hours = self.params.get("market-hours", {}) if isinstance(self.params, dict) else {}
        start_str = str(trade_window.get("start", market_hours.get("start", "09:45"))).strip()
        end_str = str(trade_window.get("end") or "15:10").strip()
        try:
            hh, mm = map(int, start_str.split(":"))
            self._trade_start_time = time(hh, mm)
        except Exception:
            self._trade_start_time = time(9, 45)
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

    def _trade_start_time_obj(self) -> time:
        return getattr(self, "_trade_start_time", time(9, 45))

    def _resolve_st_phase(self, latest: pd.Series) -> Optional[str]:
        phase_raw = latest.get("st_phase")
        phase = str(phase_raw).strip().lower() if phase_raw is not None and not pd.isna(phase_raw) else ""
        if phase in {"green", "red"}:
            return phase

        st_direction = safe_float(latest.get("st_direction"))
        if st_direction is None:
            return None
        if st_direction < 0:
            return "green"
        if st_direction > 0:
            return "red"
        return None

    def _trade_start_phase_grace_minutes(self) -> int:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        try:
            return max(0, int(sp.get("supertrend_trade_start_phase_grace_minutes", 1) or 0))
        except Exception:
            return 1

    def _capture_st_phase_passing_trade_start(self) -> None:
        if self.df_index is None or self.df_index.empty or "time" not in self.df_index.columns:
            return

        candle_times = pd.to_datetime(self.df_index["time"], errors="coerce")
        if candle_times.empty:
            return

        latest_ts = candle_times.iloc[-1]
        if pd.isna(latest_ts):
            return

        trade_start = self._trade_start_time_obj()
        if latest_ts.time() < trade_start:
            return

        day_key = latest_ts.strftime("%Y-%m-%d")
        if self._st_phase_skip_day == day_key:
            return

        same_day = candle_times.dt.strftime("%Y-%m-%d") == day_key
        at_or_before_start = same_day & (candle_times.dt.time <= trade_start)
        anchor_idx = candle_times[at_or_before_start].idxmax() if at_or_before_start.any() else None
        anchor_phase = self._resolve_st_phase(self.df_index.loc[anchor_idx]) if anchor_idx is not None else None

        if anchor_phase is None:
            at_or_after_start = same_day & (candle_times.dt.time >= trade_start)
            for idx in candle_times[at_or_after_start].sort_values().index:
                anchor_phase = self._resolve_st_phase(self.df_index.loc[idx])
                if anchor_phase is not None:
                    anchor_idx = idx
                    break

        if anchor_idx is None or anchor_phase is None:
            return

        anchor_ts = candle_times.loc[anchor_idx]
        self._st_phase_skip_day = day_key
        self._st_phase_at_trade_start = anchor_phase
        self._st_phase_waiting_for_next = True
        logger.info(
            f"Skipping Supertrend phase passing through trade start: "
            f"day={day_key}, trade_start={trade_start.strftime('%H:%M')}, "
            f"anchor_time={anchor_ts.strftime('%H:%M')}, st_phase={anchor_phase}"
        )

    def _should_absorb_trade_start_boundary_flip(self, latest: pd.Series, phase: str) -> bool:
        if phase == self._st_phase_at_trade_start:
            return False

        grace_minutes = self._trade_start_phase_grace_minutes()
        if grace_minutes <= 0:
            return False

        latest_ts = pd.to_datetime(latest.get("time"), errors="coerce")
        if pd.isna(latest_ts):
            return False
        if getattr(latest_ts, "tzinfo", None) is not None:
            latest_ts = latest_ts.tz_convert(ist).tz_localize(None)

        latest_dt = latest_ts.to_pydatetime()
        trade_start_dt = datetime.combine(latest_dt.date(), self._trade_start_time_obj())
        if latest_dt < trade_start_dt or latest_dt > trade_start_dt + timedelta(minutes=grace_minutes):
            return False

        return (
            self._indicator_flag_is_true(latest.get("st_turn_green", False))
            or self._indicator_flag_is_true(latest.get("st_turn_red", False))
        )

    def _should_skip_st_phase_passing_trade_start(self, latest: pd.Series, ref_ts: datetime) -> bool:
        self._capture_st_phase_passing_trade_start()

        phase = self._resolve_st_phase(latest)
        if phase is None:
            return True

        day_key = ref_ts.strftime("%Y-%m-%d")
        if self._st_phase_skip_day != day_key:
            logger.debug("Entry blocked until Supertrend phase passing through trade start is captured")
            return True

        if self._st_phase_waiting_for_next:
            if phase == self._st_phase_at_trade_start:
                logger.debug(
                    f"Entry blocked until Supertrend flips from phase passing through trade start; "
                    f"trade_start_st_phase={self._st_phase_at_trade_start}, current_st_phase={phase}"
                )
                return True

            if self._should_absorb_trade_start_boundary_flip(latest, phase):
                previous_phase = self._st_phase_at_trade_start
                self._st_phase_at_trade_start = phase
                logger.info(
                    f"Absorbing Supertrend flip at trade-start boundary; "
                    f"previous_captured_phase={previous_phase}, updated_skip_phase={phase}"
                )
                return True

            self._st_phase_waiting_for_next = False
            logger.info(
                f"Trade-start Supertrend phase skipped; accepting next phase "
                f"trade_start_st_phase={self._st_phase_at_trade_start}, current_st_phase={phase}"
            )

        return False

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

    def _calculate_option_buy_sl_prices(
        self,
        option_ltp: float,
        sp: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, float]:
        params_root = self.params if isinstance(self.params, dict) else {}
        sp = sp or (params_root.get("strategy-parameters") or {})
        tick = float(
            sp.get(
                "tick-size",
                sp.get("tick_size", params_root.get("tick-size", params_root.get("tick_size", 0.05))),
            )
        )
        sl_limit_gap = float(sp.get("sl-limit-gap", sp.get("sl_limit_gap", 1.0)))
        sl_offset = float(sp.get("supertrend_flip_sl_points", sp.get("supertrend_flip_sl_offset", 4.0)))

        sl_trigger = self._round_to_tick(float(option_ltp) - sl_offset, tick, "CEIL")
        sl_limit = self._round_to_tick(sl_trigger - sl_limit_gap, tick, "FLOOR")
        if sl_limit >= sl_trigger:
            sl_limit = self._round_to_tick(sl_trigger - tick, tick, "FLOOR")
        return sl_trigger, sl_limit

    def _modify_sl_open_trade_on_supertrend_flip(self, feed_response: Optional[list]) -> bool:
        if self._order_container.get("status") != constants.OPEN:
            return False

        latest = self.df_index.iloc[-1] if self.df_index is not None and not self.df_index.empty else None
        should_modify_sl, modify_reason = self._should_modify_sl_on_supertrend_flip(latest)
        if not should_modify_sl:
            return False

        trade_id = self._order_container.get("trade_id")
        instrument_key = self._order_container.get("instrument_key")
        if not trade_id or not instrument_key:
            return False
        if self.order_maneger is None or not hasattr(self.order_maneger, "modify_sl_order"):
            logger.warning(
                f"Unable to modify SL on Supertrend flip; trade_id={trade_id}, order manager unavailable"
            )
            return False

        latest_time = str(latest.get("time", "")) if latest is not None else ""
        flip_key = f"{trade_id}:{latest_time}:{modify_reason}"
        if self._order_container.get("supertrend_flip_sl_modified_key") == flip_key:
            return False

        latest_ltp = safe_float(self._order_container.get("ltp"))
        modify_ts = self._resolve_reference_ts()
        for item in feed_response or []:
            if item.get("instrument_key") != instrument_key:
                continue
            item_ltp = safe_float(item.get("ltp"))
            if item_ltp is not None:
                latest_ltp = item_ltp
            ts_ms = safe_float(item.get("ts_epoch_ms") or item.get("ltt"))
            if ts_ms is not None:
                modify_ts = datetime.fromtimestamp(int(ts_ms) / 1000, tz=ist)
            break

        if latest_ltp is None:
            logger.warning(f"Unable to modify SL on Supertrend flip; trade_id={trade_id}, ltp unavailable")
            return False

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        sl_trigger, sl_limit = self._calculate_option_buy_sl_prices(float(latest_ltp), sp)

        self._order_container["ltp"] = float(latest_ltp)
        logger.info(
            f"{modify_reason}; modify SL trade_id={trade_id}, "
            f"symbol={self._order_container.get('instrument_symbol')}, latest_ltp={latest_ltp:.2f}, "
            f"sl_trigger={sl_trigger:.2f}, sl_limit={sl_limit:.2f}"
        )
        sl_modified = self.order_maneger.modify_sl_order(
            trade_id=trade_id,
            ltp_now=float(latest_ltp),
            new_trigger=float(sl_trigger),
            new_limit=float(sl_limit),
            ts=modify_ts,
        )
        if not sl_modified:
            return False

        self._order_container["supertrend_flip_sl_modified_key"] = flip_key
        return True

    # ------------------------------------------------------------------
    # Order processing (WAITING -> OPEN -> EOD)
    # ------------------------------------------------------------------
    def _reset_order_container(self) -> None:
        self._order_container = {k: None for k in self._order_container}

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
        # Do not gate candle building here. This guard only skips option feed
        # parsing and order lifecycle work when there is no pending/open trade.
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

    def _build_itm_contract_map(self, side: str, spot_price: float) -> Dict[str, Dict[str, Any]]:
        output: Dict[str, Dict[str, Any]] = {}
        if spot_price <= 0:
            return output

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        itm_range = float(sp.get("itm_strike_range", self.params.get("itm_strike_range", 200)))

        low = spot_price - itm_range
        high = spot_price + itm_range

        for strike, contracts in self.selected_contracts.items():
            if strike == "Nifty_Future":
                continue
            if not isinstance(contracts, list) or not contracts:
                continue

            try:
                strike_price = float(contracts[0].get("strike_price"))
            except Exception:
                try:
                    strike_price = float(strike)
                except Exception:
                    continue

            side_u = str(side).upper()
            if side_u == constants.CALL:
                if not (low <= strike_price <= spot_price):
                    continue
            elif side_u == constants.PUT:
                if not (spot_price <= strike_price <= high):
                    continue
            else:
                continue

            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                instrument_type = str(contract.get("instrument_type", "")).upper()
                if side_u == constants.CALL and instrument_type in ("CE", "CALL"):
                    instrument_key = contract.get("instrument_key")
                    if instrument_key:
                        output[instrument_key] = contract
                if side_u == constants.PUT and instrument_type in ("PE", "PUT"):
                    instrument_key = contract.get("instrument_key")
                    if instrument_key:
                        output[instrument_key] = contract

        return output

    def _trade_processing(self, feed_response: list) -> None:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        if not feed_response:
            return
        sl_modified_on_supertrend_flip = False

        # 1) WAITING -> pick contract + place order
        if (
            self._order_container.get("side") is not None
            and self._order_container.get("status") == constants.WAITING
            and self._order_container.get("instrument_key") is None
        ):
            if self._is_daily_loss_limit_active():
                logger.info("Skipping order placement: daily loss guard active. No more new trades for today.")
                self._reset_order_container()
                return

            if not self.last_index_bar:
                return
            spot = safe_float(self.last_index_bar.get("close"))
            if spot is None or spot <= 0:
                return

            dict_itm = self._build_itm_contract_map(self._order_container["side"], spot)
            if not dict_itm:
                return

            logger.debug(f"ITM candidates: {list(dict_itm.keys())}")

            max_gamma = -1e18
            chosen = None
            for item in feed_response:
                ik = item.get("instrument_key")
                if ik not in dict_itm:
                    continue
                gamma = item.get("gamma")
                gamma_value = None
                if gamma is not None:
                    try:
                        gamma_value = float(gamma)
                    except Exception:
                        gamma_value = None
                if gamma_value is not None and gamma_value > max_gamma:
                    chosen = item
                    max_gamma = gamma_value

            if chosen is None:
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

            contract = dict_itm.get(chosen["instrument_key"])
            if not contract:
                return

            self._order_container["instrument_key"] = chosen["instrument_key"]
            self._order_container["ltp"] = float(chosen["ltp"])
            self._order_container["max_gamma"] = max_gamma
            self._order_container["instrument_symbol"] = contract.get("trading_symbol")

            ts = datetime.fromtimestamp(int(chosen["ts_epoch_ms"]) / 1000, tz=ist)

            lot = self._order_container.get("lot")
            lot_size = contract.get("lot_size")
            try:
                lot = int(float(lot))
                lot_size = int(float(lot_size))
            except (TypeError, ValueError):
                logger.error(f"Invalid lot/lot_size. lot={lot} lot_size={lot_size}")
                return
            qty = lot * lot_size

            entry_price = float(self._order_container["ltp"])
            tick = float(sp.get("tick-size", sp.get("tick_size", 0.05)))

            target = None
            sl_trigger = None
            option_atr = self.atr5_engine.get_atr(chosen["instrument_key"])

            atr_target_mult = float(sp.get("orb_atr_target_mult", sp.get("atr_target_mult", 10)))
            atr_sl_mult = float(sp.get("orb_atr_sl_mult", sp.get("atr_sl_mult", 4)))

            if option_atr is None or option_atr <= 0:
                logger.warning(f"Skipping order; option ATR unavailable for {chosen['instrument_key']}")
                self._order_container["instrument_key"] = None
                self._order_container["ltp"] = None
                self._order_container["max_gamma"] = None
                self._order_container["instrument_symbol"] = None
                return

            target = entry_price + (atr_target_mult * option_atr)
            sl_trigger = entry_price - (atr_sl_mult * option_atr)

            sl_limit = float(sl_trigger) - float(sp.get("sl-limit-gap", sp.get("sl_limit_gap", 1.0)))

            sl_trigger = self._round_to_tick(float(sl_trigger), tick, "CEIL")
            sl_limit = self._round_to_tick(float(sl_limit), tick, "FLOOR")
            if sl_limit >= sl_trigger:
                sl_limit = self._round_to_tick(sl_trigger - tick, tick, "FLOOR")
            target = self._round_to_tick(float(target), tick, "CEIL")

            description = f"{self._order_container['side']} {self._order_container['instrument_symbol']} entry={entry_price:.2f}"

            trade_id = self.order_maneger.buy(
                symbol=self._order_container["instrument_symbol"],
                instrument_token=self._order_container["instrument_key"],
                qty=qty,
                entry_price=entry_price,
                sl_trigger=sl_trigger,
                sl_limit=sl_limit,
                target=target,
                description=description,
                ts=ts,
            )

            logger.info(
                f"OrderInfo TradeID: {trade_id}, Entry(PU): {entry_price:.2f}, Qty: {qty}, "
                f"Target(PU): {target:.2f}, SL_trig(PU): {sl_trigger:.2f}, "
                f"SL_lim(PU): {sl_limit:.2f}, RiskMode: atr_fixed_sl, OptionATR: {option_atr}"
            )

            if trade_id:
                self._order_container["trade_id"] = trade_id
                self._order_container["status"] = constants.OPEN
                self._order_counter += 1
                logger.info(f"{self._order_container}")
            return

        # 2) OPEN -> feed LTP to OMS for fixed SL/TP monitoring
        if self._order_container.get("status") == constants.OPEN:
            sl_modified_on_supertrend_flip = self._modify_sl_open_trade_on_supertrend_flip(feed_response)
            latest_ltp = None
            ts = None
            for item in feed_response:
                if item.get("instrument_key") == self._order_container.get("instrument_key"):
                    latest_ltp = float(item["ltp"])
                    ts = datetime.fromtimestamp(int(item["ts_epoch_ms"]) / 1000, tz=ist)
                    break

            if latest_ltp is not None and ts is not None:
                self._order_container["ltp"] = float(latest_ltp)

                try:
                    if hasattr(self.order_maneger, "refresh_trade_status") and self._order_container.get("trade_id"):
                        closed_status = self.order_maneger.refresh_trade_status(self._order_container.get("trade_id"), ts=ts)
                        if closed_status in [constants.STOPLOSS_HIT, constants.TARGET_HIT, constants.MANUAL_EXIT, constants.EOD_SQUARE_OFF]:
                            self._set_post_exit_cooldown(closed_status, ts=ts)
                            trade_info = self.order_maneger.get_trade_by_id(self._order_container.get("trade_id"))
                            self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                            logger.info(f"Broker sync closed trade: {trade_info}")
                            self._reset_order_container()
                            return
                except Exception as e:
                    logger.warning(f"refresh_trade_status failed: {e}")

                _ = self.order_maneger.on_tick(
                    symbol=self._order_container["instrument_symbol"],
                    o=latest_ltp, h=latest_ltp, l=latest_ltp, c=latest_ltp,
                    ts=ts,
                )

                trade_info = self.order_maneger.get_trade_by_id(self._order_container.get("trade_id"))
                if trade_info and trade_info.get("status") in [
                    constants.TARGET_HIT,
                    constants.STOPLOSS_HIT,
                    constants.MANUAL_EXIT,
                    constants.EOD_SQUARE_OFF,
                ]:
                    self._set_post_exit_cooldown(trade_info.get("status"), ts=ts)
                    self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                    logger.debug(f"Trade closed Info: {trade_info}")
                    self._reset_order_container()
                    return

        # 3) EOD square-off
        if (
            self.curr_index_minute
            and self._trade_end_time
            and self._order_container.get("trade_id")
            and self._order_container.get("status") == constants.OPEN
        ):
            try:
                current_time = datetime.strptime(self.curr_index_minute, "%Y-%m-%d %H:%M").time()
            except Exception:
                return

            if current_time >= self._trade_end_time:
                if sl_modified_on_supertrend_flip or self._modify_sl_open_trade_on_supertrend_flip(feed_response):
                    logger.info(
                        f"Trade window end reached at {self.curr_index_minute}; "
                        "Supertrend flip SL modified, keeping open trade active."
                    )
                    return

                trade_id = self._order_container.get("trade_id")
                latest_ltp = None
                for item in feed_response:
                    if item.get("instrument_key") == self._order_container.get("instrument_key"):
                        latest_ltp = float(item["ltp"])
                        break
                if latest_ltp is None:
                    latest_ltp = float(self._order_container.get("ltp") or 0.0)

                ts = datetime.now(ist)
                try:
                    if hasattr(self.order_maneger, "refresh_trade_status"):
                        _ = self.order_maneger.refresh_trade_status(trade_id, ts=ts)
                except Exception as e:
                    logger.warning(f"refresh_trade_status (pre-eod) failed: {e}")
                self.order_maneger.square_off_trade(
                    trade_id=trade_id,
                    exit_price=float(latest_ltp),
                    ts=ts,
                    reason=constants.EOD_SQUARE_OFF,
                )
                trade_info = self.order_maneger.get_trade_by_id(trade_id)
                self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                self._reset_order_container()
