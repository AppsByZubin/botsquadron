import os
import json
import yaml
import pandas as pd
import math
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from zoneinfo import ZoneInfo
import common.constants as constants
import logger
from typing import Any, Dict, List, Optional
from datetime import datetime,time,timedelta
import numpy as np
from technicals.atr.atr_for_ticks import AtrEngine

from utils.generic_utils import (
    calculate_gap_percent,
    classify_gap_direction,
    get_previous_close_for_gap,
    safe_float,
)

ist = ZoneInfo("Asia/Kolkata")

logger = logger.create_logger("MeanRevVwapStrategyLogger")

class MeanRevVwapStrategy:
    """
    Mean-reversion strategy around session VWAP bands.

    Flow:
    1) Build index candles from ticks.
    2) Compute session VWAP bands.
    3) Enter when a completed candle body is fully outside a VWAP band.
    4) Delegate live trade lifecycle to order manager.
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
        option_seq_file=None,
        nifty_contract_seq_file=None,
        has_CDN=False,
        has_stocks=False,
    ):
        self.option_seq_file = option_seq_file or []
        self.nifty_contract_seq_file = nifty_contract_seq_file or []
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

        self.params = params if isinstance(params, dict) else self._get_params_from_yaml()
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        if not self.expiry_date:
            self.expiry_date = sp.get("trade_expiry")
        if order_manager is not None and hasattr(order_manager, "set_strategy_params"):
            order_manager.set_strategy_params(sp)
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
        self.index_fur_key = selected_index_fut_key or configured_index_fut_key

        self._present_atm_price = None

        self._oi_previous_snapshot= {}
        self._sum_oi_changes= {}
        self.enable_trading_engine = bool(sp.get("enable_trading_engine", self.params.get("enable_trading_engine", True)))
        configured_slope_window = safe_float(sp.get("slope_window", 3))
        self._slope_window = max(
            int(configured_slope_window) if configured_slope_window is not None else 3,
            1,
        )
        configured_call_angle = safe_float(sp.get("call_ema_9_angle_threshold"))
        configured_put_angle = safe_float(sp.get("put_ema_9_angle_threshold"))
        self.call_ema_9_angle_threshold = (
            configured_call_angle if configured_call_angle is not None else 45.0
        )
        self.put_ema_9_angle_threshold = (
            configured_put_angle if configured_put_angle is not None else -45.0
        )

        self._daily_sentiment = ht.get("daily", ht.get("trader-sentiment", constants.SIDEWAYS))

        self.atr5_engine = AtrEngine(atr_period=int(sp.get("option_atr_period", 6) or 6))
        self.df_index_future = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "oi": pd.Series(dtype="float64"),
        })
        self.df_index_future = self._populate_index_future_data()

        # DataFrames (initialized with fixed dtypes to avoid warnings)
        self.df_index = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "hlc3": pd.Series(dtype="float64"),
            "fut_volume": pd.Series(dtype="float64"),
            "sma_100": pd.Series(dtype="float64"),
            "ema_9": pd.Series(dtype="float64"),
            "angle_ema_9": pd.Series(dtype="float64"),
            "vwap": pd.Series(dtype="float64"),
            "upperbound": pd.Series(dtype="float64"),
            "lowerbound": pd.Series(dtype="float64"),
        })

        self._max_order_counter = int(sp.get("trade-per-day", sp.get("trade_per_day", 5)) or 5)
        self._order_counter = 0
        self._last_outside_trading_window_log_minute: Optional[str] = None
        self._last_inside_trading_window_log_minute: Optional[str] = None
        self._current_trading_window_minute: Optional[str] = None
        self._current_trading_window_open = False
        configured_post_exit_cooldown = safe_float(sp.get("post_exit_cooldown_minutes", 5))
        self._post_exit_cooldown_minutes = max(
            int(configured_post_exit_cooldown) if configured_post_exit_cooldown is not None else 5,
            0,
        )
        self._post_exit_cooldown_until: Optional[datetime] = None
        configured_daily_loss_amount = safe_float(
            sp.get("max_daily_loss_amount", 5000.0)
        )
        self._max_daily_loss_amount = (
            configured_daily_loss_amount if configured_daily_loss_amount is not None else 5000.0
        )
        self._daily_loss_blocked_day: Optional[str] = None
        self._today_realized_pnl_day: Optional[str] = None
        self._today_realized_pnl: float = 0.0
        self._today_realized_pnl_trade_ids = set()
        self.order_maneger = order_manager
        self.has_CDN = has_CDN
        self.has_stocks = has_stocks
        self._fut_vol_minute: Optional[str] = None
        self._fut_vol_start_vtt: Optional[float] = None
        self._fut_vol_last_vtt: Optional[float] = None
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
            "start_trail_after": None
        }
        self.trade_start, self.trade_end = self._init_trade_window_times()
        self._trade_end_time = self.trade_end
        self._setup_gap_state()
        self._initialize_from_intraday_candles(intraday_index_candles, intraday_future_candles)
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

    def _reset_order_container(self) -> None:
        self._order_container = {k: None for k in self._order_container}

    def _clear_waiting_order_intent(self) -> None:
        if self._order_container.get("status") == constants.WAITING:
            self._reset_order_container()

    def _update_open_order_ltp(self, feed_response) -> None:
        if self._order_container.get("status") != constants.OPEN:
            return
        for item in feed_response or []:
            if item.get("instrument_key") != self._order_container.get("instrument_key"):
                continue
            ltp = safe_float(item.get("ltp"))
            if ltp is not None:
                self._order_container["ltp"] = ltp
            return

    def _square_off_open_trade(self, reason: str) -> None:
        if self.order_maneger is None or self._order_container.get("status") != constants.OPEN:
            return
        trade_id = self._order_container.get("trade_id")
        latest_ltp = safe_float(self._order_container.get("ltp"))
        if not trade_id or latest_ltp is None:
            return

        square_ts = self._resolve_reference_ts()
        trade_closed = self.order_maneger.square_off_trade(
            trade_id=trade_id,
            exit_price=latest_ltp,
            ts=square_ts,
            reason=reason,
        )
        if trade_closed:
            trade_info = self.order_maneger.get_trade_by_id(trade_id)
            self._update_today_realized_pnl_on_trade_close(trade_info, ts=square_ts)
            self._reset_order_container()

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

    def _get_params_from_yaml(self):
        param_path = constants.PARAM_PATH
        if not os.path.exists(param_path):
            logger.warning(f"Parameter file not found at {param_path}; using defaults.")
            return {}
        with open(param_path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file) or {}

    def _empty_future_candle_frame(self) -> pd.DataFrame:
        return pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "oi": pd.Series(dtype="float64"),
        })

    def _populate_index_future_data(self):
        if not self.index_fut_path or not os.path.exists(self.index_fut_path):
            if self.index_fur_key is not None:
                self.future_data_from_parquet = True
                logger.info(f"Using parquet feed for Nifty future candles. instrument_key={self.index_fur_key}")
            else:
                logger.debug(f"Index future data file not found at {self.index_fut_path}")
                self.future_data_from_parquet = False
            return self._empty_future_candle_frame()

        self.future_data_from_parquet = False
        logger.info(f"Using index future JSON data from {self.index_fut_path}")
        with open(self.index_fut_path, "r") as f:
            data = json.load(f) or {}

        candles = None
        if isinstance(data, dict):
            if isinstance(data.get("data"), dict):
                candles = data["data"].get("candles")
            elif "candles" in data:
                candles = data.get("candles")

        if not isinstance(candles, list) or len(candles) == 0:
            logger.warning(f"Unexpected future data format, unable to parse candles from {self.index_fut_path}")
            return self._empty_future_candle_frame()

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
                candle_time = pd.to_datetime(
                    candle.get("datetime") or candle.get("time") or candle.get("date")
                ).strftime("%Y-%m-%d %H:%M")
                rows.append({
                    "time": candle_time,
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume"),
                    "oi": candle.get("open_interest") or candle.get("oi"),
                })

        df = pd.DataFrame(rows)
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
            df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M")
        return df

    def _get_index_fut_path(self) -> Optional[str]:
        configured_path = None
        if self.params and 'data-sources' in self.params:
            sources_dict = self.params['data-sources']
            if 'nifty-volume' in sources_dict:
                configured_path = sources_dict['nifty-volume']
        return configured_path

    def _get_index_fut_key(self) -> Optional[str]:
        configured_key = None
        if self.params and 'data-sources' in self.params:
            sources_dict = self.params['data-sources']
            if 'nifty-future' in sources_dict:
                configured_key = sources_dict['nifty-future']
        return configured_key

    def start(self):
        return None

    def stop(self):
        return None

    def trigger(self):
        logger.info("meanrev_vwap is driven by BotSquadron NATS ticks in live mode.")

    def on_ws_reconnected(self):
        logger.info("WebSocket reconnected; meanrev_vwap strategy state preserved.")

    def get_subscription_instruments(self) -> List[str]:
        instruments: List[str] = []

        def add_instrument(instrument_key: Any) -> None:
            key = str(instrument_key or "").strip()
            if key and key not in instruments:
                instruments.append(key)

        add_instrument(constants.NIFTY50_SYMBOL)
        for value in self.selected_contracts.values() if isinstance(self.selected_contracts, dict) else []:
            if isinstance(value, dict):
                add_instrument(value.get("instrument_key"))
            elif isinstance(value, list):
                for contract in value:
                    if isinstance(contract, dict):
                        add_instrument(contract.get("instrument_key"))
        return instruments

    def _normalize_feed_item(
        self,
        instrument_key: str,
        feed: Dict[str, Any],
        current_ts: Optional[float],
    ) -> Optional[Dict[str, Any]]:
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
        greeks = market_ff.get("optionGreeks") or market_ff.get("greeks") or first_level.get("optionGreeks") or {}
        return {
            "instrument_key": instrument_key,
            "ltp": ltp,
            "ltt": int(ltt),
            "ts_epoch_ms": int(ltt),
            "oi": safe_float(market_ff.get("oi") or first_level.get("oi")),
            "vtt": safe_float(market_ff.get("vtt")),
            "gamma": safe_float(greeks.get("gamma")),
            "one_min_open": safe_float(market_ff.get("one_min_open")),
            "one_min_high": safe_float(market_ff.get("one_min_high")),
            "one_min_low": safe_float(market_ff.get("one_min_low")),
            "one_min_close": safe_float(market_ff.get("one_min_close")),
            "one_min_volume": safe_float(market_ff.get("one_min_volume") or market_ff.get("volume")),
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
            item = self._normalize_feed_item(str(instrument_key), feed, current_ts)
            if item is not None:
                normalized.append(item)
        return normalized

    @staticmethod
    def _candle_row(candle: Any) -> Optional[Dict[str, Any]]:
        if isinstance(candle, (list, tuple)) and len(candle) >= 5:
            raw_time, open_price, high_price, low_price, close_price = candle[:5]
            volume = candle[5] if len(candle) > 5 else 0.0
            oi = candle[6] if len(candle) > 6 else None
        elif isinstance(candle, dict):
            raw_time = candle.get("time") or candle.get("datetime") or candle.get("date")
            open_price = candle.get("open")
            high_price = candle.get("high")
            low_price = candle.get("low")
            close_price = candle.get("close")
            volume = candle.get("volume", 0.0)
            oi = candle.get("open_interest") or candle.get("oi")
        else:
            return None
        ts = pd.to_datetime(raw_time, errors="coerce")
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize(ist)
        else:
            ts = ts.tz_convert(ist)
        values = [safe_float(open_price), safe_float(high_price), safe_float(low_price), safe_float(close_price)]
        if any(value is None for value in values):
            return None
        return {
            "time": ts.strftime("%Y-%m-%d %H:%M"),
            "open": float(values[0]),
            "high": float(values[1]),
            "low": float(values[2]),
            "close": float(values[3]),
            "volume": float(safe_float(volume) or 0.0),
            "oi": safe_float(oi),
        }

    def _initialize_from_intraday_candles(self, index_candles, future_candles=None) -> None:
        future_rows = [row for row in (self._candle_row(c) for c in (future_candles or [])) if row]
        future_rows.sort(key=lambda row: row["time"])
        if future_rows:
            self.df_index_future = pd.DataFrame(future_rows[:-1], columns=self.df_index_future.columns)
            self.curr_fut_candle = dict(future_rows[-1])
            self.curr_fut_minute = future_rows[-1]["time"]
            self.last_fut_bar = dict(future_rows[-2] if len(future_rows) > 1 else future_rows[-1])
            for row in future_rows:
                self.future_minutes_processed[row["time"]] = True

        index_rows = [row for row in (self._candle_row(c) for c in (index_candles or [])) if row]
        index_rows.sort(key=lambda row: row["time"])
        if not index_rows:
            return
        first = index_rows[0]
        self._update_gap_stats(first)
        completed_rows = [{key: row[key] for key in ("time", "open", "high", "low", "close")} for row in index_rows[:-1]]
        if completed_rows:
            self.df_index = pd.DataFrame(completed_rows, columns=["time", "open", "high", "low", "close"])
            self._apply_indicators()
            self.last_index_bar = self.df_index.iloc[-1].to_dict()
        current = index_rows[-1]
        self.curr_index_minute = current["time"]
        self.curr_index_candle = {key: current[key] for key in ("time", "open", "high", "low", "close")}
        self._present_atm_price = int(round(current["close"] / 50) * 50)
        for row in index_rows:
            self.index_minutes_processed[row["time"]] = True

    def _get_itm_contracts(self, side: str, index_price: float, itm_range: float) -> Dict[str, Dict[str, Any]]:
        output: Dict[str, Dict[str, Any]] = {}
        spot_price = safe_float(index_price)
        if spot_price is None or spot_price <= 0 or not isinstance(self.selected_contracts, dict):
            return output
        is_call = str(side or "").upper() in {constants.CALL, constants.CE}
        is_put = str(side or "").upper() in {constants.PUT, constants.PE}
        for strike_key, contracts in self.selected_contracts.items():
            if strike_key == "Nifty_Future" or not isinstance(contracts, list):
                continue
            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                strike = safe_float(contract.get("strike_price")) or safe_float(strike_key)
                if strike is None:
                    continue
                instrument_type = str(contract.get("instrument_type") or "").upper()
                allowed = (
                    is_call and instrument_type in {constants.CALL, constants.CE} and spot_price - itm_range <= strike <= spot_price
                ) or (
                    is_put and instrument_type in {constants.PUT, constants.PE} and spot_price <= strike <= spot_price + itm_range
                )
                instrument_key = contract.get("instrument_key")
                if allowed and instrument_key:
                    output[instrument_key] = contract
        return output

    def _restore_open_order_container_from_ordersystem(self) -> None:
        if self.order_maneger is None or not hasattr(self.order_maneger, "get_account_details"):
            return
        try:
            account = self.order_maneger.get_account_details()
        except Exception as exc:
            logger.warning(f"Open trade restore skipped; ordersystem account lookup failed: {exc}")
            return
        trades = account.get("trades") if isinstance(account, dict) else None
        open_trades = [
            trade for trade in (trades or [])
            if isinstance(trade, dict) and str(trade.get("status") or "").strip().upper() == constants.OPEN
        ]
        if not open_trades:
            return
        trade = open_trades[-1]
        trade_id = str(trade.get("id") or trade.get("trade_id") or "").strip()
        instrument_key = str(trade.get("instrument_token") or "").strip()
        if not trade_id or not instrument_key:
            logger.warning(f"Cannot restore OPEN trade with missing id/instrument_token: {trade}")
            return
        contract = self._find_selected_contract(instrument_key)
        symbol = str(trade.get("symbol") or (contract or {}).get("trading_symbol") or "").strip()
        self._order_container.update({
            "trade_id": trade_id,
            "side": self._strategy_side_from_trade(trade, contract),
            "instrument_key": instrument_key,
            "instrument_symbol": symbol,
            "status": constants.OPEN,
            "ltp": safe_float(trade.get("entry_price")),
            "lot": None,
            "max_gamma": None,
            "start_trail_after": safe_float(trade.get("start_trail_after")),
        })
        self._order_counter = max(self._order_counter, 1)
        logger.info(f"Restored OPEN trade from ordersystem into _order_container: {self._order_container}")

    def _find_selected_contract(self, instrument_key: str) -> Optional[Dict[str, Any]]:
        target = str(instrument_key or "").strip()
        for value in self.selected_contracts.values() if isinstance(self.selected_contracts, dict) else []:
            contracts = [value] if isinstance(value, dict) else value if isinstance(value, list) else []
            for contract in contracts:
                if isinstance(contract, dict) and str(contract.get("instrument_key") or "").strip() == target:
                    return contract
        return None

    @staticmethod
    def _strategy_side_from_trade(trade: Dict[str, Any], contract: Optional[Dict[str, Any]]) -> Optional[str]:
        values = [trade.get("symbol"), trade.get("description")]
        if isinstance(contract, dict):
            values.extend([contract.get("trading_symbol"), contract.get("instrument_type")])
        for value in values:
            text = str(value or "").strip().upper()
            tokens = text.replace("_", " ").replace("-", " ").replace("|", " ").split()
            if "CALL" in tokens or "CE" in tokens or text.endswith("CE"):
                return constants.CALL
            if "PUT" in tokens or "PE" in tokens or text.endswith("PE"):
                return constants.PUT
        return None

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
        if self.curr_fut_candle is None:
            return
        self._upsert_future_candle(self.curr_fut_candle)
        self.last_fut_bar = dict(self.curr_fut_candle)
        self.curr_fut_candle = None

    def _handle_fut_tick_from_feed(self, minute_key: str, item: Dict[str, Any]) -> None:
        ltp = safe_float(item.get("ltp"))
        if ltp is None or ltp <= 0:
            return
        vtt = safe_float(item.get("vtt"))

        if self.curr_fut_minute is None or minute_key != self.curr_fut_minute:
            previous_vtt = self._fut_vol_last_vtt
            if self.curr_fut_candle is not None:
                self._finalize_fut_candle()

            self.curr_fut_minute = minute_key
            self._fut_vol_minute = minute_key
            if vtt is not None:
                self._fut_vol_start_vtt = previous_vtt if previous_vtt is not None and vtt >= previous_vtt else vtt
                self._fut_vol_last_vtt = vtt
            self.curr_fut_candle = {
                "time": minute_key,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": 0.0,
                "oi": safe_float(item.get("oi")),
            }

        c = self.curr_fut_candle
        if c is None:
            return

        one_min_open = safe_float(item.get("one_min_open"))
        one_min_high = safe_float(item.get("one_min_high"))
        one_min_low = safe_float(item.get("one_min_low"))
        one_min_close = safe_float(item.get("one_min_close"))
        one_min_volume = safe_float(item.get("one_min_volume"))
        oi = safe_float(item.get("oi"))

        if one_min_open is not None and one_min_open > 0:
            c["open"] = one_min_open

        if one_min_high is not None and one_min_high > 0:
            c["high"] = one_min_high
        else:
            c["high"] = max(float(c["high"]), ltp)

        if one_min_low is not None and one_min_low > 0:
            c["low"] = one_min_low
        else:
            c["low"] = min(float(c["low"]), ltp)

        if one_min_close is not None and one_min_close > 0:
            c["close"] = one_min_close
        else:
            c["close"] = ltp

        if one_min_volume is not None and one_min_volume >= 0:
            c["volume"] = one_min_volume
        elif vtt is not None:
            if self._fut_vol_start_vtt is None or vtt < self._fut_vol_start_vtt:
                self._fut_vol_start_vtt = vtt
            self._fut_vol_last_vtt = vtt
            c["volume"] = max(vtt - float(self._fut_vol_start_vtt), 0.0)

        if oi is not None:
            c["oi"] = oi

        self._upsert_future_candle(c)
        self.last_fut_bar = dict(c)

    def on_ws_message(self, message, option_chain_data=None):
        """
        Main tick handler:
        - updates order manager with option ticks,
        - builds index candles from spot ticks,
        - evaluates the trading engine on each batch.
        """
        feed_response = self._normalize_feed_response(message)
        if not feed_response:
            return
        obj = None
        try:
            for item in feed_response:
                ltt = item.get('ltt')
                ltt_f = safe_float(ltt)
                if ltt_f is None:
                    continue
                
                ts_ms = int(ltt_f)
                dt_object = datetime.fromtimestamp(ts_ms / 1000, ist)
                minute_key = dt_object.strftime('%Y-%m-%d %H:%M')
                instrument_key = item.get("instrument_key")

                if instrument_key == constants.NIFTY50_SYMBOL:
                    obj = item
                    
                    # Guard: ltp must be numeric.
                    raw_ltp = item.get('ltp')
                    try:
                        ltp = float(raw_ltp)
                    except (TypeError, ValueError):
                        logger.error(f"Invalid ltp for index tick. instrument={item.get('instrument_key')} ltp={raw_ltp} obj={obj}")
                        continue  # skip only this bad tick

                    self._present_atm_price = (round(ltp / 50) * 50)

                    self._handle_index_tick(minute_key, ltp)
                elif self.index_fur_key is not None and instrument_key == self.index_fur_key:
                    self._handle_fut_tick_from_feed(minute_key, item)
                else:
                    ltp = safe_float(item.get("ltp"))
                    if ltp is None:
                        continue
                    self.atr5_engine.on_tick(instrument_key, ltp, dt_object)

            if self.curr_index_minute:
                current_window_open = self._is_trading_window(self.curr_index_minute)
                self._set_trading_window_state(self.curr_index_minute, current_window_open)
            self._trade_processing(feed_response)

        except Exception as e:
            logger.exception(f"An error occurred in obj {obj} on_ws_message: {e}")

    def _log_outside_trading_window(self, minute_key: str) -> None:
        if minute_key == self._last_outside_trading_window_log_minute:
            return
        logger.info(f"Outside Trading Window at {minute_key}")
        self._last_outside_trading_window_log_minute = minute_key

    def _set_trading_window_state(self, minute_key: str, is_open: bool) -> None:
        self._current_trading_window_minute = minute_key
        self._current_trading_window_open = bool(is_open)
        if is_open:
            self._last_outside_trading_window_log_minute = None
            self._log_inside_trading_window(minute_key)
            return

        self._last_inside_trading_window_log_minute = None
        self._log_outside_trading_window(minute_key)

    def _log_inside_trading_window(self, minute_key: str) -> None:
        if minute_key == self._last_inside_trading_window_log_minute:
            return
        status = self._order_container.get("status") or "IDLE"
        side = self._order_container.get("side") or "NONE"
        logger.info(
            f"Inside Trading Window at {minute_key}; "
            f"status={status}, side={side}, "
            f"trades={self._order_counter}/{self._max_order_counter}, "
            f"candles={len(self.df_index)}"
        )
        self._last_inside_trading_window_log_minute = minute_key

    def _trade_processing(self, feed_response):
        """
        Trade lifecycle processor.
        WAITING: pick best contract and place order.
        OPEN: forward latest tick to OMS and sync local state after exits.
        """
        if (
            self.curr_index_minute
            and (
                self._current_trading_window_minute != self.curr_index_minute
                or not self._current_trading_window_open
            )
        ):
            self._update_open_order_ltp(feed_response)
            self._clear_waiting_order_intent()
            self._square_off_open_trade(reason=constants.EOD_SQUARE_OFF)
            return

        if not feed_response:
            return

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        dict_itm = {}
        ts = None

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

            itm_range = float(sp.get("itm_strike_range", 200))

            if not self.last_index_bar:
                return
            dict_itm = self._get_itm_contracts(
                self._order_container["side"],
                self.last_index_bar["close"],
                itm_range
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
            atr_target_mult = float(sp.get("atr_target_mult", self.params.get("atr_target_mult", 5.0)))
  
            option_atr = self.atr5_engine.get_atr(chosen["instrument_key"])
            target = None
            sl_trigger = None
            start_trail_after = None
            risk_mode = "atr"

            if use_option_atr_risk and option_atr is not None and option_atr > 0:
                atr_to_use = option_atr
                target = entry_price + (10 * option_atr)
                sl_trigger = entry_price - (3 * option_atr)
                start_trail_after = float((2.5 * option_atr) / entry_price)

                risk_mode = "atr"
            else:
                if use_option_atr_risk and require_option_atr:
                    logger.warning(f"Skipping order; option ATR unavailable for {chosen['instrument_key']}")
                    self._order_container["instrument_key"] = None
                    self._order_container["ltp"] = None
                    self._order_container["max_gamma"] = None
                    self._order_container["instrument_symbol"] = None
                    return

                tp_pct = float(sp.get("take-profit", sp.get("take_profit", self.params.get("take-profit", self.params.get("take_profit", 0.30)))))
                sl_pct = float(sp.get("stop-loss", sp.get("stop_loss", self.params.get("stop-loss", self.params.get("stop_loss", 0.20)))))
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
                atr_to_use = safe_float(sp.get("trailing-stop-distance", sp.get("trailing_stop_distance", 10))) or 10.0

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

            description = f"{self._order_container['side']} {self._order_container['instrument_symbol']} entry={entry_price:.2f}"

            try:
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
                    ts=ts
                )
            except Exception as exc:
                logger.warning(f"Order placement deferred for {self._order_container['side']}: {exc}")
                self._order_container["instrument_key"] = None
                self._order_container["instrument_symbol"] = None
                self._order_container["ltp"] = None
                self._order_container["max_gamma"] = None
                return

            logger.info(
                f"OrderInfo TradeID: {trade_id}, Entry(PU): {entry_price:.2f}, Qty: {qty}, "
                f"Target(PU): {target:.2f}, SL_trig(PU): {sl_trigger:.2f}, "
                f"SL_lim(PU): {sl_limit:.2f}, TrailOn: {trailing_enabled}, TrailDist: {trail_points:.2f}, "
                f"TrailStartAfterPts: {(entry_price + (entry_price*start_trail_after)):.2f} start_trail_after: {start_trail_after}, "
                f"RiskMode: {risk_mode}, OptionATR: {option_atr}"
            )

            if trade_id:
                self._order_container["trade_id"] = trade_id
                self._order_container["status"] = constants.OPEN
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
                self._order_container["ltp"] = float(latest_ltp)
                self.order_maneger.on_tick(
                    symbol=self._order_container["instrument_symbol"],
                    o=latest_ltp, h=latest_ltp, l=latest_ltp, c=latest_ltp,
                    ts=ts,
                    force_trail=False,
                )

                trade_info = self.order_maneger.get_trade_by_id(self._order_container.get('trade_id'))

                if trade_info and trade_info["status"] in [constants.TARGET_HIT,constants.STOPLOSS_HIT,constants.MANUAL_EXIT,constants.EOD_SQUARE_OFF]:
                    logger.debug(f"Trade closed Info: {trade_info}")
                    self._set_post_exit_cooldown(trade_info.get("status"), ts=ts)
                    self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                    self._reset_order_container()


        if self.curr_index_minute :

            current_time = datetime.strptime(self.curr_index_minute , '%Y-%m-%d %H:%M').time()

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
                            reason=constants.EOD_SQUARE_OFF
                        )
                    trade_info = self.order_maneger.get_trade_by_id(trade_id)
                    self._update_today_realized_pnl_on_trade_close(trade_info, ts=square_ts)
                    self._reset_order_container()
    
                
    def _handle_index_tick(self, minute_key: str, ltp: float):
        """Aggregate spot ticks into 1-minute OHLC candles."""
        
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
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp
    

    def _finalize_index_candle(self):
        """Persist completed candle and run dependent analytics."""
        c = self.curr_index_candle
        if c is None:
            return
        logger.info(f"Current minute:{self.curr_index_minute}, Finalizing index candle: {c}")
        self.df_index = pd.concat([self.df_index, pd.DataFrame([c])], ignore_index=True)
        self.last_index_bar = c
        self.curr_index_candle = None
        self._apply_indicators()
        self._trading_engine_active()


    def _is_trading_window(self, time_str: str) -> bool:
        ts = pd.to_datetime(time_str, errors="coerce")
        if pd.isna(ts):
            return False
        current = ts.time()
        return self.trade_start <= current <= self.trade_end

    def _init_trade_window_times(self) -> tuple[time, time]:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        trade_window = sp.get("trade-window") or sp.get("trade_window") or self.params.get("trade-window") or self.params.get("trade_window") or {}
        if not isinstance(trade_window, dict):
            trade_window = {}
        market_hours = self.params.get("market-hours", {}) if isinstance(self.params, dict) else {}
        start_str = trade_window.get("start", market_hours.get("start", "09:45"))
        end_str = trade_window.get("end", market_hours.get("end", constants.EOD_SQUARE_OFF_TIME))
        return self._parse_hhmm(start_str, time(9, 45)), self._parse_hhmm(end_str, time(15, 15))

    @staticmethod
    def _parse_hhmm(value: Any, default: time) -> time:
        text = str(value or "").strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(text, fmt).time()
            except ValueError:
                continue
        return default

    def _resolve_reference_ts(self) -> datetime:
        if self.curr_index_minute:
            try:
                return datetime.strptime(self.curr_index_minute, "%Y-%m-%d %H:%M").replace(tzinfo=ist)
            except Exception:
                pass
        return datetime.now(ist)

    def _set_post_exit_cooldown(self, exit_status: Optional[str], ts: Optional[datetime] = None) -> None:
        status = str(exit_status or "").strip().upper()
        closed_statuses = {
            constants.STOPLOSS_HIT.upper(),
            constants.TARGET_HIT.upper(),
            constants.MANUAL_EXIT.upper(),
            constants.EOD_SQUARE_OFF.upper(),
        }
        if status not in closed_statuses:
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
        if self._max_daily_loss_amount <= 0:
            return False

        day_key = self._refresh_today_realized_pnl_cache(now_ts)
        today_loss_amount = max(-float(self._today_realized_pnl), 0.0)

        if today_loss_amount >= self._max_daily_loss_amount:
            if self._daily_loss_blocked_day != day_key:
                self._daily_loss_blocked_day = day_key
                logger.warning(
                    f"Daily loss guard activated for {day_key}. "
                    f"Loss={today_loss_amount:.2f} >= Limit={self._max_daily_loss_amount:.2f}, "
                    f"TodayRealizedPnL={self._today_realized_pnl:.2f}"
                )
            return True

        return self._daily_loss_blocked_day == day_key

    def _apply_indicators(self):
        """
        Apply session VWAP with upper/lower bounds.
        """
        self.df_index["time"] = pd.to_datetime(self.df_index["time"])
        high_series = pd.to_numeric(self.df_index["high"], errors="coerce").astype("float64")
        low_series = pd.to_numeric(self.df_index["low"], errors="coerce").astype("float64")
        close_series = pd.to_numeric(self.df_index["close"], errors="coerce").astype("float64")
        self.df_index["hlc3"] = (high_series + low_series + close_series) / 3.0

        ema_9 = close_series.ewm(span=9, adjust=False, min_periods=9).mean()
        slope_ema_9 = (ema_9 - ema_9.shift(self._slope_window)) / float(self._slope_window)
        self.df_index["ema_9"] = ema_9.to_numpy()
        self.df_index["angle_ema_9"] = np.degrees(
            np.arctan(np.clip(slope_ema_9.to_numpy(dtype="float64"), -10.0, 10.0))
        )

        self.df_index["fut_volume"] = pd.Series(np.nan, index=self.df_index.index, dtype="float64")
        self.df_index["vwap"] = pd.Series(np.nan, index=self.df_index.index, dtype="float64")
        self.df_index["upperbound"] = pd.Series(np.nan, index=self.df_index.index, dtype="float64")
        self.df_index["lowerbound"] = pd.Series(np.nan, index=self.df_index.index, dtype="float64")

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        vwap_band_multiplier = safe_float(sp.get("vwap_band_multiplier", sp.get("band_mult_1", 1.0)))
        if vwap_band_multiplier is None or vwap_band_multiplier < 0:
            vwap_band_multiplier = 1.0
        calc_mode = str(
            sp.get("vwap_band_calc_mode", sp.get("bands_calc_mode", "standard_deviation")) or "standard_deviation"
        ).strip().lower()
        use_percentage_bands = calc_mode == "percentage"

        if self.df_index_future is not None and not self.df_index_future.empty:
            df_future = self.df_index_future.copy()
            df_future["time"] = pd.to_datetime(df_future["time"], errors="coerce")
            df_future["volume"] = pd.to_numeric(df_future["volume"], errors="coerce")
            df_future = df_future.dropna(subset=["time"]).sort_values("time")
            df_future = df_future.drop_duplicates(subset=["time"], keep="last")

            df_merged = pd.merge(
                self.df_index[["time", "hlc3"]],
                df_future[["time", "volume"]],
                on="time",
                how="left",
            )
            df_merged["date"] = df_merged["time"].dt.date
            df_merged["volume"] = pd.to_numeric(df_merged["volume"], errors="coerce").fillna(0.0)
            df_merged["pv"] = df_merged["hlc3"] * df_merged["volume"]
            df_merged["p2v"] = (df_merged["hlc3"] ** 2) * df_merged["volume"]
            df_merged["cum_vol"] = df_merged.groupby("date")["volume"].cumsum()
            df_merged["cum_pv"] = df_merged.groupby("date")["pv"].cumsum()
            df_merged["cum_p2v"] = df_merged.groupby("date")["p2v"].cumsum()

            valid_volume = df_merged["cum_vol"] > 0
            vwap_values = pd.Series(np.nan, index=df_merged.index, dtype="float64")
            vwap_values.loc[valid_volume] = (
                df_merged.loc[valid_volume, "cum_pv"] / df_merged.loc[valid_volume, "cum_vol"]
            )
            variance = pd.Series(np.nan, index=df_merged.index, dtype="float64")
            variance.loc[valid_volume] = (
                df_merged.loc[valid_volume, "cum_p2v"] / df_merged.loc[valid_volume, "cum_vol"]
            ) - (vwap_values.loc[valid_volume] ** 2)
            variance = variance.clip(lower=0.0)
            stdev_abs = pd.Series(np.sqrt(variance.to_numpy(dtype="float64")), index=df_merged.index, dtype="float64")
            band_basis = (vwap_values.abs() * 0.01) if use_percentage_bands else stdev_abs

            self.df_index["fut_volume"] = pd.Series(df_merged["volume"].to_numpy(), index=self.df_index.index, dtype="float64")
            self.df_index["vwap"] = pd.Series(vwap_values.to_numpy(), index=self.df_index.index, dtype="float64")
            self.df_index["upperbound"] = pd.Series(
                (vwap_values + (band_basis * float(vwap_band_multiplier))).to_numpy(),
                index=self.df_index.index,
                dtype="float64",
            )
            self.df_index["lowerbound"] = pd.Series(
                (vwap_values - (band_basis * float(vwap_band_multiplier))).to_numpy(),
                index=self.df_index.index,
                dtype="float64",
            )

        self.df_index["time"] = pd.to_datetime(self.df_index["time"])

    def _trading_engine_active(self):
        """
        Enter from the latest completed candle's position relative to VWAP bands.
        """
        try:
            current_minute = self.curr_index_minute
            if not current_minute:
                return

            current_window_open = self._is_trading_window(current_minute)
            self._set_trading_window_state(current_minute, current_window_open)
            if not current_window_open:
                return

            if not self.enable_trading_engine:
                return
            candle_time = current_minute

            if self.df_index.empty:
                return

            ref_ts = self._resolve_reference_ts()
            latest = self.df_index.iloc[-1]
            candle_time = str(latest.get("time") or candle_time)

            if self._order_container["status"] is not None:
                return

            if self._is_post_exit_cooldown_active(ref_ts):
                cooldown_left_sec = int(max((self._post_exit_cooldown_until - ref_ts).total_seconds(), 0))
                logger.debug(
                    f"candle_time={candle_time}, Entry blocked by post-exit cooldown for {cooldown_left_sec}s "
                    f"(until {self._post_exit_cooldown_until.strftime('%H:%M:%S')})"
                )
                return

            if self._is_daily_loss_limit_active(ref_ts):
                return
            

            required_values = {
                "open": safe_float(latest.get("open")),
                "close": safe_float(latest.get("close")),
                "upperbound": safe_float(latest.get("upperbound")),
                "lowerbound": safe_float(latest.get("lowerbound")),
                "ema_9": safe_float(latest.get("ema_9")),
                "angle_ema_9": safe_float(latest.get("angle_ema_9")),
            }
            if any(value is None for value in required_values.values()):
                return

            open_price = float(required_values["open"])
            close_price = float(required_values["close"])
            upperbound = float(required_values["upperbound"])
            lowerbound = float(required_values["lowerbound"])
            ema_9 = float(required_values["ema_9"])
            angle_ema_9 = float(required_values["angle_ema_9"])
            band_width = upperbound - lowerbound
            entry_band_tick_size = self._entry_band_tick_size()
            upperbound_floor = self._round_entry_band_to_tick(
                upperbound,
                entry_band_tick_size,
                "FLOOR",
            )
            lowerbound_ceiling = self._round_entry_band_to_tick(
                lowerbound,
                entry_band_tick_size,
                "CEIL",
            )

            logger.debug(
                f"candle_time={candle_time}, Setup inputs open={open_price}, close={close_price}, "
                f"vwap_session_upperband={upperbound}, vwap_session_lowerband={lowerbound}, "
                f"upperbound_floor={upperbound_floor}, lowerbound_ceiling={lowerbound_ceiling}, "
                f"band_width={band_width}, max_band_width=60, ema_9={ema_9}, "
                f"angle_ema_9={angle_ema_9}, call_angle_threshold={self.call_ema_9_angle_threshold}, "
                f"put_angle_threshold={self.put_ema_9_angle_threshold}"
            )

            call_setup = (
                open_price <= lowerbound_ceiling
                and close_price <= lowerbound_ceiling
                and band_width < 60
                and angle_ema_9 > self.call_ema_9_angle_threshold
            )
            put_setup = (
                open_price >= upperbound_floor
                and close_price >= upperbound_floor
                and band_width < 60
                and angle_ema_9 < self.put_ema_9_angle_threshold
            )

            logger.debug(
                f"candle_time={candle_time}, Setup result call_setup={call_setup}, put_setup={put_setup}"
            )

            if call_setup and self._order_counter < self._max_order_counter:
                lot = self._calculate_lot_size(constants.CALL, True, False)
                if lot <= 0:
                    return

                self._order_container["side"] = constants.CALL
                self._order_container["status"] = constants.WAITING
                self._order_container["lot"] = int(lot)
                logger.info(f"Order intent set side={constants.CALL}, lot={lot}, status={constants.WAITING}")
                return

            if put_setup and self._order_counter < self._max_order_counter:
                lot = self._calculate_lot_size(constants.PUT, False, True)
                if lot <= 0:
                    return

                self._order_container["side"] = constants.PUT
                self._order_container["status"] = constants.WAITING
                self._order_container["lot"] = int(lot)
                logger.info(f"Order intent set side={constants.PUT}, lot={lot}, status={constants.WAITING}")
                return

        except Exception as e:
            logger.exception(f"An error occurred in _trading_engine_active: {e}")

    
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
                return medium
            elif self._daily_sentiment == constants.SIDEWAYS:
                return small
            elif self._daily_sentiment == constants.BEARISH:
                return small
        elif side == constants.PUT:
            if self._daily_sentiment == constants.BEARISH and is_bearish_thrust == True:
                return large
            elif  self._daily_sentiment == constants.BEARISH:
                return medium
            elif self._daily_sentiment == constants.SIDEWAYS:
                return small
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

    def _entry_band_tick_size(self) -> float:
        params = getattr(self, "params", {})
        params = params if isinstance(params, dict) else {}
        sp = params.get("strategy-parameters") or params.get("strategy_parameters") or {}
        sp = sp if isinstance(sp, dict) else {}
        configured_tick = sp.get(
            "entry_band_tick_size",
            sp.get(
                "entry-band-tick-size",
                params.get("entry_band_tick_size", params.get("entry-band-tick-size")),
            ),
        )
        tick_size = safe_float(configured_tick)
        return tick_size if tick_size is not None and tick_size > 0 else 0.05

    @staticmethod
    def _round_entry_band_to_tick(value: Any, tick_size: float, mode: str) -> float:
        numeric_value = safe_float(value)
        if numeric_value is None:
            return np.nan

        price = Decimal(str(numeric_value))
        tick = Decimal(str(tick_size))
        if tick <= 0:
            return float(price)

        if mode == "FLOOR":
            rounding = ROUND_FLOOR
        elif mode == "CEIL":
            rounding = ROUND_CEILING
        else:
            raise ValueError(f"Unsupported entry-band rounding mode: {mode}")

        ticks = (price / tick).to_integral_value(rounding=rounding)
        return float(ticks * tick)
