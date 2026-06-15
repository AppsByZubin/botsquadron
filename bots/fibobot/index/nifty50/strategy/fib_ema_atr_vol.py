import json
import math
import os
import sys
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

import common.constants as constants
import logger
from technicals.atr.atr_for_ticks import AtrEngine
from utils.generic_utils import safe_float


ist = ZoneInfo("Asia/Kolkata")
logger = logger.create_logger("FibEmaAtrVolStrategyLogger")


class FibEmaAtrVolStrategy:
    """
    Fibonacci pivot + EMA trend + ATR/volume filter strategy.

    The strategy builds confirmed N-minute index candles, calculates previous-day
    Fibonacci pivots, EMA 9/21, ATR 13 and optional volume filters, then creates
    CALL/PUT option intents through the shared order manager.
    """

    TRADE_START = "09:20"
    TRADE_END = "15:15"

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
        self.has_CDN = has_CDN
        self.has_stocks = has_stocks
        self.uptox_client = uptox_client
        self.previous_day_trend = previous_day_trend
        self.selected_contracts = selected_contracts or {}
        self.index_minutes_processed = index_minutes_processed or {}
        self.future_minutes_processed = future_minutes_processed or {}
        self.order_maneger = order_manager

        self.params = params if isinstance(params, dict) else self._get_params_from_yaml()
        sp = self._strategy_params()
        if self.order_maneger is not None and hasattr(self.order_maneger, "set_strategy_params"):
            self.order_maneger.set_strategy_params(sp)

        self.enable_trading_engine = self._coerce_bool(sp.get("enable_trading_engine"), True)
        self.signal_tf_min = self._coerce_int(
            sp.get("signal_tf_min", sp.get("signal-tf-min")),
            5,
            minimum=1,
            maximum=60,
        )
        self.ema_fast = self._coerce_int(sp.get("ema_fast", sp.get("ema-fast")), 9, minimum=1)
        self.ema_slow = self._coerce_int(sp.get("ema_slow", sp.get("ema-slow")), 21, minimum=1)
        if self.ema_fast >= self.ema_slow:
            logger.warning(
                f"ema_fast={self.ema_fast} should be below ema_slow={self.ema_slow}; "
                "continuing with configured values."
            )

        self.atr_len = self._coerce_int(sp.get("atr_len", sp.get("atr-len")), 13, minimum=1)
        self.atr_mult = self._coerce_float(sp.get("atr_mult", sp.get("atr-mult")), 2.718, minimum=0.0)
        self.vol_sma_len = self._coerce_int(
            sp.get("vol_sma_len", sp.get("vol-sma-len")),
            89,
            minimum=1,
        )
        self.vol_spike_threshold = self._coerce_float(
            sp.get("vol_spike_threshold", sp.get("vol-spike-threshold")),
            4.669,
            minimum=0.0,
        )
        self.min_vol_mult = self._coerce_float(
            sp.get("min_vol_mult", sp.get("min-vol-mult")),
            1.0,
            minimum=0.0,
        )
        self.disable_volume_filter = self._coerce_bool(sp.get("disable_volume_filter"), False)
        self.avoid_exhaustion_entry = self._coerce_bool(sp.get("avoid_exhaustion_entry"), True)
        self.avoid_high_volatility_entry = self._coerce_bool(sp.get("avoid_high_volatility_entry"), False)
        self.allow_breakout_entries = self._coerce_bool(sp.get("allow_breakout_entries"), True)
        self.allow_pullback_entries = self._coerce_bool(sp.get("allow_pullback_entries"), True)

        self._max_order_counter = self._coerce_int(
            sp.get("trade-per-day", sp.get("trade_per_day")),
            3,
            minimum=0,
        )
        self._order_counter = 0
        self.atr5_engine = AtrEngine(
            atr_period=self._coerce_int(sp.get("option_atr_period"), 5, minimum=1)
        )

        self.trade_start, self.trade_end = self._init_trade_window_times()
        self.prev_day_high, self.prev_day_low, self.prev_day_close = self._resolve_previous_day_hlc()
        self.today_date: Optional[str] = None
        self.today_high: Optional[float] = None
        self.today_low: Optional[float] = None
        self.today_close: Optional[float] = None

        self.curr_index_candle: Optional[Dict[str, Any]] = None
        self.curr_index_bucket: Optional[str] = None
        self.curr_index_minute: Optional[str] = None
        self._bucket_volume_by_minute: Dict[str, float] = {}
        self.last_index_bar: Optional[Dict[str, Any]] = None
        self._present_atm_price = None
        self._order_container = self._default_order_container()
        self.last_signal = constants.WAITING
        self.signals: list[Dict[str, Any]] = []

        self.df_index = pd.DataFrame(
            {
                "time": pd.Series(dtype="object"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "hlc3": pd.Series(dtype="float64"),
                "candle_range": pd.Series(dtype="float64"),
                "pp": pd.Series(dtype="float64"),
                "r1": pd.Series(dtype="float64"),
                "r2": pd.Series(dtype="float64"),
                "r3": pd.Series(dtype="float64"),
                "s1": pd.Series(dtype="float64"),
                "s2": pd.Series(dtype="float64"),
                "s3": pd.Series(dtype="float64"),
                "ema_fast": pd.Series(dtype="float64"),
                "ema_slow": pd.Series(dtype="float64"),
                "atr": pd.Series(dtype="float64"),
                "vol_sma": pd.Series(dtype="float64"),
                "high_volatility": pd.Series(dtype="bool"),
                "exhaustion_volume": pd.Series(dtype="bool"),
                "suggested_sl": pd.Series(dtype="float64"),
                "suggested_target": pd.Series(dtype="float64"),
                "signal": pd.Series(dtype="object"),
                "reason": pd.Series(dtype="object"),
            }
        )

        if intraday_index_candles is not None:
            self._initialize_from_intraday_candles(
                intraday_index_candles,
                intraday_future_candles,
            )
        self._restore_open_order_container_from_ordersystem()

    def start(self):
        return None

    def stop(self):
        return None

    def trigger(self):
        logger.info("fib_ema_atr_vol is driven by BotSquadron NATS ticks in live mode.")

    def on_ws_reconnected(self):
        logger.info("WebSocket reconnected; fib_ema_atr_vol strategy state preserved.")

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

        option_greeks = (
            market_ff.get("optionGreeks")
            or market_ff.get("greeks")
            or first_level.get("optionGreeks")
            or {}
        )
        one_min_volume = safe_float(
            feed.get("one_min_volume")
            or feed.get("volume")
            or market_ff.get("one_min_volume")
            or market_ff.get("volume")
            or index_ff.get("one_min_volume")
            or index_ff.get("volume")
        )
        return {
            "instrument_key": instrument_key,
            "ltp": ltp,
            "ltt": int(ltt),
            "ts_epoch_ms": int(ltt),
            "oi": safe_float(market_ff.get("oi") or first_level.get("oi")),
            "vtt": safe_float(market_ff.get("vtt")),
            "gamma": safe_float(option_greeks.get("gamma")),
            "one_min_volume": one_min_volume,
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

    def on_ws_message(self, message):
        feed_response = self._normalize_feed_response(message)
        if not feed_response:
            return

        for item in feed_response:
            instrument_key = item.get("instrument_key")
            if instrument_key == constants.NIFTY50_SYMBOL:
                ltp = safe_float(item.get("ltp"))
                ltt = safe_float(item.get("ltt")) or safe_float(item.get("ts_epoch_ms"))
                if ltp is None or ltt is None:
                    continue

                self._present_atm_price = int(round(ltp / 50) * 50)
                self._handle_index_tick(ltt, ltp, self._extract_index_volume(item))
                continue

            self._update_option_atr(item)

        self._trade_processing(feed_response)

    def _initialize_from_intraday_candles(self, index_candles, _future_candles=None) -> None:
        if not index_candles:
            return

        rows = []
        for candle in index_candles:
            if isinstance(candle, (list, tuple)) and len(candle) >= 5:
                raw_time, open_price, high_price, low_price, close_price = candle[:5]
                volume = candle[5] if len(candle) > 5 else 0.0
            elif isinstance(candle, dict):
                raw_time = candle.get("time") or candle.get("datetime") or candle.get("date")
                open_price = candle.get("open")
                high_price = candle.get("high")
                low_price = candle.get("low")
                close_price = candle.get("close")
                volume = candle.get("volume", 0.0)
            else:
                continue

            ts = pd.to_datetime(raw_time, errors="coerce")
            if pd.isna(ts):
                continue
            if ts.tzinfo is None:
                ts = ts.tz_localize(ist)
            else:
                ts = ts.tz_convert(ist)

            open_f = safe_float(open_price)
            high_f = safe_float(high_price)
            low_f = safe_float(low_price)
            close_f = safe_float(close_price)
            if open_f is None or high_f is None or low_f is None or close_f is None:
                continue

            bucket_dt = ts.to_pydatetime().replace(
                minute=(ts.minute // self.signal_tf_min) * self.signal_tf_min,
                second=0,
                microsecond=0,
            )
            rows.append(
                {
                    "bucket": bucket_dt.strftime("%Y-%m-%d %H:%M"),
                    "minute": ts.strftime("%Y-%m-%d %H:%M"),
                    "open": float(open_f),
                    "high": float(high_f),
                    "low": float(low_f),
                    "close": float(close_f),
                    "volume": max(float(safe_float(volume) or 0.0), 0.0),
                }
            )

        if not rows:
            return

        df = pd.DataFrame(rows).sort_values("minute")
        grouped = df.groupby("bucket", sort=True)
        boot_all = pd.DataFrame(
            {
                "time": grouped["bucket"].first(),
                "open": grouped["open"].first(),
                "high": grouped["high"].max(),
                "low": grouped["low"].min(),
                "close": grouped["close"].last(),
                "volume": grouped["volume"].sum(),
                "minute_count": grouped["minute"].nunique(),
            }
        ).reset_index(drop=True)

        if boot_all.empty:
            return

        complete_mask = pd.to_numeric(boot_all["minute_count"], errors="coerce").fillna(0) >= self.signal_tf_min
        boot = boot_all.loc[complete_mask].drop(columns=["minute_count"], errors="ignore").reset_index(drop=True)
        partial = boot_all.loc[~complete_mask].tail(1)

        if not boot.empty:
            self.df_index = pd.concat([self.df_index, boot], ignore_index=True)

        for row in boot.to_dict("records"):
            self._update_session_ohlc(row)
        for minute_key in df["minute"].astype(str):
            self.index_minutes_processed[minute_key] = True

        if not boot.empty:
            self._apply_indicators()
            signal_event = self._evaluate_latest_signal()
            if signal_event is not None:
                self._set_order_intent(signal_event)
            self.last_index_bar = self.df_index.iloc[-1].to_dict()

        if not partial.empty:
            row = partial.iloc[-1].to_dict()
            bucket_key = str(row.get("time") or "")
            self.curr_index_bucket = bucket_key
            self.curr_index_minute = bucket_key
            self.curr_index_candle = {
                "time": bucket_key,
                "open": safe_float(row.get("open")),
                "high": safe_float(row.get("high")),
                "low": safe_float(row.get("low")),
                "close": safe_float(row.get("close")),
                "volume": safe_float(row.get("volume")) or 0.0,
            }

    def _handle_index_tick(self, ts_ms: float, ltp: float, volume: float = 0.0) -> None:
        tick_dt = datetime.fromtimestamp(float(ts_ms) / 1000, ist).replace(second=0, microsecond=0)
        minute_key = tick_dt.strftime("%Y-%m-%d %H:%M")
        bucket_dt = tick_dt.replace(minute=(tick_dt.minute // self.signal_tf_min) * self.signal_tf_min)
        bucket_key = bucket_dt.strftime("%Y-%m-%d %H:%M")
        self.curr_index_minute = bucket_key

        if self.curr_index_bucket is None or bucket_key != self.curr_index_bucket:
            if self.curr_index_candle is not None:
                self._finalize_index_candle()

            self.curr_index_bucket = bucket_key
            self._bucket_volume_by_minute = {}
            self.curr_index_candle = {
                "time": bucket_key,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": 0.0,
            }
            self._roll_day_if_needed(bucket_key)
        else:
            candle = self.curr_index_candle
            if candle is None:
                return
            candle["high"] = max(float(candle["high"]), ltp)
            candle["low"] = min(float(candle["low"]), ltp)
            candle["close"] = ltp

        self._update_current_bucket_volume(minute_key, volume)

    def _finalize_index_candle(self) -> None:
        candle = self.curr_index_candle
        if candle is None:
            return

        logger.info(f"Finalizing {self.signal_tf_min}m index candle: {candle}")
        self._update_session_ohlc(candle)
        self.df_index = pd.concat([self.df_index, pd.DataFrame([candle])], ignore_index=True)
        self._apply_indicators()
        signal_event = self._evaluate_latest_signal()
        if signal_event is not None:
            self._set_order_intent(signal_event)
        self.last_index_bar = self.df_index.iloc[-1].to_dict()
        self.curr_index_candle = None

    def _apply_indicators(self) -> None:
        if self.df_index.empty:
            return

        high = pd.to_numeric(self.df_index["high"], errors="coerce").astype("float64")
        low = pd.to_numeric(self.df_index["low"], errors="coerce").astype("float64")
        close = pd.to_numeric(self.df_index["close"], errors="coerce").astype("float64")
        volume = pd.to_numeric(self.df_index["volume"], errors="coerce").fillna(0.0).astype("float64")

        self.df_index["hlc3"] = ((high + low + close) / 3.0).to_numpy()
        self.df_index["candle_range"] = (high - low).to_numpy()
        self.df_index["ema_fast"] = close.ewm(
            span=self.ema_fast,
            adjust=False,
            min_periods=1,
        ).mean().to_numpy()
        self.df_index["ema_slow"] = close.ewm(
            span=self.ema_slow,
            adjust=False,
            min_periods=1,
        ).mean().to_numpy()

        prev_close = close.shift(1)
        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        self.df_index["atr"] = self._wilder_rma(tr, self.atr_len).to_numpy()
        self.df_index["vol_sma"] = volume.rolling(
            window=self.vol_sma_len,
            min_periods=self.vol_sma_len,
        ).mean().to_numpy()

        pivots = self._pivot_levels()
        if pivots is not None:
            for key, value in pivots.items():
                self.df_index[key] = float(value)

        atr = pd.to_numeric(self.df_index["atr"], errors="coerce")
        vol_sma = pd.to_numeric(self.df_index["vol_sma"], errors="coerce")
        candle_range = pd.to_numeric(self.df_index["candle_range"], errors="coerce")
        self.df_index["high_volatility"] = (
            atr.notna() & (candle_range > (atr * self.atr_mult))
        ).to_numpy()
        self.df_index["exhaustion_volume"] = (
            (volume > 0)
            & vol_sma.notna()
            & (volume > (vol_sma * self.vol_spike_threshold))
        ).to_numpy()

    @staticmethod
    def _wilder_rma(series: pd.Series, length: int) -> pd.Series:
        length = max(1, int(length))
        numeric = pd.to_numeric(series, errors="coerce").astype("float64").reset_index(drop=True)
        values = numeric.to_list()
        result = [math.nan] * len(values)
        if len(values) < length:
            return pd.Series(result, index=series.index, dtype="float64")

        seed_window = [value for value in values[:length] if value is not None and math.isfinite(value)]
        if len(seed_window) < length:
            return pd.Series(result, index=series.index, dtype="float64")

        previous = sum(seed_window) / float(length)
        result[length - 1] = previous
        for pos in range(length, len(values)):
            value = values[pos]
            if value is None or not math.isfinite(value):
                result[pos] = previous
                continue
            previous = ((previous * (length - 1)) + value) / float(length)
            result[pos] = previous
        return pd.Series(result, index=series.index, dtype="float64")

    def _evaluate_latest_signal(self) -> Optional[Dict[str, Any]]:
        if self.df_index.empty:
            return None

        idx = self.df_index.index[-1]
        latest = self.df_index.iloc[-1]
        latest_time = str(latest.get("time") or "")
        self.df_index.loc[idx, "signal"] = constants.WAITING
        self.df_index.loc[idx, "reason"] = "No valid pivot breakout/rejection with EMA confirmation"
        self.df_index.loc[idx, "suggested_sl"] = math.nan
        self.df_index.loc[idx, "suggested_target"] = math.nan

        if not self._is_trading_window(latest_time):
            self.df_index.loc[idx, "reason"] = "Outside trading window"
            return None

        pivots = self._pivot_levels()
        if pivots is None:
            self.df_index.loc[idx, "reason"] = "Waiting for previous-day H/L/C to calculate pivots"
            return None

        if len(self.df_index) <= max(self.ema_slow, 3):
            self.df_index.loc[idx, "reason"] = (
                f"Waiting for at least {self.ema_slow} confirmed candles"
            )
            return None

        previous = self.df_index.iloc[-2]
        close = safe_float(latest.get("close"))
        open_price = safe_float(latest.get("open"))
        high = safe_float(latest.get("high"))
        low = safe_float(latest.get("low"))
        ema_fast = safe_float(latest.get("ema_fast"))
        ema_slow = safe_float(latest.get("ema_slow"))
        atr = safe_float(latest.get("atr"))
        vol_sma = safe_float(latest.get("vol_sma"))
        volume = safe_float(latest.get("volume")) or 0.0
        prev_close = safe_float(previous.get("close"))

        required = [close, open_price, high, low, ema_fast, ema_slow, prev_close]
        if any(value is None for value in required):
            self.df_index.loc[idx, "reason"] = "Waiting for indicator warmup"
            return None

        if self.avoid_high_volatility_entry and bool(latest.get("high_volatility")):
            self.df_index.loc[idx, "signal"] = "AVOID"
            self.df_index.loc[idx, "reason"] = "High-volatility candle blocked entry"
            return None

        is_bull = close > open_price
        is_bear = close < open_price
        trend_up = ema_fast > ema_slow
        trend_down = ema_fast < ema_slow

        has_volume = volume > 0 and (pd.to_numeric(self.df_index["volume"], errors="coerce").fillna(0.0) > 0).any()
        if self.disable_volume_filter or not has_volume:
            volume_ok = True
        else:
            volume_ok = vol_sma is not None and volume >= (vol_sma * self.min_vol_mult)

        exhaustion_volume = bool(latest.get("exhaustion_volume"))
        exhaustion_ok = (not self.avoid_exhaustion_entry) or (not exhaustion_volume)

        pp = pivots["pp"]
        r1 = pivots["r1"]
        r2 = pivots["r2"]
        r3 = pivots["r3"]
        s1 = pivots["s1"]
        s2 = pivots["s2"]
        s3 = pivots["s3"]

        cross_above_r1 = prev_close <= r1 < close
        cross_above_r2 = prev_close <= r2 < close
        cross_below_s1 = prev_close >= s1 > close
        cross_below_s2 = prev_close >= s2 > close

        bull_reject_pp = low <= pp and close > pp and is_bull
        bull_reject_r1 = low <= r1 and close > r1 and is_bull
        bear_reject_pp = high >= pp and close < pp and is_bear
        bear_reject_s1 = high >= s1 and close < s1 and is_bear

        call_breakout = (
            self.allow_breakout_entries
            and close > pp
            and trend_up
            and volume_ok
            and exhaustion_ok
            and is_bull
            and (cross_above_r1 or cross_above_r2)
        )
        call_pullback = (
            self.allow_pullback_entries
            and close > pp
            and trend_up
            and volume_ok
            and exhaustion_ok
            and (bull_reject_pp or bull_reject_r1)
        )
        put_breakdown = (
            self.allow_breakout_entries
            and close < pp
            and trend_down
            and volume_ok
            and exhaustion_ok
            and is_bear
            and (cross_below_s1 or cross_below_s2)
        )
        put_pullback = (
            self.allow_pullback_entries
            and close < pp
            and trend_down
            and volume_ok
            and exhaustion_ok
            and (bear_reject_pp or bear_reject_s1)
        )

        if call_breakout or call_pullback:
            sl = self._call_sl(close, atr, latest, pivots)
            target = r2 if close < r2 else r3
            reason = "CALL breakout" if call_breakout else "CALL pullback/rejection"
            return self._mark_signal(
                idx=idx,
                side=constants.CALL,
                reason=reason,
                suggested_sl=sl,
                suggested_target=target,
            )

        if put_breakdown or put_pullback:
            sl = self._put_sl(close, atr, latest, pivots)
            target = s2 if close > s2 else s3
            reason = "PUT breakdown" if put_breakdown else "PUT pullback/rejection"
            return self._mark_signal(
                idx=idx,
                side=constants.PUT,
                reason=reason,
                suggested_sl=sl,
                suggested_target=target,
            )

        if exhaustion_volume:
            self.df_index.loc[idx, "signal"] = "AVOID"
            self.df_index.loc[idx, "reason"] = "Huge volume spike: possible exhaustion candle"

        return None

    def _mark_signal(
        self,
        idx: int,
        side: str,
        reason: str,
        suggested_sl: float,
        suggested_target: float,
    ) -> Dict[str, Any]:
        latest = self.df_index.loc[idx]
        self.df_index.loc[idx, "signal"] = side
        self.df_index.loc[idx, "reason"] = reason
        self.df_index.loc[idx, "suggested_sl"] = suggested_sl
        self.df_index.loc[idx, "suggested_target"] = suggested_target
        signal_event = {
            "time": str(latest.get("time")),
            "signal": side,
            "reason": reason,
            "close": safe_float(latest.get("close")),
            "ema_fast": safe_float(latest.get("ema_fast")),
            "ema_slow": safe_float(latest.get("ema_slow")),
            "atr": safe_float(latest.get("atr")),
            "vol_sma": safe_float(latest.get("vol_sma")),
            "pp": safe_float(latest.get("pp")),
            "r1": safe_float(latest.get("r1")),
            "r2": safe_float(latest.get("r2")),
            "s1": safe_float(latest.get("s1")),
            "s2": safe_float(latest.get("s2")),
            "suggested_sl": suggested_sl,
            "suggested_target": suggested_target,
        }
        self.last_signal = side
        self.signals.append(signal_event)
        logger.info(f"FIB/EMA/ATR/VOL signal candidate: {signal_event}")
        return signal_event

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
            logger.warning("Multiple OPEN trades found during startup restore; using latest trade.")

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
        ltp = safe_float(trade.get("entry_price")) or safe_float((contract or {}).get("ltp"))

        self._order_container.update(
            {
                "trade_id": trade_id,
                "side": self._strategy_side_from_trade(trade, contract),
                "instrument_key": instrument_key,
                "instrument_symbol": instrument_symbol,
                "status": constants.OPEN,
                "ltp": ltp,
                "entry_price": ltp,
                "lot": int(qty) if qty is not None and qty > 0 else None,
                "max_gamma": None,
                "start_trail_after": safe_float(trade.get("start_trail_after")),
            }
        )
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

    def _strategy_side_from_trade(
        self,
        trade: Dict[str, Any],
        contract: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        values = [trade.get("symbol"), trade.get("description")]
        if isinstance(contract, dict):
            values.extend(
                [
                    contract.get("trading_symbol"),
                    contract.get("symbol"),
                    contract.get("instrument_type"),
                    contract.get("option_type"),
                    contract.get("type"),
                ]
            )

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

    def _get_itm_contracts(
        self,
        side: str,
        index_price: float,
        itm_range: float,
    ) -> Dict[str, Dict[str, Any]]:
        output: Dict[str, Dict[str, Any]] = {}
        spot_price = safe_float(index_price)
        if spot_price is None or spot_price <= 0 or not isinstance(self.selected_contracts, dict):
            return output

        side_key = str(side or "").strip().upper()
        low = spot_price - float(itm_range)
        high = spot_price + float(itm_range)
        call_tokens = {str(constants.CALL).upper(), str(getattr(constants, "CE", "CE")).upper(), "CALL", "CE"}
        put_tokens = {str(constants.PUT).upper(), str(getattr(constants, "PE", "PE")).upper(), "PUT", "PE"}

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

    def _set_order_intent(self, signal_event: Dict[str, Any]) -> None:
        if self.order_maneger is None:
            return
        if not self.enable_trading_engine:
            return
        if self._order_container.get("status") is not None:
            return
        if self._order_counter >= self._max_order_counter:
            return

        side = signal_event.get("signal")
        if side not in {constants.CALL, constants.PUT}:
            return

        lot = self._calculate_lot_size(side)
        if lot <= 0:
            return

        self._order_container["side"] = side
        self._order_container["status"] = constants.WAITING
        self._order_container["lot"] = lot
        self._order_container["force_trail_lock"] = False
        self._order_container["signal_context"] = signal_event
        logger.info(f"Order intent set side={side}, lot={lot}, status={constants.WAITING}")

    def _trade_processing(self, feed_response) -> None:
        if self.order_maneger is None:
            return

        if self.curr_index_minute and self._is_square_off_time(self.curr_index_minute):
            if feed_response:
                self._update_open_order_ltp(feed_response)
            self._clear_waiting_order_intent()
            self._square_off_open_trade(constants.EOD_SQUARE_OFF)
            return

        if not feed_response or not self._is_trading_window(self.curr_index_minute):
            return

        if (
            self._order_container.get("side") is not None
            and self._order_container.get("status") == constants.WAITING
            and self._order_container.get("instrument_key") is None
        ):
            self._punch_waiting_order(feed_response)
            return

        if self._order_container.get("status") == constants.OPEN:
            self._trail_open_order(feed_response)

    def _punch_waiting_order(self, feed_response) -> None:
        if self.last_index_bar is None:
            return

        sp = self._strategy_params()
        side = self._order_container.get("side")
        itm_range = self._coerce_float(sp.get("itm_strike_range"), 200.0, minimum=0.0)
        dict_itm = self._get_itm_contracts(
            side,
            float(self.last_index_bar["close"]),
            itm_range,
        )
        if not dict_itm:
            return

        chosen = self._choose_contract(feed_response, dict_itm)
        if chosen is None:
            return

        instrument_key = chosen.get("instrument_key")
        ltp = safe_float(chosen.get("ltp"))
        if instrument_key not in dict_itm or ltp is None:
            return

        contract = dict_itm[instrument_key]
        lot = self._coerce_int(self._order_container.get("lot"), 1, minimum=1)
        lot_size = self._coerce_int(contract.get("lot_size"), 1, minimum=1)
        qty = lot * lot_size
        tick = self._coerce_float(sp.get("tick-size", sp.get("tick_size")), 0.05, minimum=0.01)
        entry_price = float(ltp)

        use_option_atr_risk = self._coerce_bool(sp.get("use_option_atr_risk"), True)
        require_option_atr = self._coerce_bool(sp.get("require_option_atr"), False)
        option_atr = self.atr5_engine.get_atr(instrument_key)
        target, sl_trigger, trail_points, start_trail_after, risk_mode = self._build_risk_prices(
            entry_price=entry_price,
            option_atr=option_atr,
            use_option_atr_risk=use_option_atr_risk,
            require_option_atr=require_option_atr,
            sp=sp,
        )
        if target is None or sl_trigger is None or trail_points is None:
            self._clear_waiting_order_intent()
            return

        gap = self._coerce_float(sp.get("sl-limit-gap", sp.get("sl_limit_gap")), 0.5, minimum=0.0)
        sl_limit = float(sl_trigger) - gap
        sl_trigger = self._round_to_tick(float(sl_trigger), tick, "CEIL")
        sl_limit = self._round_to_tick(float(sl_limit), tick, "FLOOR")
        if sl_limit >= sl_trigger:
            sl_limit = self._round_to_tick(sl_trigger - tick, tick, "FLOOR")
        target = self._round_to_tick(float(target), tick, "CEIL")

        trailing_enabled = self._coerce_bool(sp.get("trailing-stop", sp.get("trailing_stop")), True)
        self._order_container["instrument_key"] = instrument_key
        self._order_container["instrument_symbol"] = contract.get("trading_symbol") or contract.get("symbol")
        self._order_container["ltp"] = entry_price
        self._order_container["max_gamma"] = safe_float(chosen.get("gamma"))
        self._order_container["start_trail_after"] = start_trail_after

        ts = self._timestamp_from_item(chosen) or self._resolve_reference_ts()
        description = self._build_order_description(side, entry_price)
        trade_id = self.order_maneger.buy(
            symbol=self._order_container["instrument_symbol"],
            instrument_token=instrument_key,
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
            f"Target(PU): {target:.2f}, SL_trig(PU): {sl_trigger:.2f}, SL_lim(PU): {sl_limit:.2f}, "
            f"TrailOn: {trailing_enabled}, TrailDist: {trail_points:.2f}, "
            f"TrailStartAfterPts: {(entry_price + (entry_price * start_trail_after)):.2f}, "
            f"RiskMode: {risk_mode}, OptionATR: {option_atr}, "
            f"SignalContext: {self._order_container.get('signal_context')}"
        )

        if trade_id:
            self._order_container["trade_id"] = trade_id
            self._order_container["status"] = constants.OPEN
            self._order_container["entry_price"] = entry_price
            self._order_container["trade_create_time"] = ts
            self._order_container["force_trail_lock"] = False
            self._order_counter += 1
            logger.info(f"{self._order_container}")

    def _trail_open_order(self, feed_response) -> None:
        latest_ltp = None
        ts = None
        for item in feed_response:
            if item.get("instrument_key") == self._order_container.get("instrument_key"):
                latest_ltp = safe_float(item.get("ltp"))
                ts = self._timestamp_from_item(item)
                break

        if latest_ltp is None or ts is None:
            return

        self._order_container["ltp"] = latest_ltp
        force_trail = self._should_force_trail_open_order(latest_ltp, ts)
        tick_result = self.order_maneger.on_tick(
            symbol=self._order_container["instrument_symbol"],
            o=latest_ltp,
            h=latest_ltp,
            l=latest_ltp,
            c=latest_ltp,
            ts=ts,
            force_trail=force_trail,
        )
        force_trail_applied = (
            tick_result is True
            or (
                isinstance(tick_result, dict)
                and self._coerce_bool(tick_result.get("force_trail_applied"), False)
            )
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
            self._order_container = self._default_order_container()

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

    def _square_off_open_trade(self, reason: str) -> None:
        if self.order_maneger is None or self._order_container.get("status") != constants.OPEN:
            return
        trade_id = self._order_container.get("trade_id")
        latest_ltp = safe_float(self._order_container.get("ltp"))
        if not trade_id or latest_ltp is None:
            return
        trade_closed = self.order_maneger.square_off_trade(
            trade_id=trade_id,
            exit_price=latest_ltp,
            ts=self._resolve_reference_ts(),
            reason=reason,
        )
        if trade_closed:
            self._order_container = self._default_order_container()

    def _build_risk_prices(
        self,
        entry_price: float,
        option_atr: Optional[float],
        use_option_atr_risk: bool,
        require_option_atr: bool,
        sp: Dict[str, Any],
    ):
        if use_option_atr_risk and option_atr is not None and option_atr > 0:
            atr_target_mult = self._coerce_float(sp.get("atr_target_mult"), 10.0, minimum=0.0)
            atr_sl_mult = self._coerce_float(sp.get("atr_sl_mult"), 3.0, minimum=0.0)
            target = entry_price + (atr_target_mult * option_atr)
            sl_trigger = entry_price - (atr_sl_mult * option_atr)
            start_trail_after = float((option_atr * 2) / entry_price) if entry_price > 0 else 0.0
            return target, sl_trigger, option_atr, start_trail_after, "atr"

        if use_option_atr_risk and require_option_atr:
            logger.warning("Skipping order; option ATR unavailable.")
            return None, None, None, None, "pct"

        tp_pct = self._coerce_float(sp.get("take-profit", sp.get("take_profit")), 0.30, minimum=0.0)
        sl_pct = self._coerce_float(sp.get("stop-loss", sp.get("stop_loss")), 0.20, minimum=0.0)
        target = entry_price * (1.0 + tp_pct)
        sl_trigger = entry_price * (1.0 - sl_pct)
        start_trail_after = self._coerce_float(
            sp.get("trail-start-after-points", sp.get("trail_start_after_points")),
            0.1,
            minimum=0.0,
        )
        trail_points = self._coerce_float(
            sp.get("trailing-stop-distance", sp.get("trailing_stop_distance")),
            10.0,
            minimum=0.0,
        )
        return target, sl_trigger, trail_points, start_trail_after, "pct"

    def _choose_contract(self, feed_response, dict_itm):
        chosen = None
        max_gamma = -1e18
        for item in feed_response:
            instrument_key = item.get("instrument_key")
            gamma = safe_float(item.get("gamma"))
            if instrument_key in dict_itm and gamma is not None and gamma > max_gamma:
                chosen = item
                max_gamma = gamma

        if chosen is not None:
            return chosen

        best_ltp = -1.0
        for item in feed_response:
            instrument_key = item.get("instrument_key")
            ltp = safe_float(item.get("ltp"))
            if instrument_key in dict_itm and ltp is not None and ltp > best_ltp:
                chosen = item
                best_ltp = ltp
        return chosen

    def _update_option_atr(self, item: Dict[str, Any]) -> None:
        instrument_key = item.get("instrument_key")
        if not instrument_key:
            return
        ltp = safe_float(item.get("ltp"))
        ts = self._timestamp_from_item(item)
        if ltp is None or ts is None:
            return
        self.atr5_engine.on_tick(instrument_key, ltp, ts)

    def _update_current_bucket_volume(self, minute_key: str, volume: float) -> None:
        if self.curr_index_candle is None:
            return
        volume = max(float(volume or 0.0), 0.0)
        previous = self._bucket_volume_by_minute.get(minute_key, 0.0)
        if volume <= previous:
            return
        self._bucket_volume_by_minute[minute_key] = volume
        self.curr_index_candle["volume"] = float(self.curr_index_candle.get("volume") or 0.0) + (
            volume - previous
        )

    def _extract_index_volume(self, item: Dict[str, Any]) -> float:
        for key in ("one_min_volume", "volume", "vol"):
            value = safe_float(item.get(key))
            if value is not None and value >= 0:
                return value
        return 0.0

    def _build_order_description(self, side: str, entry_price: float) -> str:
        symbol = self._order_container.get("instrument_symbol")
        signal_context = self._order_container.get("signal_context") or {}
        reason = signal_context.get("reason")
        pp = signal_context.get("pp")
        target = signal_context.get("suggested_target")
        if signal_context:
            return (
                f"{side} {symbol} entry={entry_price:.2f} source=FIB_EMA_ATR_VOL "
                f"reason={reason} pp={pp} pivot_target={target}"
            )
        return f"{side} {symbol} entry={entry_price:.2f} source=FIB_EMA_ATR_VOL"

    def _call_sl(
        self,
        close: float,
        atr: Optional[float],
        candle: pd.Series,
        pivots: Dict[str, float],
    ) -> float:
        candle_range = safe_float(candle.get("candle_range")) or abs(close - float(candle.get("open")))
        atr_sl = close - (atr if atr is not None else candle_range)
        structure_candidates = [pivots["pp"], pivots["r1"], pivots["r2"]]
        below = [value for value in structure_candidates if value < close]
        structure_sl = max(below) if below else min(structure_candidates)
        return min(structure_sl, atr_sl) if atr_sl > 0 else structure_sl

    def _put_sl(
        self,
        close: float,
        atr: Optional[float],
        candle: pd.Series,
        pivots: Dict[str, float],
    ) -> float:
        candle_range = safe_float(candle.get("candle_range")) or abs(close - float(candle.get("open")))
        atr_sl = close + (atr if atr is not None else candle_range)
        structure_candidates = [pivots["pp"], pivots["s1"], pivots["s2"]]
        above = [value for value in structure_candidates if value > close]
        structure_sl = min(above) if above else max(structure_candidates)
        return max(structure_sl, atr_sl)

    def _pivot_levels(self) -> Optional[Dict[str, float]]:
        if (
            self.prev_day_high is None
            or self.prev_day_low is None
            or self.prev_day_close is None
            or self.prev_day_high <= self.prev_day_low
        ):
            return None

        high = float(self.prev_day_high)
        low = float(self.prev_day_low)
        close = float(self.prev_day_close)
        pp = (high + low + close) / 3.0
        price_range = high - low
        return {
            "pp": pp,
            "r1": pp + (price_range * 0.382),
            "r2": pp + (price_range * 0.618),
            "r3": pp + price_range,
            "s1": pp - (price_range * 0.382),
            "s2": pp - (price_range * 0.618),
            "s3": pp - price_range,
        }

    def _resolve_previous_day_hlc(self):
        sp = self._strategy_params()
        input_data = self._input_data()
        use_upstox_previous_ohlc = self._coerce_bool(sp.get("use_upstox_previous_ohlc"), True)
        require_upstox_previous_ohlc = self._coerce_bool(sp.get("require_upstox_previous_ohlc"), True)

        if use_upstox_previous_ohlc and self._parse_strategy_date() is not None:
            upstox_hlc = self._fetch_previous_day_hlc_from_upstox()
            if upstox_hlc is not None:
                return upstox_hlc
            if require_upstox_previous_ohlc:
                self._exit_failure(
                    "Unable to load previous-day OHLC from Upstox historical candles. "
                    "Set a valid UPSTOX_ACCESS_TOKEN or disable require_upstox_previous_ohlc."
                )
        elif use_upstox_previous_ohlc and self.current_date:
            logger.warning(
                f"Unable to parse current_date={self.current_date}; falling back to configured previous-day OHLC."
            )

        high = self._first_float(
            sp,
            input_data,
            keys=(
                "prev_day_high",
                "prev-day-high",
                "previous_day_high",
                "previous-day-high",
                "last-day-high",
                "last_day_high",
                "day-high",
                "day_high",
            ),
        )
        low = self._first_float(
            sp,
            input_data,
            keys=(
                "prev_day_low",
                "prev-day-low",
                "previous_day_low",
                "previous-day-low",
                "last-day-low",
                "last_day_low",
                "day-low",
                "day_low",
            ),
        )
        close = self._first_float(
            sp,
            input_data,
            keys=(
                "prev_day_close",
                "prev-day-close",
                "previous_day_close",
                "previous-day-close",
                "last-day-close",
                "last_day_close",
                "previous-close",
                "previous_close",
                "day-close",
                "day_close",
            ),
        )

        if high is not None and low is not None and close is not None:
            return high, low, close

        trend_hlc = self._read_previous_day_hlc_from_trend_file()
        if trend_hlc is None:
            return high, low, close

        trend_high, trend_low, trend_close = trend_hlc
        return (
            high if high is not None else trend_high,
            low if low is not None else trend_low,
            close if close is not None else trend_close,
        )

    def _fetch_previous_day_hlc_from_upstox(self) -> Optional[tuple[float, float, float]]:
        strategy_day = self._parse_strategy_date()
        if strategy_day is None:
            logger.warning(
                f"Unable to parse current_date={self.current_date}; previous-day Upstox OHLC fetch skipped."
            )
            return None

        helper = self._build_upstox_helper()
        if helper is None:
            return None

        sp = self._strategy_params()
        lookback_days = self._coerce_int(sp.get("previous_ohlc_lookback_days"), 10, minimum=2)
        instrument_key = self._resolve_upstox_instrument_key()
        from_date = (strategy_day - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        to_date = (strategy_day - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            response = helper.get_historical_data(
                instrument_key=instrument_key,
                from_date=from_date,
                to_date=to_date,
                unit="days",
                interval=1,
            )
        except Exception as exc:
            logger.error(
                f"Failed to fetch previous-day Upstox OHLC for {instrument_key} "
                f"between {from_date} and {to_date}: {exc}"
            )
            return None

        candles = self._extract_upstox_candles(response)
        if not candles:
            logger.error(
                f"No Upstox daily candles returned for {instrument_key} between {from_date} and {to_date}."
            )
            return None

        rows = []
        strategy_date = strategy_day.date()
        for candle in candles:
            if not isinstance(candle, (list, tuple)) or len(candle) < 5:
                continue

            candle_ts = pd.to_datetime(candle[0], errors="coerce")
            open_price = safe_float(candle[1])
            high_price = safe_float(candle[2])
            low_price = safe_float(candle[3])
            close_price = safe_float(candle[4])
            if (
                pd.isna(candle_ts)
                or open_price is None
                or high_price is None
                or low_price is None
                or close_price is None
                or high_price < low_price
            ):
                continue

            candle_date = candle_ts.date()
            if candle_date >= strategy_date:
                continue

            rows.append(
                {
                    "time": candle_ts,
                    "date": candle_date,
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                }
            )

        if not rows:
            logger.error(
                f"Upstox daily candles had no usable prior candle for {instrument_key} "
                f"before {strategy_day.strftime('%Y-%m-%d')}."
            )
            return None

        rows = sorted(rows, key=lambda row: row["time"])
        previous = rows[-1]
        logger.info(
            f"Previous-day OHLC loaded from Upstox historical candles: "
            f"instrument={instrument_key}, date={previous['date']}, "
            f"O={previous['open']:.2f}, H={previous['high']:.2f}, "
            f"L={previous['low']:.2f}, C={previous['close']:.2f}"
        )
        return previous["high"], previous["low"], previous["close"]

    def _resolve_upstox_access_token(self) -> Optional[str]:
        sp = self._strategy_params()
        candidates = [
            sp.get("upstox_access_token"),
            sp.get("upstox-access-token"),
            sp.get("api_access_token"),
            sp.get("api-access-token"),
            sp.get("apiAccessToken"),
            self.params.get("upstox_access_token") if isinstance(self.params, dict) else None,
            self.params.get("upstox-access-token") if isinstance(self.params, dict) else None,
            self.params.get("api_access_token") if isinstance(self.params, dict) else None,
            self.params.get("api-access-token") if isinstance(self.params, dict) else None,
            self.params.get("apiAccessToken") if isinstance(self.params, dict) else None,
            os.getenv("UPSTOX_ACCESS_TOKEN"),
            os.getenv("UPSTOX_API_ACCESS_TOKEN"),
            os.getenv("UPSTOX_TOKEN"),
        ]
        for token in candidates:
            token_s = str(token or "").strip()
            if token_s.lower().startswith("bearer "):
                token_s = token_s[7:].strip()
            if token_s:
                return token_s
        return None

    def _validate_upstox_access_token(self, token: Optional[str]) -> str:
        token_s = str(token or "").strip()
        placeholders = {
            "token",
            "your_token",
            "your_access_token",
            "your_access_token_here",
            "upstox_access_token",
            "<token>",
            "<access_token>",
            "YOUR_ACCESS_TOKEN",
        }
        if not token_s or token_s in placeholders or token_s.lower() in {p.lower() for p in placeholders}:
            self._exit_failure(
                "Upstox access token missing or placeholder. Provide one via "
                "strategy-parameters.upstox_access_token, top-level upstox_access_token, "
                "UPSTOX_ACCESS_TOKEN, UPSTOX_API_ACCESS_TOKEN, or UPSTOX_TOKEN."
            )
        return token_s

    def _resolve_upstox_instrument_key(self) -> str:
        sp = self._strategy_params()
        input_data = self._input_data()
        candidates = [
            sp.get("upstox_instrument_key"),
            sp.get("upstox-instrument-key"),
            self.params.get("upstox_instrument_key") if isinstance(self.params, dict) else None,
            self.params.get("upstox-instrument-key") if isinstance(self.params, dict) else None,
            input_data.get("upstox_instrument_key"),
            input_data.get("upstox-instrument-key"),
            constants.NIFTY50_SYMBOL,
        ]
        for instrument_key in candidates:
            value = str(instrument_key or "").strip()
            if value:
                return value
        return constants.NIFTY50_SYMBOL

    def _resolve_upstox_sandbox(self) -> bool:
        sp = self._strategy_params()
        candidates = [
            sp.get("upstox_sandbox"),
            sp.get("upstox-sandbox"),
            self.params.get("upstox_sandbox") if isinstance(self.params, dict) else None,
            self.params.get("upstox-sandbox") if isinstance(self.params, dict) else None,
            os.getenv("UPSTOX_SANDBOX"),
        ]
        for candidate in candidates:
            if candidate is not None:
                return self._coerce_bool(candidate, False)
        return False

    def _build_upstox_helper(self) -> Optional[Any]:
        if self.uptox_client is not None and hasattr(self.uptox_client, "get_historical_data"):
            return self.uptox_client

        access_token = self._validate_upstox_access_token(self._resolve_upstox_access_token())

        try:
            from broker.upstox_helper import UpstoxHelper
        except Exception as exc:
            logger.error(f"Unable to import UpstoxHelper for previous OHLC fetch: {exc}")
            return None

        try:
            return UpstoxHelper(
                apiAccessToken=access_token,
                is_sandbox=self._resolve_upstox_sandbox(),
            )
        except Exception as exc:
            logger.error(f"Unable to initialize UpstoxHelper for previous OHLC fetch: {exc}")
            return None

    def _parse_strategy_date(self) -> Optional[datetime]:
        raw = str(self.current_date or "").strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _extract_upstox_candles(response: Any) -> list:
        payload = None
        if isinstance(response, dict):
            payload = response
        elif hasattr(response, "to_dict"):
            try:
                payload = response.to_dict()
            except Exception:
                payload = None

        if not isinstance(payload, dict):
            data_obj = getattr(response, "data", None)
            candles = getattr(data_obj, "candles", None)
            if candles is not None:
                payload = {"data": {"candles": candles}}

        if not isinstance(payload, dict):
            return []

        data = payload.get("data") or {}
        candles = data.get("candles")
        return candles if isinstance(candles, list) else []

    def _read_previous_day_hlc_from_trend_file(self) -> Optional[tuple[float, float, float]]:
        try:
            if not os.path.exists(constants.TREND_FILE):
                return None
            with open(constants.TREND_FILE, "r", encoding="utf-8") as file:
                data = json.load(file)
            if not isinstance(data, list) or not data:
                return None
            candidates = []
            if len(data) >= 2:
                candidates.append(data[-2])
            candidates.append(data[-1])
            for row in candidates:
                if not isinstance(row, dict):
                    continue
                high = safe_float(row.get("high"))
                low = safe_float(row.get("low"))
                close = safe_float(row.get("close"))
                if high is not None and low is not None and close is not None:
                    return high, low, close
        except Exception as exc:
            logger.warning(f"Unable to read previous-day HLC from {constants.TREND_FILE}: {exc}")
        return None

    def _roll_day_if_needed(self, bucket_key: str) -> None:
        day_key = bucket_key[:10]
        if self.today_date is None:
            self.today_date = day_key
            return

        if day_key != self.today_date:
            if self.today_high is not None and self.today_low is not None and self.today_close is not None:
                self.prev_day_high = self.today_high
                self.prev_day_low = self.today_low
                self.prev_day_close = self.today_close
            self.today_date = day_key
            self.today_high = None
            self.today_low = None
            self.today_close = None

    def _update_session_ohlc(self, candle: Dict[str, Any]) -> None:
        self._roll_day_if_needed(str(candle.get("time") or ""))
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))
        close = safe_float(candle.get("close"))
        if high is None or low is None or close is None:
            return
        self.today_high = high if self.today_high is None else max(self.today_high, high)
        self.today_low = low if self.today_low is None else min(self.today_low, low)
        self.today_close = close

    def _clear_waiting_order_intent(self) -> None:
        if self._order_container.get("status") == constants.WAITING:
            self._order_container = self._default_order_container()

    def _update_open_order_ltp(self, feed_response) -> None:
        if self._order_container.get("status") != constants.OPEN:
            return
        for item in feed_response:
            if item.get("instrument_key") == self._order_container.get("instrument_key"):
                latest_ltp = safe_float(item.get("ltp"))
                if latest_ltp is not None:
                    self._order_container["ltp"] = latest_ltp
                return

    def _default_order_container(self) -> Dict[str, Any]:
        return {
            "trade_id": None,
            "side": None,
            "instrument_key": None,
            "instrument_symbol": None,
            "status": None,
            "ltp": None,
            "entry_price": None,
            "lot": None,
            "max_gamma": None,
            "start_trail_after": None,
            "trade_create_time": None,
            "force_trail_lock": False,
            "signal_context": None,
        }

    def _timestamp_from_item(self, item: Dict[str, Any]) -> Optional[datetime]:
        ts_ms = safe_float(item.get("ts_epoch_ms")) or safe_float(item.get("ltt"))
        if ts_ms is not None:
            return datetime.fromtimestamp(ts_ms / 1000.0, tz=ist)

        ts_text = item.get("ts_ist") or item.get("one_min_ts") or item.get("current_ts")
        if ts_text:
            ts = pd.to_datetime(ts_text, errors="coerce")
            if pd.notna(ts):
                if ts.tzinfo is None:
                    ts = ts.tz_localize(ist)
                else:
                    ts = ts.tz_convert(ist)
                return ts.to_pydatetime()
        return None

    def _resolve_reference_ts(self) -> datetime:
        minute_key = None
        if self.last_index_bar:
            minute_key = self.last_index_bar.get("time")
        if minute_key is None:
            minute_key = self.curr_index_minute
        if minute_key:
            try:
                return datetime.strptime(str(minute_key), "%Y-%m-%d %H:%M").replace(tzinfo=ist)
            except Exception:
                pass
        return datetime.now(ist)

    def _calculate_lot_size(self, side: str) -> int:
        sp = self._strategy_params()
        lot_cfg = sp.get("lot-size") or sp.get("lot_size") or {}
        return self._coerce_int(lot_cfg.get("medium", lot_cfg.get("small")), 1, minimum=0)

    def _round_to_tick(self, value: float, tick: float, mode: str) -> float:
        value = float(value)
        tick = float(tick)
        if tick <= 0:
            return value
        ratio = value / tick
        if mode == "FLOOR":
            return math.floor(ratio) * tick
        if mode == "CEIL":
            return math.ceil(ratio) * tick
        return round(ratio) * tick

    def _is_trading_window(self, time_str: Optional[str]) -> bool:
        ts = pd.to_datetime(time_str, errors="coerce")
        if pd.isna(ts):
            return False
        current = ts.time()
        return self.trade_start <= current < self.trade_end

    def _is_square_off_time(self, time_str: Optional[str]) -> bool:
        ts = pd.to_datetime(time_str, errors="coerce")
        if pd.isna(ts):
            return False
        return ts.time() >= self.trade_end

    def _init_trade_window_times(self):
        sp = self._strategy_params()
        trade_window = sp.get("trade-window") or sp.get("trade_window") or {}
        market_hours = self.params.get("market-hours", {}) if isinstance(self.params, dict) else {}
        start_str = trade_window.get("start", market_hours.get("start", self.TRADE_START))
        end_str = trade_window.get("end", market_hours.get("end", self.TRADE_END))
        return self._parse_hhmm(start_str, time(9, 20)), self._parse_hhmm(end_str, time(15, 15))

    def _parse_hhmm(self, value: Any, default: time) -> time:
        try:
            return datetime.strptime(str(value), "%H:%M").time()
        except Exception:
            return default

    def _exit_failure(self, message: str) -> None:
        logger.error(message)
        code = constants.FAIL_CODE if constants.FAIL_CODE else 1
        sys.exit(code)

    def _coerce_int(
        self,
        value: Any,
        default: int,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
    ) -> int:
        try:
            result = int(value)
        except Exception:
            result = default
        if minimum is not None:
            result = max(minimum, result)
        if maximum is not None:
            result = min(maximum, result)
        return result

    def _coerce_float(
        self,
        value: Any,
        default: float,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
    ) -> float:
        result = safe_float(value)
        if result is None:
            result = default
        if minimum is not None:
            result = max(minimum, result)
        if maximum is not None:
            result = min(maximum, result)
        return result

    def _coerce_bool(self, value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default

    def _first_float(self, *sources: Dict[str, Any], keys) -> Optional[float]:
        for source in sources:
            if not isinstance(source, dict):
                continue
            for key in keys:
                value = safe_float(source.get(key))
                if value is not None:
                    return value
        return None

    def _input_data(self) -> Dict[str, Any]:
        if not isinstance(self.params, dict):
            return {}
        input_data = self.params.get("input-data") or self.params.get("input_data") or {}
        return input_data if isinstance(input_data, dict) else {}

    def _strategy_params(self) -> Dict[str, Any]:
        if isinstance(self.params, dict):
            sp = self.params.get("strategy-parameters") or self.params.get("strategy_parameters") or {}
            if isinstance(sp, dict):
                return sp
        return {}

    def _get_params_from_yaml(self) -> Dict[str, Any]:
        default_params = {
            "strategy-parameters": {
                "enable_trading_engine": True,
                "signal_tf_min": 5,
                "ema_fast": 9,
                "ema_slow": 21,
                "atr_len": 13,
                "atr_mult": 2.718,
                "vol_sma_len": 89,
                "vol_spike_threshold": 4.669,
                "min_vol_mult": 1.0,
                "disable_volume_filter": False,
                "avoid_exhaustion_entry": True,
                "avoid_high_volatility_entry": False,
                "allow_breakout_entries": True,
                "allow_pullback_entries": True,
                "use_upstox_previous_ohlc": True,
                "require_upstox_previous_ohlc": True,
                "previous_ohlc_lookback_days": 10,
                "trade-per-day": 1,
                "option_atr_period": 5,
                "use_option_atr_risk": True,
                "require_option_atr": False,
                "atr_target_mult": 10,
                "atr_sl_mult": 3,
                "lot-size": {
                    "small": 1,
                    "medium": 1,
                    "large": 1,
                },
                "itm_strike_range": 200.0,
                "take-profit": 0.30,
                "stop-loss": 0.20,
                "sl-limit-gap": 0.5,
                "tick-size": 0.05,
                "trailing-stop": True,
                "trailing-stop-distance": 10.0,
                "trail-start-after-points": 0.1,
                "trade-window": {
                    "start": self.TRADE_START,
                    "end": self.TRADE_END,
                },
            }
        }

        candidate_paths = []
        if self.current_date:
            candidate_paths.append(f"data/{self.current_date}/param.yaml")
        candidate_paths.append(constants.PARAM_PATH)

        params = None
        for param_path in candidate_paths:
            if not param_path or not os.path.exists(param_path):
                continue
            with open(param_path, "r", encoding="utf-8") as file:
                loaded = yaml.safe_load(file) or {}
            if isinstance(loaded, dict):
                params = loaded
                logger.info(f"Loaded parameters from {param_path}")
                break

        if not isinstance(params, dict):
            return default_params

        defaults_sp = default_params["strategy-parameters"]
        loaded_sp = params.get("strategy-parameters") or params.get("strategy_parameters") or {}
        if isinstance(loaded_sp, dict):
            params["strategy-parameters"] = {**defaults_sp, **loaded_sp}
        else:
            params["strategy-parameters"] = defaults_sp
        return params
