#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File: pcr_vwma_ema_orb.py

LIVE PCR + VWAP/EMA strategy with ORB direction filter.
Built on top of pcr_vwap_ema skeleton (poller + ws candles + OMS lifecycle),
with core ORB-gated entry logic ported from garageforbots.

Key features:
- PCR polling every N seconds -> self.pcr_value_queue
- Hybrid VWAP/VWMA using Spot Close * Future Volume
- EMA9, RSI(7) + RSI MA(14), ATR(14), EMA/VWMA/RSI-MA angles, HH/LL + thrust patterns
- Optional ORB direction lock and breakout gating
- Trading window gate
- Order lifecycle glue that calls your order_manager (sandbox/mock/prod)

Notes:
- This file DOES NOT call Upstox order APIs directly; it delegates to your order_manager.
- It assumes your engine passes WS messages in Upstox MarketDataStreamerV3 format.
"""

from __future__ import annotations

import math
import sys
import threading
from collections import deque
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
from zoneinfo import ZoneInfo
from technicals.atr.atr_for_ticks import AtrEngine
from technicals.orb.orb_state import OrbState
from index.nifty50.nifty50_utils import (
    calculate_gap_percent,
    classify_gap_direction,
    get_previous_close_for_gap,
    safe_float,
)
import common.constants as constants
import logger

IST = ZoneInfo("Asia/Kolkata")
log = logger.create_logger("PCRVwmaEmaOrbStrategyLogger")


class PCRVwmaEmaOrbStrategy:
    def __init__(
        self,
        uptox_client,
        previous_day_trend: str,
        selected_contracts: Dict[str, Any],
        order_manager=None,
        option_exipry_date: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        index_minutes_processed: Optional[Dict[str, bool]] = None,
        future_minutes_processed: Optional[Dict[str, bool]] = None,
        intraday_index_candles=None,
        intraday_future_candles=None,
    ):
        self.uptox_client = uptox_client
        self.previous_day_trend = previous_day_trend
        self.selected_contracts = selected_contracts or {}
        self.order_manager = order_manager
        self.option_exipry_date = option_exipry_date

        self.params = params or {}
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        ht: Dict[str, Any] = {}
        if isinstance(self.params, dict):
            # Support both keys while keeping backward compatibility with old configs.
            ht_legacy = self.params.get("historical-trend")
            ht_new = self.params.get("historical-trends")
            if isinstance(ht_legacy, dict):
                ht.update(ht_legacy)
            if isinstance(ht_new, dict):
                ht.update(ht_new)

        self._slope_window = int(sp.get("slope_window", self.params.get("slope_window", 3) or 3))
        self.enable_trading_engine = bool(sp.get("enable_trading_engine", self.params.get("enable_trading_engine", True)))

        # sentiments used in _calculate_lot_size (same as your mock/backtest)
        self._daily_sentiment = ht.get("daily", ht.get("trader-sentiment", constants.SIDEWAYS))
        self._trader_sentiment = ht.get("trader-sentiment", constants.SIDEWAYS)

        # ----- order gating -----
        self._max_order_counter = int(sp.get("trade-per-day", sp.get("trade_per_day", 2)) or 2)
        self._order_counter = 0
        self._order_container: Dict[str, Any] = {
            "trade_id": None,
            "side": None,
            "instrument_key": None,
            "instrument_symbol": None,
            "status": None,
            "ltp": None,
            "lot": None,
            "max_gamma": None,
            "start_trail_after": None,
        }
        self._post_exit_cooldown_minutes = int(sp.get("post_exit_cooldown_minutes", 5) or 5)
        self._post_exit_cooldown_until: Optional[datetime] = None
        self._max_daily_loss_pct_of_initial_cash = float(sp.get("max_daily_loss_pct_of_initial_cash", 0.03) or 0.03)
        self._daily_loss_blocked_day: Optional[str] = None
        self._today_realized_pnl_day: Optional[str] = None
        self._today_realized_pnl: float = 0.0
        self._today_realized_pnl_trade_ids = set()
        self._trade_end_time: Optional[dtime] = None
        self._init_trade_window_times()
        self._setup_orb_state()
        self._setup_gap_state()

        # ----- PCR polling -----
        self.pcr_value_queue = deque(maxlen=int(sp.get("pcr_queue_len", self.params.get("pcr_queue_len", 6)) or 6))
        self._last_pcr: Optional[float] = None
        self._pcr_lock = threading.Lock()
        self._pcr_poll_interval_sec = int(sp.get("pcr_poll_interval_sec", self.params.get("pcr_poll_interval_sec", 30)) or 30)
        self._pcr_poll_stop = threading.Event()
        self._pcr_poll_thread: Optional[threading.Thread] = None

        # ----- streaming instrument keys -----
        nifty_fut = self.selected_contracts.get("Nifty_Future") or {}
        self.nifty_future_key = nifty_fut.get("instrument_key")

        # ----- candles -----
        self.curr_index_candle: Optional[Dict[str, Any]] = None
        self.curr_index_minute: Optional[str] = None
        self.curr_fut_candle: Optional[Dict[str, Any]] = None
        self.curr_fut_minute: Optional[str] = None
        self.last_index_bar: Optional[Dict[str, Any]] = None
        self.last_fut_bar: Optional[Dict[str, Any]] = None
        self._fut_vol_minute = None
        self._fut_vol_start_vtt = None
        self._fut_vol_last_vtt = None
        self._fut_vol_by_minute = {}

        # processed minutes (bootstrap use)
        self.index_minutes_processed = index_minutes_processed or {}
        self.future_minutes_processed = future_minutes_processed or {}

        # ----- futures VWAP calc state (from vtt cumulative) -----
        self.cum_turnover = 0.0
        self.cum_volume = 0.0
        self.last_cum_volume: Optional[float] = None
        self.atr5_engine = AtrEngine(atr_period=int(sp.get("option_atr_period", 3)))

        # ----- dataframes with fixed dtypes to avoid pandas future warnings -----
        self.df_index = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "vwap": pd.Series(dtype="float64"),
            "vwma_25": pd.Series(dtype="float64"),
            "ema_9": pd.Series(dtype="float64"),
            "rsi_7": pd.Series(dtype="float64"),
            "rsi_ma_14": pd.Series(dtype="float64"),
            "atr_14": pd.Series(dtype="float64"),
            "angle_ema_9": pd.Series(dtype="float64"),
            "angle_vwma_25": pd.Series(dtype="float64"),
            "angle_rsi_ma_14": pd.Series(dtype="float64"),
            "candle_range": pd.Series(dtype="float64"),
            "volatile_count": pd.Series(dtype="float64"),
            "is_volatile": pd.Series(dtype="bool"),
            "recent_high_max": pd.Series(dtype="float64"),
            "recent_low_min": pd.Series(dtype="float64"),
            "is_hh": pd.Series(dtype="bool"),
            "is_ll": pd.Series(dtype="bool"),
            "is_bearish_thrust": pd.Series(dtype="bool"),
            "is_bullish_thrust": pd.Series(dtype="bool"),
        })

        self.df_fut = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "vwap": pd.Series(dtype="float64"),
        })

        self.df_merged = pd.DataFrame({
            "time": pd.Series(dtype="object"),
            "close": pd.Series(dtype="float64"),
            "close_fut": pd.Series(dtype="float64"),
            "volume_fut": pd.Series(dtype="float64"),
            "vwap_fut": pd.Series(dtype="float64"),
        })

        # bootstrap if provided
        if intraday_index_candles is not None and intraday_future_candles is not None:
            self._initialize_from_intraday_candles(intraday_index_candles, intraday_future_candles)

        # auto-start PCR poller
        if bool(sp.get("pcr_poller_enabled", True)):
            self.start()

    # ------------------------------------------------------------------
    # ORB helpers
    # ------------------------------------------------------------------
    def _setup_orb_state(self) -> None:
        self.orb = OrbState(self.params, constants.ORB_STATE_FILE)

    # ------------------------------------------------------------------
    # Gap helpers
    # ------------------------------------------------------------------
    def _setup_gap_state(self) -> None:
        self._previous_day_close: Optional[float] = get_previous_close_for_gap(self.params)
        self._gap_day: Optional[str] = None
        self._gap_open: Optional[float] = None
        self._gap_pct: Optional[float] = None
        self._gap_direction: Optional[str] = None
        self._orb_gap_bias_applied_day: Optional[str] = None

    def _extract_day_key(self, minute_key: str) -> Optional[str]:
        try:
            return datetime.strptime(str(minute_key), "%Y-%m-%d %H:%M").strftime("%Y-%m-%d")
        except Exception:
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
        log.info(
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
        open_price = safe_float(self._gap_open)
        if ltp_f is None or ltp_f <= 0 or open_price is None or open_price <= 0:
            return False

        diff_points = abs(ltp_f - open_price)
        allowed = diff_points <= threshold
        if not allowed:
            log.debug(
                f"LTP-open gate blocked: ltp={ltp_f:.2f}, open={open_price:.2f}, "
                f"distance={diff_points:.2f}, threshold={threshold:.2f}"
            )
        return allowed

    def _apply_gap_bias_to_orb_when_ready(self) -> None:
        # Once ORB range is ready, optionally lock breakout side from day gap direction.
        if not self.orb.is_enabled() or not self.orb.ready:
            return

        gap_day = self._gap_day
        if not gap_day or self._orb_gap_bias_applied_day == gap_day:
            return

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        threshold_fraction: Optional[float] = None
        # Legacy key is interpreted as fraction (e.g., 0.006 == 0.6%).
        if "orb_gap_side_override_threshold" in sp:
            threshold_fraction = safe_float(sp.get("orb_gap_side_override_threshold"))
        # pct key is interpreted as percent value and converted to fraction.
        elif "orb_gap_side_override_threshold_pct" in sp:
            threshold_pct = safe_float(sp.get("orb_gap_side_override_threshold_pct"))
            if threshold_pct is not None:
                threshold_fraction = threshold_pct / 100.0
        if threshold_fraction is None or threshold_fraction <= 0:
            threshold_fraction = 0.004

        gap_pct = safe_float(self._gap_pct)
        gap_fraction = (gap_pct / 100.0) if gap_pct is not None else None
        if gap_fraction is None or gap_fraction < threshold_fraction:
            self._orb_gap_bias_applied_day = gap_day
            return

        direction = self._gap_direction
        target_side: Optional[str] = None
        if direction == constants.GAP_UP:
            target_side = constants.CALL
        elif direction == constants.GAP_DOWN:
            target_side = constants.PUT

        # Mark as handled for the day even if FLAT/unknown to avoid repeated checks/logs.
        self._orb_gap_bias_applied_day = gap_day

        if target_side is None:
            return

        if self.orb.breakout_side != target_side:
            self.orb.force_breakout_side(
                target_side,
                reason=f"gap-direction override day={gap_day} direction={direction}",
            )

    # ------------------------------------------------------------------
    # PCR poller lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._pcr_poll_thread and self._pcr_poll_thread.is_alive():
            return
        self._pcr_poll_stop.clear()
        self._pcr_poll_thread = threading.Thread(target=self._pcr_poll_loop, name="pcr_poller", daemon=True)
        self._pcr_poll_thread.start()
        log.info(f"PCR poller started interval={self._pcr_poll_interval_sec}s")

    def stop(self) -> None:
        self._pcr_poll_stop.set()
        if self._pcr_poll_thread and self._pcr_poll_thread.is_alive():
            self._pcr_poll_thread.join(timeout=2)
        log.info("PCR poller stopped")

    def get_latest_pcr(self) -> Optional[float]:
        with self._pcr_lock:
            return self._last_pcr

    def _pcr_poll_loop(self) -> None:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        initial_delay = float(sp.get("pcr_initial_delay_sec", 2))
        self._pcr_poll_stop.wait(initial_delay)

        while not self._pcr_poll_stop.is_set():
            try:
                log.info("Polling PCR...")
                self._update_pcr_queue_once()
            except Exception as e:
                log.error(f"PCR poll error: {e}")

            self._pcr_poll_stop.wait(self._pcr_poll_interval_sec)

    def _update_pcr_queue_once(self) -> None:
        expiry = (self.params.get("strategy-parameters", {}) or {}).get("pcr_expiry") or self.params.get("pcr_expiry")
        if not expiry:
            return
        self._fetch_option_chain(symbol=constants.NIFTY50_SYMBOL, expiry=expiry)

    def _fetch_option_chain(self, *, symbol: str, expiry: str) -> None:
        option_chain = self.uptox_client.get_option_chain_by_expiry(symbol, expiry)
        chain_data = option_chain.data
        if option_chain.status != constants.SUCCESS:
            raise Exception("Option-chain fetch failed")

        rows_list = []
        for item in chain_data:
            rows_list.append({
                "strike_price": item.strike_price,
                "ce_oi": item.call_options.market_data.oi,
                "pe_oi": item.put_options.market_data.oi,
            })
        df = pd.DataFrame(rows_list)
        if df.empty:
            return

        spot_price = float(self.last_index_bar["close"]) if self.last_index_bar else 0.0
        if spot_price <= 0:
            resp = self.uptox_client.get_last_price_of_symbol(symbol).to_dict()
            if resp.get("status") == constants.SUCCESS:
                spot_price = float(resp["data"]["NSE_INDEX:Nifty 50"]["last_price"])
        if spot_price <= 0:
            return

        atm_idx = (df["strike_price"] - spot_price).abs().idxmin()
        start_idx = max(0, int(atm_idx) - 5)
        end_idx = min(len(df), int(atm_idx) + 6)
        focused = df.iloc[start_idx:end_idx].copy()

        total_ce_oi = float(focused["ce_oi"].sum())
        total_pe_oi = float(focused["pe_oi"].sum())
        concentrated_pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0.0

        self.pcr_value_queue.append(float(concentrated_pcr))
        log.info(f"PCR Queue: {self.pcr_value_queue}")
        with self._pcr_lock:
            self._last_pcr = float(concentrated_pcr)

    # ------------------------------------------------------------------
    # WS message handler (called by engine)
    # ------------------------------------------------------------------
    def on_ws_message(self, message: Dict[str, Any]):
        # Order lifecycle gets a chance on every WS message
        try:
            self._trade_processing_from_ws(message)
        except Exception as e:
            log.warning(f"_trade_processing_from_ws error: {e}")

        if "feeds" not in message:
            return

        feeds = message["feeds"]

        for ik, data in feeds.items():
            try:
                # Index tick
                if ik == constants.NIFTY50_SYMBOL:
                    ltpc = data.get("fullFeed", {}).get("indexFF", {}).get("ltpc", {})
                    if not ltpc:
                        continue
                    ltp = ltpc.get("ltp")
                    ltt = ltpc.get("ltt")
                    if ltp is None or ltt is None:
                        continue
                    try:
                        ltp = float(ltp)
                        ts_ms = int(ltt)
                    except (TypeError, ValueError):
                        continue

                    minute_key = datetime.fromtimestamp(ts_ms / 1000, IST).strftime("%Y-%m-%d %H:%M")
                    self._handle_index_tick(minute_key, ltp)
                elif self.nifty_future_key and ik == self.nifty_future_key:
                    # Futures tick
                    marketFF = data.get("fullFeed", {}).get("marketFF", {})
                    ltpc = marketFF.get("ltpc", {})
                    if not ltpc:
                        continue
                    ltp = ltpc.get("ltp")
                    ltt = ltpc.get("ltt")
                    if ltp is None or ltt is None:
                        continue
                    vtt = marketFF.get("vtt", None)
                    if vtt is None:
                        continue
                    try:
                        ltp = float(ltp)
                        ts_ms = int(ltt)
                        vtt = float(vtt)
                    except (TypeError, ValueError):
                        continue
                    minute_key = datetime.fromtimestamp(ts_ms / 1000, IST).strftime('%Y-%m-%d %H:%M')

                    # Update 1-min volume from vtt, and attach to the correct minute candle
                    finished_minute, finished_vol = self._update_1m_volume_from_vtt(minute_key, vtt)
                    if finished_minute is not None:
                        self._fut_vol_by_minute[finished_minute] = float(finished_vol)

                    self._handle_fut_tick(minute_key, ltp)
                else:
                    # Option tick -> update ATR stream for dynamic risk sizing.
                    marketFF = data.get("fullFeed", {}).get("marketFF", {})
                    ltpc = marketFF.get("ltpc", {})
                    if not ltpc:
                        continue
                    ltp = ltpc.get("ltp")
                    ltt = ltpc.get("ltt")
                    if ltp is None or ltt is None:
                        continue
                    try:
                        ltp = float(ltp)
                        ts_ms = int(ltt)
                    except (TypeError, ValueError):
                        continue

                    dt_object = datetime.fromtimestamp(ts_ms / 1000, IST)
                    self.atr5_engine.on_tick(ik, ltp, dt_object)
            except Exception as e:
                log.warning(f"Skipping malformed feed for {ik}: {e}")
                continue

    def _update_1m_volume_from_vtt(self, minute_key: str, vtt_now: float):
        """
        Compute 1-minute traded volume from cumulative vtt (total traded volume).
        Returns (finished_minute, finished_volume) when a minute completes, else (None, None).

        Notes:
        - Uses tick minute (from ltpc.ltt) as the truth clock.
        - Handles reconnect/day rollover: if vtt decreases, resets baseline.
        """
        try:
            # First tick ever
            if self._fut_vol_minute is None:
                self._fut_vol_minute = minute_key
                self._fut_vol_start_vtt = vtt_now
                self._fut_vol_last_vtt = vtt_now
                return None, None

            # Guard: vtt reset (reconnect/day rollover)
            if self._fut_vol_last_vtt is not None and vtt_now < float(self._fut_vol_last_vtt):
                self._fut_vol_minute = minute_key
                self._fut_vol_start_vtt = vtt_now
                self._fut_vol_last_vtt = vtt_now
                return None, None

            # Same minute → update last vtt only
            if minute_key == self._fut_vol_minute:
                self._fut_vol_last_vtt = vtt_now
                return None, None

            # Minute changed → finalize previous minute
            finished_minute = self._fut_vol_minute
            finished_volume = max(float(self._fut_vol_last_vtt) - float(self._fut_vol_start_vtt), 0.0)

            # Start new minute baseline
            self._fut_vol_minute = minute_key
            self._fut_vol_start_vtt = vtt_now
            self._fut_vol_last_vtt = vtt_now

            return finished_minute, finished_volume

        except Exception as e:
            log.error(f"Error in _update_1m_volume_from_vtt: {e}")
            sys.exit(constants.FAIL_CODE)

    # ------------------------------------------------------------------
    # Candle building
    # ------------------------------------------------------------------
    def _handle_index_tick(self, minute_key: str, ltp: float):
        if self.curr_index_minute is None or minute_key != self.curr_index_minute:
            if self.curr_index_candle is not None:
                self._finalize_index_candle()
            self.curr_index_minute = minute_key
            self.curr_index_candle = {"time": minute_key, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
            # Compute day gap from first observed tick/candle open for the day.
            day_key = self._extract_day_key(minute_key)
            if day_key and self._gap_day != day_key:
                self._update_gap_stats(self.curr_index_candle)
        else:
            c = self.curr_index_candle
            if c:
                c["high"] = max(float(c["high"]), ltp)
                c["low"] = min(float(c["low"]), ltp)
                c["close"] = ltp

    def _finalize_index_candle(self):
        c = self.curr_index_candle
        if c is None:
            return
        self.df_index = pd.concat([self.df_index, pd.DataFrame([c])], ignore_index=True)
        self.last_index_bar = c
        self.orb.update_from_candle(c)
        self._apply_gap_bias_to_orb_when_ready()
        log.debug(f"New index candle created: {self.last_index_bar}")
        self.curr_index_candle = None
        self._try_make_merged_bar()

        # If futures not streaming, still compute on index-only
        if self.nifty_future_key is None:
            self._apply_indicators_and_engine()

    def _handle_fut_tick(self, minute_key: str, ltp: float):
        """Build 1-minute OHLC for FUT using ltp. Volume is attached on finalize from vtt-derived dict."""
        try:
            # --- harden inputs (no behavior change, just safety) ---
            if minute_key is None:
                return
            minute_key = str(minute_key)

            try:
                ltp_f = float(ltp)
            except Exception:
                return

            if not (ltp_f == ltp_f) or ltp_f <= 0:  # NaN or non-positive
                return

            # --- minute rollover ---
            if self.curr_fut_minute != minute_key:
                if self.curr_fut_candle:
                    try:
                        self._finalize_fut_candle()
                    except Exception as e:
                        # IMPORTANT: don't kill WS thread; log and continue
                        log.error(f"Error in _finalize_fut_candle: {e}")

                self.curr_fut_minute = minute_key
                self.curr_fut_candle = {
                    "time": minute_key,
                    "open": ltp_f,
                    "high": ltp_f,
                    "low": ltp_f,
                    "close": ltp_f,
                    "volume": 0.0,   # overwritten on finalize
                    "oi": float("nan"),
                }
                return

            # --- same minute update ---
            c = self.curr_fut_candle
            if not c:
                return

            # Ensure scalars (protect against accidental list values elsewhere)
            c_high = float(c.get("high", ltp_f))
            c_low = float(c.get("low", ltp_f))

            c["high"] = max(c_high, ltp_f)
            c["low"] = min(c_low, ltp_f)
            c["close"] = ltp_f

        except Exception as e:
            # IMPORTANT: do NOT sys.exit() in a tick handler; it kills your WS loop
            log.error(f"Error in _handle_fut_tick: {e}")
            # keep running
            return

    def _finalize_fut_candle(self):
        c = self.curr_fut_candle
        if c is None:
            return

        minute = c.get("time")
        if minute in self._fut_vol_by_minute:
            c["volume"] = float(self._fut_vol_by_minute.pop(minute, 0.0))
            
        self.df_fut = pd.concat([self.df_fut,  pd.DataFrame([c])],
                ignore_index=True
            )
        self.last_fut_bar = c
        log.debug(f"New futures candle created: {self.last_fut_bar}")
        self.curr_fut_candle = None
        self._try_make_merged_bar()

    def _try_make_merged_bar(self):
        if self.last_index_bar is None or self.last_fut_bar is None:
            return
        if self.last_index_bar.get("time") != self.last_fut_bar.get("time"):
            return
        row = {
            "time": self.last_index_bar["time"],
            "close": float(self.last_index_bar.get("close", np.nan)),
            "close_fut": float(self.last_fut_bar.get("close", np.nan)),
            "volume_fut": float(self.last_fut_bar.get("volume", np.nan))
        }
        self.df_merged = pd.concat([self.df_merged, pd.DataFrame([row])], ignore_index=True)
        self._apply_indicators_and_engine()

    # ------------------------------------------------------------------
    # Indicators + price action + trading engine
    # ------------------------------------------------------------------
    def _apply_indicators_and_engine(self):
        self._apply_indicators()
        log.debug(f"Applied indicators on index candles. Dataframe length: {len(self.df_index)}")
        if self.curr_index_minute and self._is_trading_window(self.curr_index_minute):
            try:
                atr_val = float(self.df_index.iloc[-1].get("atr_14", np.nan))
            except Exception:
                atr_val = np.nan
            log.debug(f"Checking price action. atr_val: {atr_val}")
            self.check_price_action(atr_val if (atr_val is not None and not np.isnan(atr_val)) else 9.0) 
            log.debug(f"Engine processing candles")
            self._trading_engine_active()
        else:
            if self.curr_index_minute:
                log.debug(f"Outside Trading Window at {self.curr_index_minute}")

    def _apply_indicators(self):
        if self.df_index.empty:
            return

        idx = self.df_index.copy()
        idx["time_dt"] = pd.to_datetime(idx["time"], errors="coerce")
        idx = idx.dropna(subset=["time_dt"])
        if idx.empty:
            return

        if not self.df_fut.empty:
            fut = self.df_fut[["time", "volume"]].copy()
            fut["time_dt"] = pd.to_datetime(fut["time"], errors="coerce")
            fut = fut.dropna(subset=["time_dt"])
        else:
            fut = pd.DataFrame({"time_dt": idx["time_dt"], "volume": 0.0})

        merged = pd.merge(
            idx[["time_dt", "close", "high", "low", "open"]],
            fut[["time_dt", "volume"]],
            on="time_dt",
            how="left",
        )

        merged["volume"] = pd.to_numeric(merged["volume"], errors="coerce").fillna(0.0)
        merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
        merged["high"] = pd.to_numeric(merged["high"], errors="coerce")
        merged["low"] = pd.to_numeric(merged["low"], errors="coerce")
        merged["open"] = pd.to_numeric(merged["open"], errors="coerce")

        # Hybrid VWAP (daily reset)
        merged["date"] = merged["time_dt"].dt.date
        merged["pv"] = merged["close"] * merged["volume"]
        merged["cum_pv"] = merged.groupby("date")["pv"].cumsum()
        merged["cum_vol"] = merged.groupby("date")["volume"].cumsum()
        vwap_series = merged["cum_pv"] / merged["cum_vol"].replace(0, np.nan)
        # --- SAFE VWAP assignment ---
        if vwap_series is not None and len(vwap_series) > 0:
            n = min(len(self.df_index), len(vwap_series))
            if n > 0:
                self.df_index.loc[self.df_index.index[:n], "vwap"] = vwap_series.values[:n]

        # VWMA
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        vwma_len = int(sp.get("vwma_len", 25))
        roll_pv = merged["pv"].rolling(window=vwma_len, min_periods=1).sum()
        roll_vol = merged["volume"].rolling(window=vwma_len, min_periods=1).sum()
        vwma_series = roll_pv / roll_vol.replace(0, np.nan)
        # --- SAFE VWMA assignment ---
        if vwma_series is not None and len(vwma_series) > 0:
            n = min(len(self.df_index), len(vwma_series))
            if n > 0:
                self.df_index.loc[self.df_index.index[:n], "vwma_25"] = vwma_series.values[:n]

        # HH/LL + volatility filter
        hhll_window = int(sp.get("hhll_window", 4))
        range_thresh = float(sp.get("volatile_range_threshold", 8))
        volatile_need = int(sp.get("volatile_count_min", 2))

        self.df_index["candle_range"] = pd.to_numeric(self.df_index["high"], errors="coerce") - pd.to_numeric(self.df_index["low"], errors="coerce")
        self.df_index["volatile_count"] = (
            (self.df_index["candle_range"] > range_thresh).astype(int).shift(1).rolling(window=hhll_window, min_periods=1).sum()
        )
        self.df_index["is_volatile"] = self.df_index["volatile_count"] >= volatile_need
        self.df_index["recent_high_max"] = pd.to_numeric(self.df_index["high"], errors="coerce").shift(1).rolling(window=hhll_window, min_periods=1).max()
        self.df_index["recent_low_min"] = pd.to_numeric(self.df_index["low"], errors="coerce").shift(1).rolling(window=hhll_window, min_periods=1).min()
        self.df_index["is_hh"] = (pd.to_numeric(self.df_index["high"], errors="coerce") > self.df_index["recent_high_max"]) & self.df_index["is_volatile"]
        self.df_index["is_ll"] = (pd.to_numeric(self.df_index["low"], errors="coerce") < self.df_index["recent_low_min"]) & self.df_index["is_volatile"]

        # EMA/RSI/ATR + angles
        if len(self.df_index) >= 14:
            close = pd.to_numeric(self.df_index["close"], errors="coerce")
            high = pd.to_numeric(self.df_index["high"], errors="coerce")
            low = pd.to_numeric(self.df_index["low"], errors="coerce")

            self.df_index["ema_9"] = ta.ema(close, length=9)
            self.df_index["rsi_7"] = ta.rsi(close, length=7)
            self.df_index["rsi_ma_14"] = ta.sma(self.df_index["rsi_7"], length=14)
            self.df_index["atr_14"] = ta.atr(high=high, low=low, close=close, length=14)

            ema9 = pd.to_numeric(self.df_index["ema_9"], errors="coerce")
            vwma = pd.to_numeric(self.df_index["vwma_25"], errors="coerce")
            rsi_ma = pd.to_numeric(self.df_index["rsi_ma_14"], errors="coerce")
            slope_ema = (ema9 - ema9.shift(self._slope_window)) / float(self._slope_window)
            slope_vwma = (vwma - vwma.shift(self._slope_window)) / float(self._slope_window)
            slope_rsi_ma = (rsi_ma - rsi_ma.shift(self._slope_window)) / float(self._slope_window)
            self.df_index["angle_ema_9"] = np.degrees(np.arctan(np.clip(slope_ema, -10, 10)))
            self.df_index["angle_vwma_25"] = np.degrees(np.arctan(np.clip(slope_vwma, -10, 10)))
            self.df_index["angle_rsi_ma_14"] = np.degrees(np.arctan(np.clip(slope_rsi_ma, -10, 10)))
         

    def check_price_action(self, atr: float) -> None:
        if len(self.df_index) < 3:
            return

        df = self.df_index
        is_red = df["close"] < df["open"]
        is_green = df["close"] > df["open"]

        prev_low = df["low"].shift(1)
        prev_high = df["high"].shift(1)
        making_lower_low = df["low"] < prev_low
        making_higher_high = df["high"] > prev_high

        curr_range = (df["high"] - df["low"]).astype(float)
        prev_range = curr_range.shift(1)

        is_alive = (curr_range > 3) & (prev_range > 3)
        thr = float(atr) if atr is not None and not np.isnan(atr) else 10.0
        has_major_move = (curr_range > thr) | (prev_range > thr)
        is_valid_setup = is_alive & has_major_move

        df["is_bearish_thrust"] = (is_red & is_red.shift(1) & making_lower_low & is_valid_setup).fillna(False)
        df["is_bullish_thrust"] = (is_green & is_green.shift(1) & making_higher_high & is_valid_setup).fillna(False)

    def _trading_engine_active(self):
        if not self.enable_trading_engine:
            return
        
        if len(self.pcr_value_queue) < 3:
            log.debug("Waiting for PCR polling to complete")
            return

        if len(self.df_index) < 30:
            return

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        atr_14 = self.df_index.iloc[-1].get("atr_14", np.nan)
        try:
            atr_14 = float(atr_14)
        except Exception:
            return
        if atr_14 < float(sp.get("min_atr_14", 9.0)):
            return

        ref_ts = self._resolve_reference_ts()
        if self._is_post_exit_cooldown_active(ref_ts):
            cooldown_left_sec = int(max((self._post_exit_cooldown_until - ref_ts).total_seconds(), 0))
            log.debug(
                f"Entry blocked by post-exit cooldown for {cooldown_left_sec}s "
                f"(until {self._post_exit_cooldown_until.strftime('%H:%M:%S')})"
            )
            return

        if self._is_daily_loss_limit_active(ref_ts):
            return

        orb_enabled = self.orb.is_enabled()
        orb_side = self.orb.get_breakout_side(self.last_index_bar, self.curr_index_minute)
        if orb_enabled:
            # ORB gates entries; no new intent before ORB range/breakout is ready.
            if not self.orb.ready:
                return
            if orb_side is None:
                log.debug("ORB not complete.")
                return

        if bool(sp.get("trade_within_day_open_limits", False)):
            ltp = safe_float(self.df_index.iloc[-1].get("close", np.nan))
            ltp_open_max_distance_points = float(sp.get("ltp_open_max_distance_points", 210))
            if ltp is None or ltp <= 0:
                return
            if not self._is_ltp_within_open_distance(ltp,ltp_open_max_distance_points):
                return

        rsi_ma_14 = self.df_index.iloc[-1].get("rsi_ma_14", np.nan)
        prev_rsi_ma_14 = self.df_index.iloc[-2].get("rsi_ma_14", np.nan)
        if pd.isna(rsi_ma_14) or pd.isna(prev_rsi_ma_14):
            return
        rsi_ma_14 = int(math.ceil(float(rsi_ma_14)))
        prev_rsi_ma_14 = int(math.ceil(float(prev_rsi_ma_14)))

        ema_9 = float(self.df_index.iloc[-1].get("ema_9", np.nan))
        vwma_25 = float(self.df_index.iloc[-1].get("vwma_25", np.nan))
        angle_ema_9 = float(self.df_index.iloc[-1].get("angle_ema_9", np.nan))
        angle_vwma_25 = float(self.df_index.iloc[-1].get("angle_vwma_25", np.nan))
        angle_rsi_ma_14 = float(self.df_index.iloc[-1].get("angle_rsi_ma_14", np.nan))

        is_bearish_thrust = bool(self.df_index.iloc[-1].get("is_bearish_thrust", False))
        is_bullish_thrust = bool(self.df_index.iloc[-1].get("is_bullish_thrust", False))

        up_rsi_low = int(sp.get("up_rsi_low", 52))
        up_rsi_high = int(sp.get("up_rsi_high", 73))
        up_angle_ema = float(sp.get("up_angle_ema", 50))
        up_angle_vwma = float(sp.get("up_angle_vwma", 30))
        up_angle_rsi_ma = float(sp.get("up_angle_rsi_ma", 30))

        dn_rsi_low = int(sp.get("dn_rsi_low", 27))
        dn_rsi_high = int(sp.get("dn_rsi_high", 48))
        dn_angle_ema = float(sp.get("dn_angle_ema", -50))
        dn_angle_vwma = float(sp.get("dn_angle_vwma", -30))
        dn_angle_rsi_ma = float(sp.get("dn_angle_rsi_ma", -30))
        
        log.debug(
            f"Engine check rsi_ma={rsi_ma_14}/{prev_rsi_ma_14}, ema={ema_9}, vwma={vwma_25}, "
            f"angle_ema={angle_ema_9}, angle_vwma={angle_vwma_25}, angle_rsi_ma={angle_rsi_ma_14}, "
            f"orb_side={orb_side}, bullish_thrust={is_bullish_thrust}, bearish_thrust={is_bearish_thrust}"
        )

        call_setup = (
            (angle_rsi_ma_14 > up_angle_rsi_ma)
            and (up_rsi_low < prev_rsi_ma_14 < rsi_ma_14 < up_rsi_high)
            and (ema_9 > vwma_25)
            and (angle_ema_9 > up_angle_ema)
            and (angle_vwma_25 > up_angle_vwma)
            and ((not orb_enabled) or (orb_side == constants.CALL))
        )

        put_setup = (
            (angle_rsi_ma_14 < dn_angle_rsi_ma)
            and (dn_rsi_low < rsi_ma_14 < prev_rsi_ma_14 < dn_rsi_high)
            and (ema_9 < vwma_25)
            and (angle_ema_9 < dn_angle_ema)
            and (angle_vwma_25 < dn_angle_vwma)
            and ((not orb_enabled) or (orb_side == constants.PUT))
        )

        
        trader_sentiment = str(self._trader_sentiment or constants.SIDEWAYS).strip().lower()

        if call_setup and self._order_container["status"] is None and (self._order_counter < self._max_order_counter):
            lot = self._calculate_lot_size(constants.CALL, is_bullish_thrust, is_bearish_thrust)
            if lot <= 0:
                return
            is_ascending = self.pcr_value_queue[-3] <= self.pcr_value_queue[-2] <= self.pcr_value_queue[-1]
            all_pcr_bullish = all(float(v) > 1 for v in list(self.pcr_value_queue)[-3:])
            if is_ascending and all_pcr_bullish and trader_sentiment in (constants.BULLISH, constants.SUPER_BULLISH):
                lot += int(sp.get("pcr_lot_boost", 2))
            
            self._order_container["side"] = constants.CALL
            self._order_container["status"] = constants.WAITING
            self._order_container["lot"] = int(lot)
            log.info(f"Order intent set side={constants.CALL}, lot={lot}, status={constants.WAITING}")
            return

        if put_setup and self._order_container["status"] is None and (self._order_counter < self._max_order_counter):
            lot = self._calculate_lot_size(constants.PUT, is_bullish_thrust, is_bearish_thrust)
            if lot <= 0:
                return
            is_descending = self.pcr_value_queue[-3] >= self.pcr_value_queue[-2] >= self.pcr_value_queue[-1]
            all_pcr_bearish = all(float(v) < 1 for v in list(self.pcr_value_queue)[-3:])
            if is_descending and all_pcr_bearish and trader_sentiment in (constants.BEARISH, constants.SUPPER_BEARISH):
                lot += int(sp.get("pcr_lot_boost", 2))
            
            self._order_container["side"] = constants.PUT
            self._order_container["status"] = constants.WAITING
            self._order_container["lot"] = int(lot)
            log.info(f"Order intent set side={constants.PUT}, lot={lot}, status={constants.WAITING}")
            return

    # ------------------------------------------------------------------
    # Trading window + lot size (ported from your mock backtest)
    # ------------------------------------------------------------------
    def _init_trade_window_times(self):
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        tw = sp.get("trade-window") or sp.get("trade_window") or {}
        end_str = (tw.get("end") or "15:10").strip()
        try:
            hh, mm = map(int, end_str.split(":"))
            self._trade_end_time = dtime(hh, mm)
        except Exception:
            self._trade_end_time = dtime(15, 10)

    def _is_trading_window(self, time_str: str) -> bool:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        tw = sp.get("trade-window") or sp.get("trade_window") or {}
        start_str = (tw.get("start") or "09:45").strip()
        end_str = (tw.get("end") or "14:45").strip()

        try:
            current_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M").time()
            start_t = datetime.strptime(start_str, "%H:%M").time()
            end_t = datetime.strptime(end_str, "%H:%M").time()
            return start_t <= current_time <= end_t
        except Exception:
            return True

    def _resolve_reference_ts(self) -> datetime:
        if self.curr_index_minute:
            try:
                return datetime.strptime(self.curr_index_minute, "%Y-%m-%d %H:%M").replace(tzinfo=IST)
            except Exception:
                pass
        return datetime.now(IST)

    def _set_post_exit_cooldown(self, exit_status: Optional[str], ts: Optional[datetime] = None) -> None:
        status = str(exit_status or "").strip().upper()
        if status not in {constants.STOPLOSS_HIT.upper(), constants.TARGET_HIT.upper()}:
            return
        if self._post_exit_cooldown_minutes <= 0:
            return

        ref_ts = ts or self._resolve_reference_ts()
        if ref_ts.tzinfo is None:
            ref_ts = ref_ts.replace(tzinfo=IST)

        cooldown_until = ref_ts + timedelta(minutes=self._post_exit_cooldown_minutes)
        if self._post_exit_cooldown_until is None or cooldown_until > self._post_exit_cooldown_until:
            self._post_exit_cooldown_until = cooldown_until

        log.info(
            f"Entry cooldown started due to '{exit_status}' until "
            f"{self._post_exit_cooldown_until.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )

    def _is_post_exit_cooldown_active(self, now_ts: Optional[datetime] = None) -> bool:
        if self._post_exit_cooldown_until is None:
            return False

        ref_ts = now_ts or self._resolve_reference_ts()
        if ref_ts.tzinfo is None:
            ref_ts = ref_ts.replace(tzinfo=IST)

        if ref_ts >= self._post_exit_cooldown_until:
            self._post_exit_cooldown_until = None
            return False

        return True

    def _get_today_realized_snapshot(self, day_key: str) -> Optional[Dict[str, Any]]:
        orders_csv = getattr(self.order_manager, "orders_csv", None)
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
            ref_ts = ref_ts.replace(tzinfo=IST)
        else:
            ref_ts = ref_ts.astimezone(IST)

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
            # Force one-time refresh on next guard check if close payload is missing.
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
            # Close happened, but pnl not populated yet in payload; refresh lazily once.
            self._today_realized_pnl_day = None
            return

        self._today_realized_pnl += float(pnl)
        if trade_id:
            self._today_realized_pnl_trade_ids.add(trade_id)

    def _is_daily_loss_limit_active(self, now_ts: Optional[datetime] = None) -> bool:
        if self._max_daily_loss_pct_of_initial_cash <= 0:
            return False

        day_key = self._refresh_today_realized_pnl_cache(now_ts)

        initial_cash = safe_float(getattr(self.order_manager, "initial_cash", None))
        if initial_cash is None or initial_cash <= 0:
            return self._daily_loss_blocked_day == day_key

        max_loss_amount = float(initial_cash) * float(self._max_daily_loss_pct_of_initial_cash)
        today_loss_amount = max(-float(self._today_realized_pnl), 0.0)

        if today_loss_amount >= max_loss_amount:
            if self._daily_loss_blocked_day != day_key:
                self._daily_loss_blocked_day = day_key
                log.warning(
                    f"Daily loss guard activated for {day_key}. "
                    f"Loss={today_loss_amount:.2f} >= Limit={max_loss_amount:.2f} "
                    f"({self._max_daily_loss_pct_of_initial_cash * 100:.2f}% of initial cash), "
                    f"TodayRealizedPnL={self._today_realized_pnl:.2f}"
                )
            return True

        return self._daily_loss_blocked_day == day_key

    def _calculate_lot_size(self, side: str, is_bullish_thrust: bool, is_bearish_thrust: bool) -> int:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        lot_cfg = sp.get("lot-size") or sp.get("lot_size") or {}
        small = int(lot_cfg.get("small", 2) or 2)
        medium = int(lot_cfg.get("medium", 2) or 2)
        large = int(lot_cfg.get("large", 2) or 2)

        daily = self._daily_sentiment

        if side == constants.CALL:
            if daily == constants.BULLISH and is_bullish_thrust:
                return large
            if daily == constants.BULLISH:
                return medium
            if daily == constants.SIDEWAYS:
                return small
            if daily == constants.BEARISH:
                return small

        if side == constants.PUT:
            if daily == constants.BEARISH and is_bearish_thrust:
                return large
            if daily == constants.BEARISH:
                return medium
            if daily == constants.SIDEWAYS:
                return small
            if daily == constants.BULLISH:
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
    def _trade_processing_from_ws(self, message: Dict[str, Any]) -> None:
        if "feeds" not in message:
            return

        # NOTE: Do not gate candle building or PCR polling here.
        # This guard only skips option feed parsing & order management.
        st = self._order_container.get("status")
        needs_wait_pick = (
            self._order_container.get("side") is not None
            and st == constants.WAITING
            and self._order_container.get("instrument_key") is None
        )
        needs_open_manage = (st == constants.OPEN)
        if not (needs_wait_pick or needs_open_manage):
            return

        feed_response = []
        for ik, data in message["feeds"].items():
            ts_ms = None
            ltp = None
            gamma = None

            idx_ltpc = data.get("fullFeed", {}).get("indexFF", {}).get("ltpc", {})
            if idx_ltpc:
                ltp = idx_ltpc.get("ltp")
                ts_ms = idx_ltpc.get("ltt")

            mff = data.get("fullFeed", {}).get("marketFF", {})
            if mff:
                ltpc = mff.get("ltpc", {})
                if ltpc:
                    ltp = ltpc.get("ltp", ltp)
                    ts_ms = ltpc.get("ltt", ts_ms)
                greeks = mff.get("optionGreeks") or mff.get("greeks") or {}
                if isinstance(greeks, dict):
                    gamma = greeks.get("gamma", None)

            if ltp is None or ts_ms is None:
                continue

            feed_response.append({
                "instrument_key": ik,
                "ltp": float(ltp),
                "ts_epoch_ms": int(ts_ms),
                "gamma": gamma,
            })

        self._trade_processing(feed_response)

    def _build_itm_contract_map(self, side: str, spot_price: float) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if spot_price <= 0:
            return out

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        itm_range = float(sp.get("itm_strike_range", self.params.get("itm_strike_range", 200)))

        low = spot_price - itm_range
        high = spot_price + itm_range

        for strike, lst in self.selected_contracts.items():
            if strike == "Nifty_Future":
                continue

            if not isinstance(lst, list) or not lst:
                continue

            # get strike (prefer the dict value if present)
            try:
                k = float(lst[0].get("strike_price"))
            except Exception:
                try:
                    k = float(strike)
                except Exception:
                    continue

            side_u = str(side).upper()

            # ✅ keep only near-ITM band
            if side_u == constants.CALL:
                # ITM calls: strike <= spot, but not deeper than itm_range
                if not (low <= k <= spot_price):
                    continue
            elif side_u == constants.PUT:
                # ITM puts: strike >= spot, but not deeper than itm_range
                if not (spot_price <= k <= high):
                    continue
            else:
                continue

            for c in lst:
                if not isinstance(c, dict):
                    continue
                it = str(c.get("instrument_type", "")).upper()
                if side_u == constants.CALL and it in ("CE", "CALL"):
                    out[c.get("instrument_key")] = c
                if side_u == constants.PUT and it in ("PE", "PUT"):
                    out[c.get("instrument_key")] = c

        return out

    def _trade_processing(self, feed_response: list) -> None:
        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        if not feed_response:
            return

        # 1) WAITING -> pick contract + place order
        if (
            self._order_container.get("side") is not None
            and self._order_container.get("status") == constants.WAITING
            and self._order_container.get("instrument_key") is None
        ):
            if self._is_daily_loss_limit_active():
                log.info("Skipping order placement: daily loss guard active. No more new trades for today.")
                for k in list(self._order_container.keys()):
                    self._order_container[k] = None
                return

            if not self.last_index_bar:
                return
            spot = float(self.last_index_bar.get("close", 0.0))
            dict_itm = self._build_itm_contract_map(self._order_container["side"], spot)
            if not dict_itm:
                return

            log.debug(f"ITM candidates: {list(dict_itm.keys())}")
            # pick highest gamma among ITM candidates (fallback: highest ltp)
            max_gamma = -1e18
            chosen = None
            for item in feed_response:
                ik = item.get("instrument_key")
                if ik not in dict_itm:
                    continue
                g = item.get("gamma")
                gv = None
                if g is not None:
                    try:
                        gv = float(g)
                    except Exception:
                        gv = None
                if gv is not None and gv > max_gamma:
                    chosen = item
                    max_gamma = gv

            if chosen is None:
                best_ltp = -1.0
                for item in feed_response:
                    ik = item.get("instrument_key")
                    if ik in dict_itm and float(item.get("ltp", 0.0)) > best_ltp:
                        chosen = item
                        best_ltp = float(item.get("ltp", 0.0))
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

            ts = datetime.fromtimestamp(int(chosen["ts_epoch_ms"]) / 1000, tz=IST)

            lot = self._order_container.get("lot")
            lot_size = contract.get("lot_size")
            try:
                lot = int(float(lot))
                lot_size = int(float(lot_size))
            except (TypeError, ValueError):
                log.error(f"Invalid lot/lot_size. lot={lot} lot_size={lot_size}")
                return
            qty = lot * lot_size

            entry_price = float(self._order_container["ltp"])
            TICK = float(sp.get("tick-size", sp.get("tick_size", 0.05)))  # NSE FO options usually 0.05

            # --- target/sl logic ---
            start_trail_after = None
            target = None
            sl_trigger = None
            risk_mode = "pct"
            option_atr = self.atr5_engine.get_atr(chosen["instrument_key"])

            use_option_atr_risk = bool(sp.get("orb_use_option_atr_risk", True))
            require_option_atr = bool(sp.get("orb_require_option_atr", True))
            atr_target_mult = float(sp.get("orb_atr_target_mult", 5.0))
            atr_sl_mult = float(sp.get("orb_atr_sl_mult", 1.1))
            max_atr_for_contract = float(sp.get("max_atr_for_contract", 20))
            min_atr_for_contract = float(sp.get("min_atr_for_contract", 10))
            
            if use_option_atr_risk and option_atr is not None and option_atr > 0:

                target = entry_price + (atr_target_mult * option_atr)
                sl_trigger = entry_price - (atr_sl_mult * option_atr)
                start_trail_after = float(option_atr / entry_price)
                
                if option_atr > max_atr_for_contract:
                    start_trail_after = float(max_atr_for_contract / entry_price)
                
                if option_atr < min_atr_for_contract:
                    sl_trigger = entry_price - (atr_sl_mult * min_atr_for_contract)

                risk_mode = "atr"
            else:
                if use_option_atr_risk and require_option_atr:
                    log.warning(f"Skipping order; option ATR unavailable for {chosen['instrument_key']}")
                    # self.orb.disable_for_trade(
                    #     f"option ATR unavailable for {chosen['instrument_key']} while orb_require_option_atr=true"
                    # )
                    self._order_container["instrument_key"] = None
                    self._order_container["ltp"] = None
                    self._order_container["max_gamma"] = None
                    self._order_container["instrument_symbol"] = None
                    return

                tp_pct = float(sp.get("take-profit", sp.get("take_profit", 0.30)))
                sl_pct = float(sp.get("stop-loss", sp.get("stop_loss", 0.20)))
                target = entry_price * (1.0 + tp_pct)
                sl_trigger = entry_price * (1.0 - sl_pct)
                start_trail_after = float(sp.get("trail-start-after-points", sp.get("trail_start_after_points", 0.1)))
                start_trail_after = max(start_trail_after, 0.0)


            sl_limit = float(sl_trigger) - sp.get("sl-limit-gap", 1.0)

            # --- tick-size alignment (prevents Upstox/NSE rejection) ---
            sl_trigger = float(sl_trigger)
            target = float(target)

            # For BUY entry -> SELL SL: trigger rounded UP, limit rounded DOWN, ensure limit < trigger
            sl_trigger = self._round_to_tick(sl_trigger, TICK, "CEIL")
            sl_limit   = self._round_to_tick(sl_limit,   TICK, "FLOOR")
            if sl_limit >= sl_trigger:
                sl_limit = self._round_to_tick(sl_trigger - TICK, TICK, "FLOOR")

            # Target rounding (optional, but keeps orders/logic consistent)
            target = self._round_to_tick(target, TICK, "CEIL")

            # --- trailing ---
            trailing_enabled = bool(sp.get("trailing-stop", sp.get("trailing_stop", True)))
            trail_points = sp.get("trailing-stop-distance", 10)
            if option_atr is not None:
                trail_points = option_atr
                trail_points = max(trail_points, TICK)  # at least 1 tick

            if option_atr > max_atr_for_contract:
                trail_points = max_atr_for_contract

            self._order_container["start_trail_after"] = start_trail_after

            desc = f"{self._order_container['side']} {self._order_container['instrument_symbol']} entry={entry_price:.2f}"

            # TODO(OMS): Placeholder call. Replace order_manager.buy with OMS microservice create-order API.
            trade_id = self.order_manager.buy(
                symbol=self._order_container["instrument_symbol"],
                instrument_token=self._order_container["instrument_key"],
                qty=qty,
                entry_price=entry_price,
                sl_trigger=sl_trigger,
                sl_limit=sl_limit,
                target=target,
                trail_points=(trail_points if trailing_enabled else None),
                start_trail_after=start_trail_after,
                description=desc,
                ts=ts,
            )

            log.info(
                f"OrderInfo TradeID: {trade_id}, Entry(PU): {entry_price:.2f}, Qty: {qty}, "
                f"Target(PU): {target:.2f}, SL_trig(PU): {sl_trigger:.2f}, "
                f"SL_lim(PU): {sl_limit:.2f}, TrailOn: {trailing_enabled}, TrailDist: {trail_points:.2f}, "
                f"TrailStartAfterPts: {(entry_price + (entry_price*start_trail_after)):.2f}, "
                f"RiskMode: {risk_mode}, OptionATR: {option_atr}"
            )

            if trade_id:
                self._order_container["trade_id"] = trade_id
                self._order_container["status"] = constants.OPEN
                self._order_counter += 1
                log.info(f"{self._order_container}")
            return

        # 2) OPEN -> feed LTP to OMS for trailing/exit
        if self._order_container.get("status") == constants.OPEN:
            latest_ltp = None
            ts = None
            for item in feed_response:
                if item.get("instrument_key") == self._order_container.get("instrument_key"):
                    latest_ltp = float(item["ltp"])
                    ts = datetime.fromtimestamp(int(item["ts_epoch_ms"]) / 1000, tz=IST)
                    break

            if latest_ltp is not None and ts is not None:
                # Broker-side sync: if SL already executed at Upstox, stop trailing/OMS calls.
                try:
                    if hasattr(self.order_manager, "refresh_trade_status") and self._order_container.get("trade_id"):
                        # TODO(OMS): Placeholder call. Replace order_manager.refresh_trade_status with OMS trade-status API.
                        closed_status = self.order_manager.refresh_trade_status(self._order_container.get("trade_id"), ts=ts)
                        if closed_status in [constants.STOPLOSS_HIT, constants.TARGET_HIT, constants.MANUAL_EXIT, constants.EOD_SQUARE_OFF]:
                            # TODO(OMS): Placeholder call. Replace order_manager.get_trade_by_id with OMS trade-details API.
                            self._set_post_exit_cooldown(closed_status, ts=ts)
                            trade_info = self.order_manager.get_trade_by_id(self._order_container.get("trade_id"))
                            self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                            log.info(f"Broker sync closed trade: {trade_info}")
                            for k in list(self._order_container.keys()):
                                self._order_container[k] = None
                            return
                except Exception as e:
                    log.warning(f"refresh_trade_status failed: {e}")

                # TODO(OMS): Placeholder call. Replace order_manager.on_tick with OMS tick/trailing update endpoint.
                _ = self.order_manager.on_tick(
                    symbol=self._order_container["instrument_symbol"],
                    o=latest_ltp, h=latest_ltp, l=latest_ltp, c=latest_ltp,
                    ts=ts,
                )

                # TODO(OMS): Placeholder call. Replace order_manager.get_trade_by_id with OMS trade-details API.
                trade_info = self.order_manager.get_trade_by_id(self._order_container.get("trade_id"))
                if trade_info and trade_info.get("status") in [
                    constants.TARGET_HIT,
                    constants.STOPLOSS_HIT,
                    constants.MANUAL_EXIT,
                    constants.EOD_SQUARE_OFF,
                ]:
                    self._set_post_exit_cooldown(trade_info.get("status"), ts=ts)
                    self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
                    log.debug(f"Trade closed Info: {trade_info}")
                    for k in list(self._order_container.keys()):
                        self._order_container[k] = None
                    return

        # 3) EOD square-off
        if self.curr_index_minute and self._trade_end_time and self._order_container.get("trade_id") and self._order_container.get("status") == constants.OPEN:
            try:
                current_time = datetime.strptime(self.curr_index_minute, "%Y-%m-%d %H:%M").time()
            except Exception:
                return

            if current_time >= self._trade_end_time:
                trade_id = self._order_container.get("trade_id")
                latest_ltp = None
                for item in feed_response:
                    if item.get("instrument_key") == self._order_container.get("instrument_key"):
                        latest_ltp = float(item["ltp"])
                        break
                if latest_ltp is None:
                    latest_ltp = float(self._order_container.get("ltp") or 0.0)

                ts = datetime.now(IST)
                try:
                    if hasattr(self.order_manager, "refresh_trade_status"):
                        # TODO(OMS): Placeholder call. Replace order_manager.refresh_trade_status with OMS trade-status API.
                        _ = self.order_manager.refresh_trade_status(trade_id, ts=ts)
                except Exception as e:
                    log.warning(f"refresh_trade_status (pre-eod) failed: {e}")
                # TODO(OMS): Placeholder call. Replace order_manager.square_off_trade with OMS square-off API.
                self.order_manager.square_off_trade(
                    trade_id=trade_id,
                    exit_price=float(latest_ltp),
                    ts=ts,
                    reason=constants.EOD_SQUARE_OFF,
                )
                for k in list(self._order_container.keys()):
                    self._order_container[k] = None

    # ------------------------------------------------------------------
    # Bootstrap helper (optional)
    # ------------------------------------------------------------------
    def _initialize_from_intraday_candles(self, index_candles, fut_candles):
        def build_df(candles, is_index: bool):
            if not candles:
                return pd.DataFrame()
            df = pd.DataFrame(candles, columns=["time", "open", "high", "low", "close", "volume", "oi"])
            t_ = pd.to_datetime(df["time"], errors="coerce")
            df["time"] = t_.dt.strftime("%Y-%m-%d %H:%M")
            if is_index:
                df["volume"] = 0.0
            return df[["time", "open", "high", "low", "close", "volume"]]

        df_i = build_df(index_candles, True)
        df_f = build_df(fut_candles, False)
        if df_i.empty:
            return

        # Compute gap only from the first index candle in the bootstrap set.
        first_row = df_i.iloc[0]
        self._update_gap_stats({
            "time": first_row["time"],
            "open": first_row["open"],
        })

        for _, row in df_i.iterrows():
            self.orb.update_from_candle({
                "time": row["time"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
            })
        self._apply_gap_bias_to_orb_when_ready()

        self.df_index = pd.concat([self.df_index, df_i], ignore_index=True)
        self.last_index_bar = df_i.iloc[-1].to_dict()

        if not df_f.empty:
            self.df_fut = pd.concat([self.df_fut, df_f], ignore_index=True)
            self.last_fut_bar = df_f.iloc[-1].to_dict()

        self._apply_indicators_and_engine()


# Backward-compatible aliases
PCRVwmaStrategy = PCRVwmaEmaOrbStrategy
PCRVwapEmaOrbStrategy = PCRVwmaEmaOrbStrategy
PCRVwapStrategy = PCRVwmaEmaOrbStrategy
