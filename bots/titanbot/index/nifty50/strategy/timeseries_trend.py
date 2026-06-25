import json
import math
import os
import sys
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yaml

import common.constants as constants
import logger
from index.nifty50.nifty_utils import get_nifty_option_instruments
from technicals.atr.atr_for_ticks import AtrEngine
from utils.generic_utils import safe_float


ist = ZoneInfo("Asia/Kolkata")
logger = logger.create_logger("TimeseriesTrendStrategyLogger")


class TimeseriesTrendStrategy:
    """
    Multi-timeframe EMA-angle trend strategy.

    Builds 1-minute index candles, derives completed 5-minute and 10-minute
    candles, applies EMA to all three timeframes, and creates option orders
    when the three EMA-angle conditions align.
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
        self.order_manager = order_manager
        self.order_maneger = order_manager
        self.has_CDN = has_CDN
        self.has_stocks = has_stocks
        self.uptox_client = uptox_client
        self.previous_day_trend = previous_day_trend
        self.selected_contracts = selected_contracts or {}
        self.index_minutes_processed = index_minutes_processed or {}
        self.future_minutes_processed = future_minutes_processed or {}

        self.curr_index_candle: Optional[Dict[str, Any]] = None
        self.curr_index_minute: Optional[str] = None
        self.curr_fut_candle: Optional[Dict[str, Any]] = None
        self.curr_fut_minute: Optional[str] = None
        self.last_fut_bar: Optional[Dict[str, Any]] = None
        self.future_data_from_parquet = False
        self._fut_vol_minute: Optional[str] = None
        self._fut_vol_start_vtt: Optional[float] = None
        self._fut_vol_last_vtt: Optional[float] = None
        self._fut_vol_by_minute: Dict[str, float] = {}
        self._future_volume_lookup: Optional[Dict[str, float]] = None
        self._future_volume_by_minute: Dict[str, float] = {}
        self._future_row_by_minute: Dict[str, int] = {}
        self._index_row_by_minute: Dict[str, int] = {}
        self._indicators_dirty = False
        self._indicator_revision = 0
        self._last_engine_revision = -1
        self._indicator_recalc_from: Optional[int] = None
        self._last_indicator_row = -1
        self.last_index_bar: Optional[Dict[str, Any]] = None
        self.last_signal = constants.WAITING
        self._last_signal_minute: Optional[str] = None
        self.signals: list[Dict[str, Any]] = []

        self.params = params if isinstance(params, dict) else self._get_params_from_yaml()
        sp = self._strategy_params()
        if self.order_maneger is not None and hasattr(self.order_maneger, "set_strategy_params"):
            self.order_maneger.set_strategy_params(sp)

        if not self.expiry_date:
            self.expiry_date = sp.get("trade_expiry")

        ht: Dict[str, Any] = {}
        if isinstance(self.params, dict):
            for key in ("historical-trend", "historical-trends"):
                value = self.params.get(key)
                if isinstance(value, dict):
                    ht.update(value)
        self._daily_sentiment = ht.get("daily", ht.get("trader-sentiment", constants.SIDEWAYS))
        self.index_fut_path = self._get_index_fut_path()
        configured_index_fut_key = self._get_index_fut_key()
        nifty_fut = self.selected_contracts.get("Nifty_Future") if isinstance(self.selected_contracts, dict) else None
        selected_index_fut_key = nifty_fut.get("instrument_key") if isinstance(nifty_fut, dict) else None
        self.index_fur_key = selected_index_fut_key or configured_index_fut_key
        self.df_index_future = self._populate_index_future_data()
        self._rebuild_future_caches()

        self.ema_length = self._coerce_int(
            sp.get("timeseries_ema_length", sp.get("ema_length", sp.get("ema_period"))),
            9,
            minimum=1,
        )
        self.slope_window = self._coerce_int(sp.get("slope_window"), 3, minimum=1)
        self.call_angle_10m = self._coerce_float(
            sp.get("call_ema_angle_10m_threshold", sp.get("up_angle_ema_10m")),
            20.0,
        )
        self.call_angle_5m = self._coerce_float(
            sp.get("call_ema_angle_5m_threshold", sp.get("up_angle_ema_5m")),
            30.0,
        )
        self.call_angle_1m = self._coerce_float(
            sp.get("call_ema_angle_1m_threshold", sp.get("up_angle_ema_1m")),
            45.0,
        )
        self.put_angle_10m = self._coerce_float(
            sp.get("put_ema_angle_10m_threshold", sp.get("dn_angle_ema_10m")),
            -20.0,
        )
        self.put_angle_5m = self._coerce_float(
            sp.get("put_ema_angle_5m_threshold", sp.get("dn_angle_ema_5m")),
            -30.0,
        )
        self.put_angle_1m = self._coerce_float(
            sp.get("put_ema_angle_1m_threshold", sp.get("dn_angle_ema_1m")),
            -45.0,
        )

        self.enable_trading_engine = self._coerce_bool(sp.get("enable_trading_engine"), True)
        self._max_order_counter = self._coerce_int(sp.get("trade-per-day", sp.get("trade_per_day")), 2, minimum=0)
        self._order_counter = 0
        self._post_exit_cooldown_minutes = self._coerce_int(sp.get("post_exit_cooldown_minutes"), 5, minimum=0)
        self._post_exit_cooldown_until: Optional[datetime] = None
        self._max_daily_loss_pct_of_initial_cash = self._coerce_float(
            sp.get("max_daily_loss_pct_of_initial_cash"),
            0.03,
            minimum=0.0,
        )
        self._daily_loss_blocked_day: Optional[str] = None
        self._today_realized_pnl_day: Optional[str] = None
        self._today_realized_pnl: float = 0.0
        self._today_realized_pnl_trade_ids = set()
        self._last_outside_trading_window_log_minute: Optional[str] = None
        self.atr5_engine = AtrEngine(atr_period=self._coerce_int(sp.get("option_atr_period"), 5, minimum=1))
        self._order_container = self._new_order_container()
        self.trade_start, self.trade_end = self._init_trade_window_times()

        self.df_index = self._empty_index_frame()
        self.df_index_5m = self._empty_timeframe_frame()
        self.df_index_10m = self._empty_timeframe_frame()
        if intraday_index_candles is not None or intraday_future_candles is not None:
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
        logger.info("timeseries_trend is driven by BotSquadron NATS ticks in live mode.")

    def on_ws_reconnected(self):
        logger.info("WebSocket reconnected; timeseries_trend strategy state preserved.")

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
        restored_instrument_key = self._order_container.get("instrument_key")
        add_instrument(restored_instrument_key)
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
        return {
            "instrument_key": instrument_key,
            "ltp": ltp,
            "ltt": int(ltt),
            "ts_epoch_ms": int(ltt),
            "oi": safe_float(market_ff.get("oi") or first_level.get("oi")),
            "vtt": safe_float(market_ff.get("vtt")),
            "gamma": safe_float(option_greeks.get("gamma")),
            "one_min_open": safe_float(feed.get("one_min_open") or market_ff.get("one_min_open")),
            "one_min_high": safe_float(feed.get("one_min_high") or market_ff.get("one_min_high")),
            "one_min_low": safe_float(feed.get("one_min_low") or market_ff.get("one_min_low")),
            "one_min_close": safe_float(feed.get("one_min_close") or market_ff.get("one_min_close")),
            "one_min_volume": safe_float(
                feed.get("one_min_volume")
                or feed.get("volume")
                or market_ff.get("one_min_volume")
                or market_ff.get("volume")
                or index_ff.get("one_min_volume")
                or index_ff.get("volume")
            ),
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
        obj = None
        feed_response = self._normalize_feed_response(message)
        if not feed_response:
            return

        try:
            for item in feed_response:
                obj = item
                instrument_key = item.get("instrument_key")
                minute_key, tick_ts = self._minute_and_timestamp_from_tick(item)
                if minute_key is None:
                    continue

                if instrument_key == constants.NIFTY50_SYMBOL:
                    ltp = safe_float(item.get("ltp")) or safe_float(item.get("one_min_close"))
                    if ltp is None:
                        logger.error(f"Invalid index ltp. instrument={instrument_key} obj={obj}")
                        continue
                    raw_volume = safe_float(item.get("one_min_volume"))
                    volume = raw_volume if raw_volume is not None and raw_volume > 0 else 1.0
                    self._handle_index_tick(minute_key, ltp, volume)
                    continue

                if self.index_fur_key is not None and instrument_key == self.index_fur_key:
                    vtt = safe_float(item.get("vtt"))
                    if vtt is not None:
                        finished_minute, finished_vol = self._update_1m_volume_from_vtt(minute_key, vtt)
                        if finished_minute is not None and finished_vol is not None:
                            self._fut_vol_by_minute[finished_minute] = float(finished_vol)
                    self._handle_fut_tick_from_parquet(minute_key, item)
                    continue

                ltp = safe_float(item.get("ltp"))
                if ltp is None or tick_ts is None:
                    continue
                self.atr5_engine.on_tick(str(instrument_key), ltp, tick_ts)

            if self.curr_index_minute:
                self._apply_indicators_if_dirty()
                if self._is_trading_window(self.curr_index_minute):
                    self._last_outside_trading_window_log_minute = None
                    self._trading_engine_active()
                else:
                    self._log_outside_trading_window(self.curr_index_minute)

            self._trade_processing(feed_response)

        except Exception as exc:
            logger.error(f"An error occurred in obj {obj} on_ws_message: {exc}")
            sys.exit(constants.FAIL_CODE)

    # ------------------------------------------------------------------
    # Trade lifecycle, order creation and risk handling
    # ------------------------------------------------------------------
    def _trade_processing(self, feed_response):
        if self.curr_index_minute and not self._is_trading_window(self.curr_index_minute):
            if feed_response:
                self._update_open_order_ltp(feed_response)
            self._clear_waiting_order_intent()
            self._square_off_open_trade(reason=constants.EOD_SQUARE_OFF)
            return

        if self.order_maneger is None:
            return

        if not feed_response:
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

    def _punch_waiting_order(self, feed_response):
        if self.last_index_bar is None:
            logger.debug("Waiting order not punched; last_index_bar is unavailable.")
            return

        sp = self._strategy_params()
        side = self._order_container.get("side")
        logger.info(f"Need to find {side} side contracts")
        ref_ts = self._resolve_reference_ts()
        if self._is_daily_loss_limit_active(ref_ts):
            logger.info("Skipping order placement: daily loss guard active. No more new trades for today.")
            self._reset_order_container()
            return
        if self._is_post_exit_cooldown_active(ref_ts):
            logger.info("Skipping order placement: post-exit cooldown active.")
            self._clear_waiting_order_intent()
            return

        itm_range = self._coerce_float(sp.get("itm_strike_range"), 200.0, minimum=0.0)
        index_price = float(self.last_index_bar["close"])
        dict_itm = self._get_itm_contracts(
            side,
            itm_range,
            index_price,
        )
        if not dict_itm and self._refresh_selected_contracts(index_price):
            dict_itm = self._get_itm_contracts(side, itm_range, index_price)
        if not dict_itm:
            logger.warning(
                f"Waiting order not punched; no ITM contracts for side={side}, "
                f"index_price={index_price}, itm_range={itm_range}, "
                f"selected_strikes={self._selected_contract_strike_summary()}"
            )
            return

        chosen = self._choose_contract(feed_response, dict_itm)
        if chosen is None:
            logger.debug(
                f"Waiting order not punched yet; no subscribed tick in feed for side={side}, "
                f"candidate_count={len(dict_itm)}, feed_count={len(feed_response)}"
            )
            return

        instrument_key = chosen.get("instrument_key")
        ltp = safe_float(chosen.get("ltp"))
        if instrument_key not in dict_itm or ltp is None:
            return

        contract = dict_itm[instrument_key]
        self._order_container["instrument_key"] = instrument_key
        self._order_container["instrument_symbol"] = contract.get("trading_symbol")
        self._order_container["ltp"] = ltp
        self._order_container["max_gamma"] = safe_float(chosen.get("gamma"))

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
            logger.warning(
                f"Waiting order not punched; risk prices unavailable for instrument_key={instrument_key}, "
                f"entry_price={entry_price}, option_atr={option_atr}."
            )
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
        self._order_container["start_trail_after"] = start_trail_after
        ts = self._timestamp_from_item(chosen) or self._resolve_reference_ts()
        description = f"{side} {self._order_container['instrument_symbol']} entry={entry_price:.2f}"

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
            f"RiskMode: {risk_mode}, OptionATR: {option_atr}"
        )

        if trade_id:
            self._order_container["trade_id"] = trade_id
            self._order_container["status"] = constants.OPEN
            self._order_container["entry_price"] = entry_price
            self._order_container["trade_create_time"] = ts
            self._order_container["force_trail_lock"] = False
            self._order_counter += 1
            logger.info(f"{self._order_container}")
        else:
            logger.warning(
                f"Order manager did not return trade_id; clearing waiting intent for "
                f"instrument_key={instrument_key}, symbol={self._order_container.get('instrument_symbol')}."
            )
            self._clear_waiting_order_intent()

    def _get_itm_contracts(
        self,
        side: str,
        itm_range: float,
        index_price: float,
    ) -> Dict[str, Dict[str, Any]]:
        output: Dict[str, Dict[str, Any]] = {}
        spot_price = safe_float(index_price)
        if spot_price is None or spot_price <= 0 or not isinstance(self.selected_contracts, dict):
            return output

        side_key = str(side or "").strip().upper()
        low = spot_price - float(itm_range)
        high = spot_price + float(itm_range)
        call_tokens = {str(constants.CALL).upper(), str(constants.CE).upper(), "CALL", "CE"}
        put_tokens = {str(constants.PUT).upper(), str(constants.PE).upper(), "PUT", "PE"}

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

    def _refresh_selected_contracts(self, index_price: float) -> bool:
        spot_price = safe_float(index_price)
        if spot_price is None or spot_price <= 0:
            return False

        expiry = self._strategy_params().get("trade_expiry") or self.expiry_date
        if not expiry:
            logger.warning("Unable to refresh titanbot option contracts; trade expiry is unavailable.")
            return False

        atm_price = int(round(float(spot_price) / 50) * 50)
        old_keys = set(self.get_subscription_instruments())
        future_contract = (
            self.selected_contracts.get("Nifty_Future")
            if isinstance(self.selected_contracts, dict)
            else None
        )

        try:
            refreshed = get_nifty_option_instruments(atm_price, expiry)
        except Exception as exc:
            logger.warning(
                f"Unable to refresh titanbot option contracts for atm={atm_price}, expiry={expiry}: {exc}"
            )
            return False

        if not isinstance(refreshed, dict) or not refreshed:
            logger.warning(
                f"Unable to refresh titanbot option contracts; empty response for atm={atm_price}, expiry={expiry}."
            )
            return False

        if isinstance(future_contract, dict):
            refreshed["Nifty_Future"] = future_contract

        self.selected_contracts = refreshed
        self.expiry_date = expiry
        if isinstance(future_contract, dict):
            self.index_fur_key = str(future_contract.get("instrument_key") or "").strip() or None

        new_keys = set(self.get_subscription_instruments())
        logger.info(
            f"Refreshed titanbot option contracts using atm={atm_price}, spot={spot_price}, expiry={expiry}; "
            f"subscription_keys_before={len(old_keys)}, after={len(new_keys)}, "
            f"selected_strikes={self._selected_contract_strike_summary()}"
        )
        return new_keys != old_keys

    def _selected_contract_strike_summary(self) -> str:
        if not isinstance(self.selected_contracts, dict):
            return "none"

        strikes = []
        for strike_price in self.selected_contracts.keys():
            if strike_price == "Nifty_Future":
                continue
            strike = safe_float(strike_price)
            if strike is not None:
                strikes.append(float(strike))
        if not strikes:
            return "none"
        return f"{min(strikes):.0f}-{max(strikes):.0f} ({len(strikes)} strikes)"

    def _trail_open_order(self, feed_response):
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
            self._set_post_exit_cooldown(trade_info.get("status"), ts=ts)
            self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
            self._reset_order_container()

    def _should_force_trail_open_order(
        self,
        latest_ltp: Optional[float] = None,
        ts: Optional[datetime] = None,
    ) -> bool:
        if self._order_container.get("status") != constants.OPEN:
            return False
        if self._coerce_bool(self._order_container.get("force_trail_lock"), False):
            return False
        if self._coerce_bool(self._strategy_params().get("force_trail_stale_losing_order"), False):
            return self._is_stale_losing_open_order(latest_ltp, ts)
        return False

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

    def _build_risk_prices(
        self,
        entry_price: float,
        option_atr: Optional[float],
        use_option_atr_risk: bool,
        require_option_atr: bool,
        sp: Dict[str, Any],
    ):
        risk_mode = "pct"
        if use_option_atr_risk and option_atr is not None and option_atr > 0:
            atr_target_mult = self._coerce_float(sp.get("atr_target_mult"), 10.0, minimum=0.0)
            atr_sl_mult = self._coerce_float(sp.get("atr_sl_mult"), 3.0, minimum=0.0)
            max_atr_for_contract = self._coerce_float(sp.get("max_atr_for_contract"), 20.0, minimum=0.0)
            min_atr_for_contract = self._coerce_float(sp.get("min_atr_for_contract"), 10.0, minimum=0.0)
            trailing_factor = self._coerce_float(
                sp.get("trailing-factor", sp.get("trailing_factor")),
                1.0,
                minimum=0.0,
            )
            atr_to_use = float(option_atr)
            target = entry_price + (atr_target_mult * option_atr)
            sl_trigger = entry_price - (atr_sl_mult * option_atr)
            if max_atr_for_contract > 0 and option_atr > max_atr_for_contract:
                atr_to_use = max_atr_for_contract
            if min_atr_for_contract > 0 and option_atr < min_atr_for_contract:
                sl_trigger = entry_price - (atr_sl_mult * min_atr_for_contract)
            start_trail_after = float((option_atr * trailing_factor) / entry_price) if entry_price > 0 else 0.0
            return target, sl_trigger, atr_to_use, start_trail_after, "atr"

        if use_option_atr_risk and require_option_atr:
            logger.warning("Skipping order; option ATR unavailable.")
            return None, None, None, None, risk_mode

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
        return target, sl_trigger, trail_points, start_trail_after, risk_mode

    def _start_trail_after_from_vwap_session(
        self,
        entry_price: float,
        option_atr: float,
        default_atr_mult: float,
    ) -> float:
        if entry_price <= 0:
            return 0.0

        atr_mult = self._vwap_session_trail_atr_mult()
        if atr_mult is None:
            atr_mult = float(default_atr_mult)
        return float((float(option_atr) * atr_mult) / float(entry_price))

    def _vwap_session_trail_atr_mult(self) -> Optional[float]:
        band_width = self._latest_vwap_band_1_width()
        if band_width is None:
            return None
        if band_width < 100:
            return 1.0
        if 100 < band_width < 130:
            return 1.5
        if 150 < band_width < 200:
            return 2.0
        if band_width > 200:
            return 2.5
        return None

    def _latest_vwap_band_1_width(self) -> Optional[float]:
        latest = None
        if self.df_index is not None and not self.df_index.empty:
            latest = self.df_index.iloc[-1]
        elif isinstance(self.last_index_bar, dict):
            latest = self.last_index_bar
        if latest is None:
            return None

        upper = safe_float(latest.get("vwap_upper_band_1"))
        lower = safe_float(latest.get("vwap_lower_band_1"))
        if upper is None or lower is None:
            return None

        width = upper - lower
        return width if width > 0 else None

    def _clear_pending_contract(self):
        self._order_container["instrument_key"] = None
        self._order_container["ltp"] = None
        self._order_container["max_gamma"] = None
        self._order_container["instrument_symbol"] = None

    def _clear_waiting_order_intent(self):
        if self._order_container.get("status") != constants.WAITING:
            return
        self._reset_order_container()

    def _update_open_order_ltp(self, feed_response):
        if self._order_container.get("status") != constants.OPEN:
            return
        for item in feed_response or []:
            if item.get("instrument_key") == self._order_container.get("instrument_key"):
                ltp = safe_float(item.get("ltp"))
                if ltp is not None:
                    self._order_container["ltp"] = ltp
                return

    def _reset_order_container(self):
        self._order_container = self._new_order_container()

    def _new_order_container(self) -> Dict[str, Any]:
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
        }

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
        entry_price = safe_float(trade.get("entry_price")) or safe_float((contract or {}).get("ltp"))
        qty = safe_float(trade.get("qty"))
        self._order_container.update(
            {
                "trade_id": trade_id,
                "side": self._strategy_side_from_trade(trade, contract),
                "instrument_key": instrument_key,
                "instrument_symbol": instrument_symbol,
                "status": constants.OPEN,
                "ltp": entry_price,
                "entry_price": entry_price,
                "lot": int(qty) if qty is not None and qty > 0 else None,
                "max_gamma": None,
                "start_trail_after": safe_float(trade.get("start_trail_after")),
                "trade_create_time": trade.get("timestamp") or trade.get("entry_time"),
                "force_trail_lock": False,
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

    def _square_off_open_trade(self, reason: str):
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
            trade_info = self.order_maneger.get_trade_by_id(trade_id)
            self._update_today_realized_pnl_on_trade_close(trade_info, ts=self._resolve_reference_ts())
            self._reset_order_container()

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
        else:
            ref_ts = ref_ts.astimezone(ist)

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
        return {"pnl": float(pnl_s[mask].sum()), "trade_ids": trade_ids}

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

    def _update_today_realized_pnl_on_trade_close(
        self,
        trade_info: Optional[Dict[str, Any]],
        ts: Optional[datetime] = None,
    ) -> None:
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

    # ------------------------------------------------------------------
    # Candle building and indicator calculation
    # ------------------------------------------------------------------
    def _initialize_from_intraday_candles(self, index_candles, future_candles=None) -> None:
        def build_df(candles, include_volume: bool) -> pd.DataFrame:
            if not candles:
                return pd.DataFrame()

            rows = []
            for candle in candles:
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
                    oi = candle.get("oi") or candle.get("open_interest")
                else:
                    continue

                ts = pd.to_datetime(raw_time, errors="coerce")
                if pd.isna(ts):
                    continue
                if ts.tzinfo is None:
                    ts = ts.tz_localize(ist)
                else:
                    ts = ts.tz_convert(ist)

                row = {
                    "time": ts.strftime("%Y-%m-%d %H:%M"),
                    "open": safe_float(open_price),
                    "high": safe_float(high_price),
                    "low": safe_float(low_price),
                    "close": safe_float(close_price),
                    "volume": safe_float(volume) or 0.0,
                }
                if include_volume:
                    row["oi"] = safe_float(oi)
                if any(row.get(key) is None for key in ("open", "high", "low", "close")):
                    continue
                rows.append(row)

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows).sort_values("time").reset_index(drop=True)
            for column in ("open", "high", "low", "close", "volume"):
                if column in df.columns:
                    df[column] = pd.to_numeric(df[column], errors="coerce")
            if include_volume and "oi" in df.columns:
                df["oi"] = pd.to_numeric(df["oi"], errors="coerce")
            return df

        df_index = build_df(index_candles, include_volume=False)
        if not df_index.empty:
            self.df_index = pd.concat([self.df_index, df_index], ignore_index=True)
            for idx, minute_key in enumerate(self.df_index["time"].astype(str)):
                self._index_row_by_minute[minute_key] = idx
                self.index_minutes_processed[minute_key] = True
            self.last_index_bar = self.df_index.iloc[-1].to_dict()
            self._mark_indicators_dirty(0)
            self._apply_indicators_if_dirty()
            if self.last_index_bar and self._is_trading_window(str(self.last_index_bar.get("time"))):
                self._trading_engine_active()

        df_future = build_df(future_candles, include_volume=True)
        if not df_future.empty:
            self.df_index_future = pd.concat([self.df_index_future, df_future], ignore_index=True)
            self.df_index_future = self.df_index_future.drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
            for minute_key in df_future["time"].astype(str):
                self.future_minutes_processed[minute_key] = True
            self.last_fut_bar = df_future.iloc[-1].to_dict()
            self._rebuild_future_caches()

    def _empty_index_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "time": pd.Series(dtype="object"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "candle_length": pd.Series(dtype="float64"),
                "ema_1m": pd.Series(dtype="float64"),
                "angle_ema_1m": pd.Series(dtype="float64"),
                "ema_5m": pd.Series(dtype="float64"),
                "angle_ema_5m": pd.Series(dtype="float64"),
                "ema_10m": pd.Series(dtype="float64"),
                "angle_ema_10m": pd.Series(dtype="float64"),
                "ema_9": pd.Series(dtype="float64"),
                "angle_ema_9": pd.Series(dtype="float64"),
                "signal": pd.Series(dtype="object"),
            }
        )

    def _empty_timeframe_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "time": pd.Series(dtype="object"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
                "ema": pd.Series(dtype="float64"),
                "angle_ema": pd.Series(dtype="float64"),
            }
        )

    def _handle_index_tick(self, minute_key: str, ltp: float, volume: float = 1.0):
        if self.curr_index_minute is None or minute_key != self.curr_index_minute:
            self._finalize_index_candle()
            self.curr_index_minute = minute_key
            self.curr_index_candle = {
                "time": minute_key,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": volume,
            }
            return

        candle = self.curr_index_candle
        if candle is None:
            return

        candle["high"] = max(float(candle["high"]), ltp)
        candle["low"] = min(float(candle["low"]), ltp)
        candle["close"] = ltp
        candle["volume"] = max(float(candle.get("volume") or 0.0), volume)

    def _finalize_index_candle(self):
        if self.curr_index_candle is None:
            return

        candle = self.curr_index_candle
        row_idx = len(self.df_index)
        self.df_index.loc[row_idx, list(candle.keys())] = list(candle.values())
        minute_key = str(candle.get("time") or "")[:16]
        if minute_key:
            self._index_row_by_minute[minute_key] = row_idx
        self.last_index_bar = candle
        self.curr_index_candle = None

        self._mark_indicators_dirty(row_idx)

    def _mark_indicators_dirty(self, from_index: Optional[int] = None) -> None:
        self._indicators_dirty = True
        if from_index is None:
            self._indicator_recalc_from = None
            return

        from_index = max(0, int(from_index))
        if self._indicator_recalc_from is None:
            self._indicator_recalc_from = from_index
        else:
            self._indicator_recalc_from = min(self._indicator_recalc_from, from_index)

    def _apply_indicators_if_dirty(self) -> None:
        if not self._indicators_dirty:
            return

        self._apply_indicators()
        self._indicators_dirty = False
        self._indicator_recalc_from = None

    def _apply_indicators(self):
        if self.df_index.empty:
            return

        frame = self.df_index
        close = pd.to_numeric(frame["close"], errors="coerce").astype("float64")
        high = pd.to_numeric(frame["high"], errors="coerce").astype("float64")
        low = pd.to_numeric(frame["low"], errors="coerce").astype("float64")

        ema_1m = self._ema(close)
        angle_1m = self._ema_angle(ema_1m)

        self.df_index["candle_length"] = (high - low).to_numpy()
        self.df_index["ema_1m"] = ema_1m.to_numpy()
        self.df_index["angle_ema_1m"] = angle_1m.to_numpy()

        self.df_index_5m = self._build_timeframe_frame(frame, 5)
        self.df_index_10m = self._build_timeframe_frame(frame, 10)
        self._map_timeframe_values(5, self.df_index_5m)
        self._map_timeframe_values(10, self.df_index_10m)

        self.df_index["ema_9"] = ema_1m.to_numpy()
        self.df_index["angle_ema_9"] = angle_1m.to_numpy()
        self._last_indicator_row = len(self.df_index) - 1
        self._indicator_revision += 1

    def _build_timeframe_frame(self, frame: pd.DataFrame, minutes: int) -> pd.DataFrame:
        if frame.empty:
            return self._empty_timeframe_frame()

        working = frame[["time", "open", "high", "low", "close", "volume"]].copy()
        working["dt"] = pd.to_datetime(working["time"], errors="coerce")
        working = working.dropna(subset=["dt"]).sort_values("dt")
        if working.empty:
            return self._empty_timeframe_frame()

        for column in ("open", "high", "low", "close", "volume"):
            working[column] = pd.to_numeric(working[column], errors="coerce")

        rule = f"{int(minutes)}min"
        chunks = []
        for day_key, day_frame in working.groupby(working["dt"].dt.strftime("%Y-%m-%d")):
            origin = pd.Timestamp(f"{day_key} 09:15")
            indexed = day_frame.set_index("dt")
            resampled = indexed.resample(
                rule,
                origin=origin,
                label="right",
                closed="left",
            ).agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            chunks.append(resampled.dropna(subset=["close"]))

        if not chunks:
            return self._empty_timeframe_frame()

        result = pd.concat(chunks).sort_index().reset_index()
        result.rename(columns={"dt": "time"}, inplace=True)
        result["time"] = pd.to_datetime(result["time"], errors="coerce") - pd.Timedelta(minutes=1)
        result["ema"] = self._ema(pd.to_numeric(result["close"], errors="coerce"))
        result["angle_ema"] = self._ema_angle(result["ema"])
        result["time"] = result["time"].dt.strftime("%Y-%m-%d %H:%M")
        return result[["time", "open", "high", "low", "close", "volume", "ema", "angle_ema"]]

    def _map_timeframe_values(self, minutes: int, timeframe: pd.DataFrame) -> None:
        ema_col = f"ema_{minutes}m"
        angle_col = f"angle_ema_{minutes}m"

        self.df_index[ema_col] = np.nan
        self.df_index[angle_col] = np.nan
        if timeframe.empty:
            return

        base = pd.DataFrame(
            {
                "_idx": self.df_index.index,
                "dt": pd.to_datetime(self.df_index["time"], errors="coerce"),
            }
        ).dropna(subset=["dt"])
        if base.empty:
            return

        values = pd.DataFrame(
            {
                "dt": pd.to_datetime(timeframe["time"], errors="coerce"),
                ema_col: pd.to_numeric(timeframe["ema"], errors="coerce"),
                angle_col: pd.to_numeric(timeframe["angle_ema"], errors="coerce"),
            }
        ).dropna(subset=["dt"]).sort_values("dt")
        if values.empty:
            return

        mapped = pd.merge_asof(
            base.sort_values("dt"),
            values,
            on="dt",
            direction="backward",
        )
        mapped = mapped.set_index("_idx").reindex(self.df_index.index)
        self.df_index[ema_col] = mapped[ema_col].to_numpy()
        self.df_index[angle_col] = mapped[angle_col].to_numpy()

    def _ema(self, source: pd.Series) -> pd.Series:
        return source.ewm(
            span=self.ema_length,
            adjust=False,
            min_periods=self.ema_length,
        ).mean()

    def _ema_angle(self, ema: pd.Series) -> pd.Series:
        slope = (ema - ema.shift(self.slope_window)) / float(self.slope_window)
        angle = np.degrees(np.arctan(np.clip(slope.to_numpy(dtype="float64"), -10.0, 10.0)))
        return pd.Series(angle, index=ema.index, dtype="float64")

    # ------------------------------------------------------------------
    # Entry engine
    # ------------------------------------------------------------------
    def _trading_engine_active(self):
        try:
            if not self.enable_trading_engine:
                return

            current_revision = self._indicator_revision
            if self._last_engine_revision == current_revision:
                return

            if len(self.df_index) < max(self.ema_length + self.slope_window, 2):
                self._last_engine_revision = current_revision
                return

            latest = self.df_index.iloc[-1]
            latest_idx = self.df_index.index[-1]
            latest_time = str(latest.get("time"))
            if not self._is_trading_window(latest_time):
                self._last_engine_revision = current_revision
                return

            close_price = safe_float(latest.get("close"))
            angle_10m = safe_float(latest.get("angle_ema_10m"))
            angle_5m = safe_float(latest.get("angle_ema_5m"))
            angle_1m = safe_float(latest.get("angle_ema_1m"))
            ema_10m = safe_float(latest.get("ema_10m"))
            ema_5m = safe_float(latest.get("ema_5m"))
            ema_1m = safe_float(latest.get("ema_1m"))
            if any(value is None for value in (close_price, angle_10m, angle_5m, angle_1m)):
                self._last_engine_revision = current_revision
                return

            call_setup = (
                angle_10m > self.call_angle_10m
                and angle_5m > self.call_angle_5m
                and angle_1m > self.call_angle_1m
            )
            put_setup = (
                angle_10m < self.put_angle_10m
                and angle_5m < self.put_angle_5m
                and angle_1m < self.put_angle_1m
            )

            logger.debug(
                f"candle_time={latest_time}, close={close_price}, "
                f"ema_10m={ema_10m}, angle_10m={angle_10m}, threshold_call_10m={self.call_angle_10m}, threshold_put_10m={self.put_angle_10m}, "
                f"ema_5m={ema_5m}, angle_5m={angle_5m}, threshold_call_5m={self.call_angle_5m}, threshold_put_5m={self.put_angle_5m}, "
                f"ema_1m={ema_1m}, angle_1m={angle_1m}, threshold_call_1m={self.call_angle_1m}, threshold_put_1m={self.put_angle_1m}"
            )
            logger.debug(f"candle_time={latest_time}, condition check call_setup:{call_setup}, put_setup:{put_setup}")
            self._last_engine_revision = current_revision

            self.df_index.loc[latest_idx, "signal"] = constants.WAITING
            self.last_signal = constants.WAITING

            if (
                call_setup
                and self._order_container["status"] is None
                and (self._order_counter < self._max_order_counter)
            ):
                self._set_waiting_order(
                    latest_idx=latest_idx,
                    latest_time=latest_time,
                    side=constants.CALL,
                    close_price=close_price,
                    ema_10m=ema_10m,
                    ema_5m=ema_5m,
                    ema_1m=ema_1m,
                    angle_10m=angle_10m,
                    angle_5m=angle_5m,
                    angle_1m=angle_1m,
                )
                return

            if (
                put_setup
                and self._order_container["status"] is None
                and (self._order_counter < self._max_order_counter)
            ):
                self._set_waiting_order(
                    latest_idx=latest_idx,
                    latest_time=latest_time,
                    side=constants.PUT,
                    close_price=close_price,
                    ema_10m=ema_10m,
                    ema_5m=ema_5m,
                    ema_1m=ema_1m,
                    angle_10m=angle_10m,
                    angle_5m=angle_5m,
                    angle_1m=angle_1m,
                )
                return

        except Exception as exc:
            logger.error(f"An error occurred in _trading_engine_active: {exc}")
            sys.exit(constants.FAIL_CODE)

    def _set_waiting_order(
        self,
        latest_idx: int,
        latest_time: str,
        side: str,
        close_price: float,
        ema_10m: Optional[float],
        ema_5m: Optional[float],
        ema_1m: Optional[float],
        angle_10m: float,
        angle_5m: float,
        angle_1m: float,
    ) -> None:
        ref_ts = self._resolve_reference_ts()
        if self._is_daily_loss_limit_active(ref_ts):
            logger.info("Signal ignored because daily loss guard is active.")
            return
        if self._is_post_exit_cooldown_active(ref_ts):
            logger.info("Signal ignored because post-exit cooldown is active.")
            return

        lot = self._calculate_lot_size(side)
        if lot <= 0:
            return

        self.df_index.loc[latest_idx, "signal"] = side
        self.last_signal = side
        signal_event = {
            "time": latest_time,
            "signal": side,
            "close": close_price,
            "ema_10m": ema_10m,
            "ema_5m": ema_5m,
            "ema_1m": ema_1m,
            "angle_ema_10m": angle_10m,
            "angle_ema_5m": angle_5m,
            "angle_ema_1m": angle_1m,
        }
        self.signals.append(signal_event)
        logger.info(f"Timeseries Trend signal candidate: {signal_event}")

        self._order_container["side"] = side
        self._order_container["status"] = constants.WAITING
        self._order_container["lot"] = int(lot)
        self._order_container["force_trail_lock"] = False
        logger.info(f"Order intent set side={side}, lot={lot}, status={constants.WAITING}")

    def _calculate_lot_size(self, side: str) -> int:
        sp = self._strategy_params()
        lot_cfg = sp.get("lot-size") or sp.get("lot_size") or {}
        small = self._coerce_int(lot_cfg.get("small"), 1, minimum=0)
        medium = self._coerce_int(lot_cfg.get("medium"), 1, minimum=0)
        large = self._coerce_int(lot_cfg.get("large"), 1, minimum=0)

        if side == constants.CALL:
            if self._daily_sentiment == constants.BULLISH:
                return large
            if self._daily_sentiment == constants.BEARISH:
                return small
            return medium

        if side == constants.PUT:
            if self._daily_sentiment == constants.BEARISH:
                return large
            if self._daily_sentiment == constants.BULLISH:
                return small
            return medium

        return small

    # ------------------------------------------------------------------
    # Future feed helpers
    # ------------------------------------------------------------------
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

    def _invalidate_future_volume_lookup(self) -> None:
        self._future_volume_lookup = None

    def _minute_key_from_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value)
        if len(text) >= 16:
            return text[:16]
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d %H:%M")

    def _rebuild_future_caches(self) -> None:
        self._future_row_by_minute = {}
        self._future_volume_by_minute = {}
        self._invalidate_future_volume_lookup()

        if self.df_index_future is None or self.df_index_future.empty:
            return

        for idx, row in self.df_index_future.iterrows():
            minute_key = self._minute_key_from_value(row.get("time"))
            if minute_key is None:
                continue
            self._future_row_by_minute[minute_key] = int(idx)
            volume = safe_float(row.get("volume"))
            if volume is not None:
                self._future_volume_by_minute[minute_key] = float(volume)

    def _populate_index_future_data(self):
        self.future_data_from_parquet = False
        if not self.index_fut_path or not os.path.exists(self.index_fut_path):
            if self.index_fur_key is not None:
                self.future_data_from_parquet = True
                logger.info(f"Using parquet feed for Nifty future candles. instrument_key={self.index_fur_key}")
            else:
                logger.debug(f"Index future data file not found at {self.index_fut_path}")
            return self._empty_future_candle_frame()

        logger.info(f"Using index future JSON data from {self.index_fut_path}")
        with open(self.index_fut_path, "r", encoding="utf-8") as file:
            data = json.load(file) or {}

        candles = None
        if isinstance(data, dict):
            if isinstance(data.get("data"), dict):
                candles = data["data"].get("candles")
            elif "candles" in data:
                candles = data.get("candles")

        if not isinstance(candles, list) or not candles:
            logger.warning(f"Unexpected future data format, unable to parse candles from {self.index_fut_path}")
            return self._empty_future_candle_frame()

        rows = []
        for candle in candles:
            if isinstance(candle, (list, tuple)) and len(candle) >= 7:
                candle_time = pd.to_datetime(candle[0], errors="coerce")
                if pd.isna(candle_time):
                    continue
                rows.append({
                    "time": candle_time.strftime("%Y-%m-%d %H:%M"),
                    "open": candle[1],
                    "high": candle[2],
                    "low": candle[3],
                    "close": candle[4],
                    "volume": candle[5],
                    "oi": candle[6],
                })
            elif isinstance(candle, dict):
                candle_time = pd.to_datetime(
                    candle.get("datetime") or candle.get("time") or candle.get("date"),
                    errors="coerce",
                )
                if pd.isna(candle_time):
                    continue
                rows.append({
                    "time": candle_time.strftime("%Y-%m-%d %H:%M"),
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume"),
                    "oi": candle.get("open_interest") or candle.get("oi"),
                })

        df = pd.DataFrame(rows)
        if df.empty:
            return self._empty_future_candle_frame()

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M")
        for column in ["open", "high", "low", "close", "volume", "oi"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        return df

    def _get_index_fut_path(self):
        if self.params and "data-sources" in self.params:
            sources_dict = self.params["data-sources"]
            if isinstance(sources_dict, dict) and "nifty-volume" in sources_dict:
                configured_path = str(sources_dict["nifty-volume"] or "").strip()
                return configured_path or None
        return None

    def _get_index_fut_key(self):
        if self.params and "data-sources" in self.params:
            sources_dict = self.params["data-sources"]
            if isinstance(sources_dict, dict) and "nifty-future" in sources_dict:
                configured_key = str(sources_dict["nifty-future"] or "").strip()
                return configured_key or None
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
            self.df_index_future = pd.DataFrame([row], columns=self._empty_future_candle_frame().columns)
            self._future_row_by_minute[minute_key] = 0
            if row["volume"] is not None:
                self._future_volume_by_minute[minute_key] = float(row["volume"])
            self._invalidate_future_volume_lookup()
            return

        idx = self._future_row_by_minute.get(minute_key)
        if idx is not None:
            for col, value in row.items():
                self.df_index_future.at[idx, col] = value
            if row["volume"] is not None:
                self._future_volume_by_minute[minute_key] = float(row["volume"])
            self._invalidate_future_volume_lookup()
        else:
            idx = len(self.df_index_future)
            self.df_index_future.loc[idx, list(row.keys())] = list(row.values())
            self._future_row_by_minute[minute_key] = idx
            if row["volume"] is not None:
                self._future_volume_by_minute[minute_key] = float(row["volume"])
            self._invalidate_future_volume_lookup()

    def _finalize_fut_candle(self) -> None:
        if self.curr_fut_candle is None:
            return
        minute = str(self.curr_fut_candle.get("time") or "")
        if minute in self._fut_vol_by_minute:
            self.curr_fut_candle["volume"] = float(self._fut_vol_by_minute.pop(minute, 0.0))
        elif (
            minute
            and minute == self._fut_vol_minute
            and self._fut_vol_start_vtt is not None
            and self._fut_vol_last_vtt is not None
        ):
            self.curr_fut_candle["volume"] = max(
                float(self._fut_vol_last_vtt) - float(self._fut_vol_start_vtt),
                0.0,
            )
        self._upsert_future_candle(self.curr_fut_candle)
        self.last_fut_bar = dict(self.curr_fut_candle)
        self.curr_fut_candle = None

    def _update_1m_volume_from_vtt(self, minute_key: str, vtt_now: float):
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
            finished_volume = max(
                float(self._fut_vol_last_vtt or 0.0) - float(self._fut_vol_start_vtt or 0.0),
                0.0,
            )

            self._fut_vol_minute = minute_key
            self._fut_vol_start_vtt = vtt_now
            self._fut_vol_last_vtt = vtt_now
            return finished_minute, finished_volume

        except Exception as exc:
            logger.error(f"Error in _update_1m_volume_from_vtt: {exc}")
            sys.exit(constants.FAIL_CODE)

    def _handle_fut_tick_from_parquet(self, minute_key: str, item: Dict[str, Any]) -> None:
        ltp = safe_float(item.get("ltp"))
        if ltp is None or ltp <= 0:
            return

        if self.curr_fut_minute is None or minute_key != self.curr_fut_minute:
            if self.curr_fut_candle is not None:
                self._finalize_fut_candle()

            self.curr_fut_minute = minute_key
            self.curr_fut_candle = {
                "time": minute_key,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": 0.0,
                "oi": safe_float(item.get("oi")),
            }

        candle = self.curr_fut_candle
        if candle is None:
            return

        one_min_open = safe_float(item.get("one_min_open"))
        one_min_high = safe_float(item.get("one_min_high"))
        one_min_low = safe_float(item.get("one_min_low"))
        one_min_close = safe_float(item.get("one_min_close"))
        one_min_volume = safe_float(item.get("one_min_volume"))
        vtt = safe_float(item.get("vtt"))
        oi = safe_float(item.get("oi"))

        if one_min_open is not None and one_min_open > 0:
            candle["open"] = one_min_open
        if one_min_high is not None and one_min_high > 0:
            candle["high"] = one_min_high
        else:
            candle["high"] = max(float(candle["high"]), ltp)
        if one_min_low is not None and one_min_low > 0:
            candle["low"] = one_min_low
        else:
            candle["low"] = min(float(candle["low"]), ltp)
        if one_min_close is not None and one_min_close > 0:
            candle["close"] = one_min_close
        else:
            candle["close"] = ltp
        if one_min_volume is not None and one_min_volume >= 0 and vtt is None:
            candle["volume"] = one_min_volume
        if oi is not None:
            candle["oi"] = oi

        self._upsert_future_candle(candle)
        self.last_fut_bar = dict(candle)

    # ------------------------------------------------------------------
    # Time, config and type helpers
    # ------------------------------------------------------------------
    def _timestamp_from_item(self, item: Dict[str, Any]) -> Optional[datetime]:
        ts_ms = safe_float(item.get("ts_epoch_ms")) or safe_float(item.get("ltt"))
        if ts_ms is not None:
            return datetime.fromtimestamp(ts_ms / 1000.0, tz=ist)

        minute_key = self._minute_from_tick(item)
        if minute_key:
            try:
                return datetime.strptime(minute_key, "%Y-%m-%d %H:%M").replace(tzinfo=ist)
            except Exception:
                return None
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

    def _minute_from_tick(self, item: Dict[str, Any]) -> Optional[str]:
        minute_key, _ = self._minute_and_timestamp_from_tick(item)
        return minute_key

    def _minute_and_timestamp_from_tick(self, item: Dict[str, Any]) -> tuple[Optional[str], Optional[datetime]]:
        ts_ms = safe_float(item.get("ltt")) or safe_float(item.get("ts_epoch_ms"))
        if ts_ms is not None:
            ts = datetime.fromtimestamp(ts_ms / 1000.0, ist)
            return ts.strftime("%Y-%m-%d %H:%M"), ts

        ts_text = item.get("ts_ist") or item.get("one_min_ts") or item.get("current_ts")
        if ts_text:
            ts = pd.to_datetime(ts_text, errors="coerce")
            if pd.notna(ts):
                if ts.tzinfo is None:
                    ts = ts.tz_localize(ist)
                else:
                    ts = ts.tz_convert(ist)
                ts_dt = ts.to_pydatetime()
                return ts.strftime("%Y-%m-%d %H:%M"), ts_dt

        return None, None

    def _is_trading_window(self, time_str: str) -> bool:
        ts = pd.to_datetime(time_str, errors="coerce")
        if pd.isna(ts):
            return False
        current = ts.time()
        return self.trade_start <= current <= self.trade_end

    def _log_outside_trading_window(self, minute_key: str) -> None:
        if minute_key == self._last_outside_trading_window_log_minute:
            return
        logger.info(f"Outside Trading Window at {minute_key}")
        self._last_outside_trading_window_log_minute = minute_key

    def _init_trade_window_times(self) -> tuple[time, time]:
        sp = self._strategy_params()
        trade_window = sp.get("trade-window") or sp.get("trade_window") or {}
        market_hours = self.params.get("market-hours", {}) if isinstance(self.params, dict) else {}
        start_str = trade_window.get("start", market_hours.get("start", "09:20"))
        end_str = trade_window.get("end", market_hours.get("end", "15:15"))
        return self._parse_hhmm(start_str, time(9, 20)), self._parse_hhmm(end_str, time(15, 15))

    def _parse_hhmm(self, value: Any, default: time) -> time:
        try:
            return datetime.strptime(str(value), "%H:%M").time()
        except Exception:
            return default

    def _round_to_tick(self, x: float, tick: float, mode: str) -> float:
        x = float(x)
        tick = float(tick)
        if tick <= 0:
            return x
        n = x / tick
        if mode == "FLOOR":
            return math.floor(n) * tick
        if mode == "CEIL":
            return math.ceil(n) * tick
        return round(n) * tick

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

    def _strategy_params(self) -> Dict[str, Any]:
        if isinstance(self.params, dict):
            sp = self.params.get("strategy-parameters") or self.params.get("strategy_parameters") or {}
            if isinstance(sp, dict):
                return sp
        return {}

    def _get_params_from_yaml(self) -> Dict[str, Any]:
        default_params = {
            "strategy-parameters": {
                "ema_length": 9,
                "slope_window": 3,
                "call_ema_angle_10m_threshold": 20.0,
                "call_ema_angle_5m_threshold": 30.0,
                "call_ema_angle_1m_threshold": 45.0,
                "put_ema_angle_10m_threshold": -20.0,
                "put_ema_angle_5m_threshold": -30.0,
                "put_ema_angle_1m_threshold": -45.0,
                "enable_trading_engine": True,
                "trade-window": {
                    "start": "09:20",
                    "end": "15:15",
                },
            }
        }

        param_path = constants.PARAM_PATH
        if not os.path.exists(param_path):
            logger.warning(f"Parameter file not found at {param_path}; using Timeseries Trend defaults.")
            return default_params

        with open(param_path, "r", encoding="utf-8") as file:
            params = yaml.safe_load(file) or {}

        if not isinstance(params, dict):
            logger.warning(f"Invalid parameter file at {param_path}; using Timeseries Trend defaults.")
            return default_params

        return params
