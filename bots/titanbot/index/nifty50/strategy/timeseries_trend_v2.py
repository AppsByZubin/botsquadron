import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

import common.constants as constants
import logger as logger_module
from index.nifty50.strategy.timeseries_trend import TimeseriesTrendStrategy, ist
from utils.generic_utils import safe_float


logger = logger_module.create_logger("TimeseriesTrendV2StrategyLogger")


class TimeseriesTrendV2Strategy(TimeseriesTrendStrategy):
    """Timeseries trend v2 with 21 EMA / 50 SMA trail and reversal logic."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ensure_v2_columns()
        self._ensure_v2_order_container()

    def _ensure_v2_columns(self) -> None:
        if self.df_index is None:
            return
        for column in (
            "ema_21_1m",
            "sma_50_1m",
            "angle_ema_21_1m",
            "angle_sma_50_1m",
        ):
            if column not in self.df_index.columns:
                self.df_index[column] = np.nan

    def _ensure_v2_order_container(self) -> None:
        self._order_container.setdefault("option_atr", None)
        self._order_container.setdefault("enable_longer_trail", False)
        self._order_container.setdefault("skip_order_counter_increment", False)
        self._order_container.setdefault("reversal_reason", None)
        self._order_container.setdefault("reversal_from_side", None)

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
                if not self._reverse_longer_trail_trade_if_needed():
                    self._trading_engine_active()

            self._trade_processing(feed_response)

        except Exception as exc:
            logger.error(f"An error occurred in obj {obj} on_ws_message: {exc}")
            sys.exit(constants.FAIL_CODE)

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
                "ema_21_1m": pd.Series(dtype="float64"),
                "sma_50_1m": pd.Series(dtype="float64"),
                "angle_ema_1m": pd.Series(dtype="float64"),
                "angle_ema_21_1m": pd.Series(dtype="float64"),
                "angle_sma_50_1m": pd.Series(dtype="float64"),
                "ema_5m": pd.Series(dtype="float64"),
                "angle_ema_5m": pd.Series(dtype="float64"),
                "ema_10m": pd.Series(dtype="float64"),
                "angle_ema_10m": pd.Series(dtype="float64"),
                "ema_9": pd.Series(dtype="float64"),
                "angle_ema_9": pd.Series(dtype="float64"),
                "signal": pd.Series(dtype="object"),
            }
        )

    def _apply_indicators(self):
        if self.df_index.empty:
            return

        frame = self.df_index
        close = pd.to_numeric(frame["close"], errors="coerce").astype("float64")
        high = pd.to_numeric(frame["high"], errors="coerce").astype("float64")
        low = pd.to_numeric(frame["low"], errors="coerce").astype("float64")

        ema_1m = self._ema(close)
        ema_21_1m = close.ewm(span=21, adjust=False, min_periods=21).mean()
        sma_50_1m = close.rolling(window=50, min_periods=50).mean()
        angle_1m = self._ema_angle(ema_1m)
        angle_ema_21_1m = self._ema_angle(ema_21_1m)
        angle_sma_50_1m = self._ema_angle(sma_50_1m)

        self.df_index["candle_length"] = (high - low).to_numpy()
        self.df_index["ema_1m"] = ema_1m.to_numpy()
        self.df_index["ema_21_1m"] = ema_21_1m.to_numpy()
        self.df_index["sma_50_1m"] = sma_50_1m.to_numpy()
        self.df_index["angle_ema_1m"] = angle_1m.to_numpy()
        self.df_index["angle_ema_21_1m"] = angle_ema_21_1m.to_numpy()
        self.df_index["angle_sma_50_1m"] = angle_sma_50_1m.to_numpy()

        self.df_index_5m = self._build_timeframe_frame(frame, 5)
        self.df_index_10m = self._build_timeframe_frame(frame, 10)
        self._map_timeframe_values(5, self.df_index_5m)
        self._map_timeframe_values(10, self.df_index_10m)

        self.df_index["ema_9"] = ema_1m.to_numpy()
        self.df_index["angle_ema_9"] = angle_1m.to_numpy()
        self._last_indicator_row = len(self.df_index) - 1
        self._indicator_revision += 1

    def _punch_waiting_order(self, feed_response):
        if self.last_index_bar is None:
            logger.debug("Waiting order not punched; last_index_bar is unavailable.")
            return

        sp = self._strategy_params()
        side = self._order_container.get("side")
        logger.info(f"Need to find {side} side contracts")
        ref_ts = self._resolve_reference_ts()
        is_reversal_entry = self._coerce_bool(
            self._order_container.get("skip_order_counter_increment"),
            False,
        )
        if self._is_daily_loss_limit_active(ref_ts):
            logger.info("Skipping order placement: daily loss guard active. No more new trades for today.")
            self._reset_order_container()
            return
        if not is_reversal_entry and self._is_post_exit_cooldown_active(ref_ts):
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
        self._order_container["option_atr"] = option_atr

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
        reversal_reason = str(self._order_container.get("reversal_reason") or "").strip()
        if is_reversal_entry and reversal_reason:
            description = (
                f"REVERSAL {side} {self._order_container['instrument_symbol']} "
                f"entry={entry_price:.2f}; reason={reversal_reason}"
            )
        else:
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
            if is_reversal_entry:
                logger.info(f"Order counter unchanged for reversal trade. order_counter={self._order_counter}")
            else:
                self._order_counter += 1
            logger.info(f"{self._order_container}")
        else:
            logger.warning(
                f"Order manager did not return trade_id; clearing waiting intent for "
                f"instrument_key={instrument_key}, symbol={self._order_container.get('instrument_symbol')}."
            )
            self._clear_waiting_order_intent()

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
        self._enable_longer_trail_if_needed(latest_ltp=latest_ltp, ts=ts)
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
        if self._should_force_longer_trail(latest_ltp):
            return True
        if self._coerce_bool(self._strategy_params().get("force_trail_stale_losing_order"), False):
            return self._is_stale_losing_open_order(latest_ltp, ts)
        return False

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
            atr_target_mult = self._coerce_float(sp.get("atr_target_mult"), 40.0, minimum=0.0)
            atr_sl_mult = self._coerce_float(sp.get("atr_sl_mult"), 2.0, minimum=0.0)
            target = entry_price + (atr_target_mult * option_atr)
            sl_trigger = entry_price - (atr_sl_mult * option_atr)
            start_trail_after = self._set_start_trail_after(
                entry_price,
                option_atr,
                default_atr_mult=2.0,
            )
            return target, sl_trigger, option_atr, start_trail_after, "atr"

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

    def _set_start_trail_after(
        self,
        entry_price: float,
        option_atr: float,
        default_atr_mult: float,
    ) -> float:
        if entry_price <= 0:
            return 0.0

        if self._coerce_bool(self._order_container.get("enable_longer_trail"), False) or self._longer_trail_condition_active():
            atr_mult = 10.0
            self._order_container["enable_longer_trail"] = True
        else:
            if self._order_container.get("status") != constants.OPEN:
                self._order_container["enable_longer_trail"] = False
            atr_mult = self._vwap_session_trail_atr_mult()
            if atr_mult is None:
                atr_mult = float(default_atr_mult)

        start_trail_after = float((float(option_atr) * atr_mult) / float(entry_price))
        self._order_container["start_trail_after"] = start_trail_after
        return start_trail_after

    def _longer_trail_condition_active(self) -> bool:
        latest = self._latest_index_indicator_row()
        if latest is None:
            return False

        angle_ema_21_1m = safe_float(latest.get("angle_ema_21_1m"))
        angle_sma_50_1m = safe_float(latest.get("angle_sma_50_1m"))
        if angle_ema_21_1m is None or angle_sma_50_1m is None:
            return False

        return abs(angle_ema_21_1m) > 45.0 and abs(angle_sma_50_1m) > 30.0

    def _latest_index_indicator_row(self):
        if self.df_index is None or self.df_index.empty:
            return None
        return self.df_index.iloc[-1]

    def _enable_longer_trail_if_needed(self, latest_ltp: float, ts: Optional[datetime]) -> None:
        if self._coerce_bool(self._order_container.get("enable_longer_trail"), False):
            return
        if not self._longer_trail_condition_active():
            return

        entry_price = safe_float(self._order_container.get("entry_price"))
        if entry_price is None or entry_price <= 0:
            return

        instrument_key = self._order_container.get("instrument_key")
        option_atr = self.atr5_engine.get_atr(instrument_key) if instrument_key is not None else None
        option_atr = safe_float(option_atr) or safe_float(self._order_container.get("option_atr"))
        if option_atr is None or option_atr <= 0:
            logger.debug("Longer trail condition active, but option ATR is unavailable; keeping current trail settings.")
            return

        start_trail_after = self._set_start_trail_after(
            entry_price=float(entry_price),
            option_atr=float(option_atr),
            default_atr_mult=2.0,
        )
        self._order_container["option_atr"] = float(option_atr)
        self._update_open_trade_trail_anchor(
            latest_ltp=float(latest_ltp),
            start_trail_after=start_trail_after,
            ts=ts,
        )

    def _update_open_trade_trail_anchor(
        self,
        latest_ltp: float,
        start_trail_after: float,
        ts: Optional[datetime],
    ) -> None:
        if self.order_maneger is None:
            return

        trade_id = self._order_container.get("trade_id")
        if not trade_id:
            return

        trade = self.order_maneger.get_trade_by_id(trade_id)
        if not trade:
            return

        manager_side = str(trade.get("side", "")).upper()
        anchor_gap = float(latest_ltp) * float(start_trail_after)
        next_anchor = float(latest_ltp) - anchor_gap if manager_side == "SELL" else float(latest_ltp) + anchor_gap

        trade["start_trail_after"] = float(start_trail_after)
        trade["spot_ltp"] = float(latest_ltp)
        trade["_spot_trail_anchor"] = float(next_anchor)

        if hasattr(self.order_maneger, "_upsert_trade_row"):
            self.order_maneger._upsert_trade_row(trade)
        if hasattr(self.order_maneger, "_replace_trade_in_memory"):
            self.order_maneger._replace_trade_in_memory(trade)

        logger.info(
            f"Longer trail enabled for trade {trade_id}; start_trail_after={start_trail_after}, "
            f"anchor_points={next_anchor}, ts={ts}"
        )

    def _should_force_longer_trail(self, latest_ltp: Optional[float]) -> bool:
        if latest_ltp is None:
            return False
        if not self._coerce_bool(self._order_container.get("enable_longer_trail"), False):
            return False

        latest = self._latest_index_indicator_row()
        if latest is None:
            return False

        ema_21_1m = safe_float(latest.get("ema_21_1m"))
        sma_50_1m = safe_float(latest.get("sma_50_1m"))
        if ema_21_1m is None or sma_50_1m is None:
            return False

        side = self._order_container.get("side")
        if side == constants.CALL and ema_21_1m < sma_50_1m:
            logger.info(
                f"Longer trail force condition met for CALL; ema_21_1m={ema_21_1m}, "
                f"sma_50_1m={sma_50_1m}, contract_ltp={latest_ltp}"
            )
            return True
        if side == constants.PUT and sma_50_1m < ema_21_1m:
            logger.info(
                f"Longer trail force condition met for PUT; ema_21_1m={ema_21_1m}, "
                f"sma_50_1m={sma_50_1m}, contract_ltp={latest_ltp}"
            )
            return True

        return False

    def _reverse_longer_trail_trade_if_needed(self) -> bool:
        if self.order_maneger is None or self._order_container.get("status") != constants.OPEN:
            return False
        if not self._coerce_bool(self._order_container.get("enable_longer_trail"), False) and not self._longer_trail_condition_active():
            return False

        latest = self._latest_index_indicator_row()
        if latest is None:
            return False

        latest_time = str(latest.get("time") or "")
        if not self._is_trading_window(latest_time):
            return False

        side = self._order_container.get("side")
        angle_ema_21_1m = safe_float(latest.get("angle_ema_21_1m"))
        angle_sma_50_1m = safe_float(latest.get("angle_sma_50_1m"))
        if angle_ema_21_1m is None or angle_sma_50_1m is None:
            return False

        reverse_side = None
        if side == constants.CALL and angle_ema_21_1m < -45.0 and angle_sma_50_1m < -30.0:
            reverse_side = constants.PUT
        elif side == constants.PUT and angle_ema_21_1m > 45.0 and angle_sma_50_1m > 30.0:
            reverse_side = constants.CALL

        if reverse_side is None:
            return False

        lot = self._calculate_lot_size(reverse_side)
        if lot <= 0:
            logger.info(f"Skipping reversal to {reverse_side}; calculated lot size is zero.")
            return False

        latest_idx = latest.name
        close_price = safe_float(latest.get("close"))
        ema_10m = safe_float(latest.get("ema_10m"))
        ema_5m = safe_float(latest.get("ema_5m"))
        ema_1m = safe_float(latest.get("ema_1m"))
        angle_10m = safe_float(latest.get("angle_ema_10m"))
        angle_5m = safe_float(latest.get("angle_ema_5m"))
        angle_1m = safe_float(latest.get("angle_ema_1m"))
        if any(value is None for value in (close_price, angle_10m, angle_5m, angle_1m)):
            return False

        trade_id = self._order_container.get("trade_id")
        latest_ltp = safe_float(self._order_container.get("ltp"))
        if not trade_id or latest_ltp is None:
            return False

        ref_ts = self._resolve_reference_ts()
        reversal_reason = (
            f"{side} closed for longer-trail reversal because "
            f"angle_ema_21_1m={angle_ema_21_1m:.2f}, "
            f"angle_sma_50_1m={angle_sma_50_1m:.2f}; "
            f"new {reverse_side} order intent set"
        )
        logger.info(
            f"Longer-trail reversal condition met. side={side}, reverse_side={reverse_side}, "
            f"angle_ema_21_1m={angle_ema_21_1m}, angle_sma_50_1m={angle_sma_50_1m}, "
            f"exit_ltp={latest_ltp}, candle_time={latest_time}"
        )
        trade_closed = self.order_maneger.square_off_trade(
            trade_id=trade_id,
            exit_price=latest_ltp,
            ts=ref_ts,
            reason=constants.MANUAL_EXIT,
        )
        if not trade_closed:
            return False

        trade_info = self.order_maneger.get_trade_by_id(trade_id)
        self._update_today_realized_pnl_on_trade_close(trade_info, ts=ref_ts)
        self._log_reversal_event(
            "REVERSAL_CLOSE_REASON",
            trade_info,
            ref_ts,
            {
                "close_reason": constants.MANUAL_EXIT,
                "reversal_reason": reversal_reason,
                "closed_side": side,
                "new_order_side": reverse_side,
                "angle_ema_21_1m": angle_ema_21_1m,
                "angle_sma_50_1m": angle_sma_50_1m,
                "exit_ltp": latest_ltp,
                "candle_time": latest_time,
            },
        )
        self._reset_order_container()
        self._set_waiting_order(
            latest_idx=latest_idx,
            latest_time=latest_time,
            side=reverse_side,
            close_price=close_price,
            ema_10m=ema_10m,
            ema_5m=ema_5m,
            ema_1m=ema_1m,
            angle_10m=angle_10m,
            angle_5m=angle_5m,
            angle_1m=angle_1m,
        )
        self._order_container["lot"] = int(lot)
        self._order_container["enable_longer_trail"] = True
        self._order_container["skip_order_counter_increment"] = True
        self._order_container["reversal_reason"] = reversal_reason
        self._order_container["reversal_from_side"] = side
        self._log_reversal_event(
            "REVERSAL_ORDER_INTENT",
            {
                "id": None,
                "symbol": None,
                "instrument_token": None,
                "side": reverse_side,
                "qty": None,
                "status": constants.WAITING,
                "description": reversal_reason,
            },
            ref_ts,
            {
                "reversal_reason": reversal_reason,
                "closed_trade_id": trade_id,
                "closed_side": side,
                "new_order_side": reverse_side,
                "order_counter": self._order_counter,
                "enable_longer_trail": True,
                "angle_ema_21_1m": angle_ema_21_1m,
                "angle_sma_50_1m": angle_sma_50_1m,
                "candle_time": latest_time,
            },
        )
        logger.info(
            f"Reversal order intent set without changing order counter. "
            f"side={reverse_side}, order_counter={self._order_counter}"
        )
        return True

    def _log_reversal_event(
        self,
        event_type: str,
        trade: Optional[Dict[str, Any]],
        ts: datetime,
        extra: Dict[str, Any],
    ) -> None:
        if self.order_maneger is None or not hasattr(self.order_maneger, "_log_event"):
            return
        if not isinstance(trade, dict):
            trade = {}
        try:
            self.order_maneger._log_event(event_type, trade, ts=ts, extra=extra)
        except Exception as exc:
            logger.error(f"Failed to log {event_type}: {exc}")

    def _clear_pending_contract(self):
        self._order_container["instrument_key"] = None
        self._order_container["ltp"] = None
        self._order_container["max_gamma"] = None
        self._order_container["instrument_symbol"] = None
        self._order_container["option_atr"] = None
        self._order_container["start_trail_after"] = None
        if not self._coerce_bool(self._order_container.get("skip_order_counter_increment"), False):
            self._order_container["enable_longer_trail"] = False

    def _clear_waiting_order_intent(self):
        self._reset_order_container()

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
            "option_atr": None,
            "start_trail_after": None,
            "trade_create_time": None,
            "force_trail_lock": False,
            "enable_longer_trail": False,
            "skip_order_counter_increment": False,
            "reversal_reason": None,
            "reversal_from_side": None,
        }

    def _trading_engine_active(self):
        try:
            latest = self.df_index.iloc[-1] if not self.df_index.empty else None
            latest_idx = self.df_index.index[-1] if latest is not None else None
            latest_time = str(latest.get("time")) if latest is not None else None
            current_minute = self.curr_index_minute or latest_time
            if not current_minute:
                return

            current_window_open = self._is_trading_window(current_minute)
            self._set_trading_window_state(current_minute, current_window_open)

            if not self.enable_trading_engine:
                return

            current_revision = self._indicator_revision
            if self._last_engine_revision == current_revision:
                return

            if len(self.df_index) < max(self.ema_length + self.slope_window, 2):
                self._last_engine_revision = current_revision
                return

            latest_ts = pd.to_datetime(latest_time, errors="coerce")
            if pd.isna(latest_ts) or not (self.trade_start <= latest_ts.time() <= self.trade_end):
                self._last_engine_revision = current_revision
                return

            ref_ts = self._resolve_reference_ts()
            if self._is_post_exit_cooldown_active(ref_ts):
                cooldown_left_sec = int(max((self._post_exit_cooldown_until - ref_ts).total_seconds(), 0))
                logger.debug(
                    f"candle_time={latest_time}, Entry blocked by post-exit cooldown for {cooldown_left_sec}s "
                    f"(until {self._post_exit_cooldown_until.strftime('%H:%M:%S')})"
                )
                self._last_engine_revision = current_revision
                return

            if self._is_daily_loss_limit_active(ref_ts):
                self._last_engine_revision = current_revision
                return

            close_price = safe_float(latest.get("close"))
            angle_10m = safe_float(latest.get("angle_ema_10m"))
            angle_5m = safe_float(latest.get("angle_ema_5m"))
            angle_1m = safe_float(latest.get("angle_ema_1m"))
            ema_10m = safe_float(latest.get("ema_10m"))
            ema_5m = safe_float(latest.get("ema_5m"))
            ema_1m = safe_float(latest.get("ema_1m"))
            ema_21_1m = safe_float(latest.get("ema_21_1m"))
            sma_50_1m = safe_float(latest.get("sma_50_1m"))
            angle_ema_21_1m = safe_float(latest.get("angle_ema_21_1m"))
            angle_sma_50_1m = safe_float(latest.get("angle_sma_50_1m"))
            if any(value is None for value in (close_price, angle_10m, angle_5m, angle_1m)):
                self._last_engine_revision = current_revision
                return

            call_setup = (
                angle_10m > self.call_angle_10m
                and angle_5m > self.call_angle_5m
                and angle_1m > self.call_angle_1m
                and angle_ema_21_1m is not None
                and angle_ema_21_1m > self.call_angle_1m
            )
            put_setup = (
                angle_10m < self.put_angle_10m
                and angle_5m < self.put_angle_5m
                and angle_1m < self.put_angle_1m
                and angle_ema_21_1m is not None
                and angle_ema_21_1m < self.put_angle_1m
            )

            logger.debug(
                f"candle_time={latest_time}, close={close_price}, "
                f"ema_10m={ema_10m}, angle_10m={angle_10m}, threshold_call_10m={self.call_angle_10m}, threshold_put_10m={self.put_angle_10m}, "
                f"ema_5m={ema_5m}, angle_5m={angle_5m}, threshold_call_5m={self.call_angle_5m}, threshold_put_5m={self.put_angle_5m}, "
                f"ema_1m={ema_1m}, ema_21_1m={ema_21_1m}, sma_50_1m={sma_50_1m}, "
                f"angle_ema_21_1m={angle_ema_21_1m}, angle_sma_50_1m={angle_sma_50_1m}, "
                f"angle_1m={angle_1m}, threshold_call_1m={self.call_angle_1m}, threshold_put_1m={self.put_angle_1m}"
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
