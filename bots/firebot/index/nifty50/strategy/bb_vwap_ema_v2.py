import sys
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

import common.constants as constants
import logger as logger_module
from index.nifty50.strategy.bb_vwap_ema import BbVwapEmaStrategy, ist
from utils.generic_utils import safe_float


logger = logger_module.create_logger("BbVwapEmaV2StrategyLogger")

LONG_TRAIL_BELOW_ENTRY_SL_GAP = 2.0


class BbVwapEmaV2Strategy(BbVwapEmaStrategy):
    """BB/VWAP/EMA v2 with 21 EMA / 50 SMA longer trail and reversal logic."""

    def __init__(self, *args, **kwargs):
        self.last_signal = constants.WAITING
        self.signals = []
        super().__init__(*args, **kwargs)
        self.last_signal = getattr(self, "last_signal", constants.WAITING)
        self.signals = getattr(self, "signals", [])
        sp = self._strategy_params()
        self._max_daily_loss_pct_of_initial_cash = self._coerce_float(
            sp.get("max_daily_loss_pct_of_initial_cash"),
            0.08,
            minimum=0.0,
        )
        self.call_ema_21_angle_1m = self._coerce_float(
            sp.get("call_ema_21_angle_1m_threshold"),
            30.0,
        )
        self.put_ema_21_angle_1m = self._coerce_float(
            sp.get("put_ema_21_angle_1m_threshold"),
            -30.0,
        )
        self.slope_window = int(getattr(self, "_slope_window", 3) or 3)
        self._ensure_v2_columns()
        self._ensure_v2_order_container()

    def _strategy_params(self) -> Dict[str, Any]:
        if isinstance(self.params, dict):
            sp = self.params.get("strategy-parameters") or self.params.get("strategy_parameters") or {}
            if isinstance(sp, dict):
                return sp
        return {}

    def _entry_band_boundaries(self, bb_upper: float, bb_lower: float) -> tuple[float, float, float]:
        sp = self._strategy_params()
        params = self.params if isinstance(self.params, dict) else {}
        tick_size = self._coerce_float(
            sp.get(
                "bb_entry_tick_size",
                sp.get(
                    "bb-entry-tick-size",
                    params.get("bb_entry_tick_size", params.get("bb-entry-tick-size")),
                ),
            ),
            0.05,
            minimum=0.01,
        )
        return (
            self._round_entry_band_to_tick(bb_upper, tick_size, "FLOOR"),
            self._round_entry_band_to_tick(bb_lower, tick_size, "CEIL"),
            tick_size,
        )

    @staticmethod
    def _round_entry_band_to_tick(value: float, tick_size: float, mode: str) -> float:
        price = Decimal(str(value))
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
        self._order_container.setdefault("entry_price", None)
        self._order_container.setdefault("option_atr", None)
        self._order_container.setdefault("trade_create_time", None)
        self._order_container.setdefault("force_trail_lock", False)
        self._order_container.setdefault("enable_longer_trail", False)
        self._order_container.setdefault("skip_order_counter_increment", False)
        self._order_container.setdefault("reversal_reason", None)
        self._order_container.setdefault("reversal_from_side", None)

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
        entry_price = safe_float(trade.get("entry_price")) or safe_float((contract or {}).get("ltp"))

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
                "option_atr": None,
                "start_trail_after": safe_float(trade.get("start_trail_after")),
                "trade_create_time": trade.get("timestamp") or trade.get("entry_time"),
                "force_trail_lock": False,
                "enable_longer_trail": False,
                "skip_order_counter_increment": False,
                "reversal_reason": None,
                "reversal_from_side": None,
            }
        )
        logger.info(f"Restored OPEN trade from ordersystem into _order_container: {self._order_container}")
        return True

    def _apply_indicators(self):
        super()._apply_indicators()
        if self.df_index.empty:
            return

        self._ensure_v2_columns()
        close = pd.to_numeric(self.df_index["close"], errors="coerce").astype("float64")
        ema_21_1m = close.ewm(span=21, adjust=False, min_periods=21).mean()
        sma_50_1m = close.rolling(window=50, min_periods=50).mean()
        self.df_index["ema_21_1m"] = ema_21_1m.to_numpy()
        self.df_index["sma_50_1m"] = sma_50_1m.to_numpy()
        self.df_index["angle_ema_21_1m"] = self._angle_from_series(ema_21_1m).to_numpy()
        self.df_index["angle_sma_50_1m"] = self._angle_from_series(sma_50_1m).to_numpy()

    def _angle_from_series(self, series: pd.Series) -> pd.Series:
        window = max(int(getattr(self, "slope_window", getattr(self, "_slope_window", 3)) or 3), 1)
        slope = (series - series.shift(window)) / float(window)
        angle = np.degrees(np.arctan(np.clip(slope.to_numpy(dtype="float64"), -10.0, 10.0)))
        return pd.Series(angle, index=series.index, dtype="float64")

    def _apply_indicators_and_engine(self) -> None:
        self._apply_indicators()
        if not self.curr_index_minute:
            return

        if self._is_trading_window(self.curr_index_minute):
            if not self._reverse_longer_trail_trade_if_needed():
                self._trading_engine_active()
        else:
            logger.info(f"Outside Trading Window at {self.curr_index_minute}")
            self._square_off_open_trade_if_eod()

    def _trade_processing(self, feed_response):
        if not feed_response or self.order_maneger is None:
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
            self._square_off_open_trade_if_eod(feed_response)

    def _punch_waiting_order(self, feed_response):
        if self.last_index_bar is None:
            return

        sp = self._strategy_params()
        side = self._order_container.get("side")
        logger.info(f"Need to find {side} side contracts")
        if self._is_daily_loss_limit_active():
            logger.info("Skipping order placement: daily loss guard active. No more new trades for today.")
            self._reset_order_container()
            return

        index_close = safe_float(self.last_index_bar.get("close"))
        if index_close is None or index_close <= 0:
            return

        itm_range = self._coerce_float(
            sp.get("itm_strike_range", self.params.get("itm_strike_range", 200) if isinstance(self.params, dict) else 200),
            200.0,
            minimum=0.0,
        )
        dict_itm = self._get_itm_contracts(side, index_close, itm_range)
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
        self._order_container["instrument_key"] = instrument_key
        self._order_container["instrument_symbol"] = contract.get("trading_symbol")
        self._order_container["ltp"] = ltp
        self._order_container["max_gamma"] = safe_float(chosen.get("gamma"))

        lot = self._coerce_int(self._order_container.get("lot"), 1, minimum=1)
        lot_size = self._coerce_int(contract.get("lot_size"), 1, minimum=1)
        qty = lot * lot_size

        tick = self._coerce_float(
            sp.get("tick-size", sp.get("tick_size", self.params.get("tick-size", self.params.get("tick_size", 0.05)) if isinstance(self.params, dict) else 0.05)),
            0.05,
            minimum=0.01,
        )
        entry_price = float(ltp)
        use_option_atr_risk = self._coerce_bool(
            sp.get("use_option_atr_risk", self.params.get("use_option_atr_risk", True) if isinstance(self.params, dict) else True),
            True,
        )
        require_option_atr = self._coerce_bool(
            sp.get("require_option_atr", self.params.get("require_option_atr", False) if isinstance(self.params, dict) else False),
            False,
        )
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
            self._clear_pending_contract()
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
        is_reversal_entry = self._coerce_bool(self._order_container.get("skip_order_counter_increment"), False)
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
            logger.warning("Order manager did not return trade_id; clearing complete waiting intent.")
            self._reset_order_container()

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
        longer_trail_crossover = self._should_force_longer_trail(latest_ltp)
        force_trail = self._should_force_trail_open_order(
            latest_ltp,
            ts,
            longer_trail_crossover=longer_trail_crossover,
        )
        force_trail_stoploss = None
        entry_price = safe_float(self._order_container.get("entry_price"))
        if (
            longer_trail_crossover
            and entry_price is not None
            and latest_ltp < entry_price
        ):
            force_trail_stoploss = latest_ltp - LONG_TRAIL_BELOW_ENTRY_SL_GAP
        tick_result = self.order_maneger.on_tick(
            symbol=self._order_container["instrument_symbol"],
            o=latest_ltp,
            h=latest_ltp,
            l=latest_ltp,
            c=latest_ltp,
            ts=ts,
            force_trail=force_trail,
            force_trail_stoploss=force_trail_stoploss,
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
        if self._is_closed_trade_info(trade_info):
            logger.debug(f"Trade closed Info: {trade_info}")
            self._set_post_exit_cooldown(trade_info.get("status"), ts=ts)
            self._update_today_realized_pnl_on_trade_close(trade_info, ts=ts)
            self._reset_order_container()

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
            atr_target_mult = self._coerce_float(sp.get("atr_target_mult"), 40.0, minimum=0.0)
            atr_sl_mult = self._coerce_float(sp.get("atr_sl_mult"), 5.0, minimum=0.0)
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

        longer_trail_was_enabled = self._coerce_bool(
            self._order_container.get("enable_longer_trail"),
            False,
        )
        if longer_trail_was_enabled or self._longer_trail_condition_active():
            atr_mult = 3.0
            self._order_container["enable_longer_trail"] = True
        else:
            if self._order_container.get("status") != constants.OPEN:
                self._order_container["enable_longer_trail"] = False
            atr_mult = self._vwap_session_trail_atr_mult()
            if atr_mult is None:
                atr_mult = float(default_atr_mult)

        start_trail_after = float((float(option_atr) * atr_mult) / float(entry_price))
        self._order_container["start_trail_after"] = start_trail_after

        if not longer_trail_was_enabled and self._order_container["enable_longer_trail"]:
            source = "open_trade" if self._order_container.get("status") == constants.OPEN else "order_entry"
            self._log_longer_trail_enabled(
                source=source,
                entry_price=entry_price,
                option_atr=option_atr,
                start_trail_after=start_trail_after,
            )

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

    def _log_longer_trail_enabled(
        self,
        source: str,
        entry_price: Optional[float] = None,
        option_atr: Optional[float] = None,
        start_trail_after: Optional[float] = None,
    ) -> None:
        latest = self._latest_index_indicator_row()
        angle_ema_21_1m = safe_float(latest.get("angle_ema_21_1m")) if latest is not None else None
        angle_sma_50_1m = safe_float(latest.get("angle_sma_50_1m")) if latest is not None else None
        logger.info(
            f"Long trail enabled; source={source}, trade_id={self._order_container.get('trade_id')}, "
            f"side={self._order_container.get('side')}, entry_price={entry_price}, option_atr={option_atr}, "
            f"start_trail_after={start_trail_after}, angle_ema_21_1m={angle_ema_21_1m}, "
            f"angle_sma_50_1m={angle_sma_50_1m}"
        )
        self._log_order_event(
            "LONG_TRAIL_ENABLED",
            self._current_order_event_trade(),
            self._resolve_reference_ts(),
            {
                "source": source,
                "enable_longer_trail": True,
                "entry_price": entry_price,
                "option_atr": option_atr,
                "start_trail_after": start_trail_after,
                "angle_ema_21_1m": angle_ema_21_1m,
                "angle_sma_50_1m": angle_sma_50_1m,
            },
        )

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
        latest = self._latest_index_indicator_row()
        angle_ema_21_1m = safe_float(latest.get("angle_ema_21_1m")) if latest is not None else None
        angle_sma_50_1m = safe_float(latest.get("angle_sma_50_1m")) if latest is not None else None
        ema_21_1m = safe_float(latest.get("ema_21_1m")) if latest is not None else None
        sma_50_1m = safe_float(latest.get("sma_50_1m")) if latest is not None else None
        candle_time = str(latest.get("time") or "") if latest is not None else ""

        trade["start_trail_after"] = float(start_trail_after)
        trade["spot_ltp"] = float(latest_ltp)
        trade["_spot_trail_anchor"] = float(next_anchor)

        updates = {
            "start_trail_after": float(start_trail_after),
            "spot_ltp": float(latest_ltp),
            "_spot_trail_anchor": float(next_anchor),
            "spot_trail_anchor": float(next_anchor),
        }
        if hasattr(self.order_maneger, "_patch_cached_trade"):
            self.order_maneger._patch_cached_trade(trade_id, updates)
        if hasattr(self.order_maneger, "_patch_local_trade"):
            self.order_maneger._patch_local_trade(trade_id, updates)

        logger.info(
            f"Longer trail enabled trade_id={trade_id}, symbol={trade.get('symbol')}, side={self._order_container.get('side')}, "
            f"ltp={latest_ltp:.2f}, entry={safe_float(trade.get('entry_price'))}, option_atr={self._order_container.get('option_atr')}, "
            f"start_trail_after={start_trail_after:.6f}, anchor_gap={anchor_gap:.2f}, next_anchor={next_anchor:.2f}, "
            f"angle_ema_21_1m={angle_ema_21_1m}, angle_sma_50_1m={angle_sma_50_1m}, ts={ts}"
        )
        self._log_order_event(
            "LONG_TRAIL_ANCHOR_UPDATED",
            trade,
            ts=ts,
            extra={
                "enable_longer_trail": True,
                "latest_ltp": float(latest_ltp),
                "entry_price_at_enable": safe_float(trade.get("entry_price")),
                "option_atr": safe_float(self._order_container.get("option_atr")),
                "start_trail_after": float(start_trail_after),
                "anchor_gap": float(anchor_gap),
                "next_trail_anchor": float(next_anchor),
                "spot_trail_anchor": float(next_anchor),
                "ema_21_1m": ema_21_1m,
                "sma_50_1m": sma_50_1m,
                "angle_ema_21_1m": angle_ema_21_1m,
                "angle_sma_50_1m": angle_sma_50_1m,
                "candle_time": candle_time,
                "condition": "abs(angle_ema_21_1m)>45 and abs(angle_sma_50_1m)>30",
            },
        )

    def _should_force_trail_open_order(
        self,
        latest_ltp: Optional[float] = None,
        ts: Optional[datetime] = None,
        longer_trail_crossover: Optional[bool] = None,
    ) -> bool:
        del ts
        if self._order_container.get("status") != constants.OPEN:
            return False
        if self._coerce_bool(self._order_container.get("force_trail_lock"), False):
            return False
        if longer_trail_crossover is None:
            longer_trail_crossover = self._should_force_longer_trail(latest_ltp)
        if longer_trail_crossover:
            return True
        return self._last_two_closes_beyond_bb_basis()

    def _last_two_closes_beyond_bb_basis(self) -> bool:
        if self.df_index is None or len(self.df_index) < 2:
            return False

        side = self._order_container.get("side")
        if side not in {constants.CALL, constants.PUT}:
            return False

        recent = self.df_index.tail(2)
        for _, row in recent.iterrows():
            close_price = safe_float(row.get("close"))
            bb_basis = safe_float(row.get("bb_basis"))
            if (
                close_price is None
                or bb_basis is None
                or not np.isfinite(close_price)
                or not np.isfinite(bb_basis)
            ):
                return False
            if side == constants.CALL and close_price >= bb_basis:
                return False
            if side == constants.PUT and close_price <= bb_basis:
                return False
        return True

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
        high = safe_float(latest.get("high"))
        low = safe_float(latest.get("low"))
        bb_upper = safe_float(latest.get("bb_upper"))
        bb_lower = safe_float(latest.get("bb_lower"))
        candle_length = safe_float(latest.get("candle_length"))
        angle_ema_9 = safe_float(latest.get("angle_ema_9"))
        if candle_length is None and high is not None and low is not None:
            candle_length = high - low
        if any(value is None for value in (close_price, bb_upper, bb_lower, candle_length, angle_ema_9)):
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
        self._log_order_event(
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
        self.df_index.loc[latest_idx, "signal"] = reverse_side
        self.last_signal = reverse_side
        signal_event = {
            "time": latest_time,
            "signal": reverse_side,
            "close": close_price,
            "vwap": safe_float(latest.get("vwap")),
            "ema": safe_float(latest.get("ema_9")),
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "candle_length": candle_length,
            "angle_ema_9": angle_ema_9,
        }
        self.signals.append(signal_event)
        logger.info(f"BB VWAP EMA reversal signal candidate: {signal_event}")

        self._order_container["side"] = reverse_side
        self._order_container["status"] = constants.WAITING
        self._order_container["lot"] = int(lot)
        self._order_container["force_trail_lock"] = False
        self._order_container["enable_longer_trail"] = True
        self._log_longer_trail_enabled(
            source="reversal_order",
            option_atr=safe_float(self._order_container.get("option_atr")),
            start_trail_after=safe_float(self._order_container.get("start_trail_after")),
        )
        self._order_container["skip_order_counter_increment"] = True
        self._order_container["reversal_reason"] = reversal_reason
        self._order_container["reversal_from_side"] = side
        self._log_order_event(
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

    def _trading_engine_active(self):
        try:
            if not self.enable_trading_engine:
                return

            current_revision = self._indicator_revision
            if self._last_engine_revision == current_revision:
                return

            if len(self.df_index) < max(self.bb_length, self.ema_length):
                self._last_engine_revision = current_revision
                return

            latest = self.df_index.iloc[-1]
            latest_idx = self.df_index.index[-1]
            latest_time = str(latest.get("time"))
            if not self._is_trading_window(latest_time):
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

            open_price = safe_float(latest.get("open"))
            close_price = safe_float(latest.get("close"))
            high = safe_float(latest.get("high"))
            low = safe_float(latest.get("low"))
            bb_upper = safe_float(latest.get("bb_upper"))
            bb_lower = safe_float(latest.get("bb_lower"))
            candle_length = safe_float(latest.get("candle_length"))
            angle_ema_9 = safe_float(latest.get("angle_ema_9"))
            ema_21_1m = safe_float(latest.get("ema_21_1m"))
            sma_50_1m = safe_float(latest.get("sma_50_1m"))
            angle_ema_21_1m = safe_float(latest.get("angle_ema_21_1m"))
            angle_sma_50_1m = safe_float(latest.get("angle_sma_50_1m"))
            if candle_length is None and high is not None and low is not None:
                candle_length = high - low

            if (
                open_price is None
                or close_price is None
                or bb_upper is None
                or bb_lower is None
                or candle_length is None
                or angle_ema_9 is None
            ):
                self._last_engine_revision = current_revision
                return

            call_bb_upper, put_bb_lower, entry_tick_size = self._entry_band_boundaries(
                bb_upper,
                bb_lower,
            )

            call_setup = (
                (
                    (open_price >= call_bb_upper and close_price >= call_bb_upper)
                    or (
                        close_price >= call_bb_upper
                        and candle_length >= self.bb_candle_length_threshold
                    )
                )
                and angle_ema_9 > self.call_ema_angle_threshold
                and angle_ema_21_1m is not None
                and angle_ema_21_1m > self.call_ema_21_angle_1m
            )

            put_setup = (
                (
                    (open_price <= put_bb_lower and close_price <= put_bb_lower)
                    or (
                        close_price <= put_bb_lower
                        and candle_length >= self.bb_candle_length_threshold
                    )
                )
                and angle_ema_9 < self.put_ema_angle_threshold
                and angle_ema_21_1m is not None
                and angle_ema_21_1m < self.put_ema_21_angle_1m
            )

            logger.debug(
                f"candle_time={latest_time}, Engine check open={open_price}, close={close_price}, bb_upper={bb_upper}, "
                f"call_bb_upper={call_bb_upper}, bb_lower={bb_lower}, put_bb_lower={put_bb_lower}, "
                f"entry_tick_size={entry_tick_size}, candle_length={candle_length}, angle_ema={angle_ema_9}, "
                f"ema_21_1m={ema_21_1m}, sma_50_1m={sma_50_1m}, "
                f"angle_ema_21_1m={angle_ema_21_1m}, threshold_call_ema_21_1m={self.call_ema_21_angle_1m}, "
                f"threshold_put_ema_21_1m={self.put_ema_21_angle_1m}, angle_sma_50_1m={angle_sma_50_1m}"
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
                lot = self._calculate_lot_size(constants.CALL)
                if lot <= 0:
                    return
                self._set_waiting_signal(
                    latest_idx,
                    latest_time,
                    constants.CALL,
                    close_price,
                    bb_upper,
                    bb_lower,
                    candle_length,
                    angle_ema_9,
                )
                return

            if (
                put_setup
                and self._order_container["status"] is None
                and (self._order_counter < self._max_order_counter)
            ):
                lot = self._calculate_lot_size(constants.PUT)
                if lot <= 0:
                    return
                self._set_waiting_signal(
                    latest_idx,
                    latest_time,
                    constants.PUT,
                    close_price,
                    bb_upper,
                    bb_lower,
                    candle_length,
                    angle_ema_9,
                )
                return

        except Exception as exc:
            logger.error(f"An error occurred in _trading_engine_active: {exc}")
            sys.exit(constants.FAIL_CODE)

    def _set_waiting_signal(
        self,
        latest_idx: int,
        latest_time: str,
        side: str,
        close_price: float,
        bb_upper: float,
        bb_lower: float,
        candle_length: float,
        angle_ema_9: float,
    ) -> None:
        lot = self._calculate_lot_size(side)
        if lot <= 0:
            return

        self.df_index.loc[latest_idx, "signal"] = side
        self.last_signal = side
        signal_event = {
            "time": latest_time,
            "signal": side,
            "close": close_price,
            "vwap": safe_float(self.df_index.loc[latest_idx].get("vwap")),
            "ema": safe_float(self.df_index.loc[latest_idx].get("ema_9")),
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "candle_length": candle_length,
            "angle_ema_9": angle_ema_9,
        }
        self.signals.append(signal_event)
        logger.info(f"BB VWAP EMA signal candidate: {signal_event}")

        self._order_container["side"] = side
        self._order_container["status"] = constants.WAITING
        self._order_container["lot"] = int(lot)
        self._order_container["force_trail_lock"] = False
        self._order_container["enable_longer_trail"] = False
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

    def _vwap_session_trail_atr_mult(self) -> Optional[float]:
        band_width = self._latest_vwap_band_1_width()
        if band_width is None:
            return None
        if band_width < 100:
            return 1.5
        if 100 < band_width < 130:
            return 2.5
        if 150 < band_width < 200:
            return 3.0
        if band_width > 200:
            return 3.5
        return None

    def _clear_pending_contract(self):
        self._order_container["instrument_key"] = None
        self._order_container["ltp"] = None
        self._order_container["max_gamma"] = None
        self._order_container["instrument_symbol"] = None
        self._order_container["option_atr"] = None
        self._order_container["start_trail_after"] = None
        if not self._coerce_bool(self._order_container.get("skip_order_counter_increment"), False):
            self._order_container["enable_longer_trail"] = False

    def _reset_order_container(self) -> None:
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

    def _timestamp_from_item(self, item: Dict[str, Any]) -> Optional[datetime]:
        raw = safe_float(item.get("ts_epoch_ms") or item.get("ltt"))
        if raw is None:
            return None
        try:
            return datetime.fromtimestamp(raw / 1000, tz=ist)
        except Exception:
            return None

    def _square_off_open_trade_if_eod(self, feed_response=None) -> None:
        if self._order_container.get("status") != constants.OPEN:
            return
        if not self.curr_index_minute:
            return

        current_dt = self._parse_datetime_value(self.curr_index_minute)
        if current_dt is None:
            return
        current_time = current_dt.time()
        if current_time < self._trade_end_time:
            return

        latest_ltp = safe_float(self._order_container.get("ltp"))
        if feed_response:
            for item in feed_response:
                if item.get("instrument_key") == self._order_container.get("instrument_key"):
                    latest_ltp = safe_float(item.get("ltp")) or latest_ltp
                    break
        if latest_ltp is None:
            return
        self._square_off_open_trade(constants.EOD_SQUARE_OFF, latest_ltp=latest_ltp)

    def _square_off_open_trade(self, reason: str, latest_ltp: Optional[float] = None) -> bool:
        if self.order_maneger is None or self._order_container.get("status") != constants.OPEN:
            return False
        trade_id = self._order_container.get("trade_id")
        exit_ltp = safe_float(latest_ltp) or safe_float(self._order_container.get("ltp"))
        if not trade_id or exit_ltp is None:
            return False

        square_ts = self._resolve_reference_ts()
        trade_closed = self.order_maneger.square_off_trade(
            trade_id=trade_id,
            exit_price=float(exit_ltp),
            ts=square_ts,
            reason=reason,
        )
        if trade_closed:
            trade_info = self.order_maneger.get_trade_by_id(trade_id)
            self._update_today_realized_pnl_on_trade_close(trade_info, ts=square_ts)
            self._reset_order_container()
            return True
        return False

    def _current_order_event_trade(self) -> Dict[str, Any]:
        trade_id = self._order_container.get("trade_id")
        if self.order_maneger is not None and trade_id and hasattr(self.order_maneger, "get_trade_by_id"):
            trade = self.order_maneger.get_trade_by_id(trade_id)
            if isinstance(trade, dict):
                return trade
        return {
            "id": trade_id,
            "symbol": self._order_container.get("instrument_symbol"),
            "instrument_token": self._order_container.get("instrument_key"),
            "side": self._order_container.get("side"),
            "qty": None,
            "status": self._order_container.get("status"),
            "description": "Long trail enabled by bb_vwap_ema_v2",
        }

    def _log_order_event(
        self,
        event_type: str,
        trade: Optional[Dict[str, Any]],
        ts: datetime,
        extra: Dict[str, Any],
    ) -> None:
        self._log_strategy_event(event_type, trade, ts=ts, extra=extra)

    def _log_strategy_event(
        self,
        event_type: str,
        trade: Optional[Dict[str, Any]],
        ts: Optional[datetime],
        extra: Dict[str, Any],
    ) -> None:
        if self.order_maneger is None:
            return
        if not isinstance(trade, dict):
            trade = {}
        try:
            if hasattr(self.order_maneger, "_log_local_event"):
                self.order_maneger._log_local_event(event_type, trade, ts=ts, extra=extra)
            elif hasattr(self.order_maneger, "_log_event"):
                self.order_maneger._log_event(event_type, trade, ts=ts, extra=extra)
        except Exception as exc:
            logger.error(f"Failed to log {event_type}: {exc}")
