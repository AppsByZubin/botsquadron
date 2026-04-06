from __future__ import annotations

import json
import math
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import common.constants as constants
import logger

IST = ZoneInfo("Asia/Kolkata")
log = logger.create_logger("OrbStateLogger")


class OrbState:
    def __init__(self, params: Optional[Dict[str, Any]] = None, state_file: Optional[str] = None):
        self.params = params or {}
        self.state_file = Path(state_file or constants.ORB_STATE_FILE)

        sp = (self.params.get("strategy-parameters") or {}) if isinstance(self.params, dict) else {}
        orb = sp.get("orb", {}) if isinstance(sp.get("orb", {}), dict) else {}

        self.config_enabled = bool(orb.get("enabled", True))
        self.start_time = self._parse_clock(orb.get("start", "09:15"), "09:15")
        self.end_time = self._parse_clock(orb.get("end", "09:30"), "09:30")
        self.breakout_buffer = float(orb.get("breakout-buffer", orb.get("breakout_buffer", 0.0)))
        self.confirm_by_close = bool(orb.get("confirm-by-close", orb.get("confirm_by_close", True)))
        self.direction_lock = bool(orb.get("direction-lock", orb.get("direction_lock", True)))

        if self.end_time <= self.start_time:
            log.warning("Invalid ORB window; fallback 09:15-09:30")
            self.start_time = datetime.strptime("09:15", "%H:%M").time()
            self.end_time = datetime.strptime("09:30", "%H:%M").time()

        self.day = self._today_key()
        self.runtime_enabled = bool(self.config_enabled)
        self.disabled_reason: Optional[str] = None
        self.open: Optional[float] = None
        self.high: Optional[float] = None
        self.low: Optional[float] = None
        self.ready = False
        self.breakout_side: Optional[str] = None

        self._load_state()
        self._persist_state()

    def _parse_clock(self, value: str, default: str) -> dtime:
        try:
            return datetime.strptime(str(value), "%H:%M").time()
        except Exception:
            log.warning(f"Invalid ORB time '{value}', fallback={default}")
            return datetime.strptime(default, "%H:%M").time()

    def _today_key(self) -> str:
        return datetime.now(IST).strftime("%Y-%m-%d")

    def _extract_dt(self, minute_key: str) -> Optional[datetime]:
        try:
            return datetime.strptime(minute_key, "%Y-%m-%d %H:%M")
        except Exception:
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            out = float(value)
        except Exception:
            return None
        if not math.isfinite(out):
            return None
        return out

    def _build_payload(self) -> Dict[str, Any]:
        return {
            "day": self.day,
            "configured_enabled": bool(self.config_enabled),
            "runtime_enabled": bool(self.runtime_enabled),
            "enabled": self.is_enabled(),
            "disabled_reason": self.disabled_reason,
            "start": self.start_time.strftime("%H:%M"),
            "end": self.end_time.strftime("%H:%M"),
            "breakout_buffer": float(self.breakout_buffer),
            "confirm_by_close": bool(self.confirm_by_close),
            "direction_lock": bool(self.direction_lock),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "ready": bool(self.ready),
            "breakout_side": self.breakout_side,
            "updated_at": datetime.now(IST).isoformat(),
        }

    def _persist_state(self) -> None:
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with self.state_file.open("w", encoding="utf-8") as fh:
                json.dump(self._build_payload(), fh, indent=2)
        except Exception as exc:
            log.warning(f"Failed to persist ORB state: {exc}")

    def _load_state(self) -> None:
        if not self.state_file.exists():
            return
        try:
            with self.state_file.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            log.warning(f"Failed to load ORB state file {self.state_file}: {exc}")
            return

        if not isinstance(data, dict):
            return

        loaded_day = str(data.get("day") or "")
        if loaded_day != self._today_key():
            self.reset_for_day(day_key=self._today_key(), persist=False)
            return

        self.day = loaded_day
        runtime = bool(data.get("runtime_enabled", data.get("enabled", True)))
        self.runtime_enabled = bool(self.config_enabled and runtime)
        self.disabled_reason = str(data.get("disabled_reason")) if data.get("disabled_reason") else None
        self.open = self._safe_float(data.get("open"))
        self.high = self._safe_float(data.get("high"))
        self.low = self._safe_float(data.get("low"))
        self.ready = bool(data.get("ready", False))

        side_raw = str(data.get("breakout_side")).upper() if data.get("breakout_side") else None
        self.breakout_side = side_raw if side_raw in (constants.CALL, constants.PUT) else None

        if self.ready and (self.high is None or self.low is None):
            self.ready = False

    def is_enabled(self) -> bool:
        return bool(self.config_enabled and self.runtime_enabled)

    def reset_for_day(self, day_key: Optional[str] = None, persist: bool = True) -> None:
        self.day = day_key or self._today_key()
        self.runtime_enabled = bool(self.config_enabled)
        self.disabled_reason = None
        self.open = None
        self.high = None
        self.low = None
        self.ready = False
        self.breakout_side = None
        if persist:
            self._persist_state()

    def disable_for_trade(self, reason: str) -> None:
        self.day = self.day or self._today_key()
        self.runtime_enabled = False
        self.disabled_reason = str(reason)[:400]
        self._persist_state()
        log.warning(f"ORB disabled for current session. reason={self.disabled_reason}")

    def enable_for_trade(self, reset_state: bool = False) -> bool:
        """
        Enable ORB at runtime for the current day/session.
        Returns True when ORB becomes enabled, False when blocked by config.
        """
        self.day = self.day or self._today_key()
        if not self.config_enabled:
            self.runtime_enabled = False
            self.disabled_reason = "orb config enabled=false"
            self._persist_state()
            log.warning("ORB runtime enable blocked because orb.enabled is false in config.")
            return False

        self.runtime_enabled = True
        self.disabled_reason = None
        if reset_state:
            self.open = None
            self.high = None
            self.low = None
            self.ready = False
            self.breakout_side = None
        self._persist_state()
        log.info(f"ORB enabled for current session. reset_state={reset_state}")
        return True

    def force_breakout_side(self, side: str, reason: Optional[str] = None) -> bool:
        side_u = str(side).upper() if side is not None else ""
        if side_u not in (constants.CALL, constants.PUT):
            return False

        self._ensure_day(self._today_key())
        if self.breakout_side == side_u:
            return True

        self.breakout_side = side_u
        self._persist_state()
        if reason:
            log.info(f"ORB breakout side overridden to {side_u}. reason={reason}")
        else:
            log.info(f"ORB breakout side overridden to {side_u}.")
        return True

    def _ensure_day(self, day_key: Optional[str]) -> None:
        target_day = day_key or self._today_key()
        if self.day != target_day:
            self.reset_for_day(day_key=target_day, persist=True)

    def update_from_candle(self, candle: Dict[str, Any]) -> None:
        if not self.is_enabled():
            return
        try:
            minute_key = str(candle.get("time") or "")
            dt_obj = self._extract_dt(minute_key)
            if dt_obj is None:
                return
            self._ensure_day(dt_obj.strftime("%Y-%m-%d"))
            candle_time = dt_obj.time()
            if candle_time < self.start_time:
                return

            changed = False
            if candle_time <= self.end_time:
                if self.open is None:
                    candle_open = self._safe_float(candle.get("open"))
                    if candle_open is not None:
                        self.open = candle_open
                        changed = True

                candle_high = self._safe_float(candle.get("high"))
                candle_low = self._safe_float(candle.get("low"))

                if candle_high is not None:
                    new_high = candle_high if self.high is None else max(self.high, candle_high)
                    if self.high != new_high:
                        self.high = new_high
                        changed = True

                if candle_low is not None:
                    new_low = candle_low if self.low is None else min(self.low, candle_low)
                    if self.low != new_low:
                        self.low = new_low
                        changed = True

            if (not self.ready) and (candle_time >= self.end_time) and self.high is not None and self.low is not None:
                self.ready = True
                changed = True
                log.info(
                    f"ORB ready open={self.open}, high={self.high:.2f}, low={self.low:.2f}, "
                    f"window={self.start_time.strftime('%H:%M')}-{self.end_time.strftime('%H:%M')}"
                )

            if changed:
                self._persist_state()
        except Exception as exc:
            self.disable_for_trade(f"update_from_candle error: {exc}")

    def get_breakout_side(
        self,
        last_index_bar: Optional[Dict[str, Any]],
        curr_index_minute: Optional[str],
    ) -> Optional[str]:
        if not self.is_enabled():
            return None
        try:
            minute_hint = curr_index_minute or (last_index_bar or {}).get("time")
            dt_obj = self._extract_dt(str(minute_hint)) if minute_hint else None
            self._ensure_day(dt_obj.strftime("%Y-%m-%d") if dt_obj is not None else self._today_key())

            if not self.ready or last_index_bar is None:
                return None
            if self.high is None or self.low is None:
                return None
            if self.direction_lock and self.breakout_side in (constants.CALL, constants.PUT):
                return self.breakout_side

            if self.confirm_by_close:
                up_price = self._safe_float(last_index_bar.get("close"))
                down_price = self._safe_float(last_index_bar.get("close"))
            else:
                up_price = self._safe_float(last_index_bar.get("high"))
                down_price = self._safe_float(last_index_bar.get("low"))

            if up_price is None or down_price is None:
                return None

            signal_side = None
            if up_price > float(self.high + self.breakout_buffer):
                signal_side = constants.CALL
            elif down_price < float(self.low - self.breakout_buffer):
                signal_side = constants.PUT

            if signal_side and self.direction_lock and self.breakout_side is None:
                self.breakout_side = signal_side
                self._persist_state()
                log.info(f"ORB breakout locked side={signal_side} at {curr_index_minute}")

            return signal_side
        except Exception as exc:
            self.disable_for_trade(f"get_breakout_side error: {exc}")
            return None
