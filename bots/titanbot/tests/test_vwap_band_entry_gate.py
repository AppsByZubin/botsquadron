import unittest
from datetime import datetime, time

import pandas as pd

from common import constants
from index.nifty50.strategy.timeseries_trend import TimeseriesTrendStrategy, ist
from index.nifty50.strategy.timeseries_trend_v2 import TimeseriesTrendV2Strategy


class VwapBandEntryGateTest(unittest.TestCase):
    @staticmethod
    def _strategy(strategy_cls, side, band_width=None):
        strategy = strategy_cls.__new__(strategy_cls)
        angle = 70.0 if side == constants.CALL else -70.0
        band_midpoint = 22000.0
        latest = {
            "time": "2026-08-07 10:01",
            "close": band_midpoint,
            "ema_10m": band_midpoint,
            "ema_5m": band_midpoint,
            "ema_1m": band_midpoint,
            "angle_ema_10m": angle,
            "angle_ema_5m": angle,
            "angle_ema_1m": angle,
            "ema_21_1m": band_midpoint,
            "sma_50_1m": band_midpoint,
            "angle_ema_21_1m": angle,
            "angle_sma_50_1m": angle,
            "signal": None,
        }
        if band_width is not None:
            latest["vwap_upper_band_1"] = band_midpoint + (band_width / 2.0)
            latest["vwap_lower_band_1"] = band_midpoint - (band_width / 2.0)
        previous = dict(latest, time="2026-08-07 10:00")
        strategy.df_index = pd.DataFrame([previous, latest])
        strategy.curr_index_minute = latest["time"]
        strategy._is_trading_window = lambda _minute: True
        strategy._set_trading_window_state = lambda _minute, _is_open: None
        strategy.enable_trading_engine = True
        strategy._indicator_revision = 1
        strategy._last_engine_revision = -1
        strategy.ema_length = 1
        strategy.slope_window = 1
        strategy.trade_start = time(9, 15)
        strategy.trade_end = time(15, 30)
        strategy.call_angle_10m = 20.0
        strategy.call_angle_5m = 30.0
        strategy.call_angle_1m = 45.0
        strategy.put_angle_10m = -20.0
        strategy.put_angle_5m = -30.0
        strategy.put_angle_1m = -45.0
        strategy.call_ema_21_angle_1m = 30.0
        strategy.put_ema_21_angle_1m = -30.0
        strategy._order_container = {"status": None}
        strategy._order_counter = 0
        strategy._max_order_counter = 2
        strategy.last_signal = constants.WAITING

        captured_sides = []
        strategy._set_waiting_order = lambda **kwargs: captured_sides.append(kwargs["side"])
        strategy._resolve_reference_ts = lambda: datetime(2026, 8, 7, 10, 1, tzinfo=ist)
        strategy._is_post_exit_cooldown_active = lambda _ref_ts: False
        strategy._is_daily_loss_limit_active = lambda _ref_ts: False
        return strategy, captured_sides

    def test_timeseries_v1_call_and_put_require_width_strictly_greater_than_60(self):
        for side in (constants.CALL, constants.PUT):
            with self.subTest(side=side, width=60.0):
                strategy, captured_sides = self._strategy(TimeseriesTrendStrategy, side, 60.0)
                strategy._trading_engine_active()
                self.assertEqual([], captured_sides)

            with self.subTest(side=side, width=60.01):
                strategy, captured_sides = self._strategy(TimeseriesTrendStrategy, side, 60.01)
                strategy._trading_engine_active()
                self.assertEqual([side], captured_sides)

    def test_timeseries_v2_entry_does_not_require_vwap_bands(self):
        for side in (constants.CALL, constants.PUT):
            with self.subTest(side=side):
                strategy, captured_sides = self._strategy(TimeseriesTrendV2Strategy, side)
                strategy._trading_engine_active()
                self.assertEqual([side], captured_sides)

    def test_timeseries_v2_uses_fixed_eight_percent_daily_loss_limit(self):
        strategy = TimeseriesTrendV2Strategy(
            params={"strategy-parameters": {"max_daily_loss_pct_of_initial_cash": 0.11}}
        )

        self.assertEqual(0.08, strategy._max_daily_loss_pct_of_initial_cash)

    def test_timeseries_v2_empty_frame_excludes_vwap_columns(self):
        strategy = TimeseriesTrendV2Strategy.__new__(TimeseriesTrendV2Strategy)

        frame = strategy._empty_index_frame()

        self.assertTrue(
            {
                "vwap",
                "vwap_stdev",
                "vwap_upper_band_1",
                "vwap_lower_band_1",
            }.isdisjoint(frame.columns)
        )

    def test_v2_vwap_session_trail_atr_multiplier_comes_from_params(self):
        strategy = TimeseriesTrendV2Strategy.__new__(TimeseriesTrendV2Strategy)
        strategy.params = {
            "strategy-parameters": {
                "vwap_session_trail_atr_mult": {
                    "below_100": 1.1,
                    "between_100_and_130": 2.2,
                    "between_150_and_200": 3.3,
                    "above_200": 4.4,
                }
            }
        }
        strategy.df_index = None

        for width, expected in ((99, 1.1), (115, 2.2), (175, 3.3), (201, 4.4)):
            with self.subTest(width=width):
                strategy.last_index_bar = {
                    "vwap_upper_band_1": width,
                    "vwap_lower_band_1": 0,
                }
                self.assertEqual(expected, strategy._vwap_session_trail_atr_mult())

    def test_v2_vwap_session_trail_atr_multiplier_preserves_unmatched_widths(self):
        strategy = TimeseriesTrendV2Strategy.__new__(TimeseriesTrendV2Strategy)
        strategy.params = {
            "strategy-parameters": {
                "vwap_session_trail_atr_mult": {
                    "below_100": 1.1,
                    "between_100_and_130": 2.2,
                    "between_150_and_200": 3.3,
                    "above_200": 4.4,
                }
            }
        }
        strategy.df_index = None

        for width in (100, 130, 150, 200):
            with self.subTest(width=width):
                strategy.last_index_bar = {
                    "vwap_upper_band_1": width,
                    "vwap_lower_band_1": 0,
                }
                self.assertIsNone(strategy._vwap_session_trail_atr_mult())

    def test_vwap_band_values_are_populated_by_indicator_calculation(self):
        strategy = TimeseriesTrendStrategy.__new__(TimeseriesTrendStrategy)
        strategy.params = {"strategy-parameters": {}}
        strategy._future_volume_by_minute = {}
        strategy.last_index_bar = None
        strategy.df_index = pd.DataFrame(
            [
                {
                    "time": "2026-08-07 09:15",
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 1.0,
                },
                {
                    "time": "2026-08-07 09:16",
                    "high": 110.0,
                    "low": 110.0,
                    "close": 110.0,
                    "volume": 1.0,
                },
                {
                    "time": "2026-08-07 09:17",
                    "high": 120.0,
                    "low": 120.0,
                    "close": 120.0,
                    "volume": 1.0,
                },
            ]
        )

        strategy._apply_vwap_band_1(strategy.df_index)

        latest = strategy.df_index.iloc[-1]
        self.assertAlmostEqual(110.0, latest["vwap"])
        self.assertGreater(latest["vwap_upper_band_1"], latest["vwap"])
        self.assertLess(latest["vwap_lower_band_1"], latest["vwap"])
        self.assertAlmostEqual(
            latest["vwap_upper_band_1"] - latest["vwap_lower_band_1"],
            strategy._latest_vwap_band_1_width(),
        )


if __name__ == "__main__":
    unittest.main()
