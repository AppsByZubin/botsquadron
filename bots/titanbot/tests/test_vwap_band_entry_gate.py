import unittest
from datetime import datetime, time

import pandas as pd

from common import constants
from index.nifty50.strategy.timeseries_trend import TimeseriesTrendStrategy, ist
from index.nifty50.strategy.timeseries_trend_v2 import TimeseriesTrendV2Strategy


class VwapBandEntryGateTest(unittest.TestCase):
    @staticmethod
    def _strategy(strategy_cls, side, band_width):
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
            "vwap_upper_band_1": band_midpoint + (band_width / 2.0),
            "vwap_lower_band_1": band_midpoint - (band_width / 2.0),
            "signal": None,
        }
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

    def test_call_and_put_require_width_strictly_greater_than_60(self):
        for strategy_cls in (TimeseriesTrendStrategy, TimeseriesTrendV2Strategy):
            for side in (constants.CALL, constants.PUT):
                with self.subTest(strategy=strategy_cls.__name__, side=side, width=60.0):
                    strategy, captured_sides = self._strategy(strategy_cls, side, 60.0)
                    strategy._trading_engine_active()
                    self.assertEqual([], captured_sides)

                with self.subTest(strategy=strategy_cls.__name__, side=side, width=60.01):
                    strategy, captured_sides = self._strategy(strategy_cls, side, 60.01)
                    strategy._trading_engine_active()
                    self.assertEqual([side], captured_sides)

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
