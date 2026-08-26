import unittest

import pandas as pd

from index.nifty50.strategy.bb_vwap_ema_v2 import BbVwapEmaV2Strategy


class BbVwapEmaV2EntryBandTests(unittest.TestCase):
    def setUp(self):
        self.strategy = BbVwapEmaV2Strategy.__new__(BbVwapEmaV2Strategy)
        self.strategy.params = {"strategy-parameters": {"bb_entry_tick_size": 0.05}}

    def test_directional_rounding_allows_one_tick_boundary(self):
        call_upper, put_lower, tick_size = self.strategy._entry_band_boundaries(
            24326.58884850907,
            24326.51115149093,
        )

        self.assertEqual(0.05, tick_size)
        self.assertEqual(24326.55, call_upper)
        self.assertEqual(24326.55, put_lower)
        self.assertGreaterEqual(24326.55, call_upper)
        self.assertLessEqual(24326.55, put_lower)

    def test_rounding_does_not_allow_a_full_tick_breakout_gap(self):
        call_upper, put_lower, _ = self.strategy._entry_band_boundaries(
            24326.6000001,
            24326.4999999,
        )

        self.assertEqual(24326.6, call_upper)
        self.assertEqual(24326.5, put_lower)
        self.assertLess(24326.55, call_upper)
        self.assertGreater(24326.55, put_lower)

    def test_exact_tick_boundary_is_unchanged(self):
        call_upper, put_lower, _ = self.strategy._entry_band_boundaries(
            24326.55,
            24326.55,
        )

        self.assertEqual(24326.55, call_upper)
        self.assertEqual(24326.55, put_lower)

    def test_vwap_session_trail_atr_multiplier_comes_from_params(self):
        self.strategy.params["strategy-parameters"]["vwap_session_trail_atr_mult"] = {
            "below_100": 1.1,
            "between_100_and_130": 2.2,
            "between_150_and_200": 3.3,
            "above_200": 4.4,
        }

        for width, expected in ((99, 1.1), (115, 2.2), (175, 3.3), (201, 4.4)):
            with self.subTest(width=width):
                self.strategy.df_index = pd.DataFrame(
                    [{"vwap_upper_band_1": width, "vwap_lower_band_1": 0}]
                )
                self.assertEqual(expected, self.strategy._vwap_session_trail_atr_mult())

    def test_vwap_session_trail_atr_multiplier_preserves_unmatched_widths(self):
        self.strategy.params["strategy-parameters"]["vwap_session_trail_atr_mult"] = {
            "below_100": 1.1,
            "between_100_and_130": 2.2,
            "between_150_and_200": 3.3,
            "above_200": 4.4,
        }

        for width in (100, 130, 150, 200):
            with self.subTest(width=width):
                self.strategy.df_index = pd.DataFrame(
                    [{"vwap_upper_band_1": width, "vwap_lower_band_1": 0}]
                )
                self.assertIsNone(self.strategy._vwap_session_trail_atr_mult())


if __name__ == "__main__":
    unittest.main()
