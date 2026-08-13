import unittest

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


if __name__ == "__main__":
    unittest.main()
