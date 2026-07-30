import unittest

from common import constants
from oms.order_system_client import OrderSystemClient


class LongTrailStoplossTest(unittest.TestCase):
    def setUp(self):
        self.client = OrderSystemClient.__new__(OrderSystemClient)
        self.client.tick_size = 0.05
        self.client.sl_limit_gap = 1.0

    @staticmethod
    def _trade(stoploss=85.0, sl_limit=84.0):
        return {
            "entry_price": 100.0,
            "trail_points": 5.0,
            "stoploss": stoploss,
            "sl_limit": sl_limit,
            "spot_trail_anchor": 100.0,
            "side": constants.BUY,
        }

    def test_forced_long_trail_uses_requested_below_entry_stoploss(self):
        update = self.client._build_trailing_update(
            self._trade(),
            price=90.0,
            force=True,
            force_stoploss=88.0,
        )

        self.assertIsNotNone(update)
        self.assertEqual(88.0, update["stoploss"])
        self.assertEqual(87.0, update["sl_limit"])

    def test_other_forced_trails_keep_atr_distance(self):
        update = self.client._build_trailing_update(
            self._trade(stoploss=80.0, sl_limit=79.0),
            price=90.0,
            force=True,
        )

        self.assertIsNotNone(update)
        self.assertEqual(85.0, update["stoploss"])

    def test_forced_long_trail_does_not_loosen_existing_stoploss(self):
        update = self.client._build_trailing_update(
            self._trade(stoploss=89.0, sl_limit=88.0),
            price=90.0,
            force=True,
            force_stoploss=88.0,
        )

        self.assertIsNone(update)


if __name__ == "__main__":
    unittest.main()
