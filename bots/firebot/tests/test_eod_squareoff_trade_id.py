import unittest
from unittest.mock import Mock

from common import constants
from index.nifty50.strategy.bb_vwap_ema_v2 import BbVwapEmaV2Strategy


class EodSquareOffTradeIDTests(unittest.TestCase):
    def setUp(self):
        self.strategy = BbVwapEmaV2Strategy.__new__(BbVwapEmaV2Strategy)
        self.strategy.curr_index_minute = "2026-08-24 15:00"
        self.strategy._order_container = {
            "trade_id": "trade-yesterday",
            "side": constants.CALL,
            "instrument_key": "NSE_FO|123",
            "instrument_symbol": "NIFTY CE",
            "status": constants.OPEN,
            "ltp": 54.0,
        }
        self.strategy._update_today_realized_pnl_on_trade_close = Mock()

    def test_eod_clears_bot_trade_id_after_ordersystem_confirms_closed(self):
        manager = Mock()
        manager.square_off_trade.return_value = {
            "trade_id": "trade-yesterday",
            "status": constants.EOD_SQUARE_OFF,
            "exit_price": 54.0,
        }
        self.strategy.order_maneger = manager

        closed = self.strategy._square_off_open_trade(
            constants.EOD_SQUARE_OFF,
            latest_ltp=54.0,
        )

        self.assertTrue(closed)
        self.assertIsNone(self.strategy._order_container["trade_id"])
        self.assertIsNone(self.strategy._order_container["status"])
        manager.get_trade_by_id.assert_not_called()

    def test_eod_keeps_bot_trade_id_when_ordersystem_still_reports_open(self):
        manager = Mock()
        manager.square_off_trade.return_value = {
            "trade_id": "trade-yesterday",
            "status": constants.OPEN,
        }
        manager.get_trade_by_id.return_value = {
            "id": "trade-yesterday",
            "status": constants.OPEN,
        }
        self.strategy.order_maneger = manager

        closed = self.strategy._square_off_open_trade(
            constants.EOD_SQUARE_OFF,
            latest_ltp=54.0,
        )

        self.assertFalse(closed)
        self.assertEqual("trade-yesterday", self.strategy._order_container["trade_id"])
        self.assertEqual(constants.OPEN, self.strategy._order_container["status"])
        manager.get_trade_by_id.assert_called_once_with("trade-yesterday")


if __name__ == "__main__":
    unittest.main()
