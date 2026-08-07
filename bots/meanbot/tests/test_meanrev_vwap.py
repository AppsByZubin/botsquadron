import pathlib
import sys
import unittest
from datetime import datetime, time, timedelta
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pandas as pd

BOT_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BOT_DIR))

from common import constants
from index.nifty50.strategy.meanrev_vwap import MeanRevVwapStrategy
from oms.order_system_client import OrderSystemClient, OrderSystemError
from oms.mock_order_system_client import MockOrderSystemClient


class MeanRevVwapStrategyTests(unittest.TestCase):
    def test_trading_window_state_logs_once_per_minute(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy._last_outside_trading_window_log_minute = None
        strategy._last_inside_trading_window_log_minute = None
        strategy._current_trading_window_minute = None
        strategy._current_trading_window_open = False
        strategy._order_container = {"status": None, "side": None}
        strategy._order_counter = 0
        strategy._max_order_counter = 5
        strategy.df_index = pd.DataFrame()

        with patch("index.nifty50.strategy.meanrev_vwap.logger.info") as log_info:
            strategy._set_trading_window_state("2026-08-07 09:31", False)
            strategy._set_trading_window_state("2026-08-07 09:31", False)
            strategy._set_trading_window_state("2026-08-07 09:32", False)
            strategy._set_trading_window_state("2026-08-07 11:00", True)
            strategy._set_trading_window_state("2026-08-07 11:00", True)

        self.assertEqual(log_info.call_count, 3)
        log_info.assert_any_call("Outside Trading Window at 2026-08-07 09:31")
        log_info.assert_any_call("Outside Trading Window at 2026-08-07 09:32")
        self.assertTrue(
            any(
                str(call.args[0]).startswith("Inside Trading Window at 2026-08-07 11:00;")
                for call in log_info.call_args_list
            )
        )
        self.assertEqual(strategy._current_trading_window_minute, "2026-08-07 11:00")
        self.assertTrue(strategy._current_trading_window_open)

    def test_outside_trading_window_clears_waiting_order(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy.curr_index_minute = "2026-08-07 09:31"
        strategy._current_trading_window_minute = strategy.curr_index_minute
        strategy._current_trading_window_open = False
        strategy._order_container = {
            "trade_id": None,
            "side": constants.CALL,
            "instrument_key": None,
            "status": constants.WAITING,
        }
        strategy.order_maneger = None

        strategy._trade_processing([])

        self.assertTrue(all(value is None for value in strategy._order_container.values()))

    def test_outside_trading_window_updates_ltp_and_squares_off_open_order(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy.curr_index_minute = "2026-08-07 15:01"
        strategy._current_trading_window_minute = strategy.curr_index_minute
        strategy._current_trading_window_open = False
        strategy._order_container = {
            "trade_id": "trade-1",
            "side": constants.CALL,
            "instrument_key": "option-1",
            "status": constants.OPEN,
            "ltp": 100.0,
        }
        strategy.order_maneger = Mock()
        strategy.order_maneger.square_off_trade.return_value = True
        strategy.order_maneger.get_trade_by_id.return_value = {"id": "trade-1"}
        strategy._resolve_reference_ts = Mock(
            return_value=datetime(2026, 8, 7, 15, 1, tzinfo=ZoneInfo("Asia/Kolkata"))
        )
        strategy._update_today_realized_pnl_on_trade_close = Mock()

        strategy._trade_processing([{"instrument_key": "option-1", "ltp": 105.0}])

        strategy.order_maneger.square_off_trade.assert_called_once_with(
            trade_id="trade-1",
            exit_price=105.0,
            ts=strategy._resolve_reference_ts.return_value,
            reason=constants.EOD_SQUARE_OFF,
        )
        self.assertTrue(all(value is None for value in strategy._order_container.values()))

    @staticmethod
    def _strategy_with_candle(
        *,
        open_price,
        close_price,
        lowerbound,
        upperbound,
        angle_ema_9,
    ):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy.enable_trading_engine = True
        strategy.curr_index_minute = "2026-03-02 11:01"
        strategy.df_index = pd.DataFrame([{
            "time": "2026-03-02 11:00",
            "open": open_price,
            "close": close_price,
            "lowerbound": lowerbound,
            "upperbound": upperbound,
            "ema_9": close_price,
            "angle_ema_9": angle_ema_9,
        }])
        strategy._order_container = {"side": None, "status": None, "lot": None}
        strategy.trade_start = time(9, 45)
        strategy.trade_end = time(15, 15)
        strategy._last_outside_trading_window_log_minute = None
        strategy._last_inside_trading_window_log_minute = None
        strategy._current_trading_window_minute = None
        strategy._current_trading_window_open = False
        strategy._post_exit_cooldown_until = None
        strategy._max_order_counter = 5
        strategy._order_counter = 0
        strategy._is_daily_loss_limit_active = lambda _ref_ts: False
        strategy._calculate_lot_size = lambda _side, _bullish, _bearish: 2
        return strategy

    def test_call_setup_requires_body_below_lower_band(self):
        strategy = self._strategy_with_candle(
            open_price=98.0,
            close_price=99.0,
            lowerbound=100.0,
            upperbound=110.0,
            angle_ema_9=46.0,
        )

        strategy._trading_engine_active()

        self.assertEqual(strategy._order_container["side"], constants.CALL)
        self.assertEqual(strategy._order_container["status"], constants.WAITING)

    def test_put_setup_requires_body_above_upper_band(self):
        strategy = self._strategy_with_candle(
            open_price=112.0,
            close_price=111.0,
            lowerbound=100.0,
            upperbound=110.0,
            angle_ema_9=-46.0,
        )

        strategy._trading_engine_active()

        self.assertEqual(strategy._order_container["side"], constants.PUT)
        self.assertEqual(strategy._order_container["status"], constants.WAITING)

    def test_band_cross_or_width_at_limit_does_not_enter(self):
        for open_price, close_price, lowerbound, upperbound in (
            (99.0, 101.0, 100.0, 110.0),
            (98.0, 99.0, 100.0, 160.0),
        ):
            with self.subTest(open_price=open_price, close_price=close_price):
                strategy = self._strategy_with_candle(
                    open_price=open_price,
                    close_price=close_price,
                    lowerbound=lowerbound,
                    upperbound=upperbound,
                    angle_ema_9=46.0,
                )
                strategy._trading_engine_active()
                self.assertIsNone(strategy._order_container["status"])

    def test_ema_angle_thresholds_are_strict(self):
        for open_price, close_price, angle_ema_9 in (
            (98.0, 99.0, 45.0),
            (112.0, 111.0, -45.0),
        ):
            with self.subTest(angle_ema_9=angle_ema_9):
                strategy = self._strategy_with_candle(
                    open_price=open_price,
                    close_price=close_price,
                    lowerbound=100.0,
                    upperbound=110.0,
                    angle_ema_9=angle_ema_9,
                )

                strategy._trading_engine_active()

                self.assertIsNone(strategy._order_container["status"])

    def test_indicator_calculation_populates_ema_9_and_angle(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy.params = {"strategy-parameters": {}}
        strategy._slope_window = 3
        strategy.df_index_future = pd.DataFrame()
        strategy.df_index = pd.DataFrame(
            [
                {
                    "time": f"2026-08-07 09:{15 + index:02d}",
                    "open": 100.0 + (2.0 * index),
                    "high": 101.0 + (2.0 * index),
                    "low": 99.0 + (2.0 * index),
                    "close": 100.0 + (2.0 * index),
                }
                for index in range(15)
            ]
        )

        strategy._apply_indicators()

        latest = strategy.df_index.iloc[-1]
        self.assertTrue(pd.notna(latest["ema_9"]))
        self.assertGreater(latest["angle_ema_9"], 45.0)

    def test_five_minute_cooldown_boundary(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy._post_exit_cooldown_minutes = 5
        strategy._post_exit_cooldown_until = None
        exit_time = datetime(2026, 3, 2, 11, 0, tzinfo=ZoneInfo("Asia/Kolkata"))

        strategy._set_post_exit_cooldown(constants.TARGET_HIT, ts=exit_time)

        self.assertTrue(strategy._is_post_exit_cooldown_active(exit_time + timedelta(minutes=4, seconds=59)))
        self.assertFalse(strategy._is_post_exit_cooldown_active(exit_time + timedelta(minutes=5)))

    def test_normalizes_marketfeeder_envelope(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        message = {
            "feeds": {
                constants.NIFTY50_SYMBOL: {
                    "fullFeed": {
                        "indexFF": {"ltpc": {"ltp": 25123.5, "ltt": 1786083900000}}
                    }
                }
            }
        }

        normalized = strategy._normalize_feed_response(message)

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["instrument_key"], constants.NIFTY50_SYMBOL)
        self.assertEqual(normalized[0]["ltp"], 25123.5)
        self.assertEqual(normalized[0]["ts_epoch_ms"], 1786083900000)

    def test_bootstraps_live_spot_and_future_candles(self):
        class FakeOrderManager:
            @staticmethod
            def get_account_details():
                return {"trades": []}

        params = {
            "strategy-parameters": {
                "trade_expiry": "2026-08-11",
                "max_daily_loss_amount": 4200.0,
                "post_exit_cooldown_minutes": 7,
                "trade-window": {"start": "11:00", "end": "15:15"},
            }
        }
        index_candles = [
            ["2026-08-07T11:00:00+05:30", 25000, 25010, 24990, 25005, 0, 0],
            ["2026-08-07T11:01:00+05:30", 25005, 25020, 25000, 25015, 0, 0],
            ["2026-08-07T11:02:00+05:30", 25015, 25025, 25010, 25020, 0, 0],
        ]
        future_candles = [
            ["2026-08-07T11:00:00+05:30", 25020, 25030, 25010, 25025, 100, 1],
            ["2026-08-07T11:01:00+05:30", 25025, 25040, 25020, 25035, 120, 1],
            ["2026-08-07T11:02:00+05:30", 25035, 25045, 25030, 25040, 80, 1],
        ]

        strategy = MeanRevVwapStrategy(
            params=params,
            order_manager=FakeOrderManager(),
            selected_contracts={"Nifty_Future": {"instrument_key": "future-key"}},
            intraday_index_candles=index_candles,
            intraday_future_candles=future_candles,
        )

        self.assertEqual(strategy.index_fur_key, "future-key")
        self.assertEqual(strategy.curr_index_minute, "2026-08-07 11:02")
        self.assertEqual(strategy.curr_fut_minute, "2026-08-07 11:02")
        self.assertEqual(len(strategy.df_index), 2)
        self.assertEqual(len(strategy.df_index_future), 2)
        self.assertEqual(strategy._max_daily_loss_amount, 4200.0)
        self.assertEqual(strategy._post_exit_cooldown_minutes, 7)
        self.assertEqual(strategy.trade_start, time(11, 0))
        self.assertEqual(strategy.trade_end, time(15, 15))
        self.assertTrue(pd.notna(strategy.df_index.iloc[-1]["vwap"]))

    def test_selects_only_in_the_money_contracts_for_side(self):
        strategy = MeanRevVwapStrategy.__new__(MeanRevVwapStrategy)
        strategy.selected_contracts = {
            25000: [
                {"instrument_key": "call-itm", "instrument_type": "CE", "strike_price": 25000},
                {"instrument_key": "put-otm", "instrument_type": "PE", "strike_price": 25000},
            ],
            25150: [
                {"instrument_key": "put-itm", "instrument_type": "PE", "strike_price": 25150},
                {"instrument_key": "call-otm", "instrument_type": "CE", "strike_price": 25150},
            ],
        }

        calls = strategy._get_itm_contracts(constants.CALL, 25100, 200)
        puts = strategy._get_itm_contracts(constants.PUT, 25100, 200)

        self.assertEqual(set(calls), {"call-itm"})
        self.assertEqual(set(puts), {"put-itm"})


class MeanbotControlClientTests(unittest.TestCase):
    def test_block_and_resume_bot_use_order_intake_endpoints(self):
        client = OrderSystemClient(bot_name="meanbot", mode=constants.MOCK, local_copy_enabled=False)
        client._request = Mock(side_effect=[{"kill_enabled": True}, {"kill_enabled": False}])

        client.block_bot(reason="event risk")
        client.resume_bot(reason="event passed")

        self.assertEqual(
            client._request.call_args_list[0].args,
            ("POST", "/v1/bots/meanbot/block-orders"),
        )
        self.assertEqual(client._request.call_args_list[0].kwargs["json"], {"reason": "event risk"})
        self.assertEqual(
            client._request.call_args_list[1].args,
            ("POST", "/v1/bots/meanbot/resume"),
        )

    def test_mock_block_and_resume_are_local(self):
        client = MockOrderSystemClient(bot_name="meanbot", mode=constants.MOCK, local_copy_enabled=False)

        blocked = client.block_bot(reason="event risk")
        with self.assertRaises(OrderSystemError):
            client.create_trade(symbol="NIFTY", instrument_token="option-key", qty=65)
        resumed = client.resume_bot(reason="event passed")

        self.assertTrue(blocked["kill_enabled"])
        self.assertFalse(resumed["kill_enabled"])
        self.assertFalse(client.get_kill_switch()["kill_enabled"])


if __name__ == "__main__":
    unittest.main()
