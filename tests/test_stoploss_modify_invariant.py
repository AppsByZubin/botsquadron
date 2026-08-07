import importlib
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BOT_NAMES = ("solobot", "trendobot", "fibobot", "firebot", "haemabot", "titanbot", "meanbot")
BOT_LOCAL_MODULES = ("common", "logger", "oms", "utils")


def _purge_bot_local_modules():
    for module_name in tuple(sys.modules):
        if any(
            module_name == prefix or module_name.startswith(f"{prefix}.")
            for prefix in BOT_LOCAL_MODULES
        ):
            sys.modules.pop(module_name, None)


def _load_client_classes(bot_name):
    bot_root = REPO_ROOT / "bots" / bot_name
    _purge_bot_local_modules()
    sys.path.insert(0, str(bot_root))
    try:
        live_module = importlib.import_module("oms.order_system_client")
        mock_module = importlib.import_module("oms.mock_order_system_client")
        return live_module.OrderSystemClient, mock_module.MockOrderSystemClient
    finally:
        sys.path.pop(0)
        _purge_bot_local_modules()


def _load_legacy_mock_class(bot_name):
    bot_root = REPO_ROOT / "bots" / bot_name
    _purge_bot_local_modules()
    sys.path.insert(0, str(bot_root))
    try:
        module = importlib.import_module("utils.mock_order_utils")
        return module.MockOrderSystem
    finally:
        sys.path.pop(0)
        _purge_bot_local_modules()


class StoplossModifyInvariantTest(unittest.TestCase):
    @staticmethod
    def _client(client_class):
        trade = {
            "id": "trade-1",
            "stoploss": 100.0,
            "sl_order_ids": ["sl-1"],
        }
        client = client_class.__new__(client_class)
        client.mode = "sandbox"
        client.validity = "DAY"
        client._get_cached_trade = lambda trade_id: dict(trade)
        client._read_local_trade = lambda trade_id: dict(trade)
        client._log_local_event = lambda *args, **kwargs: None
        client._sync_closed_trades_from_response = lambda *args, **kwargs: []
        client._patch_local_trade = lambda *args, **kwargs: None
        client._patch_cached_trade = lambda trade_id, updates: {**trade, **updates}

        broker_requests = []

        def request(method, path, **kwargs):
            broker_requests.append((method, path, kwargs))
            return {"trade_id": "trade-1", "modified_order_ids": ["sl-1"]}

        client._request = request
        return client, broker_requests

    def test_all_live_and_mock_clients_only_accept_higher_stoploss(self):
        for bot_name in BOT_NAMES:
            live_class, mock_class = _load_client_classes(bot_name)
            for client_kind, client_class in (("live", live_class), ("mock", mock_class)):
                for force_trail in (False, True):
                    for requested_stoploss in (99.0, 100.0):
                        with self.subTest(
                            bot=bot_name,
                            client=client_kind,
                            force_trail=force_trail,
                            requested_stoploss=requested_stoploss,
                        ):
                            client, broker_requests = self._client(client_class)
                            response = client.modify_trade(
                                "trade-1",
                                stoploss=requested_stoploss,
                                sl_limit=requested_stoploss - 1.0,
                                force_trail=force_trail,
                            )

                            self.assertEqual([], response["modified_order_ids"])
                            self.assertIn("must be greater", response["message"])
                            self.assertEqual([], broker_requests)

                for force_trail in (False, True):
                    with self.subTest(
                        bot=bot_name,
                        client=client_kind,
                        force_trail=force_trail,
                        requested_stoploss=101.0,
                    ):
                        client, broker_requests = self._client(client_class)
                        response = client.modify_trade(
                            "trade-1",
                            stoploss=101.0,
                            sl_limit=100.0,
                            force_trail=force_trail,
                        )

                        self.assertEqual(["sl-1"], response["modified_order_ids"])
                        if client_kind == "live":
                            self.assertEqual(1, len(broker_requests))

    def test_legacy_mock_order_utilities_only_accept_higher_stoploss(self):
        for bot_name in BOT_NAMES:
            if bot_name == "solobot":
                continue
            mock_class = _load_legacy_mock_class(bot_name)
            cases = ((99.0, False), (100.0, False), (101.0, True))
            for requested_stoploss, expected_result in cases:
                with self.subTest(bot=bot_name, requested_stoploss=requested_stoploss):
                    order = {
                        "id": "trade-1",
                        "symbol": "NIFTY",
                        "status": "OPEN",
                        "stoploss": 100.0,
                        "target": 120.0,
                        "qty": 1,
                    }
                    client = mock_class.__new__(mock_class)
                    client.orders = [order]
                    client._update_order = lambda updated_order: None

                    result = client.modify_order("trade-1", new_sl=requested_stoploss)

                    self.assertEqual(expected_result, result)
                    self.assertEqual(101.0 if expected_result else 100.0, order["stoploss"])


if __name__ == "__main__":
    unittest.main()
