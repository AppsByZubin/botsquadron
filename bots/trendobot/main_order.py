import os
import sys
import yaml

from order_manager.upstox_order_system import  UpstoxOrderManager
from logger import create_logger
from broker.upstox_helper import UpstoxHelper
import time
import common.constants as constants
from datetime import datetime

from index.nifty50.nifty_utils import (

    get_nifty_option_instruments
)

logger = create_logger("BotMain")

if __name__ == "__main__":
    logger.info("Start Bot main.")
    api_token = os.getenv(constants.UPSTOX_API_ACCESS_TOKEN)
    if not api_token:
        logger.error("UPSTOX_API_ACCESS_TOKEN is not set.")
        sys.exit(constants.FAIL_CODE)

    param_data=None
    with open(constants.PARAM_PATH, 'r') as file:
        param_data = yaml.safe_load(file)

    entry_price = 100
    sp=(param_data.get("strategy-parameters") or {}) if isinstance(param_data, dict) else {}
    atr_for_option = 10

    upstox_helper = UpstoxHelper(api_token,is_sandbox=True)
    order_manager = UpstoxOrderManager(upstox_helper,
                                               strategy_parameters=sp,
                                               orders_csv=constants.ORDER_PROD_LOG,
                                               daily_csv=constants.DAILY_PROD_PNL,
                                               events_json_path=constants.ORDER_PROD_EVENT_LOG
                                               )
    
    # --- raw levels ---
    target = entry_price + (atr_for_option * 5)
    sl_trigger = entry_price - (1.2 * atr_for_option)

    sl_limit = sl_trigger - 1

    # NIFTY 25150 CE : NSE_FO|54580
    # NIFTY 25300 PE : NSE_FO|54587
    ts=int(time.time())
    trade_id = order_manager.buy(
                symbol="NIFTY 25150 CE 30 MAR 26",
                instrument_token="NSE_FO|54580",
                qty=65,
                entry_price=entry_price,
                sl_trigger=sl_trigger,
                sl_limit=sl_limit,
                target=target,
                description="Test order",
                ts=datetime.now(),
            )
    tag=f"stoploss{trade_id}"

    time.sleep(5)
    t = order_manager.get_trade_by_id(trade_id)
    tick_price = 102
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
    
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 104
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
    
    time.sleep(5)
    tick_price = 106
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 108
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 110
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
    
    time.sleep(5)
    tick_price = 109
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 108
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 115
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
    
    time.sleep(5)
    tick_price = 126
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
    
    time.sleep(5)
    tick_price = 130
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
    
    time.sleep(5)
    tick_price = 134
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 130
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 136
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 138
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 139
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 155
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")


    time.sleep(5)
    tick_price = 160
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")

    time.sleep(5)
    tick_price = 165
    t = order_manager.get_trade_by_id(trade_id)
    _ = order_manager.on_tick(
                symbol="NIFTY 25150 CE 30 MAR 26",
                o=tick_price, h=tick_price, l=tick_price, c=tick_price,
                ts=datetime.now(),
            )
    logger.info(f"Trade details:- tick_price: {tick_price}, stoploss: {t['stoploss']}, sl_limit: {t['_sl_limit']}, target: {t['target']}, status: {t['status']}")
