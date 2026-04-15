import sys
import os
import time as t
import asyncio
import json
import threading
from datetime import datetime, time
import pandas as pd
from zoneinfo import ZoneInfo
import nats

from common import constants
from broker.upstox_helper import UpstoxHelper
from logger import create_logger
from oms.order_system_client import OrderSystemClient

from index.nifty50.nifty50_utils import (
    premarket,
    get_instrument_intraday_data,
    get_option_contracts,
    get_nifty_option_instruments,
    get_spot_price,
    save_ohlc_to_json,
    get_previous_day_trend
)

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("Nifty50EngineLogger")

async def nifty50_engine(strategy, mode, param_data):
    logger.info(f"Starting Nifty50 Engine with strategy: {strategy}, mode: {mode}")
    
    # Connect to NATS
    nc = await nats.connect(constants.NATS_URL)
    logger.info("Connected to NATS")

    try:

        sp = {}
        try:
            sp = (param_data or {}).get("strategy-parameters", {}) if isinstance(param_data, dict) else {}
        except Exception:
            sp = {}

        def _safe_int(value, default):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return int(default)


        # Implement the logic to fetch data, analyze, and execute trades based on the strategy and

        api_token = os.getenv(constants.UPSTOX_API_ACCESS_TOKEN)
        if not api_token:
            logger.error("API token not found. Please set UPSTOX_API_ACCESS_TOKEN.")
            return

        upstox = UpstoxHelper(api_token, is_sandbox=False)
        if upstox is None:
            logger.error("Upstox client not created.")
            sys.exit(constants.FAIL_CODE)

        get_option_contracts(upstox, constants.NIFTY50_SYMBOL)

        ohlc_two_days = premarket(upstox)
        if ohlc_two_days is None:
            logger.error("Failed to fetch premarket data.")
            sys.exit(constants.FAIL_CODE)

        last_day_close=0

        if len(ohlc_two_days) ==2:
            save_ohlc_to_json(ohlc_two_days)

            trend, price_range, last_day_close = get_previous_day_trend()
            logger.info(f"Previous day trend: {trend}")
            logger.info(
                f"Price range: Support-{price_range[0]}, Deep Support-{price_range[1]}, "
                f"Resistance-{price_range[2]}, Deep Resistance-{price_range[3]}"
            )

        future_contract = upstox.get_future_contracts_by_instrument(month_offset=0)
        if future_contract is None:
            logger.error("Failed to fetch upcoming NIFTY future contract.")
            sys.exit(constants.FAIL_CODE)
 
        # expose previous-day close to strategy for gap% calculations
        if param_data and isinstance(param_data, dict):
            ht_cfg = param_data.get("historical-trend")
            if not isinstance(ht_cfg, dict):
                ht_cfg = {}
                param_data["historical-trend"] = ht_cfg
            ht_cfg["last-day-close"] = float(last_day_close)

        selected_contracts = None			 
        intraday_day_1min_candles = []
        intraday_day_future_candles = []
        minutes_processed = {}
        future_minutes_processed = {}

        now_ist = datetime.now(ist)
        current_time = now_ist.time()

        market_start_time = time(9, 15)
        market_end_time = time(15, 30)

        if current_time > market_end_time:
            logger.info("Market already closed for the day. Exiting.")
            sys.exit(constants.SUCCESS_CODE)
        
        elif current_time < market_start_time:
																		  
            market_start_dt = datetime.combine(now_ist.date(), market_start_time, tzinfo=ist)					  
            wait_time = int((market_start_dt - now_ist).total_seconds()) - 2

            while wait_time > 0:
                logger.debug(f"Waiting for market to open... {wait_time}s remaining")
                await asyncio.sleep(min(wait_time, 10))
                wait_time -= 10

            spot_price = get_spot_price(upstox, constants.NIFTY50, constants.NIFTY50_SYMBOL)
            if spot_price is None:
                logger.error("Failed to fetch nifty 50 spot price at market open.")
                sys.exit(constants.FAIL_CODE)
            
            selected_contracts = get_nifty_option_instruments(spot_price, sp.get("trade_expiry"))

            logger.info(f"Market open now. with spot price: {spot_price}. Bootstrapping intraday candles from market open.")

        else:																		  
            logger.info("Market already open. Bootstrapping intraday candles.")
            data = get_instrument_intraday_data(upstox, constants.NIFTY50_SYMBOL)
            data.reverse()
            intraday_day_1min_candles.extend(data)
            future_data = get_instrument_intraday_data(upstox, future_contract["instrument_key"])
            future_data.reverse()
            intraday_day_future_candles.extend(future_data)


        columns = ["time", "open", "high", "low", "close", "volume", "oi"]
        
        df_nifty = pd.DataFrame()
        if intraday_day_1min_candles:
            df_nifty = pd.DataFrame(intraday_day_1min_candles, columns=columns)
            df_nifty.drop(columns=["volume", "oi"], inplace=True)

            last_timestamp = df_nifty["time"].iloc[-1]
            dt = datetime.fromisoformat(last_timestamp).astimezone(ist)
            minutes_processed[dt.strftime("%Y-%m-%d %H:%M")] = True

            close_price = intraday_day_1min_candles[-1][4]
            atm_price = int(round(close_price / 50) * 50)
            selected_contracts = get_nifty_option_instruments(atm_price, sp.get("trade_expiry"))

        df_future = pd.DataFrame()
        if intraday_day_future_candles:
            df_future = pd.DataFrame(intraday_day_future_candles, columns=columns)
            future_last_timestamp = df_future["time"].iloc[-1]
            future_dt = datetime.fromisoformat(future_last_timestamp).astimezone(ist)
            future_minutes_processed[future_dt.strftime("%Y-%m-%d %H:%M")] = True

        # instruments list
        list_of_instruments = []
        selected_contracts["Nifty_Future"] = future_contract

        for key, value in selected_contracts.items():
            if key == "Nifty_Future":
                list_of_instruments.append(value["instrument_key"])
                continue
            for contract in value:
                list_of_instruments.append(contract["instrument_key"])

        # init bot
        strategy_key = str(strategy or "").strip().lower()
        pcr_vwap_ema_orb_key = str(constants.PCR_VWAP_EMA_ORB).strip().lower()

        strategy_cls = None
        if strategy_key == pcr_vwap_ema_orb_key:
            from index.nifty50.strategy.pcr_vwma_ema_orb import PCRVwapEmaOrbStrategy
            strategy_cls = PCRVwapEmaOrbStrategy
        
        if strategy_cls is None:
            logger.error(f"Unsupported strategy: {strategy}. Exiting.")
            sys.exit(constants.FAIL_CODE)

        order_manager = OrderSystemClient.from_params(
            strategy_name=strategy,
            mode=mode,
            params=param_data,
            instrument=constants.NIFTY50,
        )
        logger.info(
            "Configured ordersystem client base_url=%s bot_name=%s mode=%s orders_csv=%s",
            order_manager.base_url,
            order_manager.bot_name,
            order_manager.mode,
            order_manager.orders_csv,
        )

        bot = strategy_cls(
                    upstox, trend, selected_contracts, order_manager=order_manager,
                    option_exipry_date=sp.get("trade_expiry"),
                    params=param_data,
                    index_minutes_processed=minutes_processed,
                    future_minutes_processed=future_minutes_processed,
                    intraday_index_candles=intraday_day_1min_candles,
                    intraday_future_candles=intraday_day_future_candles,
                )

        if bot is None:
            logger.error(f"Strategy {strategy} bot not initialized.")
            sys.exit(constants.FAIL_CODE)

        # Extract instrument keys from selected contracts
        instrument_keys = []
        for strike_price, contracts in selected_contracts.items():
            for contract in contracts:
                instrument_keys.append(contract['instrument_key'])

        # Send instrument keys to marketfeeder via NATS
        bot_id = f"nifty50_{strategy}_{mode}_{int(t.time())}"
        instrument_data = {
            'bot_id': bot_id,
            'instrument_keys': instrument_keys,
            'action': 'subscribe'
            }
            
        await nc.publish(constants.NATS_SUBJECT_INSTRUMENT_KEYS, json.dumps(instrument_data).encode())
        logger.info(f"Published instrument keys to marketfeeder for bot {bot_id}: {len(instrument_keys)} instruments: {instrument_keys}")

        # NATS tick heartbeat/watchdog (equivalent intent to old WS last-msg tracking).
        instrument_keys_set = set(instrument_keys)
        last_msg_lock = threading.Lock()
        last_msg_epoch = {"t": t.time()}
        watchdog_state = {"last_resubscribe_t": 0.0}

        tick_watchdog_enabled = bool(sp.get("nats_tick_watchdog_enabled", True))
        tick_watchdog_check_sec = max(1, _safe_int(sp.get("nats_tick_watchdog_check_sec", 5), 5))
        tick_idle_timeout_sec = max(
            5,
            _safe_int(
                sp.get("nats_tick_idle_timeout_sec", sp.get("ws_idle_timeout_sec", 45)),
                45,
            ),
        )
        tick_resubscribe_cooldown_sec = max(
            5,
            _safe_int(
                sp.get("nats_tick_resubscribe_cooldown_sec", tick_idle_timeout_sec),
                tick_idle_timeout_sec,
            ),
        )

        # Subscribe to tick data
        async def tick_data_handler(msg):
            with last_msg_lock:
                last_msg_epoch["t"] = t.time()
            try:
                payload = json.loads(msg.data.decode())
                if not isinstance(payload, dict):
                    return

                # Strategy expects full-feed envelope with "feeds".
                if "feeds" in payload:
                    bot.on_ws_message(msg)
                    return

                # Heartbeat update already happened. Ignore non-feed payloads.
                if payload.get("instrument_key") in instrument_keys_set:
                    return
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode tick data: {e}")
            except Exception as e:
                logger.error(f"Error processing tick data: {e}")

        async def nats_tick_watchdog():
            while True:
                await asyncio.sleep(tick_watchdog_check_sec)
                now_epoch = t.time()
                with last_msg_lock:
                    idle_for = now_epoch - float(last_msg_epoch["t"])

                if idle_for < tick_idle_timeout_sec:
                    continue

                if (now_epoch - watchdog_state["last_resubscribe_t"]) < tick_resubscribe_cooldown_sec:
                    continue

                watchdog_state["last_resubscribe_t"] = now_epoch
                logger.warning(
                    f"[NATS Watchdog] No tick payload for {idle_for:.1f}s. "
                    f"Re-publishing subscribe for bot_id={bot_id}."
                )
                try:
                    await nc.publish(constants.NATS_SUBJECT_INSTRUMENT_KEYS, json.dumps(instrument_data).encode())
                except Exception as pub_exc:
                    logger.warning(f"[NATS Watchdog] Failed to republish instrument subscription: {pub_exc}")

        sub = await nc.subscribe(constants.NATS_SUBJECT_TICK_DATA, cb=tick_data_handler)
        logger.info("Subscribed to tick data from marketfeeder")
        logger.info(
            f"NATS tick watchdog enabled={tick_watchdog_enabled}, "
            f"idle_timeout_sec={tick_idle_timeout_sec}, check_sec={tick_watchdog_check_sec}, "
            f"resubscribe_cooldown_sec={tick_resubscribe_cooldown_sec}"
        )

        watchdog_task = None
        if tick_watchdog_enabled:
            watchdog_task = asyncio.create_task(nats_tick_watchdog(), name="nats_tick_watchdog")

        try:
            # Keep the connection alive and process messages
            while True:
                await asyncio.sleep(1)
        finally:
            if watchdog_task is not None:
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass
            await sub.unsubscribe()
                
    except Exception as e:
        logger.error(f"Error in nifty50_engine: {e}")
    finally:
        await nc.close()
