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
from utils.bot_utils import stable_bot_id

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


def _env_int(name, default):
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(f"Invalid {name}={raw!r}; using {default}")
        return default


def _env_float(name, default):
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning(f"Invalid {name}={raw!r}; using {default}")
        return default
    return value if value > 0 else default


def _coerce_epoch_ms(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    raw = str(value).strip()
    if not raw:
        return None

    try:
        return int(float(raw))
    except ValueError:
        pass

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None

    return int(dt.timestamp() * 1000)


def _normalize_tick_payload(payload, instrument_keys_set, future_key):
    if not isinstance(payload, dict):
        return None

    if "feeds" in payload:
        feeds = payload.get("feeds")
        if isinstance(feeds, dict) and any(str(key) in instrument_keys_set for key in feeds.keys()):
            return payload
        return None

    instrument_key = str(payload.get("instrument_key") or "").strip()
    if not instrument_key or instrument_key not in instrument_keys_set:
        return None

    ltp = payload.get("ltp", payload.get("price"))
    ts_ms = _coerce_epoch_ms(
        payload.get("ts_epoch_ms")
        or payload.get("timestamp_ms")
        or payload.get("ltt")
        or payload.get("timestamp")
    )
    if ltp is None or ts_ms is None:
        return None

    try:
        ltp = float(ltp)
    except (TypeError, ValueError):
        return None

    if instrument_key == constants.NIFTY50_SYMBOL:
        return {
            "feeds": {
                instrument_key: {
                    "fullFeed": {
                        "indexFF": {
                            "ltpc": {"ltp": ltp, "ltt": ts_ms}
                        }
                    }
                }
            }
        }

    market_ff = {
        "ltpc": {"ltp": ltp, "ltt": ts_ms},
    }

    # marketfeeder's flat schema documents "volume"; use it as the best
    # available cumulative volume fallback for futures VWAP candle-building.
    vtt = payload.get("vtt", payload.get("volume"))
    if vtt is not None:
        try:
            market_ff["vtt"] = float(vtt)
        except (TypeError, ValueError):
            pass

    gamma = payload.get("gamma")
    if gamma is not None:
        try:
            gamma = float(gamma)
        except (TypeError, ValueError):
            pass
        market_ff["optionGreeks"] = {"gamma": gamma}

    return {
        "feeds": {
            instrument_key: {
                "fullFeed": {
                    "marketFF": market_ff
                }
            }
        }
    }


async def _flush_nats(nc, timeout=2):
    try:
        await nc.flush(timeout=timeout)
    except TypeError:
        await nc.flush()


async def _publish_marketfeeder_subscription(nc, payload):
    await nc.publish(constants.NATS_SUBJECT_INSTRUMENT_KEYS, json.dumps(payload).encode())
    await _flush_nats(nc)


async def _publish_marketfeeder_unsubscribe(nc, bot_id, instrument_keys=None):
    bot_id = str(bot_id or "").strip()
    if not bot_id:
        logger.warning("Skipping marketfeeder unsubscribe because bot_id is empty")
        return

    payload = {
        "bot_id": bot_id,
        "instrument_keys": list(instrument_keys or []),
        "action": "unsubscribe",
    }
    await _publish_marketfeeder_subscription(nc, payload)
    logger.info(f"Published marketfeeder unsubscribe for bot_id={bot_id}")


async def _connect_nats_with_retry():
    max_attempts = _env_int("NATS_CONNECT_RETRY_MAX", 0)
    retry_wait = _env_float("NATS_CONNECT_RETRY_WAIT_SEC", 2.0)
    connect_timeout = _env_float("NATS_CONNECT_TIMEOUT_SEC", 5.0)

    attempt = 0
    while True:
        attempt += 1
        try:
            return await nats.connect(
                constants.NATS_URL,
                name="solobot",
                connect_timeout=connect_timeout,
                reconnect_time_wait=retry_wait,
                max_reconnect_attempts=-1,
            )
        except Exception as exc:
            if max_attempts > 0 and attempt >= max_attempts:
                raise
            if max_attempts > 0:
                logger.warning(
                    f"NATS connect to {constants.NATS_URL} failed on attempt "
                    f"{attempt}/{max_attempts}: {exc}; retrying in {retry_wait}s"
                )
            else:
                logger.warning(
                    f"NATS connect to {constants.NATS_URL} failed on attempt "
                    f"{attempt}: {exc}; retrying in {retry_wait}s"
                )
            await asyncio.sleep(retry_wait)

async def nifty50_engine(strategy, mode, param_data):
    logger.info(f"Starting Nifty50 Engine with strategy: {strategy}, mode: {mode}")
    
    # Connect to NATS
    nc = await _connect_nats_with_retry()
    logger.info("Connected to NATS")

    try:

        sp = {}
        try:
            sp = (param_data or {}).get("strategy-parameters", {}) if isinstance(param_data, dict) else {}
        except Exception:
            sp = {}
        logger.info(
            "Loaded strategy expiries trade_expiry=%s pcr_expiry=%s",
            sp.get("trade_expiry"),
            sp.get("pcr_expiry"),
        )

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
        market_end_time = time(15, 31)

        if current_time >= market_end_time:
            logger.info("Market stop time reached at/after 15:31 IST. Breaking marketfeeder websocket and exiting.")
            cleanup_order_manager = OrderSystemClient.from_params(
                strategy_name=strategy,
                mode=mode,
                params=param_data,
                instrument=constants.NIFTY50,
            )
            bot_id = stable_bot_id(
                cleanup_order_manager.bot_name,
                cleanup_order_manager.mode or mode,
                cleanup_order_manager.curr_date,
            )
            await _publish_marketfeeder_unsubscribe(nc, bot_id)
            return
        
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

        list_of_instruments.append(constants.NIFTY50_SYMBOL)
        
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

        try:
            account_info = order_manager.create_account()
        except Exception as exc:
            logger.error(f"Failed to create/load ordersystem account: {exc}")
            sys.exit(constants.FAIL_CODE)

        acct_id = str(
            account_info.get("account_id")
            or account_info.get("acct_id")
            or getattr(order_manager, "account_id", "")
            or ""
        ).strip()
        if not acct_id:
            logger.error(f"Ordersystem account response missing account_id: {account_info}")
            sys.exit(constants.FAIL_CODE)

        logger.info(
            "Ordersystem account ready account_id=%s bot_name=%s month_year=%s message=%s",
            acct_id,
            account_info.get("bot_name") or order_manager.bot_name,
            account_info.get("month_year") or order_manager.month_year,
            account_info.get("message"),
        )

        bot = strategy_cls(
            upstox,
            trend,
            selected_contracts,
            order_manager=order_manager,
            acct_id=acct_id,
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

        # Reuse the validated instrument list built above and avoid treating the
        # future contract dict like an iterable of option-contract dicts.
        instrument_keys = list(dict.fromkeys(list_of_instruments))

        # Keep bot_id stable for the bot/mode/trading day so pod restarts update the same handler.
        bot_id = stable_bot_id(
            account_info.get("bot_name") or order_manager.bot_name,
            order_manager.mode or mode,
            account_info.get("curr_date") or order_manager.curr_date,
        )
        instrument_data = {
            'bot_id': bot_id,
            'instrument_keys': instrument_keys,
            'action': 'subscribe'
            }
            
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
        tick_shape_state = {"flat_logged": False, "first_tick_logged": False}

        async def tick_data_handler(msg):
            try:
                payload = json.loads(msg.data.decode())
                payload_bot_id = str(payload.get("bot_id") or "").strip()
                if payload_bot_id and payload_bot_id != bot_id:
                    return

                normalized_message = _normalize_tick_payload(
                    payload,
                    instrument_keys_set,
                    bot.nifty_future_key,
                )
                if normalized_message is not None:
                    with last_msg_lock:
                        last_msg_epoch["t"] = t.time()
                    if not tick_shape_state["first_tick_logged"]:
                        tick_shape_state["first_tick_logged"] = True
                        logger.info("Received first NATS tick payload from marketfeeder")
                    if "feeds" not in payload and not tick_shape_state["flat_logged"]:
                        tick_shape_state["flat_logged"] = True
                        logger.info(
                            "Received flat marketfeeder ticks; normalizing to strategy feed envelope"
                        )
                    bot.on_ws_message(normalized_message)
                    return

                # Treat own flat non-feed payloads as heartbeat-only messages.
                if payload.get("instrument_key") in instrument_keys_set:
                    with last_msg_lock:
                        last_msg_epoch["t"] = t.time()
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
                    await _publish_marketfeeder_subscription(nc, instrument_data)
                except Exception as pub_exc:
                    logger.warning(f"[NATS Watchdog] Failed to republish instrument subscription: {pub_exc}")

        sub = await nc.subscribe(constants.NATS_SUBJECT_TICK_DATA, cb=tick_data_handler)
        await _flush_nats(nc)
        logger.info("Subscribed to tick data from marketfeeder")
        logger.info(
            f"NATS tick watchdog enabled={tick_watchdog_enabled}, "
            f"idle_timeout_sec={tick_idle_timeout_sec}, check_sec={tick_watchdog_check_sec}, "
            f"resubscribe_cooldown_sec={tick_resubscribe_cooldown_sec}"
        )

        await _publish_marketfeeder_subscription(nc, instrument_data)
        logger.info(f"Published instrument keys to marketfeeder for bot {bot_id}: {len(instrument_keys)} instruments: {instrument_keys}")

        watchdog_task = None
        if tick_watchdog_enabled:
            watchdog_task = asyncio.create_task(nats_tick_watchdog(), name="nats_tick_watchdog")

        marketfeeder_unsubscribed = False

        try:
            # Keep the connection alive and process messages
            while True:
                if datetime.now(ist).time() >= market_end_time:
                    await _publish_marketfeeder_unsubscribe(nc, bot_id, instrument_keys)
                    marketfeeder_unsubscribed = True
                    logger.info("Market stop time reached at/after 15:31 IST. Breaking marketfeeder websocket and stopping NATS subscriptions.")
                    break
                await asyncio.sleep(1)
        finally:
            if watchdog_task is not None:
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass
            if not marketfeeder_unsubscribed and datetime.now(ist).time() >= market_end_time:
                try:
                    await _publish_marketfeeder_unsubscribe(nc, bot_id, instrument_keys)
                except Exception as exc:
                    logger.warning(f"Failed marketfeeder unsubscribe during shutdown: {exc}")
            await sub.unsubscribe()
                
    except Exception as e:
        logger.error(f"Error in nifty50_engine: {e}")
    finally:
        await nc.close()
