#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        upstox_helper.py
 Author:      Amit Mohanty
 
 Notes:
    - upxtox helper class to get upstox client and fetch historical/intraday data.
    - fetch option contracts for given symbol, expiry date, strike price and option type.
==================================================
"""

import upstox_client
from logger import create_logger
import io
import gzip
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import common.constants as constants

logger =create_logger("UpstoxHelperLogger")
IST = ZoneInfo("Asia/Kolkata")

UPSTOX_INSTRUMENT_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"


def _csv_values(value):
    return [
        part.strip().upper()
        for part in str(value or "").split(",")
        if part and part.strip()
    ]


def _search_params_label(params):
    keys = ("query", "expiry", "exchanges", "segments", "instrument_types")
    return ", ".join(
        f"{key}={params[key]}"
        for key in keys
        if params.get(key) not in (None, "")
    )


def _instrument_search_candidates(params):
    """
    Build conservative fallback shapes for Upstox instrument search.

    Upstox accepts market segment filters such as FO/COMM separately from
    instrument types such as FUT. Older config in this bot used segments=FUT,
    so we retain that request and then retry using the newer split.
    """
    candidates = []
    seen = set()

    def add(candidate):
        clean = {key: value for key, value in candidate.items() if value not in (None, "")}
        marker = tuple(sorted((key, str(value)) for key, value in clean.items()))
        if marker in seen:
            return
        seen.add(marker)
        candidates.append(clean)

    add(params)

    exchanges = set(_csv_values(params.get("exchanges")))
    segments = set(_csv_values(params.get("segments")))
    instrument_types = set(_csv_values(params.get("instrument_types")))
    wants_future = "FUT" in segments or "FUT" in instrument_types

    if not wants_future:
        return candidates

    future_types = params.get("instrument_types") or "FUT"
    exchange_is_mcx = not exchanges or "ALL" in exchanges or "MCX" in exchanges
    retry_segments = ["COMM", "FO", "FUT", "ALL"] if exchange_is_mcx else ["FO", "FUT", "ALL"]

    for segment in retry_segments:
        candidate = dict(params)
        candidate["segments"] = segment
        candidate["instrument_types"] = future_types
        add(candidate)

    candidate = dict(params)
    candidate.pop("segments", None)
    candidate["instrument_types"] = future_types
    add(candidate)

    return candidates


def _expiry_date_from_epoch_ms(expiry_ms):
    try:
        return datetime.fromtimestamp(float(expiry_ms) / 1000.0, IST).date()
    except (TypeError, ValueError, OSError):
        return None


def _is_nifty_index_future(inst):
    return (
        inst.get("segment") == "NSE_FO"
        and inst.get("instrument_type") == "FUT"
        and (
            inst.get("asset_symbol") == "NIFTY"
            or inst.get("underlying_symbol") == "NIFTY"
            or inst.get("underlying_key") == constants.NIFTY50_SYMBOL
        )
    )


class UpstoxHelper:
    """
    Helper class for Upstox API operations.
    """
    
    def __init__(self, apiAccessToken,is_sandbox=True):
        self.apiAccessToken = apiAccessToken
        self.is_sandbox=is_sandbox
        self.upstox_client = self.get_upstox_client()

    def get_upstox_client(self):
        """                
        Notes:
        - Returns an authenticated Upstox client using the provided access token.
        """
        
        try:
            configuration = upstox_client.Configuration(sandbox=self.is_sandbox)
            configuration.access_token = self.apiAccessToken 
            upstox_client.ApiClient(configuration)
            return upstox_client
        except Exception as e:
            raise Exception(f"Failed to create Upstox client: {e}")

    def get_historical_data(self, instrument_key, from_date, to_date, unit, interval):
        """
        Args:
        - instrument_key: Instrument key for Nifty 50
        - from_date: Start date for historical data (YYYY-MM-DD)
        - to_date: End date for historical data (YYYY-MM-DD)
        - unit: Time unit (e.g., "days", "minutes")
        - interval: Interval for data (e.g., 1, 5, 15
                
        Notes:
        - Fetch historical data for a given instrument_key and interval.
        """
        try:
            api_instance=upstox_client.HistoryV3Api()
            api_response = api_instance.get_historical_candle_data1(instrument_key, unit, interval, to_date, from_date)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch historical data: {e}")
        

    def get_intraday_data(self, instrument_key, unit, interval):
        """
        Args:
        - instrument_key: Instrument key for Nifty 50
        - unit: Time unit (e.g., "days", "minutes")
        - interval: Interval for data (e.g., 1, 5, 15
                
        Notes:
        - Fetch intraday data for a given instrument_key and interval.
        """
        try:
            api_instance = upstox_client.HistoryV3Api()
            api_response = api_instance.get_intra_day_candle_data(instrument_key, unit, interval)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch intraday data: {e}")
    

    def get_option_contracts_instruments_by_expiry(self, symbol, expiry_date):
        """
        Args:
        - symbol: Trading symbol (e.g., "NIFTY")
        - expiry_date: Expiry date in YYYY-MM-DD format

        Notes:
        - Fetch option contracts for a given symbol, expiry date.
        """
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken
            api_instance = upstox_client.OptionsApi(upstox_client.ApiClient(configuration))
            api_response = api_instance.get_option_contracts(symbol, expiry_date=expiry_date)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch option contracts: {e}")
    
    
    def get_expires_by_instrument(self, symbol):
        """
        Args:
        - symbol: Trading symbol (e.g., "NIFTY")
 
        Notes:
        - Fetch specific option contract details based on symbol, expiry date, strike price, and option type.
        """
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken 
            api_instance = upstox_client.ExpiredInstrumentApi(upstox_client.ApiClient(configuration))
            api_response = api_instance.get_expiries(symbol)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch expiry strikes: {e}")
    

    def get_option_contracts_by_instrument(self, symbol):
        """
        Args:
        - symbol: Trading symbol (e.g., "NIFTY")
 
        Notes:
        - Fetch option contracts for a given symbol.
        """
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken 
            api_instance = upstox_client.OptionsApi(upstox_client.ApiClient(configuration))
            api_response = api_instance.get_option_contracts(symbol)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch option contracts: {e}")

    def search_instruments(
        self,
        query,
        expiry="current_month",
        atm_offset=0,
        page_number=1,
        records=30,
        exchanges="MCX",
        segments="COMM",
        instrument_types="FUT",
        timeout=15.0,
    ):
        """
        Search instruments using Upstox instruments/search.

        Notes:
        - Upstox separates segment filters (COMM/FO) from instrument type
          filters (FUT), so we retry legacy segments=FUT requests with that
          split when needed.
        """
        params = {
            "query": query,
            "expiry": expiry,
            "atm_offset": atm_offset,
            "page_number": page_number,
            "records": records,
            "exchanges": exchanges,
            "segments": segments,
        }
        if instrument_types:
            params["instrument_types"] = instrument_types

        def _fetch(fetch_params):
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {self.apiAccessToken}",
            }
            response = requests.get(
                UPSTOX_INSTRUMENT_SEARCH_URL,
                headers=headers,
                params=fetch_params,
                timeout=timeout,
            )
            try:
                payload = response.json()
            except ValueError as exc:
                raise RuntimeError(
                    f"Instrument search returned non-JSON HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                ) from exc

            if response.status_code != 200 or payload.get("status") != constants.SUCCESS:
                raise RuntimeError(
                    f"Instrument search failed with HTTP {response.status_code}: {payload}"
                )
            return payload.get("data") or []

        candidates = _instrument_search_candidates(params)
        for attempt_index, fetch_params in enumerate(candidates):
            instruments = _fetch(fetch_params)
            if instruments:
                if attempt_index > 0:
                    logger.info(
                        "Instrument search found %s instruments after retry with %s.",
                        len(instruments),
                        _search_params_label(fetch_params),
                    )
                return instruments

            next_attempt_index = attempt_index + 1
            if next_attempt_index < len(candidates):
                logger.info(
                    "No instruments found with %s. Retrying with %s.",
                    _search_params_label(fetch_params),
                    _search_params_label(candidates[next_attempt_index]),
                )

        return []

    def get_holday_list(self):
        """
        Args:
        - date: Specific date to check for holidays (optional)

        Notes:
        - Fetch the list of market holidays.
        """
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken 
            api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))
            api_response = api_instance.get_holidays()
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch holiday list: {e}")
    

    def get_future_contracts_by_instrument(self, month_offset=0):
        """
        Args:
        - month_offset: 0 -> near-month
                        1 -> next-month
 
        Notes:
        - Download the json and parse to get upcoming NIFTY future contracts.
        """
        try:

            resp = requests.get(constants.UPSTOX_NSE_INSTRUMENT_FQDN, timeout=60)   # add verify=False ONLY if you must
            resp.raise_for_status()

            # Decompress in-memory
            with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
                data_bytes = gz.read()

            instruments = json.loads(data_bytes.decode("utf-8"))

            today_ist = datetime.now(IST).date()
            nifty_futs = []
            skipped_expiring_today = 0

            for inst in instruments:
                if not _is_nifty_index_future(inst):
                    continue

                expiry_date = _expiry_date_from_epoch_ms(inst.get("expiry"))
                if expiry_date is None:
                    continue

                if expiry_date <= today_ist:
                    if expiry_date == today_ist:
                        skipped_expiring_today += 1
                    continue

                nifty_futs.append(inst)

            if skipped_expiring_today:
                logger.info(
                    "Skipping %s NIFTY future contract(s) expiring today (%s IST); selecting next expiry.",
                    skipped_expiring_today,
                    today_ist,
                )

            if not nifty_futs:
                raise RuntimeError("No upcoming NIFTY futures found after today in NSE.json")
            
            # Sort by expiry (Unix ms)
            nifty_futs.sort(key=lambda x: x["expiry"])

            index = month_offset
            if index >= len(nifty_futs):
                raise IndexError(f"Requested month_offset={month_offset} "
                                f"but only {len(nifty_futs)} contracts are available")

            chosen = nifty_futs[index]

            # Convert expiry to human-readable date
            expiry_dt = datetime.fromtimestamp(float(chosen["expiry"]) / 1000.0, IST)
            return {
                "exchange": chosen["exchange"],
                "expiry": expiry_dt.date(),
                "instrument_key": chosen["instrument_key"],
                "trading_symbol": chosen["trading_symbol"],
                "instrument_type": chosen["instrument_type"],
                "name": chosen["asset_symbol"],
                "segment": "NSE_FO",
                "asset_key": chosen["asset_key"],
                "underlying_key": chosen["underlying_key"],
                "lot_size": chosen["lot_size"]
            }
        
        except Exception as e:
            raise Exception(f"Failed to fetch future contracts: {e}")
    

    def get_option_chain_by_expiry(self,symbol, expiry_date):
        configuration = upstox_client.Configuration()
        configuration.access_token = self.apiAccessToken

        api_instance = upstox_client.OptionsApi(upstox_client.ApiClient(configuration))

        try:
            api_response = api_instance.get_put_call_option_chain(symbol, expiry_date)
            return api_response
        except Exception as e:
            raise Exception("Exception when calling OrderApi->options apis: %s\n" % e)

    def get_last_price_of_symbol(self,symbol):
        configuration = upstox_client.Configuration()
        configuration.access_token = self.apiAccessToken
        apiInstance = upstox_client.MarketQuoteV3Api(upstox_client.ApiClient(configuration))
        try:
            # For a single instrument
            response = apiInstance.get_ltp(instrument_key=symbol)
            return response
        except Exception as e:
            raise Exception("Exception when calling MarketQuoteV3Api->get_ltp: %s\n" % e)
        

    def asset_place_order(self,instrument_token: str=None,
                          quantity: int=0,
                          product: str="D",
                          validity: str="DAY",
                          price: float=0,
                          tag: str="",
                          order_type: str="MARKET",
                          transaction_type: str="BUY",
                          disclosed_quantity: int=0,
                          trigger_price: float=0,
                          is_amo: bool=False,
                          is_slice: bool=True
                          ):
        try:
            configuration = upstox_client.Configuration(sandbox=self.is_sandbox)
            configuration.access_token = self.apiAccessToken

            api_instance = upstox_client.OrderApiV3(upstox_client.ApiClient(configuration))

            body = None
            if transaction_type == constants.BUY:
                body = upstox_client.PlaceOrderV3Request(quantity=quantity, product=product,
                                                        validity=validity, price=price, tag=tag, 
                                                        instrument_token=instrument_token, order_type=order_type,
                                                        transaction_type=transaction_type, disclosed_quantity=disclosed_quantity, 
                                                        trigger_price=trigger_price, is_amo=is_amo, slice=is_slice)
            elif transaction_type == constants.SELL and order_type == constants.SL:
                body = upstox_client.PlaceOrderV3Request(quantity=quantity,product=product,
                                                        validity=validity, tag=tag, instrument_token=instrument_token, 
                                                        order_type=order_type, transaction_type=transaction_type, disclosed_quantity=disclosed_quantity, 
                                                        price=price, 
                                                        trigger_price=trigger_price, is_amo=is_amo, slice=is_slice)
            else:
                raise Exception(f"Condition not found to punch order for contract transaction_type {transaction_type}, order_type {order_type}")

            api_response = api_instance.place_order(body)
            return api_response

        except Exception as e:
            raise Exception(f"Failed to place order: {e}")

    def asset_modify_order(self,
                            sl_order_id: str,
                            quantity: int,
                            validity: str = "DAY",
                            order_type: str = "SL",
                            disclosed_quantity: int = 0,
                            trigger_price: float = 0.0,
                            price: float = 0.0,
                            is_amo: bool = False,
                            slice: bool = True,
                           ):
        try:
            configuration = upstox_client.Configuration(sandbox=self.is_sandbox)
            configuration.access_token = self.apiAccessToken

            api_instance = upstox_client.OrderApiV3(upstox_client.ApiClient(configuration))
            logger.debug(f"Modifying order {sl_order_id} with quantity {quantity}, trigger_price {trigger_price}, price {price}, order_type {order_type}")

            body = upstox_client.ModifyOrderRequest(
                int(quantity),                 # quantity
                str(validity),                 # validity ("DAY")
                float(price),                  # price (0 for SL-M)
                str(sl_order_id),              # order_id
                str(order_type),               # order_type ("SL")
                int(disclosed_quantity),       # disclosed_quantity
                float(trigger_price)          # trigger_price
            )

            return api_instance.modify_order(body)
        except Exception as e:
            raise Exception(f"Failed to trail order: {e}")
    
    def square_off_position(self, 
                        sl_order_id: str, 
                        quantity: int, 
                        validity: str = "DAY", 
                        order_type: str = "MARKET", 
                        exit_price: float = 0.0):
        try:
            configuration = upstox_client.Configuration(sandbox=self.is_sandbox)
            configuration.access_token = self.apiAccessToken

            api_instance = upstox_client.OrderApiV3(upstox_client.ApiClient(configuration))

            # LOGIC FIX: If Market order, Price MUST be 0.0
            final_price = 0.0 if order_type == "MARKET" else float(exit_price)

            body = upstox_client.ModifyOrderRequest(
                quantity=int(quantity),
                validity=str(validity),
                order_id=str(sl_order_id),
                order_type=str(order_type),
                price=final_price,        # Use the corrected price (0.0 for Market)
                trigger_price=0.0,        # Always 0.0 for Market exits
                disclosed_quantity=0
            )

            return api_instance.modify_order(body)
        except Exception as e:
            raise Exception(f"Failed to square off order: {e}")

    def exit_all_positions(self, tag, segment="NSE_FO"):
        try:
            configuration = upstox_client.Configuration(sandbox=self.is_sandbox)
            configuration.access_token = self.apiAccessToken
            api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
            param = {
                'segment': segment,
                'tag': tag
            }
            return api_instance.exit_positions(**param)
        except Exception as e:
            raise Exception(f"Failed to exit all orders: {e}")

    def cancel_order(self, order_id):
        try:
            configuration = upstox_client.Configuration(sandbox=self.is_sandbox)
            configuration.access_token = self.apiAccessToken
            api_instance = upstox_client.OrderApiV3(upstox_client.ApiClient(configuration))
            return api_instance.cancel_order(order_id)
        except Exception as e:
            raise Exception(f"Failed to cancel order: {e}")
        

    def get_all_trades_of_day(self):
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken

            api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
            api_version = '2.0'
            return api_instance.get_trade_history(api_version)
        except Exception as e:
            raise Exception(f"Failed to get all day orders: {e}")
    

    def get_details_by_order_id(self,order_id):
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken

            api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
            api_version = '2.0'
            api_response = api_instance.get_trades_by_order(order_id, api_version)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to get details by orderid: {e}")
    
    
    def get_ltp(self, instrument_key: str):
        """
        Fetch the Last Traded Price (LTP) for a given instrument key.
        """
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = self.apiAccessToken
            api_instance = upstox_client.MarketQuoteV3Api(upstox_client.ApiClient(configuration))

            api_response = api_instance.get_ltp(instrument_key=instrument_key)
            return api_response

        except Exception as e:
            raise Exception(f"Failed to fetch LTP: {e}")
