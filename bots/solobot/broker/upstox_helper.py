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
import time
import json
import requests
from datetime import datetime
from common import constants

logger =create_logger("UpstoxHelperLogger")

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

            now_ms = int(time.time() * 1000)

            # Filter NIFTY index futures
            nifty_futs = [
                inst for inst in instruments
                if inst.get("segment") == "NSE_FO"
                and inst.get("instrument_type") == "FUT"
                and inst.get("expiry", 0) >= now_ms
                and (
                    inst.get("asset_symbol") == "NIFTY"
                    or inst.get("underlying_symbol") == "NIFTY"
                    or inst.get("underlying_key") == constants.NIFTY50_SYMBOL
                )
            ]

            if not nifty_futs:
                raise RuntimeError("No upcoming NIFTY futures found in NSE.json")
            
            # Sort by expiry (Unix ms)
            nifty_futs.sort(key=lambda x: x["expiry"])

            index = month_offset
            if index >= len(nifty_futs):
                raise IndexError(f"Requested month_offset={month_offset} "
                                f"but only {len(nifty_futs)} contracts are available")

            chosen = nifty_futs[index]

            # Convert expiry to human-readable date
            expiry_dt = datetime.fromtimestamp(chosen["expiry"] / 1000.0)
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


