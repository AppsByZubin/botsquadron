import common.constants as constants
from logger import create_logger

logger =create_logger("UpstoxHelperLogger")

class UpstoxHelper:
    """
    Helper class for Upstox API operations.
    """
    
    def __init__(self, apiAccessToken,is_sandbox=False):
        self.apiAccessToken = apiAccessToken
        self.is_sandbox=is_sandbox
        self.upstox_client = self.get_upstox_client()
    
    def get_upstox_client(self):
        """                
        Notes:
        - Returns an authenticated Upstox client using the provided access token.
        """
        
        try:
            import upstox_client
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
            api_instance=self.upstox_client.HistoryV3Api()
            api_response = api_instance.get_historical_candle_data1(instrument_key, unit, interval, to_date, from_date)
            return api_response
        except Exception as e:
            raise Exception(f"Failed to fetch historical data: {e}")
