"""Market Data module for fetching stock data from Angel One or mock sources."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import pandas as pd
import numpy as np
from loguru import logger
import pyotp

from config import settings


@dataclass
class StockQuote:
    """Represents a stock quote."""
    symbol: str
    token: str
    exchange: str
    ltp: float  # Last Traded Price
    open: float
    high: float
    low: float
    close: float
    volume: int
    change: float
    change_pct: float
    timestamp: datetime


@dataclass
class OHLCVData:
    """OHLCV candlestick data."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class MockDataGenerator:
    """Generates realistic mock data for testing without API credentials."""
    
    # Popular Indian stocks with their approximate price ranges
    MOCK_STOCKS = {
        "RELIANCE": {"token": "2885", "price_range": (2400, 2600), "exchange": "NSE"},
        "TCS": {"token": "11536", "price_range": (3800, 4200), "exchange": "NSE"},
        "HDFCBANK": {"token": "1333", "price_range": (1500, 1700), "exchange": "NSE"},
        "INFY": {"token": "1594", "price_range": (1400, 1600), "exchange": "NSE"},
        "ICICIBANK": {"token": "4963", "price_range": (1000, 1150), "exchange": "NSE"},
        "HINDUNILVR": {"token": "1394", "price_range": (2400, 2700), "exchange": "NSE"},
        "SBIN": {"token": "3045", "price_range": (750, 850), "exchange": "NSE"},
        "BHARTIARTL": {"token": "10604", "price_range": (1500, 1700), "exchange": "NSE"},
        "ITC": {"token": "1660", "price_range": (450, 500), "exchange": "NSE"},
        "KOTAKBANK": {"token": "1922", "price_range": (1700, 1900), "exchange": "NSE"},
        "LT": {"token": "11483", "price_range": (3400, 3700), "exchange": "NSE"},
        "AXISBANK": {"token": "5900", "price_range": (1050, 1200), "exchange": "NSE"},
        "ASIANPAINT": {"token": "236", "price_range": (2800, 3100), "exchange": "NSE"},
        "MARUTI": {"token": "10999", "price_range": (10500, 12000), "exchange": "NSE"},
        "TATAMOTORS": {"token": "3456", "price_range": (900, 1050), "exchange": "NSE"},
        "SUNPHARMA": {"token": "3351", "price_range": (1600, 1850), "exchange": "NSE"},
        "TITAN": {"token": "3506", "price_range": (3200, 3600), "exchange": "NSE"},
        "WIPRO": {"token": "3787", "price_range": (450, 520), "exchange": "NSE"},
        "BAJFINANCE": {"token": "317", "price_range": (6800, 7500), "exchange": "NSE"},
        "NESTLEIND": {"token": "17963", "price_range": (2400, 2700), "exchange": "NSE"},
        "NIFTYBEES": {"token": "2660", "price_range": (260, 280), "exchange": "NSE"},
    }
    
    def get_quote(self, symbol: str) -> Optional[StockQuote]:
        """Generate mock quote for a symbol."""
        symbol = symbol.upper()
        if symbol not in self.MOCK_STOCKS:
            logger.warning(f"Unknown symbol: {symbol}, using default range")
            stock_info = {"token": "0", "price_range": (100, 200), "exchange": "NSE"}
        else:
            stock_info = self.MOCK_STOCKS[symbol]
        
        min_price, max_price = stock_info["price_range"]
        base_price = (min_price + max_price) / 2
        
        # Generate realistic price movements
        open_price = base_price + np.random.uniform(-base_price * 0.02, base_price * 0.02)
        high = open_price + np.random.uniform(0, base_price * 0.015)
        low = open_price - np.random.uniform(0, base_price * 0.015)
        close = np.random.uniform(low, high)
        ltp = close + np.random.uniform(-base_price * 0.005, base_price * 0.005)
        
        change = ltp - open_price
        change_pct = (change / open_price) * 100
        
        return StockQuote(
            symbol=symbol,
            token=stock_info["token"],
            exchange=stock_info["exchange"],
            ltp=round(ltp, 2),
            open=round(open_price, 2),
            high=round(high, 2),
            low=round(low, 2),
            close=round(close, 2),
            volume=np.random.randint(100000, 10000000),
            change=round(change, 2),
            change_pct=round(change_pct, 2),
            timestamp=datetime.now()
        )
    
    def get_historical_data(
        self, 
        symbol: str, 
        interval: str = "ONE_DAY",
        days: int = 100
    ) -> pd.DataFrame:
        """Generate mock historical OHLCV data."""
        symbol = symbol.upper()
        if symbol not in self.MOCK_STOCKS:
            stock_info = {"token": "0", "price_range": (100, 200), "exchange": "NSE"}
        else:
            stock_info = self.MOCK_STOCKS[symbol]
        
        min_price, max_price = stock_info["price_range"]
        base_price = (min_price + max_price) / 2
        
        # Generate dates (skip weekends)
        dates = []
        current_date = datetime.now()
        while len(dates) < days:
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                dates.append(current_date)
            current_date -= timedelta(days=1)
        dates = list(reversed(dates))
        
        # Generate price series with random walk
        data = []
        price = base_price
        
        for date in dates:
            # Random daily movement (-2% to +2%)
            daily_return = np.random.normal(0.0005, 0.015)
            price = price * (1 + daily_return)
            price = max(min_price * 0.8, min(max_price * 1.2, price))  # Keep within bounds
            
            open_price = price
            high = open_price * (1 + abs(np.random.normal(0, 0.01)))
            low = open_price * (1 - abs(np.random.normal(0, 0.01)))
            close = np.random.uniform(low, high)
            volume = np.random.randint(500000, 5000000)
            
            data.append({
                "timestamp": date,
                "open": round(open_price, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "close": round(close, 2),
                "volume": volume
            })
        
        return pd.DataFrame(data)
    
    def search_symbol(self, query: str) -> List[Dict[str, str]]:
        """Search for symbols matching query."""
        query = query.upper()
        results = []
        for symbol, info in self.MOCK_STOCKS.items():
            if query in symbol:
                results.append({
                    "symbol": symbol,
                    "token": info["token"],
                    "exchange": info["exchange"],
                    "name": symbol
                })
        return results


class AngelOneClient:
    """Client for Angel One SmartAPI."""
    
    def __init__(self):
        self.api = None
        self.connected = False
        self._session_data = None
    
    def connect(self) -> bool:
        """Connect to Angel One API."""
        if not settings.has_angel_credentials:
            logger.error("Angel One credentials not configured")
            return False
        
        try:
            from SmartApi import SmartConnect
            
            self.api = SmartConnect(api_key=settings.angel_api_key)
            totp = pyotp.TOTP(settings.angel_totp_secret).now()
            
            self._session_data = self.api.generateSession(
                settings.angel_client_id,
                settings.angel_password,
                totp
            )
            
            if self._session_data.get("status"):
                self.connected = True
                logger.success("Connected to Angel One API")
                return True
            else:
                logger.error(f"Failed to connect: {self._session_data.get('message')}")
                return False
                
        except Exception as e:
            logger.error(f"Angel One connection error: {e}")
            return False
    
    def get_quote(self, symbol: str, token: str, exchange: str = "NSE") -> Optional[StockQuote]:
        """Get live quote for a symbol."""
        if not self.connected:
            logger.error("Not connected to Angel One")
            return None
        
        try:
            ltp_data = self.api.ltpData(exchange, symbol, token)
            
            if ltp_data.get("status"):
                data = ltp_data["data"]
                return StockQuote(
                    symbol=symbol,
                    token=token,
                    exchange=exchange,
                    ltp=float(data.get("ltp", 0)),
                    open=float(data.get("open", 0)),
                    high=float(data.get("high", 0)),
                    low=float(data.get("low", 0)),
                    close=float(data.get("close", 0)),
                    volume=int(data.get("volume", 0)),
                    change=float(data.get("change", 0)),
                    change_pct=float(data.get("percentChange", 0)),
                    timestamp=datetime.now()
                )
            else:
                logger.error(f"Failed to get quote: {ltp_data.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting quote: {e}")
            return None
    
    def get_historical_data(
        self,
        symbol: str,
        token: str,
        exchange: str = "NSE",
        interval: str = "ONE_DAY",
        days: int = 100
    ) -> Optional[pd.DataFrame]:
        """Get historical candle data."""
        if not self.connected:
            logger.error("Not connected to Angel One")
            return None
        
        try:
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
            to_date = datetime.now().strftime("%Y-%m-%d %H:%M")
            
            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": interval,
                "fromdate": from_date,
                "todate": to_date
            }
            
            candle_data = self.api.getCandleData(params)
            
            if candle_data.get("status"):
                data = candle_data["data"]
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
            else:
                logger.error(f"Failed to get historical data: {candle_data.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting historical data: {e}")
            return None


class MarketData:
    """
    Unified market data interface.
    Automatically uses mock data if API credentials are not available.
    """
    
    def __init__(self, force_mock: bool = False):
        """
        Initialize market data provider.
        
        Args:
            force_mock: Force use of mock data even if API credentials exist
        """
        self.use_mock = force_mock or settings.use_mock_data or not settings.has_angel_credentials
        
        if self.use_mock:
            logger.info("Using mock market data")
            self._provider = MockDataGenerator()
        else:
            logger.info("Connecting to Angel One API...")
            self._provider = AngelOneClient()
            if not self._provider.connect():
                logger.warning("Failed to connect to API, falling back to mock data")
                self._provider = MockDataGenerator()
                self.use_mock = True
    
    def get_quote(self, symbol: str, token: str = None, exchange: str = "NSE") -> Optional[StockQuote]:
        """
        Get current quote for a symbol.
        
        Args:
            symbol: Stock symbol (e.g., "RELIANCE")
            token: Symbol token (required for live API)
            exchange: Exchange (NSE/BSE)
        
        Returns:
            StockQuote object or None
        """
        if self.use_mock:
            return self._provider.get_quote(symbol)
        else:
            if not token:
                logger.error("Token required for live API")
                return None
            return self._provider.get_quote(symbol, token, exchange)
    
    def get_historical(
        self,
        symbol: str,
        token: str = None,
        exchange: str = "NSE",
        interval: str = "ONE_DAY",
        days: int = 100
    ) -> Optional[pd.DataFrame]:
        """
        Get historical OHLCV data.
        
        Args:
            symbol: Stock symbol
            token: Symbol token (required for live API)
            exchange: Exchange
            interval: Candle interval (ONE_MINUTE, FIVE_MINUTE, ONE_DAY, etc.)
            days: Number of days of history
        
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        if self.use_mock:
            return self._provider.get_historical_data(symbol, interval, days)
        else:
            if not token:
                logger.error("Token required for live API")
                return None
            return self._provider.get_historical_data(symbol, token, exchange, interval, days)
    
    def search_symbol(self, query: str) -> List[Dict[str, str]]:
        """Search for symbols matching query."""
        if self.use_mock:
            return self._provider.search_symbol(query)
        else:
            # Angel One doesn't have a search API, would need to use master data
            logger.warning("Symbol search not available for live API, using mock")
            return MockDataGenerator().search_symbol(query)
    
    @property
    def available_symbols(self) -> List[str]:
        """Get list of available symbols (mock data only)."""
        if self.use_mock:
            return list(MockDataGenerator.MOCK_STOCKS.keys())
        return []


# Usage example
if __name__ == "__main__":
    # Initialize market data (will use mock by default)
    md = MarketData()
    
    print("\n=== Available Symbols ===")
    print(md.available_symbols)
    
    print("\n=== Quote for RELIANCE ===")
    quote = md.get_quote("RELIANCE")
    if quote:
        print(f"Symbol: {quote.symbol}")
        print(f"LTP: ₹{quote.ltp:,.2f}")
        print(f"Change: {quote.change:+.2f} ({quote.change_pct:+.2f}%)")
        print(f"High: ₹{quote.high:,.2f} | Low: ₹{quote.low:,.2f}")
        print(f"Volume: {quote.volume:,}")
    
    print("\n=== Historical Data for TCS (last 5 days) ===")
    hist = md.get_historical("TCS", days=10)
    if hist is not None:
        print(hist.tail())
