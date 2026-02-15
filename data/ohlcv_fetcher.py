"""
OHLCV Data Fetcher using Angel One Smart API
Stores data in Parquet files, updates incrementally from last stored date.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from database import get_db_session, Stock

# Angel One imports
try:
    from SmartApi import SmartConnect
    import pyotp
except ImportError:
    logger.error("smartapi-python not installed. Run: pip install smartapi-python")
    SmartConnect = None


# Paths
OHLCV_DIR = Path(__file__).parent.parent / "data" / "ohlcv"
OHLCV_DIR.mkdir(parents=True, exist_ok=True)

# Rate limiting: 180 requests/min = 3 per second
RATE_LIMIT_DELAY = 0.4  # 400ms between requests


class AngelOneClient:
    """Angel One Smart API client for fetching historical data."""
    
    def __init__(self):
        self.api = None
        self.auth_token = None
        self.feed_token = None
        self._connected = False
        
    def connect(self) -> bool:
        """Connect to Angel One API."""
        if SmartConnect is None:
            logger.error("SmartApi not available")
            return False
        
        if not settings.angel_api_key or not settings.angel_client_id:
            logger.error("Angel One credentials not configured in .env")
            return False
        
        try:
            self.api = SmartConnect(api_key=settings.angel_api_key)
            
            # Generate TOTP
            totp = pyotp.TOTP(settings.angel_totp_secret).now()
            
            # Login
            data = self.api.generateSession(
                clientCode=settings.angel_client_id,
                password=settings.angel_password,
                totp=totp
            )
            
            if data.get("status"):
                self.auth_token = data["data"]["jwtToken"]
                self.feed_token = self.api.getfeedToken()
                self._connected = True
                logger.success("Connected to Angel One API")
                return True
            else:
                logger.error(f"Login failed: {data.get('message')}")
                return False
                
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def get_historical_candles(
        self,
        symbol: str,
        token: str,
        exchange: str = "NSE",
        interval: str = "ONE_DAY",
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical candles for a symbol.
        
        Args:
            symbol: Stock symbol
            token: Angel One token for the symbol
            exchange: NSE or BSE
            interval: ONE_DAY, ONE_HOUR, etc.
            from_date: Start date (default: 2 years ago)
            to_date: End date (default: today)
        
        Returns:
            DataFrame with OHLCV data
        """
        if not self._connected:
            logger.error("Not connected to API")
            return None
        
        if to_date is None:
            to_date = datetime.now()
        if from_date is None:
            from_date = to_date - timedelta(days=730)  # 2 years
        
        try:
            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": interval,
                "fromdate": from_date.strftime("%Y-%m-%d 09:15"),
                "todate": to_date.strftime("%Y-%m-%d 15:30")
            }
            
            response = self.api.getCandleData(params)
            
            # Debug logging
            logger.debug(f"API Response for {symbol}: status={response.get('status')}, message={response.get('message')}, data_len={len(response.get('data', []))}")
            
            if response.get("status") and response.get("data"):
                df = pd.DataFrame(
                    response["data"],
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df["symbol"] = symbol
                df = df.set_index("timestamp")
                df = df.sort_index()
                return df
            else:
                logger.warning(f"No data for {symbol}: {response.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return None


class OHLCVStorage:
    """Manages OHLCV data in Parquet files."""
    
    def __init__(self, base_dir: Path = OHLCV_DIR):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def get_parquet_path(self, symbol: str) -> Path:
        """Get parquet file path for a symbol."""
        return self.base_dir / f"{symbol}.parquet"
    
    def load(self, symbol: str) -> Optional[pd.DataFrame]:
        """Load OHLCV data for a symbol."""
        path = self.get_parquet_path(symbol)
        if path.exists():
            try:
                df = pd.read_parquet(path)
                return df
            except Exception as e:
                logger.error(f"Error loading {symbol}: {e}")
        return None
    
    def save(self, symbol: str, df: pd.DataFrame) -> bool:
        """Save OHLCV data for a symbol."""
        try:
            path = self.get_parquet_path(symbol)
            df.to_parquet(path, index=True)
            return True
        except Exception as e:
            logger.error(f"Error saving {symbol}: {e}")
            return False
    
    def get_last_date(self, symbol: str) -> Optional[datetime]:
        """Get the last date in stored data."""
        df = self.load(symbol)
        if df is not None and len(df) > 0:
            last_date = df.index.max().to_pydatetime()
            if last_date.tzinfo is not None:
                last_date = last_date.replace(tzinfo=None)
            return last_date
        return None
    
    def append(self, symbol: str, new_data: pd.DataFrame) -> bool:
        """Append new data to existing parquet."""
        existing = self.load(symbol)
        
        if existing is not None:
            # Remove duplicates based on index (date)
            combined = pd.concat([existing, new_data])
            combined = combined[~combined.index.duplicated(keep='last')]
            combined = combined.sort_index()
        else:
            combined = new_data
        
        return self.save(symbol, combined)
    
    def list_symbols(self) -> List[str]:
        """List all symbols with stored data."""
        return [p.stem for p in self.base_dir.glob("*.parquet")]


class OHLCVFetcher:
    """Fetches and stores OHLCV data for stocks."""
    
    def __init__(self):
        self.client = AngelOneClient()
        self.storage = OHLCVStorage()
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to Angel One API."""
        self._connected = self.client.connect()
        return self._connected
    
    def update_stock(self, symbol: str, token: str, full_refresh: bool = False) -> bool:
        """
        Update OHLCV data for a single stock.
        
        Args:
            symbol: Stock symbol
            token: Angel One token
            full_refresh: If True, fetch all 2 years. If False, fetch from last date.
        """
        if not self._connected:
            logger.error("Not connected")
            return False
        
        # Determine date range
        if full_refresh:
            from_date = datetime.now() - timedelta(days=730)
        else:
            last_date = self.storage.get_last_date(symbol)
            if last_date:
                from_date = last_date + timedelta(days=1)
                if from_date.date() >= datetime.now().date():
                    logger.debug(f"{symbol} already up to date")
                    return True
            else:
                from_date = datetime.now() - timedelta(days=730)
        
        to_date = datetime.now()
        
        logger.info(f"Fetching {symbol}: {from_date.date()} to {to_date.date()}")
        
        # Fetch data
        df = self.client.get_historical_candles(
            symbol=symbol,
            token=token,
            from_date=from_date,
            to_date=to_date
        )
        
        if df is not None and len(df) > 0:
            if self.storage.append(symbol, df):
                logger.success(f"{symbol}: Added {len(df)} candles")
                return True
        else:
            logger.warning(f"{symbol}: No new data")
        
        return False
    
    def update_all(self, symbols: List[Dict] = None, full_refresh: bool = False) -> Dict:
        """
        Update OHLCV for all stocks.
        
        Args:
            symbols: List of {"symbol": ..., "token": ...}. If None, uses DB.
            full_refresh: Fetch full 2 years for all.
        
        Returns:
            Stats dict
        """
        if symbols is None:
            # Get from database (All active stocks with tokens)
            with get_db_session() as db:
                stocks = db.query(Stock).filter(
                    Stock.is_active == True,
                    Stock.nse_token.isnot(None)
                ).all()
                symbols = [{"symbol": s.symbol, "token": s.nse_token} for s in stocks]
        
        if not symbols:
            logger.error("No symbols to update")
            return {"updated": 0, "failed": 0}
        
        logger.info(f"Updating {len(symbols)} stocks...")
        
        stats = {"updated": 0, "failed": 0, "skipped": 0}
        
        for i, stock in enumerate(symbols):
            try:
                if self.update_stock(stock["symbol"], stock["token"], full_refresh):
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1
                
                # Rate limiting
                if i < len(symbols) - 1:
                    time.sleep(RATE_LIMIT_DELAY)
                    
            except Exception as e:
                logger.error(f"{stock['symbol']} failed: {e}")
                stats["failed"] += 1
        
        logger.success(f"Done! Updated: {stats['updated']}, Skipped: {stats['skipped']}, Failed: {stats['failed']}")
        return stats


# ============================================
# MOCK DATA FOR TESTING (when no API creds)
# ============================================

def generate_mock_ohlcv(symbol: str, days: int = 500) -> pd.DataFrame:
    """Generate mock OHLCV data for testing."""
    import numpy as np
    
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    # Random walk for price
    base_price = np.random.uniform(500, 3000)
    returns = np.random.normal(0.0005, 0.02, days)
    prices = base_price * np.cumprod(1 + returns)
    
    df = pd.DataFrame({
        "open": prices * np.random.uniform(0.99, 1.01, days),
        "high": prices * np.random.uniform(1.01, 1.03, days),
        "low": prices * np.random.uniform(0.97, 0.99, days),
        "close": prices,
        "volume": np.random.randint(100000, 5000000, days),
        "symbol": symbol
    }, index=dates)
    
    df.index.name = "timestamp"
    return df


def create_mock_data_for_nifty50():
    """Create mock parquet files for Nifty 50 (testing without API)."""
    storage = OHLCVStorage()
    
    with get_db_session() as db:
        stocks = db.query(Stock).filter_by(is_nifty50=True).all()
        symbols = [s.symbol for s in stocks]
    
    logger.info(f"Creating mock OHLCV for {len(symbols)} stocks...")
    
    for symbol in symbols:
        df = generate_mock_ohlcv(symbol)
        storage.save(symbol, df)
        logger.debug(f"Created mock data for {symbol}")
    
    logger.success(f"Created mock data for {len(symbols)} stocks")


# ============================================
# CLI
# ============================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="OHLCV Data Fetcher")
    parser.add_argument("--mock", action="store_true", help="Generate mock data (no API)")
    parser.add_argument("--full", action="store_true", help="Full refresh (2 years)")
    parser.add_argument("--symbol", type=str, help="Single symbol to update")
    parser.add_argument("--list", action="store_true", help="List stored symbols")
    
    args = parser.parse_args()
    
    if args.mock:
        create_mock_data_for_nifty50()
    
    elif args.list:
        storage = OHLCVStorage()
        symbols = storage.list_symbols()
        print(f"Stored symbols ({len(symbols)}): {symbols}")
    
    elif args.symbol:
        fetcher = OHLCVFetcher()
        if fetcher.connect():
            with get_db_session() as db:
                stock = db.query(Stock).filter_by(symbol=args.symbol.upper()).first()
                if stock and stock.nse_token:
                    fetcher.update_stock(stock.symbol, stock.nse_token, args.full)
                else:
                    print(f"Stock {args.symbol} not found or no token")
    
    else:
        # Update all Nifty 50
        fetcher = OHLCVFetcher()
        if fetcher.connect():
            fetcher.update_all(full_refresh=args.full)
        else:
            print("\nConnection failed. Creating mock data instead...")
            create_mock_data_for_nifty50()
