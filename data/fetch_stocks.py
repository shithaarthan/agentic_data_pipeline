"""
Fetch Nifty 500 list and Angel One tokens.
"""

import os
import sys
import json
import zipfile
import io
from datetime import datetime
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "httpx"])
    import httpx

from database import init_db, get_db_session, Stock


# NSE headers to avoid blocking
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


def fetch_nifty_500_from_nse() -> list:
    """Fetch Nifty 500 constituents from NSE API."""
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"
    
    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            # First hit main page to get cookies
            client.get("https://www.nseindia.com/", headers=NSE_HEADERS)
            
            # Then fetch the index data
            response = client.get(url, headers=NSE_HEADERS)
            response.raise_for_status()
            data = response.json()
            
            stocks = []
            for item in data.get("data", []):
                if item.get("symbol") and item.get("symbol") != "NIFTY 500":
                    stocks.append({
                        "symbol": item.get("symbol"),
                        "name": item.get("meta", {}).get("companyName", item.get("symbol")),
                        "isin": item.get("meta", {}).get("isin"),
                        "sector": item.get("meta", {}).get("industry"),
                    })
            
            logger.success(f"Fetched {len(stocks)} stocks from NSE")
            return stocks
            
    except Exception as e:
        logger.error(f"Failed to fetch from NSE: {e}")
        return []


def fetch_angel_master_file() -> dict:
    """
    Fetch Angel One master symbols file.
    Returns dict: {symbol: {"token": ..., "name": ..., "exchange": ...}}
    """
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    
    try:
        with httpx.Client(timeout=60) as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Create lookup by symbol
            symbol_map = {}
            for item in data:
                # Only NSE-EQ (equity) segment
                if item.get("exch_seg") == "NSE" and item.get("symbol"):
                    # Extract base symbol (remove -EQ suffix)
                    symbol = item.get("symbol", "").replace("-EQ", "")
                    if symbol and symbol not in symbol_map:
                        symbol_map[symbol] = {
                            "token": item.get("token"),
                            "name": item.get("name"),
                            "exchange": "NSE",
                            "isin": item.get("isin"),
                        }
                
                # Also get BSE tokens
                elif item.get("exch_seg") == "BSE" and item.get("symbol"):
                    symbol = item.get("symbol", "")
                    if symbol and symbol not in symbol_map:
                        symbol_map[f"BSE:{symbol}"] = {
                            "token": item.get("token"),
                            "name": item.get("name"),
                            "exchange": "BSE",
                        }
            
            logger.success(f"Loaded {len(symbol_map)} symbols from Angel master file")
            return symbol_map
            
    except Exception as e:
        logger.error(f"Failed to fetch Angel master: {e}")
        return {}


def load_nifty_500_with_tokens():
    """Load full Nifty 500 with Angel tokens."""
    init_db()
    
    # Fetch data
    logger.info("Fetching Nifty 500 list from NSE...")
    nse_stocks = fetch_nifty_500_from_nse()
    
    logger.info("Fetching Angel One master file...")
    angel_map = fetch_angel_master_file()
    
    if not nse_stocks:
        logger.warning("NSE fetch failed. Using fallback method...")
        # Use existing stocks from DB + some additions
        return update_existing_with_tokens(angel_map)
    
    with get_db_session() as db:
        # Clear existing and reload
        db.query(Stock).delete()
        
        loaded = 0
        for stock_data in nse_stocks:
            symbol = stock_data["symbol"]
            angel_info = angel_map.get(symbol, {})
            
            stock = Stock(
                symbol=symbol,
                name=stock_data.get("name") or angel_info.get("name") or symbol,
                isin=stock_data.get("isin") or angel_info.get("isin"),
                sector=stock_data.get("sector"),
                nse_token=angel_info.get("token"),
                is_nifty500=True,
                is_active=True
            )
            db.add(stock)
            loaded += 1
        
        logger.success(f"Loaded {loaded} Nifty 500 stocks with tokens")
        
        # Log stats
        with_tokens = db.query(Stock).filter(Stock.nse_token.isnot(None)).count()
        logger.info(f"Stocks with Angel tokens: {with_tokens}/{loaded}")
        
        return loaded


def update_existing_with_tokens(angel_map: dict = None):
    """Update existing stocks with Angel tokens."""
    if angel_map is None:
        logger.info("Fetching Angel One master file...")
        angel_map = fetch_angel_master_file()
    
    if not angel_map:
        logger.error("Could not fetch Angel master file")
        return 0
    
    with get_db_session() as db:
        stocks = db.query(Stock).all()
        updated = 0
        
        for stock in stocks:
            angel_info = angel_map.get(stock.symbol, {})
            if angel_info.get("token"):
                stock.nse_token = angel_info["token"]
                if not stock.name or stock.name == stock.symbol:
                    stock.name = angel_info.get("name", stock.name)
                if not stock.isin and angel_info.get("isin"):
                    stock.isin = angel_info["isin"]
                updated += 1
        
        logger.success(f"Updated {updated} stocks with Angel tokens")
        return updated


def get_stock_token(symbol: str) -> str:
    """Get Angel token for a symbol."""
    with get_db_session() as db:
        stock = db.query(Stock).filter_by(symbol=symbol).first()
        return stock.nse_token if stock else None


def list_stocks_without_tokens():
    """List stocks missing Angel tokens."""
    with get_db_session() as db:
        stocks = db.query(Stock).filter(Stock.nse_token.is_(None)).all()
        return [s.symbol for s in stocks]


if __name__ == "__main__":
    print("=" * 50)
    print("Loading Nifty 500 with Angel One tokens...")
    print("=" * 50)
    
    count = load_nifty_500_with_tokens()
    
    print(f"\nTotal stocks loaded: {count}")
    
    # Show sample
    with get_db_session() as db:
        samples = db.query(Stock).filter(Stock.nse_token.isnot(None)).limit(10).all()
        print("\nSample stocks with tokens:")
        for s in samples:
            print(f"  {s.symbol}: token={s.nse_token}, sector={s.sector}")
    
    # Show missing tokens
    missing = list_stocks_without_tokens()
    if missing:
        print(f"\nStocks without tokens ({len(missing)}): {missing[:10]}...")
